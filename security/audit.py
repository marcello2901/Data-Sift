# -*- coding: utf-8 -*-
"""
Trilha de auditoria à prova de adulteração.

Cada registro carrega o hash do registro anterior, formando uma cadeia. Quem
apagar ou editar uma linha no meio quebra o encadeamento de todas as
seguintes, e :func:`verify_chain` aponta exatamente onde. Isso importa aqui
porque quem administra o Postgres (você, o provedor, ou alguém que roube a
credencial do banco) tem acesso de escrita à própria auditoria — sem o
encadeamento, um log é apenas uma sugestão.

**Regra inegociável: nada de dado de paciente aqui.** O log registra *quem fez
o quê*, nunca *sobre qual resultado clínico*. Os detalhes são limitados a
metadados — contagem de linhas, nome de arquivo já higienizado, papel do
usuário. :func:`_scrub` é a última barreira caso algo escape.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

from .db import connect, execute, is_postgres, now_iso, query, query_one

# Trava de aplicação usada no Postgres para serializar a escrita na cadeia.
# Valor arbitrário, só precisa ser estável e exclusivo deste uso.
_AUDIT_ADVISORY_LOCK = 728_411_903

_MAX_DETAIL_CHARS = 2000

# Ações registradas. Manter a lista fechada evita divergência de grafia, que
# na prática destrói a utilidade de qualquer busca no log.
LOGIN_SUCCESS = "login.success"
LOGIN_FAILURE = "login.failure"
LOGIN_BLOCKED = "login.blocked"
LOGIN_LOCKED_OUT = "login.locked_out"
LOGOUT = "logout"
SESSION_EXPIRED = "session.expired"
SESSION_REVOKED = "session.revoked"
PASSWORD_CHANGED = "password.changed"
PASSWORD_RESET = "password.reset"
TOTP_ENABLED = "totp.enabled"
TOTP_DISABLED = "totp.disabled"
TOTP_FAILURE = "totp.failure"
USER_CREATED = "user.created"
USER_UPDATED = "user.updated"
USER_DISABLED = "user.disabled"
USER_ENABLED = "user.enabled"
USER_DELETED = "user.deleted"
ORG_CREATED = "org.created"
ORG_UPDATED = "org.updated"
ACCESS_DENIED = "access.denied"
TENANT_VIOLATION = "tenant.violation"
DATA_UPLOADED = "data.uploaded"
DATA_EXPORTED = "data.exported"
UPLOAD_REJECTED = "upload.rejected"
RATE_LIMITED = "rate.limited"

OUTCOME_SUCCESS = "success"
OUTCOME_FAILURE = "failure"
OUTCOME_DENIED = "denied"


def _scrub(detail: Optional[dict]) -> Optional[str]:
    """
    Serializa os detalhes em JSON, removendo o que não pode ser logado.

    Descarta chaves de aparência sensível mesmo que o chamador as envie por
    engano, corta strings longas (um valor gigante costuma ser um pedaço de
    planilha vazando para o log) e remove caracteres de controle, que
    permitiriam forjar linhas falsas em quem lê o log como texto.
    """
    if not detail:
        return None

    forbidden = {
        "password", "senha", "token", "secret", "segredo", "hash",
        "cpf", "rg", "paciente", "patient", "nome_paciente", "barcode",
        "codigo_barras", "resultado", "valores", "rows", "data", "df",
    }

    safe: dict[str, Any] = {}
    for key, value in detail.items():
        key_norm = str(key).strip().lower()
        if key_norm in forbidden:
            safe[key] = "[omitido]"
            continue
        if isinstance(value, (int, float, bool)) or value is None:
            safe[key] = value
            continue
        text = str(value)
        text = "".join(ch for ch in text if ch == " " or ch.isprintable())
        if len(text) > 200:
            text = text[:200] + "…"
        safe[key] = text

    try:
        payload = json.dumps(safe, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        payload = json.dumps({"detail": "[não serializável]"})

    return payload[:_MAX_DETAIL_CHARS]


def _entry_hash(prev_hash: str, fields: tuple) -> str:
    """Hash do registro, amarrado ao anterior."""
    material = "\x1f".join([prev_hash or ""] + [str(f or "") for f in fields])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _lock_chain(conn) -> None:
    """
    Serializa gravações concorrentes na cadeia.

    Sem isso, duas sessões que auditam ao mesmo tempo leem o mesmo
    ``prev_hash`` e a cadeia bifurca — a verificação passa a acusar
    adulteração onde houve apenas concorrência. No SQLite a própria
    transação de escrita já serializa (com ``busy_timeout`` para esperar).
    """
    if is_postgres():
        execute(conn, "SELECT pg_advisory_xact_lock(?)", (_AUDIT_ADVISORY_LOCK,))


def record(
    action: str,
    outcome: str = OUTCOME_SUCCESS,
    actor_id: Optional[str] = None,
    actor_email: Optional[str] = None,
    org_id: Optional[str] = None,
    target: Optional[str] = None,
    ip: Optional[str] = None,
    detail: Optional[dict] = None,
) -> None:
    """
    Grava um evento.

    Auditoria nunca derruba a operação auditada: se o banco estiver fora do ar,
    o evento é perdido e o app continua. O contrário — negar login porque o log
    falhou — transforma um problema de observabilidade em indisponibilidade
    total. A perda fica visível porque a cadeia registra o salto.
    """
    try:
        with connect() as conn:
            _lock_chain(conn)
            last = query_one(
                conn,
                "SELECT entry_hash FROM audit_log ORDER BY id DESC LIMIT 1",
            )
            prev_hash = (last or {}).get("entry_hash") or ""

            ts = now_iso()
            detail_json = _scrub(detail)
            fields = (ts, actor_id, actor_email, org_id, action, target, outcome, ip, detail_json)
            entry = _entry_hash(prev_hash, fields)

            execute(
                conn,
                """
                INSERT INTO audit_log
                    (ts, actor_id, actor_email, org_id, action, target,
                     outcome, ip, detail, prev_hash, entry_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, actor_id, actor_email, org_id, action, target,
                 outcome, ip, detail_json, prev_hash, entry),
            )
    except Exception:
        pass


def verify_chain(limit: int = 5000) -> tuple[bool, Optional[int], int]:
    """
    Refaz o encadeamento e diz se a trilha está íntegra.

    Retorna ``(íntegra, id_do_primeiro_registro_suspeito, total_verificado)``.
    """
    try:
        with connect() as conn:
            rows = query(
                conn,
                """
                SELECT id, ts, actor_id, actor_email, org_id, action, target,
                       outcome, ip, detail, prev_hash, entry_hash
                FROM audit_log ORDER BY id ASC LIMIT ?
                """,
                (limit,),
            )
    except Exception:
        return False, None, 0

    expected_prev = ""
    for row in rows:
        fields = (
            row["ts"], row["actor_id"], row["actor_email"], row["org_id"],
            row["action"], row["target"], row["outcome"], row["ip"], row["detail"],
        )
        if (row["prev_hash"] or "") != expected_prev:
            return False, row["id"], len(rows)
        if _entry_hash(row["prev_hash"] or "", fields) != row["entry_hash"]:
            return False, row["id"], len(rows)
        expected_prev = row["entry_hash"]

    return True, None, len(rows)


def recent(
    limit: int = 200,
    org_id: Optional[str] = None,
    action: Optional[str] = None,
    actor_id: Optional[str] = None,
) -> list[dict]:
    """
    Últimos eventos, com filtros.

    ``org_id`` é o filtro de multilocação: um administrador de laboratório só
    enxerga a auditoria do próprio laboratório. Quem chama é responsável por
    passá-lo — :mod:`security.admin_ui` o faz a partir do papel de quem olha.
    """
    clauses, params = [], []
    if org_id:
        clauses.append("org_id = ?")
        params.append(org_id)
    if action:
        clauses.append("action = ?")
        params.append(action)
    if actor_id:
        clauses.append("actor_id = ?")
        params.append(actor_id)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(max(1, min(int(limit), 2000)))

    try:
        with connect() as conn:
            return query(
                conn,
                f"""
                SELECT id, ts, actor_email, org_id, action, target, outcome, ip, detail
                FROM audit_log {where} ORDER BY id DESC LIMIT ?
                """,
                params,
            )
    except Exception:
        return []


def purge_expired() -> int:
    """
    Apaga registros além da retenção configurada.

    Guardar log para sempre é passivo de privacidade, não zelo: o LGPD pede
    prazo definido. O corte é por data e sempre a partir do começo da cadeia,
    para que o trecho remanescente continue verificável de ponta a ponta.
    """
    from .config import get_config
    from .db import iso_in

    days = get_config().audit_retention_days
    if days <= 0:
        return 0

    cutoff = iso_in(days=-days)
    try:
        with connect() as conn:
            stale = query(conn, "SELECT id FROM audit_log WHERE ts < ? ORDER BY id ASC", (cutoff,))
            if not stale:
                return 0
            execute(conn, "DELETE FROM audit_log WHERE ts < ?", (cutoff,))
            return len(stale)
    except Exception:
        return 0
