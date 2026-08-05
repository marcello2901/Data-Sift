# -*- coding: utf-8 -*-
"""
Acesso a dados de identidade: organizações, usuários, sessões e histórico de senha.

Esta é a **única** camada que escreve SQL de negócio. O resto do projeto
conversa por funções nomeadas. O motivo é de segurança, não de estilo: o
isolamento entre laboratórios só é confiável se existir um lugar único onde
``org_id`` entra na cláusula ``WHERE``. Espalhar consultas pela interface
significa que um único ``SELECT`` esquecido vaza dados de outro laboratório.

Toda consulta que devolve mais de um usuário exige ``scope_org_id``
explicitamente. Quem precisa atravessar organizações passa
``ACROSS_ALL_ORGS``, um sentinela que o chamador tem de escrever de propósito
— ``None`` por descuido não vira "todos", vira erro.
"""

from __future__ import annotations

import re
import uuid
from typing import Any, Optional

from .config import get_config
from .crypto import generate_token, hash_token
from .db import (
    connect,
    execute,
    iso_in,
    now_iso,
    query,
    query_one,
)
from .models import (
    ROLE_SUPERADMIN,
    STATUS_ACTIVE,
    STATUS_DISABLED,
    Organization,
    User,
    org_from_row,
    user_from_row,
)


class ACROSS_ALL_ORGS:
    """Sentinela: consulta global, atravessando organizações."""


class TenantScopeError(RuntimeError):
    """Consulta multi-registro sem escopo de organização definido."""


def _new_id() -> str:
    return str(uuid.uuid4())


def normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(name or "").strip().lower()).strip("-")
    return slug[:60] or f"org-{uuid.uuid4().hex[:8]}"


def _require_scope(scope_org_id: Any) -> Optional[str]:
    """Traduz o escopo pedido, recusando ``None`` acidental."""
    if scope_org_id is ACROSS_ALL_ORGS:
        return None
    if not scope_org_id:
        raise TenantScopeError(
            "Consulta sem escopo de organização. Passe um org_id ou "
            "ACROSS_ALL_ORGS explicitamente."
        )
    return str(scope_org_id)


# --------------------------------------------------------------------------
# Organizações
# --------------------------------------------------------------------------

def create_organization(name: str, created_by: Optional[str] = None,
                        notes: Optional[str] = None) -> Organization:
    org_id = _new_id()
    base_slug = slugify(name)
    with connect() as conn:
        slug, suffix = base_slug, 1
        while query_one(conn, "SELECT id FROM organizations WHERE slug = ?", (slug,)):
            suffix += 1
            slug = f"{base_slug}-{suffix}"
        execute(
            conn,
            "INSERT INTO organizations (id, name, slug, status, created_at, created_by, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (org_id, str(name).strip()[:120], slug, STATUS_ACTIVE, now_iso(), created_by, notes),
        )
        row = query_one(conn, "SELECT * FROM organizations WHERE id = ?", (org_id,))
    return org_from_row(row)


def get_organization(org_id: str) -> Optional[Organization]:
    with connect() as conn:
        row = query_one(conn, "SELECT * FROM organizations WHERE id = ?", (org_id,))
    return org_from_row(row) if row else None


def list_organizations() -> list[Organization]:
    with connect() as conn:
        rows = query(conn, "SELECT * FROM organizations ORDER BY name ASC")
    return [org_from_row(r) for r in rows]


def set_organization_status(org_id: str, status: str) -> None:
    with connect() as conn:
        execute(conn, "UPDATE organizations SET status = ? WHERE id = ?", (status, org_id))


def rename_organization(org_id: str, name: str) -> None:
    with connect() as conn:
        execute(
            conn, "UPDATE organizations SET name = ? WHERE id = ?",
            (str(name).strip()[:120], org_id),
        )


def count_users_in_org(org_id: str) -> int:
    with connect() as conn:
        row = query_one(conn, "SELECT COUNT(*) AS n FROM users WHERE org_id = ?", (org_id,))
    return int((row or {}).get("n") or 0)


# --------------------------------------------------------------------------
# Usuários
# --------------------------------------------------------------------------

def create_user(
    org_id: str,
    email: str,
    display_name: str,
    password_hash: str,
    role: str,
    created_by: Optional[str] = None,
    must_change_password: bool = True,
) -> User:
    user_id = _new_id()
    email_norm = normalize_email(email)
    with connect() as conn:
        execute(
            conn,
            """
            INSERT INTO users
                (id, org_id, email, email_norm, display_name, password_hash, role,
                 status, must_change_password, password_changed_at, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id, org_id, str(email).strip()[:200], email_norm,
                str(display_name).strip()[:120] or email_norm, password_hash, role,
                STATUS_ACTIVE, 1 if must_change_password else 0,
                now_iso(), now_iso(), created_by,
            ),
        )
        execute(
            conn,
            "INSERT INTO password_history (user_id, password_hash, created_at) VALUES (?, ?, ?)",
            (user_id, password_hash, now_iso()),
        )
        row = query_one(conn, "SELECT * FROM users WHERE id = ?", (user_id,))
    return user_from_row(row)


def email_exists(email: str) -> bool:
    with connect() as conn:
        row = query_one(
            conn, "SELECT id FROM users WHERE email_norm = ?", (normalize_email(email),)
        )
    return row is not None


def get_user(user_id: str) -> Optional[User]:
    with connect() as conn:
        row = query_one(conn, "SELECT * FROM users WHERE id = ?", (user_id,))
    return user_from_row(row) if row else None


def get_auth_row(email: str) -> Optional[dict]:
    """
    Linha completa, com hash de senha e segredo TOTP.

    Usada **somente** por :mod:`security.auth`. Nenhuma tela deve chamar isto:
    o resto do sistema trabalha com :class:`~security.models.User`, que não
    carrega material secreto e portanto não pode vazá-lo por acidente em um
    ``st.write`` de depuração.
    """
    with connect() as conn:
        return query_one(conn, "SELECT * FROM users WHERE email_norm = ?", (normalize_email(email),))


def get_auth_row_by_id(user_id: str) -> Optional[dict]:
    with connect() as conn:
        return query_one(conn, "SELECT * FROM users WHERE id = ?", (user_id,))


def list_users(scope_org_id: Any, include_disabled: bool = True) -> list[User]:
    org_id = _require_scope(scope_org_id)
    clauses, params = [], []
    if org_id is not None:
        clauses.append("org_id = ?")
        params.append(org_id)
    if not include_disabled:
        clauses.append("status = ?")
        params.append(STATUS_ACTIVE)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with connect() as conn:
        rows = query(conn, f"SELECT * FROM users {where} ORDER BY display_name ASC", params)
    return [user_from_row(r) for r in rows]


def count_active_superadmins() -> int:
    """
    Quantos superadmins ativos existem.

    Guarda contra o pior erro operacional possível: rebaixar, desativar ou
    apagar a última conta capaz de administrar o sistema. Sem isso, um clique
    tranca você para fora do seu próprio app sem caminho de volta pela
    interface.
    """
    with connect() as conn:
        row = query_one(
            conn,
            "SELECT COUNT(*) AS n FROM users WHERE role = ? AND status = ?",
            (ROLE_SUPERADMIN, STATUS_ACTIVE),
        )
    return int((row or {}).get("n") or 0)


def update_user_profile(user_id: str, display_name: str) -> None:
    with connect() as conn:
        execute(
            conn, "UPDATE users SET display_name = ? WHERE id = ?",
            (str(display_name).strip()[:120], user_id),
        )


def set_user_role(user_id: str, role: str) -> None:
    with connect() as conn:
        execute(conn, "UPDATE users SET role = ? WHERE id = ?", (role, user_id))


def set_user_status(user_id: str, status: str) -> None:
    disabled_at = now_iso() if status == STATUS_DISABLED else None
    with connect() as conn:
        execute(
            conn, "UPDATE users SET status = ?, disabled_at = ? WHERE id = ?",
            (status, disabled_at, user_id),
        )


def move_user_to_org(user_id: str, org_id: str) -> None:
    """
    Troca o usuário de laboratório.

    As sessões abertas são derrubadas junto: elas carregam o ``org_id`` antigo
    e continuariam enxergando os dados do laboratório anterior até expirarem.
    """
    with connect() as conn:
        execute(conn, "UPDATE users SET org_id = ? WHERE id = ?", (org_id, user_id))
        execute(
            conn,
            "UPDATE sessions SET revoked_at = ?, revoked_reason = ? "
            "WHERE user_id = ? AND revoked_at IS NULL",
            (now_iso(), "org_changed", user_id),
        )


def delete_user(user_id: str) -> None:
    with connect() as conn:
        execute(conn, "DELETE FROM users WHERE id = ?", (user_id,))


# --------------------------------------------------------------------------
# Senha
# --------------------------------------------------------------------------

def set_password(user_id: str, password_hash: str, must_change: bool = False) -> None:
    """
    Grava a nova senha e encerra as outras sessões do usuário.

    Trocar a senha tem de expulsar quem já estava dentro — é o gesto que a
    pessoa faz justamente quando suspeita de invasão. A sessão atual é
    revalidada logo em seguida por :mod:`security.auth`, que emite um token
    novo para quem trocou.
    """
    with connect() as conn:
        execute(
            conn,
            "UPDATE users SET password_hash = ?, must_change_password = ?, "
            "password_changed_at = ?, failed_attempts = 0, locked_until = NULL WHERE id = ?",
            (password_hash, 1 if must_change else 0, now_iso(), user_id),
        )
        execute(
            conn,
            "INSERT INTO password_history (user_id, password_hash, created_at) VALUES (?, ?, ?)",
            (user_id, password_hash, now_iso()),
        )
        execute(
            conn,
            "UPDATE sessions SET revoked_at = ?, revoked_reason = ? "
            "WHERE user_id = ? AND revoked_at IS NULL",
            (now_iso(), "password_changed", user_id),
        )
        keep = max(1, get_config().password_history)
        stale = query(
            conn,
            "SELECT id FROM password_history WHERE user_id = ? ORDER BY id DESC",
            (user_id,),
        )
        for row in stale[keep:]:
            execute(conn, "DELETE FROM password_history WHERE id = ?", (row["id"],))


def recent_password_hashes(user_id: str) -> list[str]:
    keep = max(1, get_config().password_history)
    with connect() as conn:
        rows = query(
            conn,
            "SELECT password_hash FROM password_history WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, keep),
        )
    return [r["password_hash"] for r in rows]


def register_failed_login(user_id: str) -> int:
    """
    Soma uma falha e tranca a conta ao atingir o limite.

    O bloqueio por conta complementa o limite de taxa: mesmo que o atacante
    troque de IP a cada tentativa, a conta trava.
    """
    cfg = get_config()
    with connect() as conn:
        row = query_one(conn, "SELECT failed_attempts FROM users WHERE id = ?", (user_id,))
        attempts = int((row or {}).get("failed_attempts") or 0) + 1
        locked_until = (
            iso_in(minutes=cfg.lockout_minutes) if attempts >= cfg.max_login_attempts else None
        )
        execute(
            conn,
            "UPDATE users SET failed_attempts = ?, locked_until = ? WHERE id = ?",
            (attempts, locked_until, user_id),
        )
    return attempts


def register_successful_login(user_id: str) -> None:
    with connect() as conn:
        execute(
            conn,
            "UPDATE users SET failed_attempts = 0, locked_until = NULL, last_login_at = ? WHERE id = ?",
            (now_iso(), user_id),
        )


def unlock_user(user_id: str) -> None:
    with connect() as conn:
        execute(
            conn,
            "UPDATE users SET failed_attempts = 0, locked_until = NULL WHERE id = ?",
            (user_id,),
        )


# --------------------------------------------------------------------------
# TOTP
# --------------------------------------------------------------------------

def set_totp_secret(user_id: str, secret_enc: Optional[str], enabled: bool) -> None:
    with connect() as conn:
        execute(
            conn,
            "UPDATE users SET totp_secret_enc = ?, totp_enabled = ?, totp_last_counter = NULL "
            "WHERE id = ?",
            (secret_enc, 1 if enabled else 0, user_id),
        )


def set_totp_counter(user_id: str, counter: int) -> None:
    with connect() as conn:
        execute(conn, "UPDATE users SET totp_last_counter = ? WHERE id = ?", (counter, user_id))


# --------------------------------------------------------------------------
# Sessões
# --------------------------------------------------------------------------

def create_session(
    user_id: str,
    org_id: str,
    ip: Optional[str] = None,
    client_fingerprint: Optional[str] = None,
) -> tuple[str, str]:
    """
    Abre uma sessão e devolve ``(session_id, token_em_claro)``.

    O token em claro só existe neste retorno e no ``st.session_state`` do
    navegador; o banco guarda apenas o SHA-256. Quem ler a tabela ``sessions``
    não consegue se passar por ninguém.
    """
    cfg = get_config()
    session_id = _new_id()
    token = generate_token()
    with connect() as conn:
        execute(
            conn,
            """
            INSERT INTO sessions
                (id, token_hash, user_id, org_id, created_at, last_seen_at,
                 expires_at, ip, client_fingerprint)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id, hash_token(token), user_id, org_id, now_iso(), now_iso(),
                iso_in(hours=cfg.session_absolute_hours), ip, client_fingerprint,
            ),
        )
    return session_id, token


def get_session_by_token(token: str) -> Optional[dict]:
    with connect() as conn:
        return query_one(
            conn,
            "SELECT * FROM sessions WHERE token_hash = ? AND revoked_at IS NULL",
            (hash_token(token),),
        )


def touch_session(session_id: str) -> None:
    with connect() as conn:
        execute(
            conn, "UPDATE sessions SET last_seen_at = ? WHERE id = ?", (now_iso(), session_id)
        )


def revoke_session(session_id: str, reason: str = "logout") -> None:
    with connect() as conn:
        execute(
            conn,
            "UPDATE sessions SET revoked_at = ?, revoked_reason = ? "
            "WHERE id = ? AND revoked_at IS NULL",
            (now_iso(), reason, session_id),
        )


def revoke_all_sessions(user_id: str, reason: str = "admin_revoked") -> int:
    with connect() as conn:
        rows = query(
            conn, "SELECT id FROM sessions WHERE user_id = ? AND revoked_at IS NULL", (user_id,)
        )
        execute(
            conn,
            "UPDATE sessions SET revoked_at = ?, revoked_reason = ? "
            "WHERE user_id = ? AND revoked_at IS NULL",
            (now_iso(), reason, user_id),
        )
    return len(rows)


def list_active_sessions(user_id: str) -> list[dict]:
    with connect() as conn:
        return query(
            conn,
            "SELECT id, created_at, last_seen_at, expires_at, ip FROM sessions "
            "WHERE user_id = ? AND revoked_at IS NULL AND expires_at > ? "
            "ORDER BY last_seen_at DESC",
            (user_id, now_iso()),
        )


def purge_expired_sessions() -> int:
    """
    Remove sessões vencidas ou revogadas há mais de 7 dias.

    A carência existe para que a auditoria de um incidente recente ainda possa
    cruzar o ``session_id`` registrado no log com a linha correspondente.
    """
    cutoff = iso_in(days=-7)
    with connect() as conn:
        rows = query(
            conn,
            "SELECT id FROM sessions WHERE expires_at < ? OR (revoked_at IS NOT NULL AND revoked_at < ?)",
            (now_iso(), cutoff),
        )
        execute(
            conn,
            "DELETE FROM sessions WHERE expires_at < ? OR (revoked_at IS NOT NULL AND revoked_at < ?)",
            (now_iso(), cutoff),
        )
    return len(rows)


def stats() -> dict:
    """Números do painel de administração."""
    with connect() as conn:
        def scalar(sql: str, params: tuple = ()) -> int:
            row = query_one(conn, sql, params)
            return int((row or {}).get("n") or 0)

        return {
            "orgs": scalar("SELECT COUNT(*) AS n FROM organizations"),
            "users": scalar("SELECT COUNT(*) AS n FROM users"),
            "active_users": scalar(
                "SELECT COUNT(*) AS n FROM users WHERE status = ?", (STATUS_ACTIVE,)
            ),
            "locked_users": scalar(
                "SELECT COUNT(*) AS n FROM users WHERE locked_until IS NOT NULL AND locked_until > ?",
                (now_iso(),),
            ),
            "active_sessions": scalar(
                "SELECT COUNT(*) AS n FROM sessions WHERE revoked_at IS NULL AND expires_at > ?",
                (now_iso(),),
            ),
            "audit_entries": scalar("SELECT COUNT(*) AS n FROM audit_log"),
        }
