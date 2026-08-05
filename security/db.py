# -*- coding: utf-8 -*-
"""
Camada de banco: conexão, dialeto e esquema.

Dois drivers, uma interface. Em produção (Streamlit Community Cloud) o alvo é
um Postgres gerenciado, porque **o disco do Streamlit Cloud é efêmero**: um
SQLite ali seria apagado a cada redeploy, levando junto usuários, sessões e
auditoria. O SQLite continua disponível para rodar o app na sua máquina sem
depender de rede.

Todo SQL do projeto é escrito com placeholder ``?`` e traduzido para ``%s``
quando o driver é Postgres. Isso é seguro porque **nenhuma query deste projeto
concatena valor de usuário na string** — os valores viajam sempre como
parâmetro. Datas são gravadas como texto ISO-8601 em UTC nos dois bancos, o
que evita divergência de fuso e de tipo entre dialetos.

Uma conexão por operação, sem pool próprio: é o padrão que sobrevive melhor ao
modelo de threads do Streamlit e às reinicializações do Cloud. Com Supabase,
use a *connection string* do **pooler** (porta 6543) para que essa abertura
seja barata.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional, Sequence
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .config import get_config

_init_lock = threading.Lock()
_initialized = False


# --------------------------------------------------------------------------
# Tempo — sempre UTC, sempre ISO-8601
# --------------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utcnow().isoformat(timespec="seconds")


def iso_in(**delta) -> str:
    return (utcnow() + timedelta(**delta)).isoformat(timespec="seconds")


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Lê um timestamp do banco tolerando ``Z`` e valores nulos/corrompidos."""
    if not value:
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (ValueError, TypeError):
        return None


def is_past(value: Optional[str]) -> bool:
    """
    ``True`` se o instante já passou.

    Um valor ausente significa "sem prazo" e portanto não está vencido; um
    valor ilegível é tratado como vencido, porque na dúvida a resposta segura
    é encerrar a sessão, não estendê-la.
    """
    if not value:
        return False
    parsed = parse_iso(value)
    if parsed is None:
        return True
    return parsed <= utcnow()


# --------------------------------------------------------------------------
# Dialeto
# --------------------------------------------------------------------------

def is_postgres() -> bool:
    return get_config().is_postgres


def _translate(sql: str) -> str:
    """Converte o placeholder ``?`` para ``%s`` quando o driver é Postgres."""
    if not is_postgres():
        return sql
    return sql.replace("?", "%s")


def _normalized_pg_dsn(url: str) -> str:
    """
    Garante TLS e timeout na string de conexão.

    Supabase e Neon exigem TLS, mas uma URL colada à mão costuma vir sem
    ``sslmode``. Sem isso o driver pode negociar texto puro contra um proxy
    intermediário e as credenciais do banco trafegam abertas.
    """
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query))
    params.setdefault("sslmode", "require")
    params.setdefault("connect_timeout", "10")
    params.setdefault("application_name", "datasift-security")
    return urlunparse(parsed._replace(query=urlencode(params)))


@contextlib.contextmanager
def connect():
    """
    Conexão transacional. Faz commit no sucesso e rollback em qualquer exceção.

    Uso::

        with connect() as conn:
            rows = query(conn, "SELECT ...", (param,))
    """
    cfg = get_config()

    if cfg.is_postgres:
        conn = _connect_postgres(cfg.database_url)
    else:
        conn = _connect_sqlite(cfg.db_path)

    try:
        yield conn
        conn.commit()
    except Exception:
        with contextlib.suppress(Exception):
            conn.rollback()
        raise
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def _connect_postgres(url: str):
    dsn = _normalized_pg_dsn(url)
    try:
        import psycopg  # psycopg 3

        return psycopg.connect(dsn)
    except ImportError:
        pass
    try:
        import psycopg2

        return psycopg2.connect(dsn)
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "DATASIFT_DATABASE_URL aponta para Postgres, mas nenhum driver está "
            "instalado. Adicione 'psycopg[binary]' ao requirements.txt."
        ) from exc


def _connect_sqlite(path):
    path = os.fspath(path)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, timeout=15, isolation_level="DEFERRED")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")       # leitor não bloqueia escritor
    conn.execute("PRAGMA busy_timeout=15000")     # espera em vez de falhar
    conn.execute("PRAGMA foreign_keys=ON")        # ON DELETE CASCADE de verdade
    conn.execute("PRAGMA synchronous=FULL")       # não perder auditoria em crash
    return conn


# --------------------------------------------------------------------------
# Execução
# --------------------------------------------------------------------------

def execute(conn, sql: str, params: Sequence[Any] = ()) -> None:
    cur = conn.cursor()
    try:
        cur.execute(_translate(sql), tuple(params))
    finally:
        with contextlib.suppress(Exception):
            cur.close()


def query(conn, sql: str, params: Sequence[Any] = ()) -> list[dict]:
    """Executa e devolve linhas como dicionários, igual nos dois drivers."""
    cur = conn.cursor()
    try:
        cur.execute(_translate(sql), tuple(params))
        rows = cur.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], sqlite3.Row):
            return [dict(row) for row in rows]
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]
    finally:
        with contextlib.suppress(Exception):
            cur.close()


def query_one(conn, sql: str, params: Sequence[Any] = ()) -> Optional[dict]:
    rows = query(conn, sql, params)
    return rows[0] if rows else None


# --------------------------------------------------------------------------
# Esquema
# --------------------------------------------------------------------------

def _schema_statements() -> Iterable[str]:
    """
    DDL idempotente, válido nos dois dialetos.

    Só há ramificação onde os dialetos realmente divergem: chave
    autoincremental. Todo o resto usa ``TEXT``/``INTEGER``, que ambos aceitam.
    """
    serial = "BIGSERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"

    yield """
    CREATE TABLE IF NOT EXISTS organizations (
        id                  TEXT PRIMARY KEY,
        name                TEXT NOT NULL,
        slug                TEXT NOT NULL UNIQUE,
        status              TEXT NOT NULL DEFAULT 'active',
        created_at          TEXT NOT NULL,
        created_by          TEXT,
        notes               TEXT
    )
    """

    yield """
    CREATE TABLE IF NOT EXISTS users (
        id                     TEXT PRIMARY KEY,
        org_id                 TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
        email                  TEXT NOT NULL,
        email_norm             TEXT NOT NULL UNIQUE,
        display_name           TEXT NOT NULL,
        password_hash          TEXT NOT NULL,
        role                   TEXT NOT NULL,
        status                 TEXT NOT NULL DEFAULT 'active',
        must_change_password   INTEGER NOT NULL DEFAULT 0,
        password_changed_at    TEXT,
        totp_secret_enc        TEXT,
        totp_enabled           INTEGER NOT NULL DEFAULT 0,
        totp_last_counter      INTEGER,
        failed_attempts        INTEGER NOT NULL DEFAULT 0,
        locked_until           TEXT,
        last_login_at          TEXT,
        created_at             TEXT NOT NULL,
        created_by             TEXT,
        disabled_at            TEXT
    )
    """

    yield """
    CREATE TABLE IF NOT EXISTS password_history (
        id            {serial},
        user_id       TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        password_hash TEXT NOT NULL,
        created_at    TEXT NOT NULL
    )
    """.replace("{serial}", serial)

    yield """
    CREATE TABLE IF NOT EXISTS sessions (
        id               TEXT PRIMARY KEY,
        token_hash       TEXT NOT NULL UNIQUE,
        user_id          TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        org_id           TEXT NOT NULL,
        created_at       TEXT NOT NULL,
        last_seen_at     TEXT NOT NULL,
        expires_at       TEXT NOT NULL,
        revoked_at       TEXT,
        revoked_reason   TEXT,
        ip               TEXT,
        client_fingerprint TEXT
    )
    """

    yield """
    CREATE TABLE IF NOT EXISTS audit_log (
        id           {serial},
        ts           TEXT NOT NULL,
        actor_id     TEXT,
        actor_email  TEXT,
        org_id       TEXT,
        action       TEXT NOT NULL,
        target       TEXT,
        outcome      TEXT NOT NULL,
        ip           TEXT,
        detail       TEXT,
        prev_hash    TEXT,
        entry_hash   TEXT NOT NULL
    )
    """.replace("{serial}", serial)

    yield """
    CREATE TABLE IF NOT EXISTS rate_limits (
        bucket        TEXT PRIMARY KEY,
        counter       INTEGER NOT NULL DEFAULT 0,
        window_start  TEXT NOT NULL,
        blocked_until TEXT,
        strikes       INTEGER NOT NULL DEFAULT 0,
        updated_at    TEXT NOT NULL
    )
    """

    yield "CREATE INDEX IF NOT EXISTS idx_users_org ON users(org_id)"
    yield "CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)"
    yield "CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)"
    yield "CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts)"
    yield "CREATE INDEX IF NOT EXISTS idx_audit_org ON audit_log(org_id)"
    yield "CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_log(actor_id)"
    yield "CREATE INDEX IF NOT EXISTS idx_pwhist_user ON password_history(user_id)"


def init_db(force: bool = False) -> None:
    """
    Cria o esquema se necessário. Idempotente e seguro sob concorrência.

    Streamlit roda cada sessão em sua própria thread e todas sobem o mesmo
    módulo; sem o lock, várias threads tentariam criar as tabelas ao mesmo
    tempo no primeiro acesso.
    """
    global _initialized
    if _initialized and not force:
        return
    with _init_lock:
        if _initialized and not force:
            return
        with connect() as conn:
            for statement in _schema_statements():
                execute(conn, statement)
        _initialized = True


def healthcheck() -> tuple[bool, str]:
    """Testa a conexão. Usado na tela de administração para diagnóstico."""
    try:
        with connect() as conn:
            query_one(conn, "SELECT 1 AS ok")
        return True, "Postgres" if is_postgres() else f"SQLite ({get_config().db_path})"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
