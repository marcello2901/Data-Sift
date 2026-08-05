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
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional, Sequence
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .config import get_config

_init_lock = threading.Lock()
_initialized = False

# --- Disjuntor de conexão ---------------------------------------------------
# Depois de uma falha ao conectar, novas tentativas são recusadas localmente
# durante alguns segundos, sem tocar a rede.
#
# Isto existe por causa de um incidente real: o Streamlit reexecuta o script
# inteiro a cada interação do usuário, e cada reexecução tentava conectar de
# novo. Com a credencial errada, isso vira uma rajada de falhas de
# autenticação contra o banco — o Supabase interpreta como ataque e aciona o
# próprio disjuntor ("ECIRCUITBREAKER: too many authentication failures"),
# bloqueando o projeto por vários minutos. O resultado é que consertar a senha
# não resolve na hora, porque o bloqueio persiste.
#
# Falhar rápido aqui protege o banco de nós mesmos e devolve a mensagem de
# erro original, que é o que o administrador precisa ler.
_FAILURE_BACKOFF_SECONDS = 20
_failure_lock = threading.Lock()
_last_failure: Optional[tuple[float, Exception]] = None


def _note_connection_failure(exc: Exception) -> None:
    global _last_failure
    with _failure_lock:
        _last_failure = (time.monotonic(), exc)


def _clear_connection_failure() -> None:
    global _last_failure
    if _last_failure is not None:
        with _failure_lock:
            _last_failure = None


def _recent_failure() -> Optional[Exception]:
    """Falha recente ainda dentro da janela de espera, se houver."""
    with _failure_lock:
        if _last_failure is None:
            return None
        occurred_at, exc = _last_failure
    if (time.monotonic() - occurred_at) >= _FAILURE_BACKOFF_SECONDS:
        return None
    return exc


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

    recent = _recent_failure()
    if recent is not None:
        # Repropaga o erro original: é ele que diz o que corrigir. Trocá-lo por
        # "aguarde" esconderia a causa justo de quem está diagnosticando.
        raise recent

    try:
        if cfg.is_postgres:
            conn = _connect_postgres(cfg.database_url)
        else:
            conn = _connect_sqlite(cfg.db_path)
    except Exception as exc:
        _note_connection_failure(exc)
        raise

    _clear_connection_failure()

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

        # prepare_threshold=None desliga os prepared statements automáticos.
        #
        # O pooler do Supabase na porta 6543 opera em modo transação: o
        # pgBouncer devolve a conexão de servidor ao pool no fim de cada
        # transação e a próxima pode cair em outra conexão. Um statement
        # preparado numa delas não existe na seguinte, e o erro aparece
        # intermitentemente ("prepared statement ... does not exist"),
        # normalmente só sob carga — o pior tipo de falha para diagnosticar.
        #
        # O custo de desligar é irrelevante aqui: as consultas de identidade
        # são poucas e curtas, e cada operação já abre a própria conexão.
        return psycopg.connect(dsn, prepare_threshold=None)
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


def redact_dsn(url: Optional[str]) -> str:
    """
    Descreve a conexão sem revelar a senha.

    Existe porque diagnosticar falha de banco exige ver host, porta e usuário —
    e é justamente aí que mora a maioria dos erros de configuração. Imprimir a
    URL inteira resolveria, mas colocaria a senha do Postgres na tela e, pior,
    no print que a pessoa manda para pedir ajuda.
    """
    if not url:
        return "(não definido)"

    # A redação vem ANTES do parse, por regex, e não depende dele.
    #
    # urlparse levanta ValueError ao ler a porta quando a senha contém
    # colchetes — o caso de quem esqueceu de substituir [YOUR-PASSWORD]. Se a
    # redação dependesse do parse, a função falharia exatamente na situação
    # que ela precisa diagnosticar, e ainda arriscaria devolver a URL crua.
    redacted = re.sub(r"(://[^:/@]+:)[^@]*(@)", r"\1***\2", url)

    tem_senha = bool(re.search(r"://[^:/@]+:[^@]+@", url))
    marcador = "[YOUR-PASSWORD]" in url or "[SUA-SENHA]" in url

    try:
        parsed = urlparse(redacted)
        host = parsed.hostname or "?"
        try:
            port = parsed.port or "?"
        except ValueError:
            port = "?"
        user = parsed.username or "?"
        database = (parsed.path or "/?").lstrip("/") or "?"
        estado = (
            "NÃO substituída (ainda é o marcador)" if marcador
            else ("definida" if tem_senha else "AUSENTE")
        )
        return f"{user}@{host}:{port}/{database} (senha: {estado})"
    except Exception:
        # Último recurso: devolve a string já redigida, que ainda mostra host
        # e porta e continua sem expor a senha.
        return redacted


def connection_diagnostics() -> list[tuple[str, str]]:
    """
    Checagens que identificam a causa de uma falha de conexão.

    Devolve pares (verificação, resultado) prontos para exibição. Cada item
    corresponde a um erro real e distinto — driver ausente, porta errada,
    marcador de senha não substituído — porque "não foi possível conectar"
    sozinho não diz em qual deles você caiu.
    """
    cfg = get_config()
    checks: list[tuple[str, str]] = []

    if not cfg.database_url:
        checks.append(("Modo", "SQLite local — DATASIFT_DATABASE_URL não foi lido"))
        return checks

    checks.append(("Modo", "Postgres"))
    checks.append(("Destino", redact_dsn(cfg.database_url)))

    # find_spec em vez de import: aqui só interessa saber se o driver existe.
    # Importar de verdade traria o módulo inteiro para dentro de uma função de
    # diagnóstico que pode rodar com o banco fora do ar.
    from importlib.util import find_spec

    if find_spec("psycopg") is not None:
        checks.append(("Driver", "psycopg 3 instalado"))
    elif find_spec("psycopg2") is not None:
        checks.append(("Driver", "psycopg2 (alternativo) instalado"))
    else:
        checks.append((
            "Driver",
            "AUSENTE — nenhum driver Postgres instalado. Confirme que o "
            "requirements.txt do repositório contém 'psycopg[binary]' e "
            "reinicie o app (Manage app → Reboot).",
        ))

    url = cfg.database_url
    if "[YOUR-PASSWORD]" in url or "[SUA-SENHA]" in url:
        checks.append((
            "Senha",
            "O marcador [YOUR-PASSWORD] continua na string — substitua pela "
            "senha real, sem os colchetes.",
        ))

    # Um segundo "@" depois do esquema significa que a senha contém um "@"
    # literal. O parser corta a URL no ÚLTIMO "@", então parte da senha vira
    # nome de host e a conexão falha com um erro que não menciona a senha.
    depois_do_esquema = url.split("://", 1)[-1]
    if depois_do_esquema.count("@") > 1:
        checks.append((
            "Senha",
            "A senha parece conter um '@' literal, que precisa ser escrito "
            "como %40 dentro da URL. Outros caracteres que exigem o mesmo: "
            "'#' vira %23, '/' vira %2F, ':' vira %3A, '%' vira %25. "
            "Alternativa mais simples: gere uma senha nova pelo Supabase "
            "(Database → Reset database password), que não usa esses símbolos.",
        ))

    try:
        # Mesma redação usada acima: sem ela, urlparse levanta ValueError ao ler
        # a porta quando a senha traz colchetes.
        parsed = urlparse(re.sub(r"(://[^:/@]+:)[^@]*(@)", r"\1***\2", url))
        if parsed.port == 5432 and "pooler" not in (parsed.hostname or ""):
            checks.append((
                "Porta",
                "5432 com host direto. No Streamlit Cloud isso costuma falhar: "
                "a conexão direta do Supabase só responde em IPv6. Use a string "
                "do Transaction pooler, na porta 6543.",
            ))
        elif parsed.port != 6543:
            checks.append((
                "Porta",
                f"{parsed.port} — o esperado para o pooler do Supabase é 6543.",
            ))
    except Exception:
        pass

    return checks
