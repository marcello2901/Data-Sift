# -*- coding: utf-8 -*-
"""
Configuração de segurança.

Regra de ouro: **nenhum segredo no código-fonte**. Tudo vem de variável de
ambiente ou de ``.streamlit/secrets.toml`` (que está no ``.gitignore``).
A variável de ambiente sempre vence, para que o mesmo código rode em Docker,
VPS e Streamlit Cloud sem alteração.

Chaves reconhecidas (prefixo ``DATASIFT_``)::

    DATASIFT_DATABASE_URL          Postgres (postgresql://...). Se ausente, usa SQLite.
    DATASIFT_DB_PATH               caminho do SQLite (default ./.datasift/datasift.db)
    DATASIFT_PEPPER                pimenta do hash de senha (segredo do servidor)
    DATASIFT_BOOTSTRAP_ADMIN_EMAIL e-mail do superadmin criado no 1º boot
    DATASIFT_BOOTSTRAP_ADMIN_PASSWORD senha inicial do superadmin
    DATASIFT_SESSION_IDLE_MINUTES     inatividade que encerra a sessão (default 30)
    DATASIFT_SESSION_ABSOLUTE_HOURS   duração máxima absoluta (default 8)
    DATASIFT_MAX_LOGIN_ATTEMPTS       tentativas antes do bloqueio (default 5)
    DATASIFT_LOCKOUT_MINUTES          base do bloqueio exponencial (default 5)
    DATASIFT_PASSWORD_MIN_LENGTH      tamanho mínimo de senha (default 12)
    DATASIFT_PASSWORD_HISTORY         senhas antigas bloqueadas p/ reuso (default 5)
    DATASIFT_PASSWORD_MAX_AGE_DAYS    expiração de senha, 0 desliga (default 0)
    DATASIFT_MAX_UPLOAD_MB            teto de upload em MB (default 140)
    DATASIFT_MAX_UNCOMPRESSED_MB      teto do conteúdo descompactado (default 500)
    DATASIFT_MAX_COMPRESSION_RATIO    razão máxima de descompressão (default 120)
    DATASIFT_TRUST_PROXY_HEADERS      "1" só se houver proxy reverso confiável
    DATASIFT_TRUSTED_PROXY_HOPS       nº de proxies confiáveis à frente (default 1)
    DATASIFT_SESSION_REVALIDATE_SECONDS janela de revalidação da sessão (default 30)
    DATASIFT_REQUIRE_2FA_FOR_ADMIN    "1" exige TOTP para papéis administrativos
    DATASIFT_AUDIT_RETENTION_DAYS     retenção da auditoria (default 730)

No Streamlit Community Cloud estas chaves vão em *Settings → Secrets*, que
persistem mesmo quando o container é recriado. Como o disco lá é efêmero,
``DATASIFT_DATABASE_URL`` é obrigatório em produção: sem ele o banco de
usuários é apagado a cada redeploy.

Em ``secrets.toml`` as mesmas chaves podem aparecer sem o prefixo, dentro de
uma seção ``[datasift]``::

    [datasift]
    PEPPER = "..."
    BOOTSTRAP_ADMIN_EMAIL = "voce@exemplo.com"
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

_ENV_PREFIX = "DATASIFT_"
_SECRETS_SECTION = "datasift"


def _from_secrets(name: str) -> Optional[str]:
    """
    Lê ``name`` de ``st.secrets``, tolerando a ausência do arquivo.

    O acesso a ``st.secrets`` levanta exceção quando não existe
    ``secrets.toml``; por isso todo o corpo é defensivo. Também não
    importamos streamlit no topo do módulo para que este pacote continue
    testável fora de um runtime Streamlit.
    """
    try:
        import streamlit as st
    except Exception:
        return None

    for key in (f"{_ENV_PREFIX}{name}", name):
        try:
            if key in st.secrets:
                return str(st.secrets[key])
        except Exception:
            return None

    try:
        section = st.secrets.get(_SECRETS_SECTION)
        if section is not None and name in section:
            return str(section[name])
    except Exception:
        return None
    return None


def get_setting(name: str, default: Optional[str] = None) -> Optional[str]:
    """Env var tem prioridade; depois secrets; depois o default."""
    value = os.environ.get(f"{_ENV_PREFIX}{name}")
    if value not in (None, ""):
        return value
    value = _from_secrets(name)
    if value not in (None, ""):
        return value
    return default


def _int(name: str, default: int) -> int:
    raw = get_setting(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def _bool(name: str, default: bool = False) -> bool:
    raw = get_setting(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on", "sim")


@dataclass(frozen=True)
class SecurityConfig:
    """Parâmetros efetivos desta instância. Imutável depois de resolvido."""

    database_url: Optional[str]
    db_path: Path
    pepper: Optional[str]
    bootstrap_admin_email: Optional[str]
    bootstrap_admin_password: Optional[str]

    session_idle_minutes: int
    session_absolute_hours: int
    session_revalidate_seconds: int

    max_login_attempts: int
    lockout_minutes: int

    password_min_length: int
    password_history: int
    password_max_age_days: int

    max_upload_mb: int
    max_uncompressed_mb: int
    max_compression_ratio: int

    trust_proxy_headers: bool
    trusted_proxy_hops: int
    require_2fa_for_admin: bool
    audit_retention_days: int

    @property
    def max_upload_bytes(self) -> int:
        return self.max_upload_mb * 1024 * 1024

    @property
    def max_uncompressed_bytes(self) -> int:
        return self.max_uncompressed_mb * 1024 * 1024

    @property
    def is_postgres(self) -> bool:
        return bool(self.database_url)

    def warnings(self) -> list[str]:
        """
        Problemas de configuração que o superadmin precisa ver na tela.

        São avisos, não erros: o app precisa subir mesmo mal configurado, senão
        o próprio admin fica trancado do lado de fora e não consegue corrigir.
        """
        issues: list[str] = []
        if not self.database_url:
            issues.append(
                "DATASIFT_DATABASE_URL não definido: o app está usando SQLite "
                "em disco local. Isso serve para desenvolvimento, mas em "
                "Streamlit Community Cloud o disco é efêmero e TODOS os "
                "usuários, sessões e registros de auditoria serão perdidos no "
                "próximo redeploy. Aponte para um Postgres gerenciado."
            )
        if not self.pepper:
            issues.append(
                "DATASIFT_PEPPER não definido: os hashes de senha ficam sem "
                "pimenta do servidor. Defina um valor longo e aleatório em "
                "secrets.toml para que um vazamento do banco não baste para "
                "atacar as senhas offline."
            )
        if self.bootstrap_admin_password:
            issues.append(
                "DATASIFT_BOOTSTRAP_ADMIN_PASSWORD ainda está configurado. "
                "Remova-o após o primeiro login: ele só serve para criar o "
                "superadmin inicial."
            )
        if self.trust_proxy_headers:
            issues.append(
                "DATASIFT_TRUST_PROXY_HEADERS está ligado. Mantenha assim "
                "somente se o app estiver atrás de um proxy reverso que "
                "sobrescreve X-Forwarded-For; caso contrário o IP registrado "
                "na auditoria pode ser forjado pelo cliente."
            )
        return issues


@lru_cache(maxsize=1)
def get_config() -> SecurityConfig:
    """Configuração resolvida uma vez por processo."""
    default_db = Path(__file__).resolve().parent.parent / ".datasift" / "datasift.db"
    raw_path = get_setting("DB_PATH")
    db_path = Path(raw_path).expanduser() if raw_path else default_db

    # DATABASE_URL sem prefixo também é aceito: é a convenção que Supabase,
    # Neon e Heroku já usam, e evita duplicar a mesma string em dois lugares.
    database_url = get_setting("DATABASE_URL") or os.environ.get("DATABASE_URL") or None

    return SecurityConfig(
        database_url=database_url,
        db_path=db_path,
        pepper=get_setting("PEPPER"),
        bootstrap_admin_email=get_setting("BOOTSTRAP_ADMIN_EMAIL"),
        bootstrap_admin_password=get_setting("BOOTSTRAP_ADMIN_PASSWORD"),
        session_idle_minutes=_int("SESSION_IDLE_MINUTES", 30),
        session_absolute_hours=_int("SESSION_ABSOLUTE_HOURS", 8),
        session_revalidate_seconds=_int("SESSION_REVALIDATE_SECONDS", 30),
        max_login_attempts=_int("MAX_LOGIN_ATTEMPTS", 5),
        lockout_minutes=_int("LOCKOUT_MINUTES", 5),
        password_min_length=_int("PASSWORD_MIN_LENGTH", 12),
        password_history=_int("PASSWORD_HISTORY", 5),
        password_max_age_days=_int("PASSWORD_MAX_AGE_DAYS", 0),
        max_upload_mb=_int("MAX_UPLOAD_MB", 140),
        max_uncompressed_mb=_int("MAX_UNCOMPRESSED_MB", 500),
        max_compression_ratio=_int("MAX_COMPRESSION_RATIO", 120),
        trust_proxy_headers=_bool("TRUST_PROXY_HEADERS", False),
        trusted_proxy_hops=max(1, _int("TRUSTED_PROXY_HOPS", 1)),
        require_2fa_for_admin=_bool("REQUIRE_2FA_FOR_ADMIN", False),
        audit_retention_days=_int("AUDIT_RETENTION_DAYS", 730),
    )
