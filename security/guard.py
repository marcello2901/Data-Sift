# -*- coding: utf-8 -*-
"""
Ponto único de aplicação do controle de acesso.

Toda página do DataSift começa com::

    from security.guard import require_login
    user = require_login()

Por que **toda** página, e não só o ``app.py``: no Streamlit, cada arquivo
dentro de ``pages/`` é uma rota própria e independente. Digitar
``…/Analise_de_Repeticoes`` na barra de endereço executa aquele script do
começo ao fim, sem passar pelo ``app.py``. Uma página sem ``require_login`` no
topo é uma página pública, por mais protegido que o resto do app esteja.

Esconder a página do menu lateral **não** protege nada: a rota continua
funcionando. O CSS que oculta a área administrativa existe para não confundir
quem não tem acesso, não como barreira — a barreira é a chamada de
``require_login`` no início do arquivo.

Sobre desempenho: o Streamlit reexecuta o script inteiro a cada clique, o que
significaria uma ida ao Postgres por interação. A sessão é revalidada contra o
banco no máximo a cada ``SESSION_REVALIDATE_SECONDS`` (padrão 30) e sempre que
``force=True`` — usado pelas telas administrativas. A consequência honesta: ao
desativar um usuário, a sessão dele morre em até 30 segundos, não
instantaneamente. Para expulsar alguém na hora, revogue as sessões pelo painel
(:func:`security.repository.revoke_all_sessions`), que é verificado sem atraso.
"""

from __future__ import annotations

import time
from typing import Optional

import streamlit as st

from . import audit, auth, tenancy
from .config import get_config
from .db import init_db
from .models import User

_TOKEN_KEY = "_sec_token"
_USER_KEY = "_sec_user"
_VALIDATED_AT_KEY = "_sec_validated_at"
_BOOTSTRAP_KEY = "_sec_bootstrap_password"


def _boot() -> None:
    """Prepara o banco e o superadmin inicial. Idempotente."""
    if st.session_state.get("_sec_booted"):
        return
    try:
        init_db()
        generated = auth.bootstrap_if_needed()
        if generated:
            st.session_state[_BOOTSTRAP_KEY] = generated
        st.session_state["_sec_booted"] = True
    except Exception as exc:
        st.error(
            "Não foi possível conectar ao banco de segurança. O acesso está "
            "bloqueado até que a conexão seja restabelecida."
        )

        # O diagnóstico fica aberto por padrão. Uma falha de banco no boot só
        # acontece durante a configuração inicial, e nesse momento esconder a
        # causa atrás de um clique não protege ninguém — só atrasa quem está
        # tentando resolver. A senha nunca aparece: ver db.redact_dsn.
        from .db import connection_diagnostics

        with st.expander("Diagnóstico", expanded=True):
            for label, result in connection_diagnostics():
                st.markdown(f"- **{label}:** {result}")

            st.markdown("**Erro retornado pelo banco:**")
            st.code(f"{type(exc).__name__}: {exc}")

            texto = str(exc).lower()

            if "circuitbreaker" in texto or "too many authentication failures" in texto:
                st.warning(
                    "**O Supabase bloqueou temporariamente novas conexões** "
                    "após seguidas falhas de autenticação. Duas coisas "
                    "precisam acontecer, nesta ordem:\n\n"
                    "1. **Corrija a credencial.** No pooler, o usuário tem de "
                    "ser `postgres.<referência-do-projeto>` — não apenas "
                    "`postgres`. Confira o campo *Destino* acima: se ele "
                    "mostra `postgres@…` sem o ponto e o código do projeto, é "
                    "essa a causa. Copie de novo a string do **Transaction "
                    "pooler**, que já vem com o usuário correto.\n"
                    "2. **Espere o bloqueio expirar** — normalmente de 5 a 30 "
                    "minutos. Enquanto ele durar, nem a credencial certa "
                    "conecta. Corrigir os Secrets agora e voltar depois é o "
                    "caminho mais rápido."
                )
            elif "password authentication failed" in texto or "auth" in texto:
                st.warning(
                    "**Falha de autenticação.** Confira o campo *Destino* "
                    "acima: no pooler do Supabase o usuário precisa ter a "
                    "forma `postgres.<referência-do-projeto>`, e a senha é a "
                    "do banco (definida ao criar o projeto), não a da sua "
                    "conta Supabase. Se não a tiver, redefina em "
                    "*Project Settings → Database → Reset database password*."
                )
            elif "could not translate host" in texto or "name or service not known" in texto:
                st.warning(
                    "**O endereço do servidor não foi encontrado.** Confira se "
                    "o host foi copiado por inteiro e sem espaços."
                )
            elif "timeout" in texto or "timed out" in texto:
                st.warning(
                    "**Tempo esgotado ao conectar.** Costuma indicar projeto "
                    "Supabase pausado por inatividade (o plano gratuito pausa "
                    "após 7 dias) ou uso da conexão direta na porta 5432, que "
                    "só responde em IPv6. Verifique o estado do projeto no "
                    "painel do Supabase."
                )
            else:
                st.caption(
                    "Causas mais comuns: driver ausente (falta reiniciar o app "
                    "após atualizar o requirements.txt); marcador "
                    "[YOUR-PASSWORD] não substituído; porta 5432 em vez de "
                    "6543; senha com caractere especial que precisa de "
                    "codificação URL (@ vira %40, # vira %23)."
                )
        st.stop()


def current_user(force_revalidate: bool = False) -> Optional[User]:
    """
    Usuário autenticado, ou ``None``. **Não** interrompe a execução.

    Use quando a página precisa se adaptar ao estado de login; use
    :func:`require_login` quando o acesso for obrigatório.
    """
    token = st.session_state.get(_TOKEN_KEY)
    if not token:
        return None

    cached_user: Optional[User] = st.session_state.get(_USER_KEY)
    validated_at = float(st.session_state.get(_VALIDATED_AT_KEY) or 0)
    window = get_config().session_revalidate_seconds

    if cached_user is not None and not force_revalidate:
        if (time.time() - validated_at) < window:
            return cached_user

    state = auth.validate_session(token)
    if not state.valid:
        _clear_session_state()
        return None

    st.session_state[_USER_KEY] = state.user
    st.session_state[_VALIDATED_AT_KEY] = time.time()
    return state.user


def _clear_session_state() -> None:
    for key in (_TOKEN_KEY, _USER_KEY, _VALIDATED_AT_KEY):
        st.session_state.pop(key, None)
    tenancy.wipe_session_data()


def establish_session(user: User, token: str) -> None:
    """Registra a sessão recém-criada. Chamado apenas pela tela de login."""
    tenancy.wipe_session_data()  # não herdar dados de quem usou a aba antes
    st.session_state[_TOKEN_KEY] = token
    st.session_state[_USER_KEY] = user
    st.session_state[_VALIDATED_AT_KEY] = time.time()


def sign_out() -> None:
    """Encerra a sessão e limpa todo o estado que carrega dado clínico."""
    token = st.session_state.get(_TOKEN_KEY)
    user = st.session_state.get(_USER_KEY)
    if token:
        auth.logout(token, user)
    _clear_session_state()


def require_login(
    permission: Optional[str] = None,
    force_revalidate: bool = False,
    page_name: Optional[str] = None,
) -> User:
    """
    Exige sessão válida e, opcionalmente, uma permissão.

    Interrompe o script com ``st.stop()`` quando o acesso é negado — nada
    abaixo da chamada é executado. É por isso que ela precisa vir **antes** de
    qualquer leitura de arquivo ou consulta na página.
    """
    from . import ui  # importado aqui para evitar ciclo com o módulo de telas

    _boot()

    user = current_user(force_revalidate=force_revalidate)

    if user is None:
        ui.render_login_page()
        st.stop()

    if user.must_change_password:
        ui.render_forced_password_change(user)
        st.stop()

    if permission and not user.has_permission(permission):
        audit.record(
            audit.ACCESS_DENIED,
            audit.OUTCOME_DENIED,
            actor_id=user.id,
            actor_email=user.email,
            org_id=user.org_id,
            target=page_name or permission,
            ip=auth.client_ip(),
            detail={"permissao": permission, "papel": user.role},
        )
        ui.render_access_denied(user, permission, page_name)
        st.stop()

    return user


def require_permission(user: User, permission: str, what: str = "") -> bool:
    """
    Verifica permissão sem interromper, registrando a negativa.

    Para ações dentro de uma página já liberada — por exemplo, esconder o botão
    de exportar de quem é ``viewer``.
    """
    if user.has_permission(permission):
        return True
    audit.record(
        audit.ACCESS_DENIED,
        audit.OUTCOME_DENIED,
        actor_id=user.id,
        actor_email=user.email,
        org_id=user.org_id,
        target=what or permission,
        ip=auth.client_ip(),
        detail={"permissao": permission, "papel": user.role},
    )
    return False


def hide_admin_nav(user: Optional[User]) -> None:
    """
    Oculta a página de administração do menu para quem não é admin.

    Reforço visual, **não** controle de acesso: a rota continua existindo e
    quem digitar o endereço chega nela. Quem barra é o ``require_login`` no
    topo de ``pages/9_Administracao.py``.
    """
    from .models import PERM_USERS_MANAGE

    if user is not None and user.has_permission(PERM_USERS_MANAGE):
        return

    st.markdown(
        """
        <style>
            [data-testid="stSidebarNav"] a[href$="Administracao"],
            [data-testid="stSidebarNav"] li:has(a[href$="Administracao"]) {
                display: none !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
