# -*- coding: utf-8 -*-
"""
Telas de autenticação e da conta do usuário.

Nenhuma destas telas usa ``unsafe_allow_html`` com dado vindo do banco. Nome
de usuário e nome de laboratório são escritos por administradores, mas um
administrador de laboratório é um usuário como outro qualquer no modelo de
ameaça — se ele puder cadastrar um colega chamado ``<img onerror=…>`` e isso
executar no seu navegador quando você abrir o painel, ele escala do próprio
laboratório para superadmin. Por isso: widgets nativos do Streamlit, que já
escapam o conteúdo.
"""

from __future__ import annotations

from typing import Optional

import streamlit as st

from . import auth, guard, repository
from .config import get_config
from .models import (
    PERM_USERS_MANAGE,
    ROLE_LABELS,
    User,
)
from .sanitize import clean_text


def _brand_header() -> None:
    """Cabeçalho enxuto: nada de logo em base64 antes de autenticar."""
    st.markdown("## DataSift")
    st.caption("Plataforma de análise de dados laboratoriais — acesso restrito")


def _show_bootstrap_password() -> None:
    """Exibe uma única vez a senha do superadmin criada no primeiro boot."""
    password = st.session_state.pop("_sec_bootstrap_password", None)
    if not password:
        return
    st.success("Conta de superadministrador criada.")
    st.warning(
        "Esta senha aparece **uma única vez**. Copie agora, faça login e "
        "troque-a — o sistema vai exigir a troca no primeiro acesso."
    )
    st.code(password, language=None)


def render_login_page() -> None:
    """Tela de login. Encerra o script pelo chamador (``require_login``)."""
    _brand_header()
    _show_bootstrap_password()

    reason = st.session_state.pop("_sec_logout_reason", None)
    if reason:
        st.info(reason)

    st.divider()

    needs_totp = bool(st.session_state.get("_sec_needs_totp"))

    with st.form("datasift_login", clear_on_submit=False):
        email = st.text_input("E-mail", autocomplete="username", max_chars=200)
        password = st.text_input(
            "Senha", type="password", autocomplete="current-password", max_chars=256
        )
        totp = ""
        if needs_totp:
            totp = st.text_input(
                "Código de verificação",
                max_chars=6,
                help="Os 6 dígitos do seu aplicativo autenticador.",
            )
        submitted = st.form_submit_button("Entrar", type="primary", use_container_width=True)

    if submitted:
        result = auth.authenticate(email, password, totp or None)

        if result.needs_totp:
            st.session_state["_sec_needs_totp"] = True
            st.warning(result.reason)
            st.rerun()

        if not result.ok:
            st.session_state.pop("_sec_needs_totp", None)
            st.error(result.reason)
            return

        st.session_state.pop("_sec_needs_totp", None)
        guard.establish_session(result.user, result.token)
        st.rerun()

    with st.expander("Não consigo entrar"):
        st.markdown(
            "- **Esqueci a senha** — só o administrador redefine. Peça a ele "
            "uma senha provisória; você troca no primeiro acesso.\n"
            "- **Conta bloqueada** — o bloqueio por excesso de tentativas "
            "expira sozinho. O administrador pode liberar antes.\n"
            "- **Fui desconectado sem motivo** — recarregar a página encerra a "
            "sessão por segurança. Isso é esperado."
        )


def render_forced_password_change(user: User) -> None:
    """
    Troca obrigatória de senha, antes de liberar qualquer tela.

    Aparece quando a conta nasceu com senha provisória, quando o administrador
    redefiniu a senha, ou quando a senha venceu.
    """
    _brand_header()
    st.divider()
    st.subheader("Defina uma nova senha")
    st.info(
        "Sua senha atual é provisória ou expirou. Escolha uma nova senha para "
        "continuar."
    )

    cfg = get_config()
    st.caption(
        f"Mínimo de {cfg.password_min_length} caracteres. Não pode conter seu "
        f"nome ou e-mail, nem repetir as últimas {cfg.password_history} senhas. "
        "Uma frase longa e memorável é mais segura que uma senha curta cheia de símbolos."
    )

    with st.form("forced_password_change"):
        current = st.text_input("Senha atual (a provisória)", type="password", max_chars=256)
        new_password = st.text_input("Nova senha", type="password", max_chars=256)
        confirm = st.text_input("Repita a nova senha", type="password", max_chars=256)
        submitted = st.form_submit_button("Salvar nova senha", type="primary")

    if submitted:
        if new_password != confirm:
            st.error("As duas senhas não coincidem.")
            return

        ok, message = auth.change_own_password(user, current, new_password)
        if not ok:
            st.error(message)
            return

        # A troca de senha revoga todas as sessões, inclusive esta. Forçar
        # novo login com a senha nova é o comportamento correto: confirma que
        # a pessoa memorizou a senha que acabou de definir.
        guard._clear_session_state()
        st.session_state["_sec_logout_reason"] = (
            "Senha alterada com sucesso. Entre novamente com a nova senha."
        )
        st.rerun()

    if st.button("Sair"):
        guard.sign_out()
        st.rerun()


def render_access_denied(user: User, permission: str, page_name: Optional[str]) -> None:
    """Negativa de acesso, sem revelar o que existe do outro lado."""
    _brand_header()
    st.divider()
    st.error("Você não tem permissão para acessar esta área.")
    st.caption(
        f"Seu perfil é **{ROLE_LABELS.get(user.role, user.role)}**. "
        "Se precisa deste acesso, peça ao administrador do seu laboratório."
    )
    if st.button("Voltar ao início"):
        st.switch_page("app.py")


def render_account_sidebar(user: User) -> None:
    """
    Bloco de conta na barra lateral: quem sou, onde estou, e sair.

    Mostrar o laboratório ativo é controle de segurança, não enfeite: quem usa
    mais de uma conta precisa enxergar em qual contexto está antes de enviar
    uma planilha para o lugar errado.
    """
    org = repository.get_organization(user.org_id)

    with st.sidebar:
        st.markdown("---")
        st.markdown(f"**{clean_text(user.display_name, 60)}**")
        st.caption(f"{clean_text(user.email, 60)}")
        st.caption(f"{ROLE_LABELS.get(user.role, user.role)}")
        if org:
            st.caption(f"Laboratório: {clean_text(org.name, 60)}")

        if not user.totp_enabled:
            st.caption("⚠️ Verificação em duas etapas desativada")

        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("Conta", use_container_width=True):
                st.session_state["_sec_show_account"] = True
                st.rerun()
        with col_b:
            if st.button("Sair", use_container_width=True, type="primary"):
                guard.sign_out()
                st.session_state["_sec_logout_reason"] = "Sessão encerrada."
                st.rerun()

        if user.has_permission(PERM_USERS_MANAGE):
            st.caption("Administração disponível no menu de páginas.")


def render_account_panel(user: User) -> bool:
    """
    Painel da própria conta: senha, 2FA e sessões ativas.

    Devolve ``True`` quando está aberto, para que a página o desenhe no lugar
    do conteúdo normal.
    """
    if not st.session_state.get("_sec_show_account"):
        return False

    st.subheader("Minha conta")
    if st.button("← Voltar"):
        st.session_state.pop("_sec_show_account", None)
        st.rerun()

    tab_pwd, tab_2fa, tab_sessions = st.tabs(
        ["Senha", "Verificação em duas etapas", "Sessões ativas"]
    )

    with tab_pwd:
        _render_change_password(user)

    with tab_2fa:
        _render_totp_panel(user)

    with tab_sessions:
        _render_sessions_panel(user)

    return True


def _render_change_password(user: User) -> None:
    cfg = get_config()
    st.caption(
        f"Mínimo de {cfg.password_min_length} caracteres, sem repetir as "
        f"últimas {cfg.password_history}."
    )
    with st.form("change_password"):
        current = st.text_input("Senha atual", type="password", max_chars=256)
        new_password = st.text_input("Nova senha", type="password", max_chars=256)
        confirm = st.text_input("Repita a nova senha", type="password", max_chars=256)
        submitted = st.form_submit_button("Alterar senha", type="primary")

    if submitted:
        if new_password != confirm:
            st.error("As duas senhas não coincidem.")
            return
        ok, message = auth.change_own_password(user, current, new_password)
        if ok:
            guard._clear_session_state()
            st.session_state["_sec_logout_reason"] = message
            st.rerun()
        else:
            st.error(message)


def _render_totp_panel(user: User) -> None:
    """
    Cadastro e remoção do segundo fator.

    O segredo só é gravado depois que o usuário prova ter um código válido —
    ativar antes disso é a forma mais comum de alguém se trancar para fora.
    """
    if user.totp_enabled:
        st.success("Verificação em duas etapas **ativada**.")
        st.caption(
            "Desativar reduz a proteção da conta. Faz sentido apenas se você "
            "perdeu o acesso ao aplicativo autenticador."
        )
        with st.form("disable_totp"):
            password = st.text_input("Confirme sua senha", type="password", max_chars=256)
            submitted = st.form_submit_button("Desativar")
        if submitted:
            row = repository.get_auth_row_by_id(user.id)
            from .crypto import verify_password

            if not row or not verify_password(password, row["password_hash"]):
                st.error("Senha incorreta.")
                return
            ok, message = auth.disable_totp(user, user, verified_password=True)
            st.success(message) if ok else st.error(message)
            st.rerun()
        return

    st.info(
        "Com a verificação em duas etapas, saber sua senha não basta para "
        "entrar na sua conta. Recomendado para todos, obrigatório na prática "
        "para quem administra."
    )

    if "_sec_totp_secret" not in st.session_state:
        if st.button("Ativar verificação em duas etapas", type="primary"):
            st.session_state["_sec_totp_secret"] = auth.begin_totp_enrollment()
            st.rerun()
        return

    secret = st.session_state["_sec_totp_secret"]
    st.markdown("**1.** Abra seu aplicativo autenticador e cadastre esta chave:")
    st.code(secret, language=None)
    st.caption("Google Authenticator, Microsoft Authenticator, Authy, 1Password, Bitwarden.")

    with st.expander("Ou use o endereço de configuração"):
        st.code(auth_provisioning_uri(user, secret), language=None)

    st.markdown("**2.** Digite o código que o aplicativo mostra:")
    with st.form("confirm_totp"):
        code = st.text_input("Código de 6 dígitos", max_chars=6)
        submitted = st.form_submit_button("Confirmar e ativar", type="primary")

    if submitted:
        ok, message = auth.confirm_totp_enrollment(user, secret, code)
        if ok:
            st.session_state.pop("_sec_totp_secret", None)
            st.success(message)
            st.rerun()
        else:
            st.error(message)

    if st.button("Cancelar"):
        st.session_state.pop("_sec_totp_secret", None)
        st.rerun()


def auth_provisioning_uri(user: User, secret: str) -> str:
    from .crypto import totp_provisioning_uri

    return totp_provisioning_uri(secret, user.email)


def _render_sessions_panel(user: User) -> None:
    """
    Sessões abertas da conta.

    É o mecanismo de autoatendimento contra sequestro de sessão: se você
    reconhece um acesso que não é seu, encerra tudo sem depender do
    administrador.
    """
    sessions = repository.list_active_sessions(user.id)
    if not sessions:
        st.caption("Nenhuma outra sessão ativa.")
    else:
        st.caption(f"{len(sessions)} sessão(ões) ativa(s).")
        for item in sessions:
            st.markdown(
                f"- Iniciada em **{item['created_at']}** · "
                f"última atividade **{item['last_seen_at']}** · "
                f"origem `{item.get('ip') or 'não registrada'}`"
            )

    st.caption(
        "O IP só aparece quando o app está atrás de um proxy confiável "
        "configurado. No Streamlit Community Cloud ele costuma ficar em branco."
    )

    if st.button("Encerrar todas as sessões", type="primary"):
        repository.revoke_all_sessions(user.id, reason="user_revoked_all")
        guard._clear_session_state()
        st.session_state["_sec_logout_reason"] = (
            "Todas as sessões foram encerradas. Entre novamente."
        )
        st.rerun()


def render_security_notices(user: User) -> None:
    """
    Avisos de configuração, visíveis apenas ao superadmin.

    Ficam na tela porque configuração de segurança que só aparece em log não é
    lida por ninguém.
    """
    from .models import PERM_SECURITY_SETTINGS

    if not user.has_permission(PERM_SECURITY_SETTINGS):
        return

    warnings = get_config().warnings()
    if not warnings:
        return

    with st.expander(f"⚠️ {len(warnings)} aviso(s) de configuração de segurança"):
        for item in warnings:
            st.warning(item)
