# -*- coding: utf-8 -*-
"""
Console de administração.

Todas as ações passam por três verificações, nesta ordem: **permissão**
(o papel permite?), **escopo de laboratório** (o alvo está no meu tenant?) e
**invariantes de segurança** (esta ação deixa o sistema sem administrador? Ela
promove alguém acima de quem a executa?).

A terceira é a que costuma faltar em painéis administrativos e a que produz os
piores incidentes: um clique que rebaixa o último superadmin não dá erro, dá
um sistema sem dono. Aqui essas condições são recusadas antes de tocar o banco.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
import streamlit as st

from . import audit, auth, ratelimit, repository, tenancy
from .config import get_config
from .crypto import generate_temp_password, hash_password
from .db import healthcheck
from .models import (
    PERM_AUDIT_VIEW_ALL,
    PERM_AUDIT_VIEW_ORG,
    PERM_ORGS_MANAGE,
    PERM_SECURITY_SETTINGS,
    ROLE_DESCRIPTIONS,
    ROLE_LABELS,
    ROLE_SUPERADMIN,
    STATUS_ACTIVE,
    STATUS_DISABLED,
    User,
    assignable_roles,
)
from .repository import ACROSS_ALL_ORGS
from .sanitize import clean_text, safe_email


def render(admin: User) -> None:
    """Desenha o console inteiro. O chamador já validou ``PERM_USERS_MANAGE``."""
    st.markdown("## Administração")

    decision = ratelimit.check_admin_action(admin.id)
    if not decision.allowed:
        st.error(
            f"Limite de ações administrativas atingido. Aguarde "
            f"{decision.retry_after_human}."
        )
        return

    tabs = ["Usuários", "Auditoria"]
    if admin.has_permission(PERM_ORGS_MANAGE):
        tabs.insert(1, "Laboratórios")
    if admin.has_permission(PERM_SECURITY_SETTINGS):
        tabs.append("Segurança")

    rendered = st.tabs(tabs)
    for name, container in zip(tabs, rendered):
        with container:
            if name == "Usuários":
                _render_users(admin)
            elif name == "Laboratórios":
                _render_orgs(admin)
            elif name == "Auditoria":
                _render_audit(admin)
            elif name == "Segurança":
                _render_security(admin)


# --------------------------------------------------------------------------
# Usuários
# --------------------------------------------------------------------------

def _scope_selector(admin: User) -> tuple[object, Optional[str]]:
    """
    Define o escopo de listagem.

    Superadmin escolhe o laboratório (ou todos). Os demais ficam presos ao
    próprio, sem widget para trocar — a restrição está no valor devolvido, não
    na interface, para que não dependa do que a tela desenha.
    """
    if not admin.has_permission(PERM_ORGS_MANAGE):
        return admin.org_id, admin.org_id

    orgs = repository.list_organizations()
    options = {"Todos os laboratórios": None}
    options.update({f"{org.name}": org.id for org in orgs})
    choice = st.selectbox("Laboratório", list(options.keys()), index=0)
    org_id = options[choice]
    return (ACROSS_ALL_ORGS if org_id is None else org_id), org_id


def _render_users(admin: User) -> None:
    scope, selected_org = _scope_selector(admin)

    with st.expander("➕ Criar novo usuário", expanded=False):
        _render_create_user(admin, selected_org)

    try:
        users = repository.list_users(scope)
    except repository.TenantScopeError:
        st.error("Escopo inválido.")
        return

    if not users:
        st.info("Nenhum usuário cadastrado neste escopo.")
        return

    st.caption(f"{len(users)} usuário(s).")

    orgs_by_id = {org.id: org.name for org in repository.list_organizations()}
    table = pd.DataFrame([
        {
            "Nome": user.display_name,
            "E-mail": user.email,
            "Perfil": ROLE_LABELS.get(user.role, user.role),
            "Laboratório": orgs_by_id.get(user.org_id, "—"),
            "Situação": "Ativo" if user.is_active else "Desativado",
            "2FA": "Sim" if user.totp_enabled else "Não",
            "Último acesso": user.last_login_at or "nunca",
        }
        for user in users
    ])
    st.dataframe(table, use_container_width=True, hide_index=True)

    st.markdown("### Gerenciar usuário")
    labels = {f"{u.display_name} · {u.email}": u for u in users}
    chosen_label = st.selectbox("Selecione", list(labels.keys()))
    target = labels[chosen_label]

    _render_user_actions(admin, target)


def _render_create_user(admin: User, default_org: Optional[str]) -> None:
    allowed_roles = assignable_roles(admin.role)
    if not allowed_roles:
        st.warning("Seu perfil não permite criar usuários.")
        return

    if admin.has_permission(PERM_ORGS_MANAGE):
        orgs = repository.list_organizations()
        if not orgs:
            st.warning("Crie um laboratório antes de cadastrar usuários.")
            return
        org_options = {org.name: org.id for org in orgs}
        default_index = 0
        if default_org:
            for index, org in enumerate(orgs):
                if org.id == default_org:
                    default_index = index
                    break
        org_name = st.selectbox("Laboratório do novo usuário", list(org_options.keys()),
                                index=default_index, key="new_user_org")
        target_org = org_options[org_name]
    else:
        target_org = admin.org_id

    with st.form("create_user"):
        email = st.text_input("E-mail", max_chars=200)
        display_name = st.text_input("Nome completo", max_chars=120)
        role = st.selectbox(
            "Perfil de acesso",
            allowed_roles,
            format_func=lambda r: ROLE_LABELS.get(r, r),
        )
        st.caption(ROLE_DESCRIPTIONS.get(role, ""))
        submitted = st.form_submit_button("Criar usuário", type="primary")

    if not submitted:
        return

    normalized = safe_email(email)
    if not normalized:
        st.error("E-mail inválido.")
        return

    name = clean_text(display_name, 120)
    if len(name) < 2:
        st.error("Informe o nome completo do usuário.")
        return

    if role not in assignable_roles(admin.role):
        # Revalidação no servidor: o valor do widget pode ter sido manipulado.
        st.error("Você não pode atribuir esse perfil.")
        return

    if not tenancy.can_access_org(admin, target_org):
        tenancy.assert_org_access(admin, target_org, "criar_usuario")

    if repository.email_exists(normalized):
        st.error("Já existe um usuário com este e-mail.")
        return

    temp_password = generate_temp_password()
    user = repository.create_user(
        org_id=target_org,
        email=normalized,
        display_name=name,
        password_hash=hash_password(temp_password),
        role=role,
        created_by=admin.id,
        must_change_password=True,
    )

    audit.record(
        audit.USER_CREATED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
        actor_email=admin.email, org_id=target_org, target=normalized,
        ip=auth.client_ip(), detail={"papel": role},
    )

    st.success(f"Usuário **{user.display_name}** criado.")
    st.warning(
        "Esta senha provisória aparece **uma única vez**. Entregue-a por um "
        "canal seguro — de preferência não pelo mesmo e-mail que serve de login."
    )
    st.code(temp_password, language=None)
    st.caption("O usuário será obrigado a trocá-la no primeiro acesso.")


def _render_user_actions(admin: User, target: User) -> None:
    """Ações sobre um usuário, com as invariantes verificadas antes de agir."""
    if not tenancy.can_access_org(admin, target.org_id):
        st.error("Este usuário pertence a outro laboratório.")
        return

    is_self = admin.id == target.id
    last_superadmin = (
        target.role == ROLE_SUPERADMIN and repository.count_active_superadmins() <= 1
    )

    col1, col2 = st.columns(2)

    # --- Perfil ---
    with col1:
        st.markdown("**Perfil de acesso**")
        options = assignable_roles(admin.role)
        if target.role not in options:
            st.caption(
                f"Perfil atual: {ROLE_LABELS.get(target.role, target.role)} — "
                "acima do que você pode alterar."
            )
        else:
            new_role = st.selectbox(
                "Alterar perfil",
                options,
                index=options.index(target.role),
                format_func=lambda r: ROLE_LABELS.get(r, r),
                key=f"role_{target.id}",
            )
            if st.button("Salvar perfil", key=f"save_role_{target.id}"):
                if new_role == target.role:
                    st.info("Nenhuma alteração.")
                elif last_superadmin and new_role != ROLE_SUPERADMIN:
                    st.error(
                        "Este é o último superadministrador ativo. Promova "
                        "outro antes de rebaixá-lo, ou o sistema fica sem "
                        "ninguém capaz de administrá-lo."
                    )
                elif is_self and new_role != admin.role:
                    st.error(
                        "Você não pode alterar o próprio perfil. Peça a outro "
                        "administrador — isso impede que um clique errado "
                        "remova seu próprio acesso."
                    )
                else:
                    repository.set_user_role(target.id, new_role)
                    repository.revoke_all_sessions(target.id, "role_changed")
                    audit.record(
                        audit.USER_UPDATED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                        actor_email=admin.email, org_id=target.org_id,
                        target=target.email, ip=auth.client_ip(),
                        detail={"de": target.role, "para": new_role},
                    )
                    st.success("Perfil alterado. As sessões do usuário foram encerradas.")
                    st.rerun()

    # --- Situação ---
    with col2:
        st.markdown("**Situação da conta**")
        if target.is_active:
            if st.button("Desativar acesso", key=f"disable_{target.id}"):
                if is_self:
                    st.error("Você não pode desativar a própria conta.")
                elif last_superadmin:
                    st.error("Não é possível desativar o último superadministrador ativo.")
                else:
                    repository.set_user_status(target.id, STATUS_DISABLED)
                    revoked = repository.revoke_all_sessions(target.id, "user_disabled")
                    audit.record(
                        audit.USER_DISABLED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                        actor_email=admin.email, org_id=target.org_id,
                        target=target.email, ip=auth.client_ip(),
                        detail={"sessoes_encerradas": revoked},
                    )
                    st.success(f"Acesso desativado. {revoked} sessão(ões) encerrada(s).")
                    st.rerun()
        else:
            if st.button("Reativar acesso", key=f"enable_{target.id}"):
                repository.set_user_status(target.id, STATUS_ACTIVE)
                repository.unlock_user(target.id)
                audit.record(
                    audit.USER_ENABLED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                    actor_email=admin.email, org_id=target.org_id,
                    target=target.email, ip=auth.client_ip(),
                )
                st.success("Acesso reativado.")
                st.rerun()

    st.markdown("---")

    col3, col4, col5 = st.columns(3)

    with col3:
        if st.button("Redefinir senha", key=f"reset_{target.id}"):
            ok, temp = auth.admin_reset_password(admin, target)
            if ok:
                st.warning("Senha provisória — anote agora, não será exibida de novo:")
                st.code(temp, language=None)
                st.caption("O usuário terá de trocá-la no primeiro acesso.")

    with col4:
        if st.button("Encerrar sessões", key=f"revoke_{target.id}"):
            revoked = repository.revoke_all_sessions(target.id, "admin_revoked")
            audit.record(
                audit.SESSION_REVOKED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                actor_email=admin.email, org_id=target.org_id, target=target.email,
                ip=auth.client_ip(), detail={"quantidade": revoked},
            )
            st.success(f"{revoked} sessão(ões) encerrada(s) imediatamente.")

    with col5:
        if target.locked_until:
            if st.button("Desbloquear conta", key=f"unlock_{target.id}"):
                repository.unlock_user(target.id)
                ratelimit.clear_for_identity("login:acct", target.email)
                audit.record(
                    audit.USER_UPDATED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                    actor_email=admin.email, org_id=target.org_id,
                    target=target.email, ip=auth.client_ip(),
                    detail={"acao": "desbloqueio"},
                )
                st.success("Conta desbloqueada.")
                st.rerun()
        elif target.totp_enabled and st.button("Remover 2FA", key=f"untotp_{target.id}"):
            # Caminho de recuperação para quem perdeu o celular. Fica na
            # auditoria porque também é o caminho que um admin comprometido
            # usaria para derrubar o segundo fator de alguém.
            ok, message = auth.disable_totp(admin, target)
            st.success(message) if ok else st.error(message)
            st.rerun()

    with st.expander("Excluir usuário permanentemente"):
        st.caption(
            "A exclusão apaga a conta e o histórico de senhas. Os registros de "
            "auditoria são preservados. Na maioria dos casos, **desativar** é a "
            "opção correta: preserva a rastreabilidade de quem fez o quê."
        )
        confirmation = st.text_input(
            "Digite o e-mail do usuário para confirmar", key=f"del_confirm_{target.id}"
        )
        if st.button("Excluir definitivamente", key=f"delete_{target.id}"):
            if is_self:
                st.error("Você não pode excluir a própria conta.")
            elif last_superadmin:
                st.error("Não é possível excluir o último superadministrador ativo.")
            elif confirmation.strip().lower() != target.email.lower():
                st.error("O e-mail digitado não confere.")
            else:
                repository.revoke_all_sessions(target.id, "user_deleted")
                repository.delete_user(target.id)
                audit.record(
                    audit.USER_DELETED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                    actor_email=admin.email, org_id=target.org_id,
                    target=target.email, ip=auth.client_ip(),
                )
                st.success("Usuário excluído.")
                st.rerun()


# --------------------------------------------------------------------------
# Laboratórios
# --------------------------------------------------------------------------

def _render_orgs(admin: User) -> None:
    if not admin.has_permission(PERM_ORGS_MANAGE):
        st.error("Sem permissão.")
        return

    with st.expander("➕ Criar laboratório"):
        with st.form("create_org"):
            name = st.text_input("Nome do laboratório", max_chars=120)
            notes = st.text_input("Observações (opcional)", max_chars=200)
            submitted = st.form_submit_button("Criar", type="primary")
        if submitted:
            clean_name = clean_text(name, 120)
            if len(clean_name) < 2:
                st.error("Informe o nome do laboratório.")
            else:
                org = repository.create_organization(
                    clean_name, created_by=admin.id, notes=clean_text(notes, 200) or None
                )
                audit.record(
                    audit.ORG_CREATED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                    actor_email=admin.email, org_id=org.id, target=org.name,
                    ip=auth.client_ip(),
                )
                st.success(f"Laboratório **{org.name}** criado.")
                st.rerun()

    orgs = repository.list_organizations()
    if not orgs:
        st.info("Nenhum laboratório cadastrado.")
        return

    st.dataframe(
        pd.DataFrame([
            {
                "Laboratório": org.name,
                "Identificador": org.slug,
                "Situação": "Ativo" if org.is_active else "Suspenso",
                "Usuários": repository.count_users_in_org(org.id),
                "Criado em": org.created_at,
            }
            for org in orgs
        ]),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### Gerenciar laboratório")
    by_name = {org.name: org for org in orgs}
    target = by_name[st.selectbox("Selecione", list(by_name.keys()))]

    col1, col2 = st.columns(2)
    with col1:
        new_name = st.text_input("Renomear", value=target.name, max_chars=120,
                                 key=f"rename_{target.id}")
        if st.button("Salvar nome", key=f"save_name_{target.id}"):
            cleaned = clean_text(new_name, 120)
            if len(cleaned) < 2:
                st.error("Nome inválido.")
            else:
                repository.rename_organization(target.id, cleaned)
                audit.record(
                    audit.ORG_UPDATED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                    actor_email=admin.email, org_id=target.id, target=cleaned,
                    ip=auth.client_ip(),
                )
                st.success("Nome atualizado.")
                st.rerun()

    with col2:
        if target.is_active:
            st.caption(
                "Suspender bloqueia o login de todos os usuários deste "
                "laboratório e derruba as sessões abertas."
            )
            if st.button("Suspender laboratório", key=f"suspend_{target.id}"):
                if target.id == admin.org_id:
                    st.error("Você não pode suspender o próprio laboratório.")
                else:
                    repository.set_organization_status(target.id, STATUS_DISABLED)
                    affected = 0
                    for user in repository.list_users(target.id):
                        affected += repository.revoke_all_sessions(user.id, "org_suspended")
                    audit.record(
                        audit.ORG_UPDATED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                        actor_email=admin.email, org_id=target.id, target=target.name,
                        ip=auth.client_ip(),
                        detail={"acao": "suspensao", "sessoes_encerradas": affected},
                    )
                    st.success(f"Laboratório suspenso. {affected} sessão(ões) encerrada(s).")
                    st.rerun()
        else:
            if st.button("Reativar laboratório", key=f"resume_{target.id}"):
                repository.set_organization_status(target.id, STATUS_ACTIVE)
                audit.record(
                    audit.ORG_UPDATED, audit.OUTCOME_SUCCESS, actor_id=admin.id,
                    actor_email=admin.email, org_id=target.id, target=target.name,
                    ip=auth.client_ip(), detail={"acao": "reativacao"},
                )
                st.success("Laboratório reativado.")
                st.rerun()


# --------------------------------------------------------------------------
# Auditoria
# --------------------------------------------------------------------------

def _render_audit(admin: User) -> None:
    if admin.has_permission(PERM_AUDIT_VIEW_ALL):
        orgs = {"Todos": None}
        orgs.update({org.name: org.id for org in repository.list_organizations()})
        chosen = st.selectbox("Escopo", list(orgs.keys()))
        org_filter = orgs[chosen]
    elif admin.has_permission(PERM_AUDIT_VIEW_ORG):
        org_filter = admin.org_id
        st.caption("Exibindo apenas eventos do seu laboratório.")
    else:
        st.error("Sem permissão.")
        return

    col1, col2 = st.columns(2)
    with col1:
        action_filter = st.text_input(
            "Filtrar por ação (ex.: login.failure)", max_chars=60
        ).strip() or None
    with col2:
        limit = st.number_input("Quantidade", min_value=20, max_value=2000, value=200, step=20)

    entries = audit.recent(limit=int(limit), org_id=org_filter, action=action_filter)
    if not entries:
        st.info("Nenhum evento encontrado.")
        return

    st.dataframe(
        pd.DataFrame([
            {
                "Quando": row["ts"],
                "Quem": row.get("actor_email") or "—",
                "Ação": row["action"],
                "Alvo": row.get("target") or "—",
                "Resultado": row["outcome"],
                "Origem": row.get("ip") or "—",
                "Detalhe": row.get("detail") or "",
            }
            for row in entries
        ]),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("---")
    st.markdown("**Integridade da trilha**")
    st.caption(
        "Cada registro carrega o hash do anterior. Se alguém apagar ou editar "
        "uma linha diretamente no banco, a verificação aponta onde a cadeia "
        "foi rompida."
    )
    if st.button("Verificar integridade"):
        intact, broken_id, checked = audit.verify_chain()
        if intact:
            st.success(f"Cadeia íntegra. {checked} registro(s) verificado(s).")
        else:
            st.error(
                f"Cadeia rompida a partir do registro #{broken_id}. "
                f"{checked} registro(s) examinado(s). Isso indica alteração "
                "direta no banco — investigue os acessos ao Postgres."
            )


# --------------------------------------------------------------------------
# Segurança
# --------------------------------------------------------------------------

def _render_security(admin: User) -> None:
    if not admin.has_permission(PERM_SECURITY_SETTINGS):
        st.error("Sem permissão.")
        return

    cfg = get_config()
    numbers = repository.stats()

    cols = st.columns(4)
    cols[0].metric("Laboratórios", numbers["orgs"])
    cols[1].metric("Usuários ativos", numbers["active_users"])
    cols[2].metric("Sessões abertas", numbers["active_sessions"])
    cols[3].metric("Contas bloqueadas", numbers["locked_users"])

    st.markdown("### Configuração vigente")
    ok, backend = healthcheck()
    st.markdown(
        f"- Banco: **{backend}** — {'conectado' if ok else 'INDISPONÍVEL'}\n"
        f"- Sessão: expira com **{cfg.session_idle_minutes} min** de inatividade, "
        f"máximo de **{cfg.session_absolute_hours} h**\n"
        f"- Revalidação da sessão: a cada **{cfg.session_revalidate_seconds} s**\n"
        f"- Login: **{cfg.max_login_attempts}** tentativas, bloqueio exponencial "
        f"a partir de **{cfg.lockout_minutes} min**\n"
        f"- Senha: mínimo de **{cfg.password_min_length}** caracteres, "
        f"histórico de **{cfg.password_history}**\n"
        f"- Upload: até **{cfg.max_upload_mb} MB**, descompactado até "
        f"**{cfg.max_uncompressed_mb} MB**\n"
        f"- Auditoria: retenção de **{cfg.audit_retention_days}** dias"
    )

    from .crypto import ARGON2_AVAILABLE

    if ARGON2_AVAILABLE:
        st.success("Hash de senha: Argon2id.")
    else:
        st.warning(
            "Hash de senha: PBKDF2-SHA256 (argon2-cffi não instalado). "
            "Funciona, mas é bem mais fraco contra ataque com GPU. "
            "Instale `argon2-cffi` — os hashes migram sozinhos no próximo login."
        )

    for warning in cfg.warnings():
        st.warning(warning)

    st.markdown("### Manutenção")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Limpar sessões expiradas"):
            removed = repository.purge_expired_sessions()
            st.success(f"{removed} registro(s) removido(s).")

    with col2:
        if st.button("Aplicar retenção da auditoria"):
            removed = audit.purge_expired()
            st.success(f"{removed} registro(s) removido(s).")

    with col3:
        if st.button("Esvaziar cache de dados"):
            tenancy.clear_caches()
            audit.record(
                "cache.cleared", audit.OUTCOME_SUCCESS, actor_id=admin.id,
                actor_email=admin.email, org_id=admin.org_id, ip=auth.client_ip(),
            )
            st.success(
                "Cache esvaziado. Nenhum dado clínico permanece na memória "
                "compartilhada do processo."
            )
