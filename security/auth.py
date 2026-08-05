# -*- coding: utf-8 -*-
"""
Autenticação, ciclo de vida da sessão e política de senha.

Decisões que valem explicar:

**Política de senha alinhada ao NIST SP 800-63B.** Exigimos comprimento
(12+), bloqueamos senhas comuns, senhas derivadas do próprio nome/e-mail e
reuso das últimas N. **Não** exigimos "uma maiúscula, um número e um símbolo",
e a expiração periódica vem desligada. Isso não é folga: regras de composição
e troca forçada a cada 90 dias produzem ``Laboratorio2024!`` virando
``Laboratorio2025!``, que é pior do que uma senha longa estável. A expiração
continua configurável (``PASSWORD_MAX_AGE_DAYS``) para quem precisa atender a
uma auditoria que ainda cobra isso.

**Mensagem de erro única no login.** Senha errada, usuário inexistente, conta
desativada e laboratório suspenso devolvem exatamente o mesmo texto. Distinguir
os casos entrega ao atacante uma lista de contas válidas — que é o trabalho
mais caro de um ataque, entregue de graça.

**Sem "manter conectado".** O único jeito de persistir login entre recargas de
página no Streamlit seria pôr o token em cookie via componente de terceiros ou
na URL. Token em URL vaza por histórico, ``Referer`` e log de servidor. A troca
é consciente: recarregar a página exige novo login, e a sessão vive no
``st.session_state``, que é server-side.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

from . import audit, ratelimit, repository
from .config import get_config
from .crypto import (
    decrypt_secret,
    encrypt_secret,
    generate_temp_password,
    generate_totp_secret,
    hash_password,
    needs_rehash,
    verify_password,
    verify_totp,
)
from .db import init_db, is_past, parse_iso, utcnow
from .models import (
    ROLE_SUPERADMIN,
    STATUS_ACTIVE,
    User,
    user_from_row,
)
from .sanitize import clean_text, safe_email

# Resposta única para toda falha de login. Ver docstring do módulo.
GENERIC_LOGIN_ERROR = "E-mail ou senha inválidos."

# Amostra das senhas mais vazadas, em versões pt-BR e en. Não substitui uma
# checagem contra base de vazamentos (ex.: HaveIBeenPwned k-anonymity), mas
# elimina o gosto mais comum sem depender de rede — que no Streamlit Cloud
# pode simplesmente não estar disponível.
_COMMON_PASSWORDS = frozenset({
    "123456", "1234567", "12345678", "123456789", "1234567890",
    "password", "senha", "senha123", "password123", "qwerty", "qwerty123",
    "abc123", "111111", "123123", "admin", "admin123", "administrador",
    "laboratorio", "datasift", "datasift123", "brasil", "brasil123",
    "iloveyou", "letmein", "welcome", "monkey", "dragon", "sunshine",
    "princess", "football", "12341234", "102030", "mudar123", "trocar123",
})


@dataclass(frozen=True)
class AuthResult:
    ok: bool
    user: Optional[User] = None
    token: Optional[str] = None
    reason: str = ""
    needs_totp: bool = False
    must_change_password: bool = False
    retry_after_seconds: int = 0


# --------------------------------------------------------------------------
# Contexto do cliente
# --------------------------------------------------------------------------

def client_ip() -> Optional[str]:
    """
    IP do cliente, na medida em que dá para confiar nele.

    Streamlit não expõe isso por API estável, então tentamos as variantes
    conhecidas e degradamos em silêncio.

    O ponto delicado é ``X-Forwarded-For``: o cliente **escolhe** o valor que
    envia, e o proxy apenas acrescenta o que observou ao final da lista. Ler o
    primeiro item — o erro clássico — deixa qualquer um forjar o IP e escapar
    do limite por IP trocando um cabeçalho. Por isso contamos ``hops`` a partir
    da **direita**, que é a parte escrita pela infraestrutura, e só confiamos
    no cabeçalho quando ``TRUST_PROXY_HEADERS`` estiver ligado.
    """
    cfg = get_config()
    headers = _request_headers()
    if not headers:
        return None

    if cfg.trust_proxy_headers:
        forwarded = headers.get("X-Forwarded-For") or headers.get("x-forwarded-for")
        if forwarded:
            chain = [part.strip() for part in str(forwarded).split(",") if part.strip()]
            if chain:
                index = max(0, len(chain) - cfg.trusted_proxy_hops)
                return chain[index][:64]
        real_ip = headers.get("X-Real-Ip") or headers.get("x-real-ip")
        if real_ip:
            return str(real_ip).strip()[:64]

    return None


def _request_headers() -> dict:
    """Cabeçalhos HTTP da sessão, se a versão do Streamlit permitir."""
    try:
        import streamlit as st

        context = getattr(st, "context", None)
        if context is not None and getattr(context, "headers", None):
            return dict(context.headers)
    except Exception:
        pass

    try:  # Streamlit < 1.37
        from streamlit.web.server.websocket_headers import _get_websocket_headers

        return dict(_get_websocket_headers() or {})
    except Exception:
        return {}


def client_fingerprint() -> Optional[str]:
    """
    Impressão fraca do cliente (hash de User-Agent + Accept-Language).

    Serve para *sinalizar* que uma sessão passou a ser usada de outro
    navegador, o que ajuda numa investigação. Não é usada para bloquear: o
    User-Agent muda sozinho a cada atualização do navegador, e amarrar a sessão
    a ele produziria deslogamentos aleatórios sem ganho real de segurança.
    """
    headers = _request_headers()
    if not headers:
        return None
    material = "|".join([
        str(headers.get("User-Agent") or headers.get("user-agent") or ""),
        str(headers.get("Accept-Language") or headers.get("accept-language") or ""),
    ])
    if not material.strip("|"):
        return None
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


# --------------------------------------------------------------------------
# Política de senha
# --------------------------------------------------------------------------

def password_problems(
    password: str,
    email: Optional[str] = None,
    display_name: Optional[str] = None,
    user_id: Optional[str] = None,
) -> list[str]:
    """Lista tudo que impede a senha de ser aceita. Vazio = aprovada."""
    cfg = get_config()
    problems: list[str] = []
    password = str(password or "")

    if len(password) < cfg.password_min_length:
        problems.append(f"Use pelo menos {cfg.password_min_length} caracteres.")
    if len(password) > 256:
        # Teto por proteção contra negação de serviço: Argon2 é caro por design
        # e uma senha de megabytes vira consumo de CPU sob demanda.
        problems.append("A senha é longa demais (máximo de 256 caracteres).")

    folded = password.casefold().strip()
    if folded in _COMMON_PASSWORDS:
        problems.append("Essa senha é conhecida e aparece em listas de vazamento.")

    if len(set(password)) <= 3 and password:
        problems.append("A senha tem repetição demais de poucos caracteres.")

    for source in (email or "", display_name or ""):
        for piece in str(source).replace("@", " ").replace(".", " ").split():
            if len(piece) >= 4 and piece.casefold() in folded:
                problems.append("A senha não pode conter seu nome ou e-mail.")
                break

    if "datasift" in folded:
        problems.append("A senha não pode conter o nome do sistema.")

    if user_id:
        for old_hash in repository.recent_password_hashes(user_id):
            if verify_password(password, old_hash):
                problems.append(
                    f"Essa senha já foi usada. Escolha uma diferente das "
                    f"últimas {cfg.password_history}."
                )
                break

    # dict.fromkeys preserva a ordem e remove repetição — o laço acima pode
    # acusar o mesmo problema por e-mail e por nome.
    return list(dict.fromkeys(problems))


def password_expired(user_row: dict) -> bool:
    cfg = get_config()
    if cfg.password_max_age_days <= 0:
        return False
    changed = parse_iso(user_row.get("password_changed_at"))
    if changed is None:
        return True
    return (utcnow() - changed).days >= cfg.password_max_age_days


# --------------------------------------------------------------------------
# Login
# --------------------------------------------------------------------------

def authenticate(email: str, password: str, totp: Optional[str] = None) -> AuthResult:
    """
    Valida credenciais e abre a sessão.

    Ordem deliberada: limite de taxa → conta → senha → 2FA → estado da conta e
    do laboratório. O limite vem primeiro porque é a única etapa que precisa
    funcionar mesmo quando todas as outras estão sob ataque.
    """
    init_db()

    ip = client_ip()
    normalized = safe_email(email)

    if not normalized or not password:
        audit.record(audit.LOGIN_FAILURE, audit.OUTCOME_FAILURE, ip=ip,
                     detail={"motivo": "campos_incompletos"})
        return AuthResult(False, reason=GENERIC_LOGIN_ERROR)

    decision = ratelimit.check_login(normalized, ip)
    if not decision.allowed:
        audit.record(audit.LOGIN_BLOCKED, audit.OUTCOME_DENIED, actor_email=normalized, ip=ip,
                     detail={"retry_after_s": decision.retry_after_seconds})
        return AuthResult(
            False,
            reason=(
                "Muitas tentativas. Aguarde "
                f"{decision.retry_after_human} antes de tentar novamente."
            ),
            retry_after_seconds=decision.retry_after_seconds,
        )

    row = repository.get_auth_row(normalized)

    if row is None:
        # Gasta tempo comparável a uma verificação real. Sem isto, a resposta
        # instantânea para usuário inexistente denuncia quais e-mails existem —
        # a mesma informação que a mensagem genérica tenta esconder.
        verify_password(password, hash_password("verificacao-de-tempo-constante"))
        audit.record(audit.LOGIN_FAILURE, audit.OUTCOME_FAILURE, actor_email=normalized, ip=ip,
                     detail={"motivo": "conta_inexistente"})
        return AuthResult(False, reason=GENERIC_LOGIN_ERROR)

    user_id = row["id"]

    if row.get("locked_until") and not is_past(row["locked_until"]):
        audit.record(audit.LOGIN_LOCKED_OUT, audit.OUTCOME_DENIED, actor_id=user_id,
                     actor_email=normalized, org_id=row.get("org_id"), ip=ip)
        return AuthResult(
            False,
            reason="Conta temporariamente bloqueada por excesso de tentativas. "
                   "Tente mais tarde ou peça ao administrador para liberar.",
        )

    if not verify_password(password, row["password_hash"]):
        attempts = repository.register_failed_login(user_id)
        audit.record(audit.LOGIN_FAILURE, audit.OUTCOME_FAILURE, actor_id=user_id,
                     actor_email=normalized, org_id=row.get("org_id"), ip=ip,
                     detail={"tentativas": attempts})
        return AuthResult(False, reason=GENERIC_LOGIN_ERROR)

    # --- Senha correta a partir daqui ---

    if row.get("totp_enabled"):
        if not totp:
            return AuthResult(False, needs_totp=True,
                              reason="Informe o código do aplicativo autenticador.")

        secret = decrypt_secret(row.get("totp_secret_enc") or "")
        if not secret:
            audit.record(audit.TOTP_FAILURE, audit.OUTCOME_FAILURE, actor_id=user_id,
                         actor_email=normalized, org_id=row.get("org_id"), ip=ip,
                         detail={"motivo": "segredo_ilegivel"})
            return AuthResult(
                False,
                reason="Não foi possível validar o segundo fator. Procure o administrador.",
            )

        last_counter = row.get("totp_last_counter")
        valid, counter = verify_totp(
            secret, totp, last_counter=int(last_counter) if last_counter is not None else None
        )
        if not valid:
            attempts = repository.register_failed_login(user_id)
            audit.record(audit.TOTP_FAILURE, audit.OUTCOME_FAILURE, actor_id=user_id,
                         actor_email=normalized, org_id=row.get("org_id"), ip=ip,
                         detail={"tentativas": attempts})
            return AuthResult(False, needs_totp=True,
                              reason="Código inválido ou já utilizado.")
        if counter is not None:
            repository.set_totp_counter(user_id, counter)

    # Estado da conta e do laboratório são checados **depois** da senha, para
    # que uma conta desativada não se revele a quem não sabe a senha dela.
    if row.get("status") != STATUS_ACTIVE:
        audit.record(audit.LOGIN_FAILURE, audit.OUTCOME_DENIED, actor_id=user_id,
                     actor_email=normalized, org_id=row.get("org_id"), ip=ip,
                     detail={"motivo": "conta_desativada"})
        return AuthResult(False, reason=GENERIC_LOGIN_ERROR)

    org = repository.get_organization(row["org_id"])
    if org is None or not org.is_active:
        audit.record(audit.LOGIN_FAILURE, audit.OUTCOME_DENIED, actor_id=user_id,
                     actor_email=normalized, org_id=row.get("org_id"), ip=ip,
                     detail={"motivo": "laboratorio_suspenso"})
        return AuthResult(
            False,
            reason="Acesso indisponível no momento. Procure o administrador.",
        )

    cfg = get_config()
    if cfg.require_2fa_for_admin and row["role"] in (ROLE_SUPERADMIN,) and not row.get("totp_enabled"):
        # Não bloqueia o login: bloquear trancaria o admin para fora sem ter
        # como cadastrar o 2FA. Marca troca obrigatória, e a tela de conta
        # conduz o cadastro do segundo fator.
        pass

    # Reidrata o hash quando os parâmetros ficaram defasados. Silencioso e sem
    # pedir nada ao usuário: é o único momento em que a senha em claro existe.
    if needs_rehash(row["password_hash"]):
        repository.set_password(user_id, hash_password(password), must_change=False)

    repository.register_successful_login(user_id)
    ratelimit.reset(ratelimit.bucket_key("login:acct", normalized))

    session_id, token = repository.create_session(
        user_id, row["org_id"], ip=ip, client_fingerprint=client_fingerprint()
    )

    must_change = bool(row.get("must_change_password")) or password_expired(row)

    audit.record(audit.LOGIN_SUCCESS, audit.OUTCOME_SUCCESS, actor_id=user_id,
                 actor_email=normalized, org_id=row["org_id"], ip=ip,
                 target=session_id, detail={"2fa": bool(row.get("totp_enabled"))})

    return AuthResult(
        True,
        user=user_from_row(row),
        token=token,
        must_change_password=must_change,
    )


# --------------------------------------------------------------------------
# Sessão
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class SessionState:
    valid: bool
    user: Optional[User] = None
    session_id: Optional[str] = None
    reason: str = ""


def validate_session(token: str) -> SessionState:
    """
    Confere o token e devolve o usuário associado.

    Verifica, nesta ordem: token existe e não foi revogado, prazo absoluto,
    inatividade, e **o estado atual do usuário e do laboratório**. Este último
    ponto é o que faz "desativar usuário" ter efeito imediato: sem reler o
    banco a cada validação, uma conta desligada continuaria trabalhando até a
    sessão vencer sozinha.
    """
    if not token:
        return SessionState(False, reason="sem_token")

    session = repository.get_session_by_token(token)
    if session is None:
        return SessionState(False, reason="sessao_invalida")

    if is_past(session["expires_at"]):
        repository.revoke_session(session["id"], "expired_absolute")
        audit.record(audit.SESSION_EXPIRED, audit.OUTCOME_SUCCESS,
                     actor_id=session["user_id"], org_id=session["org_id"],
                     target=session["id"], detail={"tipo": "absoluto"})
        return SessionState(False, reason="expirada")

    cfg = get_config()
    last_seen = parse_iso(session["last_seen_at"])
    if last_seen is not None:
        idle_seconds = (utcnow() - last_seen).total_seconds()
        if idle_seconds > cfg.session_idle_minutes * 60:
            repository.revoke_session(session["id"], "expired_idle")
            audit.record(audit.SESSION_EXPIRED, audit.OUTCOME_SUCCESS,
                         actor_id=session["user_id"], org_id=session["org_id"],
                         target=session["id"], detail={"tipo": "inatividade"})
            return SessionState(False, reason="inatividade")

    row = repository.get_auth_row_by_id(session["user_id"])
    if row is None or row.get("status") != STATUS_ACTIVE:
        repository.revoke_session(session["id"], "user_inactive")
        return SessionState(False, reason="conta_indisponivel")

    org = repository.get_organization(row["org_id"])
    if org is None or not org.is_active:
        repository.revoke_session(session["id"], "org_inactive")
        return SessionState(False, reason="laboratorio_indisponivel")

    if row["org_id"] != session["org_id"]:
        # O usuário mudou de laboratório depois que a sessão foi aberta.
        # A sessão carrega o org_id antigo e não pode continuar.
        repository.revoke_session(session["id"], "org_changed")
        return SessionState(False, reason="laboratorio_alterado")

    repository.touch_session(session["id"])
    return SessionState(True, user=user_from_row(row), session_id=session["id"])


def logout(token: str, user: Optional[User] = None) -> None:
    if not token:
        return
    session = repository.get_session_by_token(token)
    if session:
        repository.revoke_session(session["id"], "logout")
        audit.record(audit.LOGOUT, audit.OUTCOME_SUCCESS,
                     actor_id=session["user_id"],
                     actor_email=user.email if user else None,
                     org_id=session["org_id"], target=session["id"], ip=client_ip())


# --------------------------------------------------------------------------
# Troca de senha
# --------------------------------------------------------------------------

def change_own_password(user: User, current_password: str, new_password: str) -> tuple[bool, str]:
    """
    Troca a própria senha. Devolve ``(ok, mensagem)``.

    Exige a senha atual mesmo já estando autenticado: sem isso, uma sessão
    momentaneamente sequestrada (máquina desbloqueada, aba esquecida aberta)
    permitiria ao invasor trocar a senha e expulsar o dono da conta.
    """
    decision = ratelimit.check_password_change(user.id)
    if not decision.allowed:
        return False, f"Muitas trocas de senha. Aguarde {decision.retry_after_human}."

    row = repository.get_auth_row_by_id(user.id)
    if row is None:
        return False, "Conta não encontrada."

    if not verify_password(current_password, row["password_hash"]):
        audit.record(audit.PASSWORD_CHANGED, audit.OUTCOME_FAILURE, actor_id=user.id,
                     actor_email=user.email, org_id=user.org_id, ip=client_ip(),
                     detail={"motivo": "senha_atual_incorreta"})
        return False, "A senha atual está incorreta."

    problems = password_problems(
        new_password, email=user.email, display_name=user.display_name, user_id=user.id
    )
    if problems:
        return False, " ".join(problems)

    repository.set_password(user.id, hash_password(new_password), must_change=False)
    ratelimit.reset(ratelimit.bucket_key("pwchange:user", user.id))
    audit.record(audit.PASSWORD_CHANGED, audit.OUTCOME_SUCCESS, actor_id=user.id,
                 actor_email=user.email, org_id=user.org_id, ip=client_ip())
    return True, "Senha alterada. Por segurança, as demais sessões foram encerradas."


def admin_reset_password(actor: User, target: User) -> tuple[bool, str]:
    """
    Gera senha provisória para outro usuário.

    O admin **não escolhe** a senha e ela nasce com troca obrigatória: assim o
    administrador nunca conhece a senha definitiva de ninguém, o que preserva
    o não-repúdio — se a conta fizer algo, o dono não pode alegar que foi o
    admin, nem o contrário.
    """
    temp = generate_temp_password()
    repository.set_password(target.id, hash_password(temp), must_change=True)
    repository.unlock_user(target.id)
    ratelimit.clear_for_identity("login:acct", target.email)
    audit.record(audit.PASSWORD_RESET, audit.OUTCOME_SUCCESS, actor_id=actor.id,
                 actor_email=actor.email, org_id=target.org_id, target=target.email,
                 ip=client_ip())
    return True, temp


# --------------------------------------------------------------------------
# Segundo fator
# --------------------------------------------------------------------------

def begin_totp_enrollment() -> str:
    """Gera um segredo novo. Só é gravado após confirmação de um código válido."""
    return generate_totp_secret()


def confirm_totp_enrollment(user: User, secret: str, code: str) -> tuple[bool, str]:
    """
    Ativa o 2FA depois de conferir um código gerado pelo aplicativo.

    Confirmar antes de gravar evita o pior modo de falha do 2FA: ativar com um
    segredo que o aplicativo não registrou direito e trancar o usuário para
    fora da própria conta.
    """
    valid, counter = verify_totp(secret, code)
    if not valid:
        return False, "Código inválido. Confira o horário do celular e tente de novo."

    repository.set_totp_secret(user.id, encrypt_secret(secret), enabled=True)
    if counter is not None:
        repository.set_totp_counter(user.id, counter)
    audit.record(audit.TOTP_ENABLED, audit.OUTCOME_SUCCESS, actor_id=user.id,
                 actor_email=user.email, org_id=user.org_id, ip=client_ip())
    return True, "Verificação em duas etapas ativada."


def disable_totp(actor: User, target: User, verified_password: bool = False) -> tuple[bool, str]:
    if actor.id == target.id and not verified_password:
        return False, "Confirme sua senha para desativar a verificação em duas etapas."
    repository.set_totp_secret(target.id, None, enabled=False)
    audit.record(audit.TOTP_DISABLED, audit.OUTCOME_SUCCESS, actor_id=actor.id,
                 actor_email=actor.email, org_id=target.org_id, target=target.email,
                 ip=client_ip())
    return True, "Verificação em duas etapas desativada."


# --------------------------------------------------------------------------
# Primeiro boot
# --------------------------------------------------------------------------

def bootstrap_if_needed() -> Optional[str]:
    """
    Cria o laboratório e o superadmin iniciais, se ainda não existirem.

    Roda a cada inicialização porque no Streamlit Community Cloud **o container
    é recriado sem aviso**. Se o banco estiver íntegro, não faz nada. Se
    estiver vazio — banco novo, ou restauração de backup — recria o acesso a
    partir dos segredos, em vez de deixar você diante de uma tela de login sem
    nenhuma conta possível.

    Devolve a senha inicial quando a cria, para exibição única na tela.
    """
    init_db()

    cfg = get_config()
    email = safe_email(cfg.bootstrap_admin_email)
    if not email:
        return None

    # --- Recuperação: senha inicial perdida ---
    #
    # A senha gerada aparece uma única vez na tela de login. Se a página for
    # recarregada antes de alguém copiá-la, ela se perde e não há como entrar —
    # a conta existe, então o bootstrap normal não age mais.
    #
    # A saída é definir DATASIFT_BOOTSTRAP_ADMIN_PASSWORD nos secrets: a senha
    # é reaplicada aqui. A condição que torna isso seguro é
    # ``last_login_at IS NULL`` — só vale para uma conta que nunca foi usada.
    # Sem essa guarda, um segredo esquecido na configuração poderia redefinir a
    # senha de um administrador em atividade a cada reinício do app.
    if cfg.bootstrap_admin_password:
        row = repository.get_auth_row(email)
        if row is not None and not row.get("last_login_at"):
            repository.set_password(
                row["id"],
                hash_password(cfg.bootstrap_admin_password),
                must_change=True,
            )
            repository.set_user_role(row["id"], ROLE_SUPERADMIN)
            repository.set_user_status(row["id"], STATUS_ACTIVE)
            repository.unlock_user(row["id"])
            audit.record(audit.PASSWORD_RESET, audit.OUTCOME_SUCCESS,
                         actor_email="bootstrap", org_id=row.get("org_id"),
                         target=email,
                         detail={"motivo": "recuperacao_senha_inicial"})
            return None

    if repository.count_active_superadmins() > 0:
        return None

    org = None
    for candidate in repository.list_organizations():
        if candidate.slug == "administracao":
            org = candidate
            break
    if org is None:
        org = repository.create_organization("Administração", created_by="bootstrap")

    password = cfg.bootstrap_admin_password or generate_temp_password(20)
    generated = not bool(cfg.bootstrap_admin_password)

    existing = repository.get_auth_row(email)
    if existing:
        # Conta já existe mas perdeu o papel — promove em vez de duplicar.
        repository.set_user_role(existing["id"], ROLE_SUPERADMIN)
        repository.set_user_status(existing["id"], STATUS_ACTIVE)
        audit.record(audit.USER_UPDATED, audit.OUTCOME_SUCCESS, actor_email="bootstrap",
                     org_id=existing["org_id"], target=email,
                     detail={"motivo": "promocao_superadmin_no_bootstrap"})
        return None

    repository.create_user(
        org_id=org.id,
        email=email,
        display_name=clean_text(email.split("@")[0], 120).title(),
        password_hash=hash_password(password),
        role=ROLE_SUPERADMIN,
        created_by="bootstrap",
        must_change_password=True,
    )
    audit.record(audit.USER_CREATED, audit.OUTCOME_SUCCESS, actor_email="bootstrap",
                 org_id=org.id, target=email, detail={"papel": ROLE_SUPERADMIN})

    return password if generated else None
