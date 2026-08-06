# -*- coding: utf-8 -*-
"""
Primitivas criptográficas.

Decisões e o porquê de cada uma:

**Senha — Argon2id.** Vencedor da Password Hashing Competition e recomendação
atual do OWASP. É *memory-hard*: encarece ataque com GPU/ASIC, que é como
senhas vazadas são realmente quebradas. Se ``argon2-cffi`` não estiver
instalado, caímos para PBKDF2-HMAC-SHA256 com 600.000 iterações (piso do
OWASP para SHA-256) — funciona, mas é a opção fraca; o hash guarda qual
algoritmo foi usado e :func:`needs_rehash` promove a senha para Argon2id no
próximo login bem-sucedido, sem pedir nada ao usuário.

**Pimenta (pepper).** Antes do hash, a senha passa por HMAC-SHA256 com um
segredo que vive só no servidor (``DATASIFT_PEPPER``), nunca no banco. O sal
protege contra rainbow tables; a pimenta protege contra o cenário em que o
atacante leva o banco inteiro mas não o processo — sem ela, um dump do
Postgres já permite ataque offline. O marcador ``pep$`` no início do hash diz
se a pimenta foi aplicada, o que permite ligar ou desligar a pimenta sem
invalidar as senhas existentes.

**Tokens de sessão.** 256 bits de ``secrets.token_urlsafe``. No banco guardamos
só SHA-256 do token: quem ler a tabela ``sessions`` não consegue se passar por
ninguém. Como o token já é aleatório e de alta entropia, hash rápido é o
correto aqui — Argon2 só faz sentido contra segredos de baixa entropia.

**TOTP (RFC 6238)** implementado em stdlib, sem dependência nova, com proteção
a replay: o contador usado fica registrado e um código só vale uma vez.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import secrets
import struct
import time
import unicodedata
from typing import Optional, Tuple

from .config import get_config

# --- Parâmetros Argon2id ---------------------------------------------------
#
# O padrão continua m=64 MiB, t=3, p=2. Ele é seguro, mas é caro justamente na
# máquina em que este app roda: no Streamlit Community Cloud a CPU é
# compartilhada e a verificação chega a passar de um segundo, além de exigir um
# pico de 64 MiB num contêiner de 1 GB. Como o custo aceitável depende do
# servidor, os três parâmetros são configuráveis.
#
# A referência atual do OWASP (2024) para Argon2id é m=19 MiB, t=2, p=1 — ou
# seja, definir DATASIFT_ARGON2_MEMORY_KIB=19456, _TIME_COST=2 e
# _PARALLELISM=1 deixa o login perceptivelmente mais rápido e **continua**
# dentro da recomendação. Hashes antigos seguem válidos: o custo fica gravado
# em cada hash, e a biblioteca usa os parâmetros do próprio hash ao verificar.
def _param_int(nome: str, padrao: int) -> int:
    bruto = os.environ.get(f"DATASIFT_ARGON2_{nome}")
    try:
        valor = int(str(bruto).strip())
        return valor if valor > 0 else padrao
    except (TypeError, ValueError):
        return padrao


_ARGON2_TIME_COST = _param_int("TIME_COST", 3)
_ARGON2_MEMORY_KIB = _param_int("MEMORY_KIB", 65536)  # 64 MiB
_ARGON2_PARALLELISM = _param_int("PARALLELISM", 2)
_ARGON2_HASH_LEN = 32
_ARGON2_SALT_LEN = 16

_PBKDF2_ITERATIONS = 600_000
_PEPPER_MARKER = "pep$"

try:  # pragma: no cover - depende do ambiente
    from argon2 import PasswordHasher
    from argon2 import exceptions as argon2_exceptions
    from argon2.low_level import Type as _Argon2Type

    _HASHER = PasswordHasher(
        time_cost=_ARGON2_TIME_COST,
        memory_cost=_ARGON2_MEMORY_KIB,
        parallelism=_ARGON2_PARALLELISM,
        hash_len=_ARGON2_HASH_LEN,
        salt_len=_ARGON2_SALT_LEN,
        type=_Argon2Type.ID,
    )
    ARGON2_AVAILABLE = True
except Exception:  # pragma: no cover
    _HASHER = None
    argon2_exceptions = None
    ARGON2_AVAILABLE = False


# --------------------------------------------------------------------------
# Normalização e pimenta
# --------------------------------------------------------------------------

def _normalize_password(password: str) -> str:
    """
    Normaliza a senha em NFKC.

    Sem isso, um "ç" digitado como caractere único e o mesmo "ç" digitado como
    "c" + cedilha combinante produzem bytes diferentes e a senha correta é
    rejeitada dependendo do teclado ou sistema operacional. Como boa parte dos
    usuários é brasileira, isso não é hipotético.
    """
    return unicodedata.normalize("NFKC", password)


def _apply_pepper(password: str) -> Tuple[str, bool]:
    """Devolve (segredo_a_hashear, pimenta_aplicada)."""
    pepper = get_config().pepper
    normalized = _normalize_password(password)
    if not pepper:
        return normalized, False
    digest = hmac.new(pepper.encode("utf-8"), normalized.encode("utf-8"), hashlib.sha256)
    return digest.hexdigest(), True


# --------------------------------------------------------------------------
# Hash de senha
# --------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """Gera o hash de armazenamento da senha."""
    secret, peppered = _apply_pepper(password)

    if ARGON2_AVAILABLE:
        stored = _HASHER.hash(secret)
    else:
        salt = os.urandom(16)
        derived = hashlib.pbkdf2_hmac(
            "sha256", secret.encode("utf-8"), salt, _PBKDF2_ITERATIONS
        )
        stored = "pbkdf2_sha256${}${}${}".format(
            _PBKDF2_ITERATIONS,
            base64.b64encode(salt).decode("ascii"),
            base64.b64encode(derived).decode("ascii"),
        )

    return (_PEPPER_MARKER + stored) if peppered else stored


def verify_password(password: str, stored_hash: str) -> bool:
    """
    Confere a senha contra o hash guardado.

    Nunca levanta exceção por hash malformado: um registro corrompido deve
    negar o acesso, não derrubar a tela de login.
    """
    if not stored_hash or not password:
        return False

    was_peppered = stored_hash.startswith(_PEPPER_MARKER)
    payload = stored_hash[len(_PEPPER_MARKER):] if was_peppered else stored_hash

    normalized = _normalize_password(password)
    pepper = get_config().pepper
    if was_peppered:
        if not pepper:
            # A senha foi criada com pimenta e o segredo sumiu da configuração.
            # Não há como validar; negar é a única resposta correta.
            return False
        secret = hmac.new(
            pepper.encode("utf-8"), normalized.encode("utf-8"), hashlib.sha256
        ).hexdigest()
    else:
        secret = normalized

    try:
        if payload.startswith("$argon2"):
            if not ARGON2_AVAILABLE:
                return False
            try:
                _HASHER.verify(payload, secret)
                return True
            except argon2_exceptions.VerificationError:
                return False
            except argon2_exceptions.InvalidHashError:
                return False

        if payload.startswith("pbkdf2_sha256$"):
            _, iterations, salt_b64, hash_b64 = payload.split("$", 3)
            derived = hashlib.pbkdf2_hmac(
                "sha256",
                secret.encode("utf-8"),
                base64.b64decode(salt_b64),
                int(iterations),
            )
            return hmac.compare_digest(derived, base64.b64decode(hash_b64))
    except Exception:
        return False

    return False


def needs_rehash(stored_hash: str) -> bool:
    """
    Diz se o hash deve ser regravado no próximo login bem-sucedido.

    Cobre três casos: PBKDF2 antigo com Argon2 agora disponível, parâmetros do
    Argon2 endurecidos desde que a senha foi criada, e pimenta ligada depois
    que o usuário já existia.
    """
    if not stored_hash:
        return True

    was_peppered = stored_hash.startswith(_PEPPER_MARKER)
    payload = stored_hash[len(_PEPPER_MARKER):] if was_peppered else stored_hash

    if bool(get_config().pepper) != was_peppered:
        return True

    if payload.startswith("pbkdf2_sha256$"):
        return ARGON2_AVAILABLE

    if payload.startswith("$argon2") and ARGON2_AVAILABLE:
        try:
            return _HASHER.check_needs_rehash(payload)
        except Exception:
            return False

    return False


# --------------------------------------------------------------------------
# Tokens opacos
# --------------------------------------------------------------------------

def generate_token(nbytes: int = 32) -> str:
    """Token aleatório de 256 bits, seguro para uso como credencial."""
    return secrets.token_urlsafe(nbytes)


def hash_token(token: str) -> str:
    """SHA-256 hex do token — é isto que vai para o banco, nunca o token."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def constant_time_eq(a: str, b: str) -> bool:
    """Comparação sem vazamento de tempo."""
    return hmac.compare_digest(str(a), str(b))


def generate_temp_password(length: int = 16) -> str:
    """
    Senha provisória legível para o admin repassar.

    Evita caracteres ambíguos (0/O, 1/l/I) porque essa senha é lida em voz alta
    ou copiada à mão, e garante ao menos um de cada classe para nunca nascer
    reprovada pela própria política de senha.
    """
    lowers = "abcdefghijkmnopqrstuvwxyz"
    uppers = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    digits = "23456789"
    symbols = "!@#$%&*?-_"
    alphabet = lowers + uppers + digits + symbols

    while True:
        chars = [
            secrets.choice(lowers),
            secrets.choice(uppers),
            secrets.choice(digits),
            secrets.choice(symbols),
        ]
        chars += [secrets.choice(alphabet) for _ in range(max(0, length - 4))]
        secrets.SystemRandom().shuffle(chars)
        candidate = "".join(chars)
        if len(candidate) >= 8:
            return candidate


# --------------------------------------------------------------------------
# Cifra simétrica para o segredo TOTP em repouso
# --------------------------------------------------------------------------

def _fernet():
    """
    Fernet com chave derivada da pimenta via HKDF-SHA256.

    Devolve ``None`` quando não há pimenta ou a lib ``cryptography`` não está
    instalada — nesse caso o segredo TOTP é gravado em claro e o chamador
    marca isso explicitamente, em vez de fingir que há proteção.
    """
    pepper = get_config().pepper
    if not pepper:
        return None
    try:
        from cryptography.fernet import Fernet
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    except Exception:
        return None

    key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"datasift.totp.v1",
        info=b"totp-secret-encryption",
    ).derive(pepper.encode("utf-8"))
    return Fernet(base64.urlsafe_b64encode(key))


def encrypt_secret(plaintext: str) -> str:
    """Cifra um segredo para repouso. Prefixo declara o estado real."""
    box = _fernet()
    if box is None:
        return "plain:" + plaintext
    return "enc1:" + box.encrypt(plaintext.encode("utf-8")).decode("ascii")


def decrypt_secret(stored: str) -> Optional[str]:
    """Inverso de :func:`encrypt_secret`; ``None`` se não for possível abrir."""
    if not stored:
        return None
    if stored.startswith("plain:"):
        return stored[len("plain:"):]
    if stored.startswith("enc1:"):
        box = _fernet()
        if box is None:
            return None
        try:
            return box.decrypt(stored[len("enc1:"):].encode("ascii")).decode("utf-8")
        except Exception:
            return None
    return None


# --------------------------------------------------------------------------
# TOTP (RFC 6238)
# --------------------------------------------------------------------------

_TOTP_PERIOD = 30
_TOTP_DIGITS = 6


def generate_totp_secret() -> str:
    """Segredo base32 de 160 bits, o tamanho que os apps autenticadores esperam."""
    return base64.b32encode(os.urandom(20)).decode("ascii").rstrip("=")


def totp_code(secret_b32: str, at: Optional[float] = None) -> str:
    """Código TOTP para o instante ``at`` (default: agora)."""
    padding = "=" * (-len(secret_b32) % 8)
    key = base64.b32decode(secret_b32 + padding, casefold=True)
    counter = int((at if at is not None else time.time()) // _TOTP_PERIOD)
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    truncated = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(truncated % (10 ** _TOTP_DIGITS)).zfill(_TOTP_DIGITS)


def verify_totp(
    secret_b32: str,
    code: str,
    last_counter: Optional[int] = None,
    window: int = 1,
) -> Tuple[bool, Optional[int]]:
    """
    Valida um código TOTP.

    ``window=1`` aceita o passo anterior e o seguinte (±30s), tolerando relógio
    dessincronizado sem alargar demais a janela de ataque.

    ``last_counter`` é o maior contador já aceito para este usuário. Códigos de
    contador igual ou menor são recusados mesmo estando corretos — sem isso, um
    código observado por cima do ombro (ou capturado em log) continua válido por
    até 90 segundos e pode ser reapresentado.

    Retorna ``(válido, contador_usado)`` para o chamador persistir o contador.
    """
    code = re.sub(r"\D", "", str(code or ""))
    if len(code) != _TOTP_DIGITS or not secret_b32:
        return False, None

    now = int(time.time() // _TOTP_PERIOD)
    for drift in range(-window, window + 1):
        counter = now + drift
        if last_counter is not None and counter <= last_counter:
            continue
        candidate = totp_code(secret_b32, at=counter * _TOTP_PERIOD)
        if hmac.compare_digest(candidate, code):
            return True, counter
    return False, None


def totp_provisioning_uri(secret_b32: str, account: str, issuer: str = "DataSift") -> str:
    """URI ``otpauth://`` para cadastrar no Google Authenticator, Authy, etc."""
    from urllib.parse import quote

    label = quote(f"{issuer}:{account}", safe="")
    return (
        f"otpauth://totp/{label}"
        f"?secret={secret_b32}"
        f"&issuer={quote(issuer, safe='')}"
        f"&algorithm=SHA1&digits={_TOTP_DIGITS}&period={_TOTP_PERIOD}"
    )
