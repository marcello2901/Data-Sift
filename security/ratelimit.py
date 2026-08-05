# -*- coding: utf-8 -*-
"""
Limites de taxa e bloqueio progressivo.

No Streamlit Community Cloud não existe Nginx, Cloudflare ou API Gateway na
sua frente para segurar abuso — se o limite não estiver no Python, ele não
existe. Por isso este módulo é obrigatório, não um extra.

O estado vive no banco, então sobrevive ao restart do container e continua
valendo se um dia o app rodar com mais de uma réplica. Se o banco cair, há um
espelho em memória do processo: perde-se o compartilhamento entre réplicas,
mas o atacante continua limitado. Falhar *aberto* aqui anularia justamente a
proteção que mais importa durante um incidente.

Duas dimensões são contadas em paralelo no login: **por conta** (impede força
bruta contra um alvo) e **por IP** (impede varredura de senha comum contra
muitas contas — o ataque que a contagem por conta não enxerga).
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from .config import get_config
from .db import connect, execute, is_past, now_iso, parse_iso, query_one, utcnow

_memory_lock = threading.Lock()
_memory_buckets: dict[str, dict] = {}

# Teto do bloqueio exponencial. Sem teto, poucos erros seguidos trancariam a
# conta por semanas e o suporte viraria o verdadeiro vetor de ataque.
_MAX_BLOCK = timedelta(hours=12)


@dataclass(frozen=True)
class RateDecision:
    allowed: bool
    retry_after_seconds: int = 0
    remaining: int = 0

    @property
    def retry_after_human(self) -> str:
        seconds = max(0, int(self.retry_after_seconds))
        if seconds < 60:
            return f"{seconds} segundo{'s' if seconds != 1 else ''}"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes} minuto{'s' if minutes != 1 else ''}"
        hours = minutes // 60
        return f"{hours} hora{'s' if hours != 1 else ''}"


def bucket_key(scope: str, identity: str) -> str:
    """
    Monta a chave do balde.

    A identidade é hasheada: e-mails e IPs são dado pessoal sob LGPD e não
    precisam estar legíveis para que a contagem funcione.
    """
    digest = hashlib.sha256(str(identity or "").strip().lower().encode("utf-8")).hexdigest()[:32]
    return f"{scope}:{digest}"


def _block_duration(strikes: int) -> timedelta:
    """Bloqueio dobra a cada rodada de excessos: 5min, 10, 20, 40... até o teto."""
    base_minutes = max(1, get_config().lockout_minutes)
    duration = timedelta(minutes=base_minutes * (2 ** max(0, strikes - 1)))
    return min(duration, _MAX_BLOCK)


def consume(bucket: str, limit: int, window_seconds: int) -> RateDecision:
    """
    Contabiliza uma tentativa e diz se ela pode prosseguir.

    Janela fixa: dentro de ``window_seconds`` são permitidas ``limit``
    tentativas. Estourou, entra em bloqueio exponencial. Chame **antes** de
    executar a operação cara.
    """
    try:
        return _consume_db(bucket, limit, window_seconds)
    except Exception:
        return _consume_memory(bucket, limit, window_seconds)


def _consume_db(bucket: str, limit: int, window_seconds: int) -> RateDecision:
    with connect() as conn:
        row = query_one(
            conn,
            "SELECT bucket, counter, window_start, blocked_until, strikes "
            "FROM rate_limits WHERE bucket = ?",
            (bucket,),
        )
        now = utcnow()

        if row and row.get("blocked_until") and not is_past(row["blocked_until"]):
            blocked_until = parse_iso(row["blocked_until"])
            retry = int((blocked_until - now).total_seconds()) if blocked_until else window_seconds
            return RateDecision(False, max(1, retry), 0)

        window_start = parse_iso(row["window_start"]) if row else None
        window_expired = (
            window_start is None
            or (now - window_start).total_seconds() >= window_seconds
        )

        if row is None or window_expired:
            # Janela nova. Os strikes NÃO zeram aqui: eles medem o histórico de
            # abuso e são o que faz o bloqueio endurecer a cada reincidência.
            execute(
                conn,
                """
                INSERT INTO rate_limits (bucket, counter, window_start, blocked_until, strikes, updated_at)
                VALUES (?, 1, ?, NULL, ?, ?)
                ON CONFLICT (bucket) DO UPDATE SET
                    counter = 1, window_start = ?, blocked_until = NULL, updated_at = ?
                """,
                (bucket, now_iso(), (row or {}).get("strikes", 0), now_iso(), now_iso(), now_iso()),
            )
            return RateDecision(True, 0, max(0, limit - 1))

        counter = int(row.get("counter") or 0) + 1
        if counter > limit:
            strikes = int(row.get("strikes") or 0) + 1
            block_for = _block_duration(strikes)
            blocked_until = (now + block_for).isoformat(timespec="seconds")
            execute(
                conn,
                "UPDATE rate_limits SET counter = ?, blocked_until = ?, strikes = ?, updated_at = ? "
                "WHERE bucket = ?",
                (counter, blocked_until, strikes, now_iso(), bucket),
            )
            return RateDecision(False, int(block_for.total_seconds()), 0)

        execute(
            conn,
            "UPDATE rate_limits SET counter = ?, updated_at = ? WHERE bucket = ?",
            (counter, now_iso(), bucket),
        )
        return RateDecision(True, 0, max(0, limit - counter))


def _consume_memory(bucket: str, limit: int, window_seconds: int) -> RateDecision:
    """Espelho em memória, usado só quando o banco está indisponível."""
    now = utcnow()
    with _memory_lock:
        state = _memory_buckets.get(bucket)

        if state and state.get("blocked_until") and state["blocked_until"] > now:
            retry = int((state["blocked_until"] - now).total_seconds())
            return RateDecision(False, max(1, retry), 0)

        if state is None or (now - state["window_start"]).total_seconds() >= window_seconds:
            _memory_buckets[bucket] = {
                "counter": 1,
                "window_start": now,
                "blocked_until": None,
                "strikes": (state or {}).get("strikes", 0),
            }
            return RateDecision(True, 0, max(0, limit - 1))

        state["counter"] += 1
        if state["counter"] > limit:
            state["strikes"] += 1
            block_for = _block_duration(state["strikes"])
            state["blocked_until"] = now + block_for
            return RateDecision(False, int(block_for.total_seconds()), 0)

        return RateDecision(True, 0, max(0, limit - state["counter"]))


def peek(bucket: str) -> RateDecision:
    """Consulta o bloqueio sem contar tentativa. Útil para mostrar aviso na tela."""
    try:
        with connect() as conn:
            row = query_one(
                conn, "SELECT blocked_until FROM rate_limits WHERE bucket = ?", (bucket,)
            )
        if row and row.get("blocked_until") and not is_past(row["blocked_until"]):
            blocked_until = parse_iso(row["blocked_until"])
            retry = int((blocked_until - utcnow()).total_seconds()) if blocked_until else 60
            return RateDecision(False, max(1, retry), 0)
    except Exception:
        with _memory_lock:
            state = _memory_buckets.get(bucket)
        if state and state.get("blocked_until") and state["blocked_until"] > utcnow():
            retry = int((state["blocked_until"] - utcnow()).total_seconds())
            return RateDecision(False, max(1, retry), 0)
    return RateDecision(True, 0, 0)


def reset(bucket: str) -> None:
    """
    Zera o balde após sucesso legítimo.

    Zera inclusive os strikes: quem provou a identidade não deve continuar
    pagando pelas tentativas erradas anteriores.
    """
    try:
        with connect() as conn:
            execute(conn, "DELETE FROM rate_limits WHERE bucket = ?", (bucket,))
    except Exception:
        pass
    with _memory_lock:
        _memory_buckets.pop(bucket, None)


def clear_for_identity(scope: str, identity: str) -> None:
    """Libera manualmente (usado pelo admin ao destravar uma conta)."""
    reset(bucket_key(scope, identity))


# --------------------------------------------------------------------------
# Políticas prontas
# --------------------------------------------------------------------------

def check_login(email: str, ip: Optional[str]) -> RateDecision:
    """
    Limite do login nas duas dimensões.

    Por conta: ``max_login_attempts`` a cada 15 min.
    Por IP: 5x esse valor na mesma janela — folgado o bastante para um
    laboratório inteiro atrás de um NAT, apertado o bastante para barrar
    varredura automatizada.
    """
    cfg = get_config()
    decision = consume(bucket_key("login:acct", email), cfg.max_login_attempts, 900)
    if not decision.allowed:
        return decision
    if ip:
        return consume(bucket_key("login:ip", ip), cfg.max_login_attempts * 5, 900)
    return decision


def check_upload(user_id: str) -> RateDecision:
    """Uploads: 30 por 10 minutos por usuário."""
    return consume(bucket_key("upload:user", user_id), 30, 600)


def check_processing(user_id: str) -> RateDecision:
    """
    Processamento pesado (filtro, estratificação, Harris-Boyd): 60 por 5 min.

    Cada execução varre a planilha inteira; sem teto, uma aba deixada
    clicando derruba o desempenho de todos os outros laboratórios no mesmo
    processo Streamlit.
    """
    return consume(bucket_key("proc:user", user_id), 60, 300)


def check_export(user_id: str) -> RateDecision:
    """
    Exportação: 40 por 10 minutos.

    É o limite que mais interessa contra exfiltração — uma conta comprometida
    baixando a base inteira em lote esbarra aqui e aparece na auditoria.
    """
    return consume(bucket_key("export:user", user_id), 40, 600)


def check_password_change(user_id: str) -> RateDecision:
    """Trocas de senha: 5 por hora, para dificultar drenar o histórico de senhas."""
    return consume(bucket_key("pwchange:user", user_id), 5, 3600)


def check_admin_action(user_id: str) -> RateDecision:
    """Ações administrativas: 100 por 5 min, teto contra script com sessão roubada."""
    return consume(bucket_key("admin:user", user_id), 100, 300)
