# -*- coding: utf-8 -*-
"""
Sanitização de entrada.

O DataSift monta SQL para o DuckDB por concatenação de texto e injeta HTML com
``unsafe_allow_html=True``. Nos dois casos entra conteúdo que vem de arquivo
enviado pelo usuário — inclusive **os nomes das colunas da planilha**, que são
tratados como se fossem confiáveis por serem "estrutura".

Não são. O cabeçalho de um CSV é texto arbitrário escolhido por quem monta o
arquivo. Uma coluna chamada::

    Idade" OR 1=1 --

escapa das aspas em ``f'"{col}"'`` e reescreve a cláusula ``WHERE``. Como o
DuckDB roda em processo, com acesso a disco, isso é execução de consulta
arbitrária no servidor, não só um filtro errado.

A defesa aqui é em duas camadas, e as duas são necessárias:

1. **Lista de permissão** — o identificador precisa existir de fato nas
   colunas do DataFrame carregado (:func:`resolve_column`). É a barreira forte.
2. **Escape correto** — mesmo já validado, o identificador é citado segundo a
   regra do SQL (:func:`quote_identifier`). É a rede de proteção para o caso
   de alguém, no futuro, esquecer a camada 1.
"""

from __future__ import annotations

import html
import os
import re
import unicodedata
import uuid
from typing import Any, Iterable, Optional

# Operadores aceitos na montagem de SQL. Lista fechada: qualquer coisa fora
# dela vira ``FALSE``, nunca é repassada ao motor. O código original fazia
# ``OPERATOR_MAP.get(op, op)``, que devolve intacto o que não conhece — ou
# seja, repassava texto arbitrário para dentro da consulta.
_SQL_OPERATORS = {
    "=": "=",
    "==": "=",
    "is equal to": "=",
    "Is equal to": "=",
    "!=": "!=",
    "<>": "!=",
    "Not equal to": "!=",
    "Is not equal to": "!=",
    ">": ">",
    "<": "<",
    ">=": ">=",
    "<=": "<=",
    "≥": ">=",
    "≤": "<=",
}

# Conectores lógicos aceitos entre duas condições.
_LOGICAL_OPERATORS = {"AND": "AND", "OR": "OR", "BETWEEN": "BETWEEN"}

_MAX_IDENTIFIER_LEN = 200
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class SanitizationError(ValueError):
    """Entrada rejeitada por não passar na validação."""


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------

def quote_identifier(name: str) -> str:
    """
    Cita um identificador SQL com escape correto.

    Em SQL padrão (e no DuckDB), aspas duplas dentro de um identificador citado
    são escapadas duplicando-as. ``ab"c`` vira ``"ab""c"``. É essa duplicação
    que o código original não fazia.

    O byte NUL é rejeitado em vez de escapado: ele trunca strings em C e faz o
    parser enxergar algo diferente do que a validação em Python examinou.
    """
    text = str(name or "")
    if "\x00" in text:
        raise SanitizationError("Identificador contém byte nulo.")
    if not text:
        raise SanitizationError("Identificador vazio.")
    if len(text) > _MAX_IDENTIFIER_LEN:
        raise SanitizationError("Identificador longo demais.")
    return '"' + text.replace('"', '""') + '"'


def resolve_column(name: Any, allowed_columns: Iterable[str]) -> Optional[str]:
    """
    Confere se ``name`` é mesmo uma coluna do DataFrame carregado.

    Esta é a barreira principal contra injeção: o identificador não é
    "limpo", é **conferido contra a lista real**. Devolve o nome exato como
    está no DataFrame, ou ``None`` se não existir — nunca uma variação
    aproximada, porque adivinhar aqui reintroduz o problema.
    """
    if name is None or allowed_columns is None:
        return None
    target = str(name)
    # list() direto, sem `or []`: um pandas Index levanta ValueError quando
    # avaliado como booleano ("truth value of an Index is ambiguous"), e é
    # exatamente isso que `df.columns` entrega aqui.
    columns = list(allowed_columns)
    if target in columns:
        return target
    # Comparação relaxada só para espaços nas pontas e caixa, que é o erro
    # humano comum ao digitar o nome de uma coluna.
    folded = target.strip().casefold()
    for column in columns:
        if str(column).strip().casefold() == folded:
            return column
    return None


def safe_column_ref(name: Any, allowed_columns: Iterable[str]) -> Optional[str]:
    """Valida contra a lista e devolve o identificador já citado, pronto para o SQL."""
    resolved = resolve_column(name, allowed_columns)
    if resolved is None:
        return None
    try:
        return quote_identifier(resolved)
    except SanitizationError:
        return None


def normalize_operator(op: Any) -> Optional[str]:
    """Traduz o operador da interface; ``None`` se não estiver na lista."""
    if op is None:
        return None
    return _SQL_OPERATORS.get(str(op).strip())


def normalize_logical_operator(op: Any) -> Optional[str]:
    if op is None:
        return None
    return _LOGICAL_OPERATORS.get(str(op).strip().upper())


def quote_literal(value: Any) -> str:
    """
    Cita um literal de texto para SQL.

    Usada apenas onde o DuckDB recebe SQL montado sem parâmetro. Duplica a
    aspa simples e recusa o byte nulo, mesma lógica do identificador.
    """
    text = str(value if value is not None else "")
    if "\x00" in text:
        raise SanitizationError("Literal contém byte nulo.")
    return "'" + text.replace("'", "''") + "'"


def safe_number(value: Any) -> Optional[float]:
    """
    Converte para float aceitando vírgula decimal (padrão brasileiro).

    Recusa ``inf`` e ``nan``: ambos atravessam ``float()`` sem erro e produzem
    comparações SQL que nunca são verdadeiras, o que vira filtro silenciosamente
    quebrado em vez de erro visível.
    """
    if value is None or value == "":
        return None
    try:
        number = float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


# --------------------------------------------------------------------------
# HTML
# --------------------------------------------------------------------------

def escape_html(value: Any) -> str:
    """
    Escapa texto que vai para dentro de ``st.markdown(..., unsafe_allow_html=True)``.

    ``unsafe_allow_html`` desliga a proteção do Streamlit para o bloco inteiro.
    Qualquer valor vindo de planilha interpolado ali é XSS armazenado: quem
    monta o arquivo escolhe o HTML que roda no navegador de quem o abrir —
    incluindo o seu, ao revisar os dados de um cliente.
    """
    return html.escape(str(value if value is not None else ""), quote=True)


def escape_attr(value: Any) -> str:
    """
    Escapa texto destinado a atributo HTML (ex.: ``title`` dos ícones de ajuda).

    Além do escape padrão, remove quebras de linha, que encerram atributos em
    alguns parsers e permitiriam emendar um atributo novo, como ``onmouseover``.
    """
    return escape_html(value).replace("\n", " ").replace("\r", " ")


# --------------------------------------------------------------------------
# Texto e arquivos
# --------------------------------------------------------------------------

def clean_text(value: Any, max_length: int = 500) -> str:
    """
    Normaliza texto livre vindo de formulário.

    Remove caracteres de controle (que corrompem log e CSV), normaliza para
    NFKC — impedindo que dois nomes visualmente idênticos sejam tratados como
    distintos — e limita o tamanho.
    """
    text = unicodedata.normalize("NFKC", str(value if value is not None else ""))
    text = _CONTROL_CHARS.sub("", text)
    return text.strip()[:max_length]


def safe_filename(name: Any, default: str = "arquivo", max_length: int = 120) -> str:
    """
    Gera um nome de arquivo seguro para download.

    Descarta qualquer componente de diretório e recusa nomes reservados do
    Windows (``CON``, ``PRN``, ``NUL``, ``COM1``...), que causam
    comportamento estranho e às vezes travam a máquina de quem baixa. O nome
    também não pode começar com ponto nem hífen — invisível em Unix e
    interpretado como opção por ferramentas de linha de comando.
    """
    text = clean_text(name, max_length=max_length * 2)
    text = os.path.basename(text.replace("\\", "/"))
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
    text = text.strip(" .-")

    stem, dot, ext = text.rpartition(".")
    base = stem if dot else text
    if re.fullmatch(r"(?i)(con|prn|aux|nul|com[1-9]|lpt[1-9])", base):
        text = f"_{text}"

    text = text[:max_length]
    return text or default


def safe_email(value: Any) -> Optional[str]:
    """
    Valida formato de e-mail de forma conservadora.

    Deliberadamente mais restrita que a RFC 5322: o e-mail aqui é chave de
    login, não destino de mensagem. Recusar um endereço exótico e válido custa
    um cadastro manual; aceitar algo que normaliza de forma ambígua custa uma
    conta duplicada com o mesmo dono aparente.
    """
    text = clean_text(value, max_length=200).lower()
    if not text or len(text) > 200:
        return None
    if not re.fullmatch(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", text):
        return None
    if ".." in text or text.startswith(".") or "@." in text:
        return None
    return text


def redact_error(exc: BaseException) -> tuple[str, str]:
    """
    Transforma exceção em mensagem segura para a tela.

    Devolve ``(mensagem, id_de_correlação)``. O app hoje mostra
    ``f"SQL Processing Error: {e}"``, que entrega ao usuário a consulta
    montada, nomes de coluna e caminhos internos — material de reconhecimento
    para quem está sondando o sistema. O detalhe real vai para a auditoria,
    amarrado ao mesmo identificador que aparece na tela.
    """
    correlation_id = uuid.uuid4().hex[:12]
    return (
        "Não foi possível concluir a operação. Se o problema persistir, "
        f"informe o código {correlation_id} ao administrador.",
        correlation_id,
    )


def error_fingerprint(exc: BaseException, limit: int = 300) -> str:
    """Resumo da exceção para o log — tipo e mensagem truncada, sem stack trace."""
    return clean_text(f"{type(exc).__name__}: {exc}", max_length=limit)
