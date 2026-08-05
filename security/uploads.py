# -*- coding: utf-8 -*-
"""
Validação de arquivos enviados.

O app aceita ``.csv``, ``.xlsx``, ``.xls`` e ``.zip``, confiando apenas na
extensão do nome. Três problemas concretos com isso:

**1. Zip bomb.** O código atual chama ``z.read(nome)`` direto, sem olhar o
tamanho descompactado. Um ZIP de 40 KB pode expandir para dezenas de GB e
derrubar o processo — que no Streamlit Community Cloud é *compartilhado por
todos os laboratórios*. Não é travar a própria sessão: é negação de serviço
para todo mundo. Aqui o tamanho declarado, o total acumulado e a razão de
compressão são verificados **antes** de qualquer leitura.

**2. Extensão não é tipo.** ``.csv`` no nome não diz nada sobre o conteúdo. A
verificação de assinatura (*magic bytes*) confere se o arquivo é mesmo do tipo
que diz ser, antes de entregá-lo ao openpyxl ou ao pandas.

**3. Nome de entrada do ZIP.** Nomes com ``../`` ou caminho absoluto só são
perigosos ao extrair para disco, mas a extensão do nome vira sufixo de arquivo
temporário no código atual. Nomes assim são recusados de saída — nenhum caso
legítimo depende deles.
"""

from __future__ import annotations

import contextlib
import os
import zipfile
from dataclasses import dataclass
from typing import Optional

from .config import get_config
from .sanitize import safe_filename

_ZIP_MAGIC = b"PK\x03\x04"
_OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"  # .xls antigo

ALLOWED_EXTENSIONS = (".csv", ".xlsx", ".xls", ".zip")

# Teto de entradas dentro do ZIP. Milhares de arquivos minúsculos são a outra
# forma de zip bomb — a que passa por qualquer verificação de tamanho total.
_MAX_ZIP_ENTRIES = 50


@dataclass(frozen=True)
class UploadCheck:
    ok: bool
    reason: str = ""
    safe_name: str = ""
    size_bytes: int = 0
    detail: Optional[dict] = None


def _extension(name: str) -> str:
    return os.path.splitext(str(name or "").lower())[1]


def validate_upload(uploaded_file) -> UploadCheck:
    """
    Valida um ``UploadedFile`` do Streamlit antes de qualquer processamento.

    Retorna :class:`UploadCheck` — o chamador deve interromper quando
    ``ok`` for falso e mostrar ``reason``, que é texto seguro para a tela.
    """
    cfg = get_config()

    if uploaded_file is None:
        return UploadCheck(False, "Nenhum arquivo enviado.")

    raw_name = getattr(uploaded_file, "name", "") or ""
    safe_name = safe_filename(raw_name, default="planilha")
    extension = _extension(raw_name)

    if extension not in ALLOWED_EXTENSIONS:
        return UploadCheck(
            False,
            f"Formato não aceito. Use {', '.join(ALLOWED_EXTENSIONS)}.",
            safe_name,
        )

    size = int(getattr(uploaded_file, "size", 0) or 0)
    if size <= 0:
        return UploadCheck(False, "Arquivo vazio.", safe_name)
    if size > cfg.max_upload_bytes:
        return UploadCheck(
            False,
            f"Arquivo de {size / 1024 / 1024:.1f} MB excede o limite de {cfg.max_upload_mb} MB.",
            safe_name,
            size,
        )

    try:
        position = uploaded_file.tell()
    except Exception:
        position = 0

    try:
        uploaded_file.seek(0)
        header = uploaded_file.read(8)
        uploaded_file.seek(0)

        signature_check = _check_signature(extension, header)
        if not signature_check.ok:
            return UploadCheck(signature_check.ok, signature_check.reason, safe_name, size)

        if extension == ".zip":
            zip_check = _inspect_zip(uploaded_file, cfg)
            if not zip_check.ok:
                return UploadCheck(zip_check.ok, zip_check.reason, safe_name, size, zip_check.detail)
    except zipfile.BadZipFile:
        return UploadCheck(False, "Arquivo ZIP corrompido ou ilegível.", safe_name, size)
    except Exception:
        return UploadCheck(False, "Não foi possível ler o arquivo enviado.", safe_name, size)
    finally:
        with contextlib.suppress(Exception):
            uploaded_file.seek(position)

    return UploadCheck(True, "", safe_name, size, {"extension": extension})


def _check_signature(extension: str, header: bytes) -> UploadCheck:
    """Confere a assinatura do arquivo contra o que a extensão promete."""
    if extension in (".zip", ".xlsx"):
        if not header.startswith(_ZIP_MAGIC):
            return UploadCheck(
                False,
                "O conteúdo do arquivo não corresponde à extensão informada.",
            )
        return UploadCheck(True)

    if extension == ".xls":
        # .xls pode ser OLE2 legítimo ou um .xlsx renomeado, o que é comum e
        # inofensivo — o openpyxl/pandas resolve. O que não pode é ser outra coisa.
        if header.startswith(_OLE2_MAGIC) or header.startswith(_ZIP_MAGIC):
            return UploadCheck(True)
        return UploadCheck(
            False, "O conteúdo do arquivo não corresponde a uma planilha Excel."
        )

    if extension == ".csv":
        # CSV não tem assinatura. O que dá para afirmar é que um CSV não começa
        # com um contêiner binário — sinal de arquivo renomeado para escapar do
        # filtro de extensão.
        if header.startswith(_ZIP_MAGIC) or header.startswith(_OLE2_MAGIC):
            return UploadCheck(
                False,
                "O arquivo tem extensão .csv mas o conteúdo é binário. "
                "Envie o arquivo no formato correto.",
            )
        if b"\x00" in header:
            return UploadCheck(
                False, "O arquivo .csv contém bytes nulos e não é texto válido."
            )
        return UploadCheck(True)

    return UploadCheck(True)


def _inspect_zip(uploaded_file, cfg) -> UploadCheck:
    """
    Inspeciona o ZIP **sem descompactar**.

    Trabalha só sobre o índice central do arquivo. É o único ponto em que dá
    para recusar uma bomba de descompressão: depois de chamar ``read()`` a
    memória já foi alocada e o estrago já aconteceu.
    """
    uploaded_file.seek(0)
    with zipfile.ZipFile(uploaded_file) as archive:
        entries = [item for item in archive.infolist() if not item.is_dir()]
        entries = [item for item in entries if not item.filename.startswith("__MACOSX/")]

        if not entries:
            return UploadCheck(False, "O ZIP não contém arquivos.")
        if len(entries) > _MAX_ZIP_ENTRIES:
            return UploadCheck(
                False,
                f"O ZIP tem {len(entries)} arquivos; o limite é {_MAX_ZIP_ENTRIES}.",
            )

        total_uncompressed = 0
        for item in entries:
            name = item.filename

            if name.startswith("/") or ".." in name.replace("\\", "/").split("/"):
                return UploadCheck(
                    False, "O ZIP contém caminhos inválidos e foi recusado."
                )

            if _extension(name) == ".zip":
                return UploadCheck(
                    False, "ZIP aninhado não é aceito."
                )

            total_uncompressed += item.file_size

            if item.compress_size > 0:
                ratio = item.file_size / item.compress_size
                if ratio > cfg.max_compression_ratio and item.file_size > 5 * 1024 * 1024:
                    return UploadCheck(
                        False,
                        "Arquivo recusado: taxa de compressão anormal, "
                        "característica de bomba de descompressão.",
                        detail={"ratio": round(ratio, 1)},
                    )

        if total_uncompressed > cfg.max_uncompressed_bytes:
            return UploadCheck(
                False,
                f"O conteúdo descompactado teria "
                f"{total_uncompressed / 1024 / 1024:.0f} MB e excede o limite de "
                f"{cfg.max_uncompressed_mb} MB.",
                detail={"uncompressed_mb": round(total_uncompressed / 1024 / 1024, 1)},
            )

        valid = [
            item.filename
            for item in entries
            if _extension(item.filename) in (".csv", ".xlsx", ".xls")
        ]
        if not valid:
            return UploadCheck(False, "O ZIP não contém CSV ou Excel válidos.")

    return UploadCheck(True, detail={"entries": len(entries)})


@contextlib.contextmanager
def secure_tempfile(suffix: str = ""):
    """
    Arquivo temporário que **sempre** é apagado.

    O ``load_dataframe`` atual usa ``NamedTemporaryFile(delete=False)`` e chama
    ``os.remove`` no caminho feliz. Quando a leitura falha no meio — planilha
    corrompida, coluna estranha, memória insuficiente — a exceção pula o
    ``remove`` e a planilha clínica fica no disco do servidor por tempo
    indeterminado. Este ``finally`` fecha esse caminho.

    O sufixo é higienizado porque hoje vem de ``os.path.splitext`` do nome
    enviado pelo usuário.
    """
    import tempfile

    clean_suffix = ""
    if suffix:
        clean_suffix = "." + "".join(
            ch for ch in str(suffix).lstrip(".") if ch.isalnum()
        )[:10]

    fd, path = tempfile.mkstemp(suffix=clean_suffix)
    os.close(fd)
    # mkstemp já cria com 0600, mas deixamos explícito: em host compartilhado
    # é isto que impede outro processo de ler a planilha clínica.
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)
    try:
        yield path
    finally:
        with contextlib.suppress(OSError):
            if os.path.exists(path):
                os.remove(path)
