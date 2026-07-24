# -*- coding: utf-8 -*-
"""
Análise de Impacto dos Resultados — DataSift
============================================

Avalia o impacto de repetir um conjunto de amostras (no mínimo 3) de um mesmo
teste/equipamento. Para cada amostra o usuário informa o código de barras, o
Resultado 1 e o Resultado 2; a ferramenta usa o Erro Total Máximo (ETM) e o
Intervalo de Referência (IR) — puxados automaticamente da base de dados a partir
do nome do teste — para decidir se a diferença entre os dois resultados tem
impacto analítico (excede o ETM) e/ou clínico (muda a interpretação).

Base de dados (colunas): Teste, Equipamento, ETM, IR.

Página do app DataSift (pasta ``pages/``). Também roda de forma independente com
``streamlit run pages/2_Analise_de_Impacto.py``.
"""

import io
import os
import re

import numpy as np
import pandas as pd
import streamlit as st

# --------------------------------------------------------------------------- #
# Identidade visual (mesma paleta do DataSift)
# --------------------------------------------------------------------------- #
COLOR_PRIMARY = "#073B4C"
COLOR_SECONDARY = "#00E5FF"
COLOR_TERTIARY = "#118AB2"
COLOR_BG = "#F8F9FA"

try:
    st.set_page_config(page_title="DataSift · Impacto", page_icon="🎯", layout="wide")
except Exception:
    pass

st.markdown(
    f"""
    <style>
        .stApp {{ background-color: {COLOR_BG} !important; }}
        h1, h2, h3, h4 {{ color: {COLOR_PRIMARY} !important; font-weight: 800 !important; }}
        p, span, div[data-testid="stMarkdownContainer"], label {{ color: #212529 !important; }}
        div[data-testid="stMetricValue"] {{ color: {COLOR_PRIMARY} !important; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------------------------------- #
# Conformidade LGPD / GDPR — mesma tela do app.py; chave de sessão compartilhada
# --------------------------------------------------------------------------- #
GDPR_TERMS = """
This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, national ID numbers, health information, etc.), the processing of which is regulated by data protection laws like the General Data Protection Regulation (GDPR or LGPD).

It is your sole responsibility to ensure that all data used in this tool complies with applicable data protection regulations. We strongly recommend that you only use previously anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been properly handled and anonymized.
"""

if "lgpd_accepted" not in st.session_state:
    st.session_state.lgpd_accepted = False

if not st.session_state.lgpd_accepted:
    st.header("Terms of Use and Data Protection Compliance")
    st.markdown(GDPR_TERMS)
    accepted = st.checkbox("By checking this box, I confirm that the data provided is anonymized.")
    if st.button("Continue", type="primary", disabled=not accepted):
        st.session_state.lgpd_accepted = True
        st.rerun()
    st.stop()

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --------------------------------------------------------------------------- #
# Funções auxiliares
# --------------------------------------------------------------------------- #
def normalizar_serie_numerica(serie: pd.Series) -> pd.Series:
    """Converte texto/misto em float, aceitando vírgula ou ponto como decimal."""
    def _conv(x):
        if pd.isna(x):
            return np.nan
        s = str(x).strip()
        if s == "" or s.lower() in ("nan", "none", "na", "n/a", "-", "--", "."):
            return np.nan
        s = s.replace("\xa0", "").replace(" ", "")
        s = re.sub(r"[^0-9,.\-+]", "", s)
        if s in ("", "+", "-", ".", ","):
            return np.nan
        has_dot, has_comma = "." in s, "," in s
        if has_dot and has_comma:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif has_comma:
            s = s.replace(",", ".")
        if s.count(".") > 1:
            partes = s.split(".")
            s = "".join(partes[:-1]) + "." + partes[-1]
        try:
            return float(s)
        except ValueError:
            return np.nan
    return serie.apply(_conv)


def parse_ref_range(txt):
    """Extrai (limite_inferior, limite_superior) de um IR ('136 - 145', '< 190', '> 40')."""
    if pd.isna(txt):
        return (None, None)
    s = str(txt).strip()
    if s == "":
        return (None, None)
    low = s.lower()
    nums = re.findall(r"\d+(?:[.,]\d+)?", s)
    vals = [float(n.replace(",", ".")) for n in nums]
    if not vals:
        return (None, None)
    if ("<" in s or "≤" in s or "menor" in low or "até" in low or "up to" in low):
        return (None, vals[-1])
    if (">" in s or "≥" in s or "maior" in low or "acima" in low):
        return (vals[0], None)
    if len(vals) >= 2:
        return (min(vals[0], vals[1]), max(vals[0], vals[1]))
    return (None, None)


def classificar_ref(valor, limite_inf, limite_sup):
    """Classifica em Baixo / Normal / Alto; '—' se não houver intervalo."""
    if pd.isna(valor):
        return "—"
    inf_ok = limite_inf is not None and pd.notna(limite_inf)
    sup_ok = limite_sup is not None and pd.notna(limite_sup)
    if not inf_ok and not sup_ok:
        return "—"
    if inf_ok and valor < limite_inf:
        return "Baixo"
    if sup_ok and valor > limite_sup:
        return "Alto"
    return "Normal"


_COLS_ESPERADAS = ("Teste", "Equipamento", "ETM", "IR")


def _ler_tabela(conteudo: bytes, nome: str) -> pd.DataFrame:
    """
    Lê CSV ou Excel a partir dos bytes. Para CSV, testa combinações de
    separador/decimal/encoding e escolhe a primeira que traz as colunas
    esperadas — evita 'mojibake' (UTF-8 lido como latin-1) e separador errado.
    """
    nome = (nome or "").lower()
    if nome.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(conteudo), engine="openpyxl")
    tentativas = [
        dict(sep=";", decimal=",", encoding="utf-8-sig"),
        dict(sep=";", decimal=",", encoding="latin-1"),
        dict(sep=",", decimal=".", encoding="utf-8-sig"),
        dict(sep=",", decimal=".", encoding="latin-1"),
    ]
    ultimo = None
    for t in tentativas:
        try:
            df = pd.read_csv(io.BytesIO(conteudo), engine="python", **t)
        except Exception:
            continue
        cols = [str(c).strip() for c in df.columns]
        df.columns = cols
        if all(e in cols for e in _COLS_ESPERADAS):
            return df
        ultimo = df
    return ultimo


@st.cache_data(show_spinner="Lendo base de dados...")
def carregar_base(conteudo: bytes | None, nome: str | None) -> pd.DataFrame | None:
    """
    Carrega a base de testes. Prioridade: arquivo enviado pelo usuário >
    base_dados_testes.csv na raiz do projeto. Devolve None se nada existir.
    """
    df = None
    if conteudo is not None:
        df = _ler_tabela(conteudo, nome)
    else:
        caminho = os.path.join(APP_DIR, "base_dados_testes.csv")
        if os.path.exists(caminho):
            with open(caminho, "rb") as fh:
                df = _ler_tabela(fh.read(), "base_dados_testes.csv")
    if df is None:
        return None
    df.columns = [str(c).strip() for c in df.columns]
    return df


def to_excel(df: pd.DataFrame, cols_2dec=None) -> bytes:
    """Exporta .xlsx centralizado, com autofit e 2 casas nas colunas indicadas."""
    from openpyxl.styles import Alignment
    from openpyxl.utils import get_column_letter
    cols_2dec = set(cols_2dec or [])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Impacto")
        ws = writer.sheets["Impacto"]
        centro = Alignment(horizontal="center", vertical="center")
        n = len(df)
        for i, col in enumerate(df.columns, start=1):
            letra = get_column_letter(i)
            largura = max([len(str(col))] + [len(str(v)) for v in df[col].values])
            ws.column_dimensions[letra].width = min(largura + 2, 60)
            for r in range(1, n + 2):
                ws.cell(row=r, column=i).alignment = centro
            if col in cols_2dec:
                for r in range(2, n + 2):
                    ws.cell(row=r, column=i).number_format = "0.00"
    return output.getvalue()


# =========================================================================== #
#                                INTERFACE
# =========================================================================== #
st.markdown("## 🎯 Análise de Impacto dos Resultados")
st.caption(
    "Avalia o impacto de repetir um conjunto de amostras (mínimo 3) de um mesmo "
    "teste/equipamento. O Erro Total Máximo (ETM) e o Intervalo de Referência (IR) "
    "são puxados automaticamente da base de dados pelo nome do teste."
)

# ---- 1 · Base de dados ---------------------------------------------------- #
with st.container(border=True):
    st.markdown("### 1 · Base de dados de testes")
    st.caption("Colunas esperadas: **Teste**, **Equipamento**, **ETM**, **IR**. "
               "Se você não enviar um arquivo, uso o `base_dados_testes.csv` do projeto.")
    arq_base = st.file_uploader("Base de dados (opcional se já houver no projeto)",
                                type=["csv", "xlsx", "xls"], key="base_uploader")

base = carregar_base(arq_base.getvalue() if arq_base is not None else None,
                     arq_base.name if arq_base is not None else None)

if base is None or base.empty:
    st.info("📄 Envie a base de dados (colunas Teste, Equipamento, ETM, IR) ou inclua "
            "o arquivo `base_dados_testes.csv` na raiz do projeto para começar.")
    st.stop()

faltando = [c for c in ["Teste", "Equipamento", "ETM", "IR"] if c not in base.columns]
if faltando:
    st.error(f"A base de dados não tem a(s) coluna(s): {', '.join(faltando)}. "
             f"Colunas encontradas: {', '.join(map(str, base.columns))}.")
    st.stop()

# Normaliza ETM para número (aceita vírgula / '%')
base = base.copy()
base["_ETM_num"] = normalizar_serie_numerica(base["ETM"])

# ---- 2 · Seleção do teste e equipamento ----------------------------------- #
with st.container(border=True):
    st.markdown("### 2 · Teste e equipamento")
    testes = sorted(base["Teste"].dropna().astype(str).str.strip().unique().tolist())
    equipamentos = sorted(base["Equipamento"].dropna().astype(str).str.strip().unique().tolist())

    s1, s2 = st.columns(2)
    with s1:
        teste_sel = st.selectbox("Nome do teste", testes, index=0 if testes else None)
    with s2:
        equip_sel = st.selectbox("Equipamento", equipamentos, index=0 if equipamentos else None)

    # ETM e IR puxados automaticamente pelo nome do teste (1ª linha correspondente)
    linha = base[base["Teste"].astype(str).str.strip() == str(teste_sel)]
    etm = float(linha["_ETM_num"].dropna().iloc[0]) if not linha["_ETM_num"].dropna().empty else None
    ir_txt = str(linha["IR"].dropna().iloc[0]) if not linha["IR"].dropna().empty else ""
    lo_ir, hi_ir = parse_ref_range(ir_txt)

    a1, a2 = st.columns(2)
    with a1:
        st.metric("Erro Total Máximo (ETM)", f"{etm:.2f} %" if etm is not None else "—")
    with a2:
        st.metric("Intervalo de Referência (IR)", ir_txt if ir_txt else "—")

    if etm is None:
        st.warning("Este teste não tem ETM numérico na base — o critério de erro total ficará indisponível.")
    if lo_ir is None and hi_ir is None:
        st.warning("Este teste não tem IR interpretável na base — o critério de mudança de interpretação ficará indisponível.")

# ---- 3 · Amostras (mínimo 3) ---------------------------------------------- #
with st.container(border=True):
    st.markdown("### 3 · Amostras (mínimo 3)")
    st.caption("Informe o código de barras, o Resultado 1 e o Resultado 2 de cada amostra. "
               "Use o botão ➕ da tabela para adicionar mais linhas (aceita vírgula ou ponto).")
    seed = pd.DataFrame({"Código de barras": ["", "", ""],
                         "Resultado 1": ["", "", ""],
                         "Resultado 2": ["", "", ""]})
    entrada = st.data_editor(
        seed, num_rows="dynamic", use_container_width=True, key="amostras_editor",
        column_config={
            "Código de barras": st.column_config.TextColumn("Código de barras"),
            "Resultado 1": st.column_config.TextColumn("Resultado 1"),
            "Resultado 2": st.column_config.TextColumn("Resultado 2"),
        },
    )

# ---- Cálculo -------------------------------------------------------------- #
am = entrada.copy()
am["Código de barras"] = am["Código de barras"].astype(str).str.strip()
am["R1"] = normalizar_serie_numerica(am["Resultado 1"])
am["R2"] = normalizar_serie_numerica(am["Resultado 2"])
val = am[(am["Código de barras"] != "") & (am["Código de barras"].str.lower() != "nan")
         & am["R1"].notna() & am["R2"].notna() & (am["R1"] != 0)].reset_index(drop=True)

if len(val) < 3:
    st.info(f"ℹ️ Informe **no mínimo 3 amostras** válidas (código de barras + Resultado 1 e 2 "
            f"numéricos, R1 ≠ 0). No momento há {len(val)}.")
    st.stop()

# Critério analítico: erro total (R1-R2)/R1 vs ETM
val["Erro total %"] = (val["R1"] - val["R2"]) / val["R1"] * 100
if etm is not None:
    val["Excede ETM"] = val["Erro total %"].abs() > etm
else:
    val["Excede ETM"] = False

# Critério clínico: mudança de interpretação pelo IR
val["Interpretação R1"] = val["R1"].apply(lambda v: classificar_ref(v, lo_ir, hi_ir))
val["Interpretação R2"] = val["R2"].apply(lambda v: classificar_ref(v, lo_ir, hi_ir))
val["Mudou interpretação"] = ((val["Interpretação R1"] != val["Interpretação R2"])
                              & (val["Interpretação R1"] != "—") & (val["Interpretação R2"] != "—"))

val["Impacto"] = np.where(val["Excede ETM"] | val["Mudou interpretação"], "Com impacto", "Sem impacto")
val.insert(0, "Teste", teste_sel)
val.insert(1, "Equipamento", equip_sel)

# ---- 4 · Resultado da análise --------------------------------------------- #
st.markdown("### 4 · Resultado da análise de impacto")
n = len(val)
n_etm = int(val["Excede ETM"].sum())
n_interp = int(val["Mudou interpretação"].sum())
n_impacto = int((val["Impacto"] == "Com impacto").sum())

m1, m2, m3, m4 = st.columns(4)
m1.metric("Amostras analisadas", f"{n}")
m2.metric("Excedem o ETM", f"{n_etm}", help=f"|(R1−R2)/R1| acima de {etm:.2f}%." if etm is not None else "ETM indisponível.")
m3.metric("Mudam de interpretação", f"{n_interp}", help=f"Cruzam o IR {ir_txt}." if ir_txt else "IR indisponível.")
m4.metric("Com impacto (combinado)", f"{n_impacto}")

if n_impacto:
    st.error(f"⚠️ {n_impacto} de {n} amostra(s) **com impacto** (excedem o ETM e/ou mudam de "
             f"interpretação). Avalie o equipamento/reagente **{equip_sel}** para o teste "
             f"**{teste_sel}** antes de liberar.")
else:
    st.success(f"✅ Nenhuma das {n} amostras apresentou impacto para **{teste_sel}** "
               f"em **{equip_sel}**.")

# Tabela por amostra (destaca as com impacto)
tab = val.rename(columns={"Código de barras": "Código de barras"})[
    ["Código de barras", "R1", "R2", "Erro total %", "Excede ETM",
     "Interpretação R1", "Interpretação R2", "Mudou interpretação", "Impacto"]
].rename(columns={"R1": "Resultado 1", "R2": "Resultado 2"})

def _hl(v):
    return ("background-color:#FFE3E3; color:#9B1C1C; font-weight:700" if v == "Com impacto"
            else "background-color:#E7F6EC; color:#0F5132")
st.dataframe(tab.style.format({"Erro total %": "{:.2f}", "Resultado 1": "{:.3f}",
                               "Resultado 2": "{:.3f}"}).map(_hl, subset=["Impacto"]),
             use_container_width=True)

# ---- 5 · Exportar --------------------------------------------------------- #
st.markdown("### 5 · Exportar")
export = val[["Teste", "Equipamento", "Código de barras", "R1", "R2", "Erro total %",
              "Excede ETM", "Interpretação R1", "Interpretação R2",
              "Mudou interpretação", "Impacto"]].rename(
    columns={"R1": "Resultado 1", "R2": "Resultado 2"}).copy()
export["Erro total %"] = export["Erro total %"].round(2)
export.insert(3, "ETM (%)", etm)
export.insert(4, "IR", ir_txt)

d1, d2 = st.columns(2)
with d1:
    st.download_button("⬇️ Baixar (Excel)",
                       data=to_excel(export, cols_2dec=["Erro total %", "Resultado 1", "Resultado 2"]),
                       file_name="analise_impacto.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with d2:
    csv_bytes = export.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("⬇️ Baixar (CSV)", data=csv_bytes,
                       file_name="analise_impacto.csv", mime="text/csv")
