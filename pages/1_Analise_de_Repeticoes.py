# -*- coding: utf-8 -*-
"""
Análise de Repetições (Duplicatas) — DataSift
==============================================

Ferramenta de controle da qualidade analítica baseada em resultados repetidos
(R1 e R2) de amostras de pacientes. O objetivo é detectar problemas de
equipamento/reagente avaliando o quanto a segunda medição (R2) concorda com a
primeira (R1).

Blocos de análise:
  1) Estatística analítica das duplicatas: média, DP (desvio-padrão de
     repetibilidade), CV%, erro aleatório (DP x 1,96) e erro total analítico.
  2) Análise por RCV (Reference Change Value / Valor de Referência para
     Mudança), usando um banco de CVi (variação biológica intraindividual).

Este arquivo é uma PÁGINA do app DataSift (pasta ``pages/``), mas também roda
de forma independente com ``streamlit run pages/1_Analise_de_Repeticoes.py``.
"""

import io
import os
import re
import zipfile
import tempfile
import shutil

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# --------------------------------------------------------------------------- #
# Configuração de página / identidade visual (mesma paleta do DataSift)
# --------------------------------------------------------------------------- #
COLOR_PRIMARY = "#073B4C"
COLOR_SECONDARY = "#00E5FF"
COLOR_TERTIARY = "#118AB2"
COLOR_BG = "#F8F9FA"

try:
    st.set_page_config(page_title="DataSift · Repetições", page_icon="🔁", layout="wide")
except Exception:
    # Em app multipágina o set_page_config pode já ter sido definido; ignore.
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

# Caminho da pasta do app (para localizar o cvi_database.csv na raiz do projeto)
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --------------------------------------------------------------------------- #
# 1. Normalização numérica (decimais com "," ou ".", milhar, unidades, etc.)
# --------------------------------------------------------------------------- #
def normalizar_serie_numerica(serie: pd.Series) -> pd.Series:
    """
    Converte uma coluna de texto/misto para float, lidando com:
      - vírgula OU ponto como separador decimal ("12,5" e "12.5");
      - separador de milhar ("1.234,56" -> 1234.56 e "1,234.56" -> 1234.56);
      - unidades/símbolos junto ao número ("12,5 mg/dL", "> 200") -> extrai 12.5 / 200;
      - células vazias, textos ("indetectável", "N/A") -> NaN.

    Regra do separador decimal: quando há "," e "." na mesma célula, o separador
    decimal é o que aparece POR ÚLTIMO; o outro é tratado como milhar. Quando há
    apenas ",", ela é tratada como decimal (padrão brasileiro).
    """
    def _conv(x):
        if pd.isna(x):
            return np.nan
        s = str(x).strip()
        if s == "" or s.lower() in ("nan", "none", "na", "n/a", "-", "--", "."):
            return np.nan
        # remove espaços (inclusive não separável) e mantém só dígitos/sinais/separadores
        s = s.replace("\xa0", "").replace(" ", "")
        s = re.sub(r"[^0-9,.\-+]", "", s)
        if s in ("", "+", "-", ".", ","):
            return np.nan
        has_dot, has_comma = "." in s, "," in s
        if has_dot and has_comma:
            if s.rfind(",") > s.rfind("."):      # vírgula é o decimal
                s = s.replace(".", "").replace(",", ".")
            else:                                # ponto é o decimal
                s = s.replace(",", "")
        elif has_comma:                          # só vírgula -> decimal
            s = s.replace(",", ".")
        # se houver mais de um ponto (milhar tipo "1.234.567"), remove todos menos o último
        if s.count(".") > 1:
            partes = s.split(".")
            s = "".join(partes[:-1]) + "." + partes[-1]
        try:
            return float(s)
        except ValueError:
            return np.nan

    return serie.apply(_conv)


# --------------------------------------------------------------------------- #
# 2. Leitura robusta da planilha (csv / xlsx / xls / zip)
# --------------------------------------------------------------------------- #
def _ler_csv(path):
    """Tenta o padrão brasileiro (;, decimal ,) e cai para (, decimal .)."""
    try:
        return pd.read_csv(path, sep=";", decimal=",", encoding="latin-1", engine="python")
    except Exception:
        return pd.read_csv(path, sep=",", decimal=".", encoding="utf-8", engine="python")


@st.cache_data(show_spinner="Lendo planilha...")
def carregar_planilha(conteudo: bytes, nome: str) -> pd.DataFrame:
    """Recebe os bytes do arquivo enviado e devolve um DataFrame."""
    nome = nome.lower()
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(nome)[1]) as tmp:
        tmp.write(conteudo)
        tmp_path = tmp.name
    try:
        if nome.endswith(".zip"):
            with zipfile.ZipFile(tmp_path) as z:
                validos = [f for f in z.namelist()
                           if not f.startswith("__MACOSX/")
                           and f.lower().endswith((".csv", ".xlsx", ".xls"))]
                if not validos:
                    raise ValueError("O ZIP não contém CSV ou Excel válidos.")
                with tempfile.NamedTemporaryFile(delete=False,
                                                 suffix=os.path.splitext(validos[0])[1]) as inner:
                    inner.write(z.read(validos[0]))
                    inner_path = inner.name
                df = (_ler_csv(inner_path) if validos[0].lower().endswith(".csv")
                      else pd.read_excel(inner_path, engine="openpyxl"))
                os.remove(inner_path)
        elif nome.endswith(".csv"):
            df = _ler_csv(tmp_path)
        else:
            df = pd.read_excel(tmp_path, engine="openpyxl")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    return df


# --------------------------------------------------------------------------- #
# 3. Banco de CVi (variação biológica intraindividual)
# --------------------------------------------------------------------------- #
# Fallback embutido caso o arquivo cvi_database.csv não seja enviado.
CVI_PADRAO = {
    "Glicose": 5.6, "Ureia": 12.3, "Creatinina": 5.9, "Acido urico": 8.6,
    "Colesterol total": 5.9, "HDL colesterol": 7.4, "LDL colesterol": 8.3,
    "Triglicerides": 20.9, "Sodio": 0.6, "Potassio": 4.6, "Cloreto": 1.2,
    "Calcio": 1.9, "Fosforo": 8.5, "Magnesio": 3.6, "Proteinas totais": 2.7,
    "Albumina": 3.1, "Bilirrubina total": 21.8, "ALT (TGP)": 18.0,
    "AST (TGO)": 12.3, "GGT": 13.8, "Fosfatase alcalina": 6.5, "TSH": 19.3,
    "T4 livre": 5.7, "HbA1c": 1.6, "Hemoglobina": 2.8, "Leucocitos": 10.9,
    "Plaquetas": 9.1, "PSA total": 18.1, "Ferritina": 14.9, "PCR": 42.2,
}


@st.cache_data(show_spinner=False)
def carregar_cvi_db(conteudo_custom: bytes | None = None) -> pd.DataFrame:
    """
    Devolve o banco de CVi como DataFrame (colunas: Analito, CVi_percent, ...).
    Prioridade: arquivo enviado pelo usuário > cvi_database.csv na raiz >
    dicionário embutido CVI_PADRAO.
    """
    df = None
    if conteudo_custom is not None:
        try:
            df = _ler_csv(io.BytesIO(conteudo_custom))
        except Exception:
            df = None
    if df is None:
        caminho = os.path.join(APP_DIR, "cvi_database.csv")
        if os.path.exists(caminho):
            try:
                df = _ler_csv(caminho)
            except Exception:
                df = None
    if df is None:
        df = pd.DataFrame({"Analito": list(CVI_PADRAO.keys()),
                           "CVi_percent": list(CVI_PADRAO.values())})
    # normaliza nomes de coluna esperados
    df.columns = [str(c).strip() for c in df.columns]
    if "CVi_percent" in df.columns:
        df["CVi_percent"] = normalizar_serie_numerica(df["CVi_percent"])
    return df


# --------------------------------------------------------------------------- #
# 4. Cálculos estatísticos das duplicatas
# --------------------------------------------------------------------------- #
def calcular_metricas(df: pd.DataFrame, col_r1: str, col_r2: str, z: float = 1.96):
    """
    Recebe o DataFrame já com as colunas R1/R2 e devolve:
      - tabela por par (R1, R2, média, diferença, erro relativo %, RPD %)
      - dicionário com as métricas agregadas.
    """
    base = pd.DataFrame({
        "R1": normalizar_serie_numerica(df[col_r1]),
        "R2": normalizar_serie_numerica(df[col_r2]),
    })
    n_total = len(base)
    base = base.dropna(subset=["R1", "R2"])
    base = base[(base["R1"] != 0)]  # evita divisão por zero em (R1-R2)/R1
    n_validos = len(base)

    base["Media_par"] = (base["R1"] + base["R2"]) / 2.0
    base["Diferenca"] = base["R1"] - base["R2"]              # d = R1 - R2
    base["Dif_abs"] = base["Diferenca"].abs()
    # DP e CV de cada par (n=2): DP = |R1-R2|/sqrt(2)
    base["DP_par"] = base["Dif_abs"] / np.sqrt(2)
    base["CV_par_%"] = np.where(base["Media_par"] != 0,
                                base["DP_par"] / base["Media_par"] * 100, np.nan)
    # Erro total analítico (fórmula do usuário): (R1 - R2) / R1  em %
    base["ETA_%"] = base["Diferenca"] / base["R1"] * 100
    base["ETA_abs_%"] = base["ETA_%"].abs()
    # RPD simétrico (referência = média do par), para comparação
    base["RPD_%"] = np.where(base["Media_par"] != 0,
                             base["Dif_abs"] / base["Media_par"] * 100, np.nan)

    d = base["Diferenca"].to_numpy()
    media_global = float(base["Media_par"].mean()) if n_validos else np.nan
    # DP de repetibilidade (a partir das diferenças): Sr = sqrt( sum(d^2) / (2n) )
    dp_repet = float(np.sqrt(np.sum(d ** 2) / (2 * n_validos))) if n_validos else np.nan
    cv_analitico = (dp_repet / media_global * 100) if media_global else np.nan
    vies_medio = float(base["Diferenca"].mean()) if n_validos else np.nan   # bias
    vies_medio_pct = (vies_medio / media_global * 100) if media_global else np.nan
    erro_aleatorio = z * dp_repet                                            # DP x 1,96
    # Erro total analítico "clássico" (modelo Westgard): |bias%| + z * CV%
    eta_westgard = abs(vies_medio_pct) + z * cv_analitico if n_validos else np.nan

    resumo = {
        "n_total": n_total,
        "n_validos": n_validos,
        "media_global": media_global,
        "dp_repet": dp_repet,
        "cv_analitico": cv_analitico,
        "vies_medio": vies_medio,
        "vies_medio_pct": vies_medio_pct,
        "erro_aleatorio": erro_aleatorio,
        "eta_medio_pct": float(base["ETA_%"].mean()) if n_validos else np.nan,
        "eta_medio_abs_pct": float(base["ETA_abs_%"].mean()) if n_validos else np.nan,
        "eta_westgard": eta_westgard,
        "z": z,
    }
    return base, resumo


def calcular_rcv(cva: float, cvi: float, z: float = 1.96) -> float:
    """RCV = sqrt(2) * Z * sqrt(CVa^2 + CVi^2)  (modelo clássico de Fraser/Harris)."""
    return float(np.sqrt(2) * z * np.sqrt(cva ** 2 + cvi ** 2))


# --------------------------------------------------------------------------- #
# 5. Exportação
# --------------------------------------------------------------------------- #
def to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Repeticoes")
    return output.getvalue()


# =========================================================================== #
#                                INTERFACE
# =========================================================================== #
st.markdown("## 🔁 Análise de Repetições (Duplicatas) — Controle da Qualidade Analítica")
st.caption(
    "Avalia a concordância entre um primeiro resultado (R1) e sua repetição (R2) "
    "para detectar problemas de equipamento/reagente. Complementa a análise de "
    "média/mediana móvel já existente no DataSift."
)

# ---- Upload da planilha --------------------------------------------------- #
with st.container(border=True):
    st.markdown("### 1 · Envie a planilha de resultados")
    arquivo = st.file_uploader(
        "Arquivo com as colunas R1 e R2 (uma linha por amostra)",
        type=["csv", "xlsx", "xls", "zip"],
    )

if arquivo is None:
    st.info(
        "📄 Envie um arquivo para começar. Ele deve conter, no mínimo, duas colunas: "
        "o primeiro resultado (R1) e a repetição (R2). Colunas de analito, data ou "
        "identificação do paciente são opcionais."
    )
    st.stop()

df = carregar_planilha(arquivo.getvalue(), arquivo.name)
if df is None or df.empty:
    st.error("Não foi possível ler a planilha ou ela está vazia.")
    st.stop()

st.success(f"Planilha carregada: {len(df)} linhas × {len(df.columns)} colunas.")
with st.expander("👁️ Ver amostra dos dados (10 primeiras linhas)"):
    st.dataframe(df.head(10), use_container_width=True)

# ---- Mapeamento de colunas ------------------------------------------------ #
with st.container(border=True):
    st.markdown("### 2 · Indique as colunas")
    colunas = list(df.columns)
    c1, c2 = st.columns(2)
    with c1:
        col_r1 = st.selectbox("Coluna do **R1** (1º resultado)", colunas, index=0)
    with c2:
        idx2 = 1 if len(colunas) > 1 else 0
        col_r2 = st.selectbox("Coluna do **R2** (repetição)", colunas, index=idx2)

    c3, c4 = st.columns(2)
    with c3:
        opc = ["(nenhuma)"] + colunas
        col_analito = st.selectbox("Coluna de analito/exame (opcional)", opc, index=0)
    with c4:
        z_opt = st.selectbox("Nível de confiança (Z)",
                             ["95% bilateral (Z = 1,96)", "95% unilateral (Z = 1,65)"], index=0)
    z = 1.96 if "1,96" in z_opt else 1.65

if col_r1 == col_r2:
    st.warning("R1 e R2 estão apontando para a mesma coluna. Selecione colunas diferentes.")
    st.stop()

# Filtro opcional por analito
df_uso = df
if col_analito != "(nenhuma)":
    valores = ["(todos)"] + sorted(df[col_analito].dropna().astype(str).unique().tolist())
    escolha = st.selectbox("Filtrar por analito", valores, index=0)
    if escolha != "(todos)":
        df_uso = df[df[col_analito].astype(str) == escolha]

# ---- Cálculo -------------------------------------------------------------- #
base, resumo = calcular_metricas(df_uso, col_r1, col_r2, z=z)

if resumo["n_validos"] == 0:
    st.error(
        "Nenhum par R1/R2 válido após a normalização. Verifique se as colunas "
        "escolhidas contêm números (a normalização aceita vírgula ou ponto)."
    )
    st.stop()

descartados = resumo["n_total"] - resumo["n_validos"]
if descartados > 0:
    st.warning(
        f"{descartados} linha(s) descartada(s) por valor ausente/não numérico em "
        f"R1 ou R2, ou por R1 = 0 (indefinido em (R1−R2)/R1)."
    )

# ---- Bloco de resultados: estatística analítica --------------------------- #
st.markdown("### 3 · Estatística analítica das duplicatas")
m1, m2, m3, m4 = st.columns(4)
m1.metric("Nº de pares válidos", f"{resumo['n_validos']}")
m2.metric("Média global", f"{resumo['media_global']:.3f}")
m3.metric("DP (repetibilidade)", f"{resumo['dp_repet']:.3f}")
m4.metric("CV analítico", f"{resumo['cv_analitico']:.2f} %")

m5, m6, m7, m8 = st.columns(4)
m5.metric(f"Erro aleatório (DP × {z})", f"{resumo['erro_aleatorio']:.3f}")
m6.metric("Viés médio (R1−R2)", f"{resumo['vies_medio']:.3f}",
          help="Diferença sistemática média entre R1 e R2. Próximo de zero é o esperado.")
m7.metric("Erro total médio |(R1−R2)/R1|", f"{resumo['eta_medio_abs_pct']:.2f} %")
m8.metric(f"Erro total (Westgard: |viés%|+{z}·CV%)", f"{resumo['eta_westgard']:.2f} %")

with st.expander("ℹ️ Como cada número é calculado"):
    st.markdown(
        f"""
- **Média global** = média dos pontos médios de cada par, (R1+R2)/2.
- **DP (repetibilidade)** = desvio-padrão analítico estimado a partir das
  diferenças das duplicatas: `Sr = √( Σ(R1−R2)² / (2·n) )`. É a forma correta de
  estimar a imprecisão em duplicatas — o DP simples de todos os valores mede a
  variação **entre pacientes**, não a do método.
- **CV analítico (%)** = `Sr / Média × 100`. Este é o **CVa** usado no RCV abaixo.
- **Erro aleatório** = `DP × {z}` — limite do erro aleatório para o Z escolhido
  (Z = 1,96 ≈ 95% bilateral; Z = 1,65 ≈ 95% unilateral).
- **Viés médio** = média de (R1−R2); indica erro **sistemático** entre a 1ª e a 2ª medição.
- **Erro total médio |(R1−R2)/R1|** = média do módulo da sua fórmula, em %.
- **Erro total (Westgard)** = `|viés%| + {z}·CV%` — modelo clássico de erro total
  analítico (sistemático + aleatório), mostrado para comparação com a sua fórmula.
"""
    )

# Limite de aceitação para o erro total (%), para sinalizar pares suspeitos
lim_eta = st.number_input(
    "Limite de aceitação para |Erro total (R1−R2)/R1| — sinaliza pares acima deste valor (%)",
    min_value=0.0, value=10.0, step=0.5,
    help="Defina conforme o erro total admissível do seu analito (ex.: especificação "
         "por variação biológica, CLIA, RDC). Pares acima disto ficam marcados.",
)
base["Suspeito"] = base["ETA_abs_%"] > lim_eta
n_susp = int(base["Suspeito"].sum())
if n_susp:
    st.error(f"⚠️ {n_susp} par(es) ({n_susp/resumo['n_validos']*100:.1f}%) acima do limite de {lim_eta:.1f}%.")
else:
    st.success(f"Nenhum par acima do limite de {lim_eta:.1f}%.")

# Tabela por par
tabela = base.rename(columns={
    "Media_par": "Média", "Diferenca": "R1−R2", "Dif_abs": "|R1−R2|",
    "DP_par": "DP par", "CV_par_%": "CV par %", "ETA_%": "(R1−R2)/R1 %",
    "ETA_abs_%": "|(R1−R2)/R1| %", "RPD_%": "RPD % (simétrico)",
})
st.dataframe(
    tabela[["R1", "R2", "Média", "R1−R2", "|R1−R2|", "CV par %",
            "(R1−R2)/R1 %", "|(R1−R2)/R1| %", "RPD % (simétrico)", "Suspeito"]]
    .style.format(precision=3),
    use_container_width=True, height=320,
)

# ---- Gráficos ------------------------------------------------------------- #
st.markdown("#### Gráficos de apoio")
g1, g2 = st.columns(2)

with g1:
    # Bland-Altman: média do par (x) vs diferença (y)
    fig, ax = plt.subplots(figsize=(5, 3.6))
    ax.scatter(base["Media_par"], base["Diferenca"], s=14, alpha=0.6, color=COLOR_TERTIARY)
    md = resumo["vies_medio"]
    sd = base["Diferenca"].std(ddof=1)
    ax.axhline(md, color=COLOR_PRIMARY, lw=1.5, label=f"Viés = {md:.3f}")
    ax.axhline(md + 1.96 * sd, color="#EF476F", ls="--", lw=1, label="±1,96 DP")
    ax.axhline(md - 1.96 * sd, color="#EF476F", ls="--", lw=1)
    ax.set_xlabel("Média do par (R1+R2)/2")
    ax.set_ylabel("Diferença (R1−R2)")
    ax.set_title("Bland-Altman")
    ax.legend(fontsize=7)
    ax.grid(alpha=0.2)
    st.pyplot(fig, clear_figure=True)

with g2:
    # Média móvel da diferença na ordem de análise (detecta desvio ao longo do tempo)
    janela = st.slider("Janela da média móvel das diferenças", 3, 50, 10)
    serie_dif = base["Diferenca"].reset_index(drop=True)
    mm = serie_dif.rolling(janela, min_periods=1).mean()
    fig2, ax2 = plt.subplots(figsize=(5, 3.6))
    ax2.plot(serie_dif.index, serie_dif.values, ".", ms=4, alpha=0.35,
             color="#999", label="Diferença")
    ax2.plot(mm.index, mm.values, "-", lw=2, color=COLOR_SECONDARY, label=f"Média móvel ({janela})")
    ax2.axhline(0, color=COLOR_PRIMARY, lw=1)
    ax2.set_xlabel("Ordem da amostra")
    ax2.set_ylabel("Diferença (R1−R2)")
    ax2.set_title("Média móvel da diferença (deriva do sistema)")
    ax2.legend(fontsize=7)
    ax2.grid(alpha=0.2)
    st.pyplot(fig2, clear_figure=True)

# ---- Bloco de RCV --------------------------------------------------------- #
st.markdown("### 4 · Análise por RCV (Valor de Referência para Mudança)")
st.caption(
    "O RCV avalia se a diferença entre dois resultados **do mesmo paciente** é maior "
    "do que a variação esperada (analítica + biológica). Fórmula clássica: "
    "RCV = √2 · Z · √(CVa² + CVi²)."
)

cvi_up = st.file_uploader("Banco de CVi personalizado (opcional, CSV: Analito;CVi_percent)",
                          type=["csv"], key="cvi_uploader")
cvi_df = carregar_cvi_db(cvi_up.getvalue() if cvi_up is not None else None)

rc1, rc2, rc3 = st.columns(3)
with rc1:
    fonte_cva = st.radio("CVa (imprecisão analítica)",
                         ["Usar CV calculado acima", "Informar manualmente"], index=0)
    if fonte_cva == "Usar CV calculado acima":
        cva = float(resumo["cv_analitico"])
        st.metric("CVa (%)", f"{cva:.2f}")
    else:
        cva = st.number_input("CVa (%)", min_value=0.0, value=float(round(resumo["cv_analitico"], 2)), step=0.1)
with rc2:
    analitos = cvi_df["Analito"].astype(str).tolist() if "Analito" in cvi_df.columns else []
    sel = st.selectbox("Analito (para buscar o CVi no banco)", ["(informar manualmente)"] + analitos)
    if sel != "(informar manualmente)" and "CVi_percent" in cvi_df.columns:
        cvi_lookup = cvi_df.loc[cvi_df["Analito"].astype(str) == sel, "CVi_percent"]
        cvi_default = float(cvi_lookup.iloc[0]) if len(cvi_lookup) else 5.0
    else:
        cvi_default = 5.0
    cvi = st.number_input("CVi (%)", min_value=0.0, value=float(cvi_default), step=0.1)
with rc3:
    z_rcv = 1.96 if "1,96" in z_opt else 1.65
    st.metric("Z aplicado", f"{z_rcv}")
    rcv = calcular_rcv(cva, cvi, z_rcv)
    st.metric("RCV (%)", f"{rcv:.1f}")

st.latex(r"RCV = \sqrt{2}\times %.2f \times \sqrt{%.2f^2 + %.2f^2} = %.1f\%%"
         % (z_rcv, cva, cvi, rcv))

# Avalia cada par: a variação relativa entre R1 e R2 excede o RCV?
base["Variacao_%"] = base["Diferenca"] / base["R1"] * 100
base["Excede_RCV"] = base["Variacao_%"].abs() > rcv
n_rcv = int(base["Excede_RCV"].sum())
st.write(
    f"**{n_rcv}** de **{resumo['n_validos']}** pares "
    f"({n_rcv/resumo['n_validos']*100:.1f}%) apresentam variação R1→R2 maior que o RCV "
    f"de {rcv:.1f}% — ou seja, uma mudança estatisticamente significativa."
)
st.info(
    "Atenção conceitual: o RCV foi concebido para **amostras seriadas do mesmo paciente** "
    "(coletas em tempos diferentes), onde entra a variação biológica (CVi). Se R1 e R2 são "
    "reanálises da **mesma amostra**, o esperado é não haver variação biológica — nesse caso "
    "use os indicadores analíticos do bloco 3 (DP, CV, erro total). Use o RCV quando R1/R2 "
    "forem resultados do paciente em momentos distintos."
)
with st.expander("👁️ Ver pares que excedem o RCV"):
    st.dataframe(
        base.loc[base["Excede_RCV"], ["R1", "R2", "Variacao_%"]]
        .rename(columns={"Variacao_%": "Variação R1→R2 %"})
        .style.format(precision=3),
        use_container_width=True,
    )

# ---- Downloads ------------------------------------------------------------ #
st.markdown("### 5 · Exportar resultados")
export = base.copy()
d1, d2 = st.columns(2)
with d1:
    st.download_button("⬇️ Baixar tabela (Excel)", data=to_excel(export),
                       file_name="analise_repeticoes.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with d2:
    csv_bytes = export.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("⬇️ Baixar tabela (CSV)", data=csv_bytes,
                       file_name="analise_repeticoes.csv", mime="text/csv")
