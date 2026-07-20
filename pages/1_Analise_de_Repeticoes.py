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
  2) Mudança de interpretação entre R1 e R2 com base em um intervalo de
     referência / limite de decisão médica definido pelo usuário.

Cada amostra é identificada pelo código de barras, para rastrear qual paciente
ficou suspeito. A média móvel das diferenças pode ser ordenada pela data/hora
do resultado.

Este arquivo é uma PÁGINA do app DataSift (pasta ``pages/``), mas também roda
de forma independente com ``streamlit run pages/1_Analise_de_Repeticoes.py``.
"""

import io
import os
import re
import zipfile
import tempfile

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


def parse_limite(txt: str):
    """Converte um limite digitado (aceita vírgula) para float; vazio -> None."""
    if txt is None:
        return None
    v = normalizar_serie_numerica(pd.Series([txt])).iloc[0]
    return None if pd.isna(v) else float(v)


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


def montar_datahora(df: pd.DataFrame, col_data: str | None, col_hora: str | None):
    """Combina as colunas de data e hora num datetime (dd/mm/aaaa, dayfirst)."""
    if not col_data or col_data not in df.columns:
        return None
    d = df[col_data].astype(str).str.strip()
    if col_hora and col_hora in df.columns:
        combo = d + " " + df[col_hora].astype(str).str.strip()
    else:
        combo = d
    return pd.to_datetime(combo, errors="coerce", dayfirst=True)


# --------------------------------------------------------------------------- #
# 3. Cálculos estatísticos das duplicatas
# --------------------------------------------------------------------------- #
def calcular_metricas(df: pd.DataFrame, col_r1: str, col_r2: str,
                      col_id: str | None = None, datahora=None, z: float = 1.96):
    """
    Recebe o DataFrame e devolve:
      - tabela por par (ID, R1, R2, média, diferença, erro relativo %, RPD %, DataHora)
      - dicionário com as métricas agregadas.
    A coluna ``ID`` guarda o código de barras (ou o nº da linha, se não informado).
    """
    n = len(df)
    if col_id and col_id in df.columns:
        ids = df[col_id].astype(str).values
    else:
        ids = np.array([f"linha {i + 1}" for i in range(n)])

    dados = {
        "ID": ids,
        "R1": normalizar_serie_numerica(df[col_r1]).values,
        "R2": normalizar_serie_numerica(df[col_r2]).values,
    }
    if datahora is not None:
        dados["DataHora"] = pd.to_datetime(datahora).values
    base = pd.DataFrame(dados)

    n_total = len(base)
    base = base.dropna(subset=["R1", "R2"])
    base = base[base["R1"] != 0].reset_index(drop=True)  # evita divisão por zero em (R1-R2)/R1
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


# --------------------------------------------------------------------------- #
# 4. Classificação por intervalo de referência / limite de decisão médica
# --------------------------------------------------------------------------- #
def classificar_ref(valor, limite_inf, limite_sup):
    """
    Classifica um valor em Baixo / Normal / Alto a partir do intervalo de
    referência. Limites são inclusivos (Normal = inf <= valor <= sup). Qualquer
    limite pode ficar em branco (None) para representar "sem limite" daquele lado.
    """
    if pd.isna(valor):
        return "—"
    if limite_inf is not None and valor < limite_inf:
        return "Baixo"
    if limite_sup is not None and valor > limite_sup:
        return "Alto"
    return "Normal"


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
    "para detectar problemas de equipamento/reagente, e verifica se a interpretação "
    "clínica mudou entre as duas medições. Complementa a análise de média/mediana "
    "móvel já existente no DataSift."
)

# ---- Upload da planilha --------------------------------------------------- #
with st.container(border=True):
    st.markdown("### 1 · Envie a planilha de resultados")
    arquivo = st.file_uploader(
        "Arquivo com as colunas R1, R2 e o código de barras (uma linha por amostra)",
        type=["csv", "xlsx", "xls", "zip"],
    )

if arquivo is None:
    st.info(
        "📄 Envie um arquivo para começar. Ele deve conter, no mínimo, três colunas: "
        "o primeiro resultado (R1), a repetição (R2) e o código de barras que identifica "
        "a amostra. Colunas de analito, data e hora são opcionais."
    )
    st.stop()

df = carregar_planilha(arquivo.getvalue(), arquivo.name)
if df is None or df.empty:
    st.error("Não foi possível ler a planilha ou ela está vazia.")
    st.stop()

st.success(f"Planilha carregada: {len(df)} linhas × {len(df.columns)} colunas.")

# ---- Mapeamento de colunas ------------------------------------------------ #
with st.container(border=True):
    st.markdown("### 2 · Indique as colunas")
    colunas = list(df.columns)
    opc = ["(nenhuma)"] + colunas

    c1, c2, c3 = st.columns(3)
    with c1:
        col_r1 = st.selectbox("Coluna do **R1** (1º resultado)", colunas, index=0)
    with c2:
        idx2 = 1 if len(colunas) > 1 else 0
        col_r2 = st.selectbox("Coluna do **R2** (repetição)", colunas, index=idx2)
    with c3:
        col_id = st.selectbox("Coluna do **código de barras** (identificador)",
                              ["(usar nº da linha)"] + colunas, index=0)
        col_id = None if col_id == "(usar nº da linha)" else col_id

    c4, c5, c6 = st.columns(3)
    with c4:
        col_analito = st.selectbox("Coluna de analito/exame (opcional)", opc, index=0)
    with c5:
        col_data = st.selectbox("Coluna de **data** (opcional)", opc, index=0)
        col_data = None if col_data == "(nenhuma)" else col_data
    with c6:
        col_hora = st.selectbox("Coluna de **hora** (opcional)", opc, index=0)
        col_hora = None if col_hora == "(nenhuma)" else col_hora

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
datahora = montar_datahora(df_uso, col_data, col_hora)
base, resumo = calcular_metricas(df_uso, col_r1, col_r2, col_id=col_id, datahora=datahora, z=z)

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

# ---- Bloco 3: estatística analítica --------------------------------------- #
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
- **CV analítico (%)** = `Sr / Média × 100`. Mede a imprecisão relativa do método.
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
base["Suspeito_erro"] = base["ETA_abs_%"] > lim_eta
n_susp = int(base["Suspeito_erro"].sum())
if n_susp:
    st.error(f"⚠️ {n_susp} par(es) ({n_susp/resumo['n_validos']*100:.1f}%) acima do limite de {lim_eta:.1f}%.")
else:
    st.success(f"Nenhum par acima do limite de {lim_eta:.1f}%.")

# Tabela por par (com código de barras)
tabela = base.rename(columns={
    "ID": "Código de barras", "Media_par": "Média", "Diferenca": "R1−R2",
    "Dif_abs": "|R1−R2|", "CV_par_%": "CV par %", "ETA_%": "(R1−R2)/R1 %",
    "ETA_abs_%": "|(R1−R2)/R1| %", "RPD_%": "RPD % (simétrico)",
    "Suspeito_erro": "Suspeito",
})
st.dataframe(
    tabela[["Código de barras", "R1", "R2", "Média", "R1−R2", "|R1−R2|", "CV par %",
            "(R1−R2)/R1 %", "|(R1−R2)/R1| %", "RPD % (simétrico)", "Suspeito"]]
    .style.format(precision=3),
    use_container_width=True, height=320,
)
if n_susp:
    with st.expander(f"🔎 Ver só os {n_susp} suspeito(s) pelo erro total"):
        st.dataframe(
            tabela.loc[tabela["Suspeito"], ["Código de barras", "R1", "R2",
                                            "(R1−R2)/R1 %", "|(R1−R2)/R1| %"]]
            .style.format(precision=3),
            use_container_width=True,
        )

# ---- Gráficos ------------------------------------------------------------- #
st.markdown("#### Gráficos de apoio")
md = resumo["vies_medio"]
sd = base["Diferenca"].std(ddof=1)
g1, g2 = st.columns(2)

with g1:
    # Bland-Altman: média do par (x) vs diferença (y)
    fig, ax = plt.subplots(figsize=(5, 3.6))
    ax.scatter(base["Media_par"], base["Diferenca"], s=14, alpha=0.6, color=COLOR_TERTIARY)
    ax.axhline(md, color=COLOR_PRIMARY, lw=1.5, label=f"Viés = {md:.3f}")
    if pd.notna(sd):
        ax.axhline(md + 1.96 * sd, color="#EF476F", ls="--", lw=1, label="±1,96 DP")
        ax.axhline(md - 1.96 * sd, color="#EF476F", ls="--", lw=1)
    ax.set_xlabel("Média do par (R1+R2)/2")
    ax.set_ylabel("Diferença (R1−R2)")
    ax.set_title("Bland-Altman")
    ax.legend(fontsize=7)
    ax.grid(alpha=0.2)
    st.pyplot(fig, clear_figure=True)

with g2:
    # Média móvel da diferença — ordenada por data/hora se disponível, senão por ordem
    janela = st.slider("Janela da média móvel das diferenças", 3, 50, 10)
    tem_dt = "DataHora" in base.columns and base["DataHora"].notna().any()
    if tem_dt:
        ordf = base.dropna(subset=["DataHora"]).sort_values("DataHora").reset_index(drop=True)
        x, xlabel = ordf["DataHora"], "Data/hora do resultado"
        sem_dt = int(base["DataHora"].isna().sum())
    else:
        ordf = base.reset_index(drop=True)
        x, xlabel, sem_dt = ordf.index, "Ordem da amostra", 0
    y = ordf["Diferenca"]
    mm = y.rolling(janela, min_periods=1).mean()
    fig2, ax2 = plt.subplots(figsize=(5, 3.6))
    ax2.plot(x, y.values, ".", ms=4, alpha=0.35, color="#999", label="Diferença")
    ax2.plot(x, mm.values, "-", lw=2, color=COLOR_SECONDARY, label=f"Média móvel ({janela})")
    ax2.axhline(0, color=COLOR_PRIMARY, lw=1)
    ax2.set_xlabel(xlabel)
    ax2.set_ylabel("Diferença (R1−R2)")
    ax2.set_title("Média móvel da diferença (deriva do sistema)")
    ax2.legend(fontsize=7)
    ax2.grid(alpha=0.2)
    if tem_dt:
        fig2.autofmt_xdate()
    st.pyplot(fig2, clear_figure=True)
    if tem_dt and sem_dt:
        st.caption(f"{sem_dt} amostra(s) sem data/hora válida não entraram neste gráfico.")
    elif not tem_dt and (col_data or col_hora):
        st.caption("Não foi possível interpretar a data/hora; gráfico ordenado pela ordem da amostra.")

# Tabela dos pontos fora dos limites de concordância (±1,96 DP) do Bland-Altman
if pd.notna(sd) and sd > 0:
    lim_ba = 1.96 * sd
    fora = base[(base["Diferenca"] - md).abs() > lim_ba]
    st.markdown(f"**Amostras fora dos limites de concordância do Bland-Altman "
                f"(viés {md:.3f} ± {lim_ba:.3f})**")
    if len(fora):
        tab_fora = fora.rename(columns={"ID": "Código de barras",
                                        "Diferenca": "R1−R2"})[["Código de barras", "R1", "R2", "R1−R2"]]
        st.dataframe(tab_fora.style.format(precision=3), use_container_width=True)
    else:
        st.success("Nenhuma amostra fora dos limites de ±1,96 DP.")

with st.expander("ℹ️ Como interpretar os gráficos"):
    st.markdown(
        """
**Gráfico de Bland-Altman** — mostra, para cada amostra, a **média do par**
`(R1+R2)/2` no eixo X e a **diferença** `R1−R2` no eixo Y. Serve para enxergar a
concordância entre a 1ª e a 2ª medição ao longo de toda a faixa de concentração.

- A **linha cheia central** é o **viés médio**. Se ela está bem afastada do zero,
  há um **erro sistemático** entre R1 e R2 (o sistema tende a ler mais alto ou mais
  baixo na repetição).
- As **linhas tracejadas** são os **limites de concordância** (viés ± 1,96·DP);
  espera-se que ~95% dos pontos fiquem dentro delas. Os pontos **fora** aparecem
  listados na tabela logo acima, com o código de barras — são as amostras mais
  discrepantes, candidatas a suspeitas.
- Se a nuvem de pontos **abre como um funil** ou **inclina** conforme a concentração
  aumenta, o erro é **proporcional à concentração** (típico de problema de calibração
  ou linearidade), e não um erro constante.
- Idealmente os pontos ficam espalhados **simetricamente em torno do zero**, sem padrão.

**Média móvel da diferença** — mostra a diferença `R1−R2` de cada amostra na
**ordem cronológica** (quando você informa data e hora) ou na ordem da planilha, com
uma **média móvel** (linha destacada) que suaviza o ruído e revela **tendências ao
longo do tempo/corrida**.

- A média móvel deve **oscilar em torno do zero**. Uma **subida ou descida
  sustentada** indica uma **deriva** do sistema (reagente envelhecendo, calibração
  saindo do lugar, degradação do equipamento) — mesmo que cada par individual pareça
  aceitável.
- Um **degrau/salto abrupto** costuma marcar um **evento**: troca de lote de reagente,
  recalibração, manutenção. Cruzar a posição do salto com o log do equipamento (pela
  data/hora) ajuda a achar a causa.
- Use a **janela** para ajustar a sensibilidade: janela pequena reage rápido a
  mudanças (mais ruído); janela grande evidencia tendências longas (mais suave).
"""
    )

# ---- Bloco 4: mudança de interpretação ------------------------------------ #
st.markdown("### 4 · Mudança de interpretação entre R1 e R2")
st.caption(
    "Informe **um** intervalo de referência ou limite de decisão médica. Cada resultado "
    "é classificado em Baixo / Normal / Alto e a amostra é marcada como **suspeita** "
    "quando a interpretação muda de R1 para R2 (troca que alteraria a conduta clínica)."
)

ci1, ci2 = st.columns(2)
with ci1:
    txt_inf = st.text_input("Limite inferior do normal (deixe vazio se não usar)", value="")
with ci2:
    txt_sup = st.text_input("Limite superior do normal (deixe vazio se não usar)", value="")
lim_inf, lim_sup = parse_limite(txt_inf), parse_limite(txt_sup)

if lim_inf is None and lim_sup is None:
    st.info("Informe pelo menos um dos limites (inferior e/ou superior) para rodar esta análise.")
elif lim_inf is not None and lim_sup is not None and lim_inf > lim_sup:
    st.warning("O limite inferior é maior que o superior. Verifique os valores.")
else:
    faixa_txt = (f"{lim_inf if lim_inf is not None else '−∞'} a "
                 f"{lim_sup if lim_sup is not None else '+∞'}")
    st.caption(f"Faixa normal considerada: **{faixa_txt}** (limites inclusivos).")

    base["Interp_R1"] = base["R1"].apply(lambda v: classificar_ref(v, lim_inf, lim_sup))
    base["Interp_R2"] = base["R2"].apply(lambda v: classificar_ref(v, lim_inf, lim_sup))
    base["Mudou_interp"] = base["Interp_R1"] != base["Interp_R2"]
    n_mudou = int(base["Mudou_interp"].sum())

    cA, cB = st.columns(2)
    cA.metric("Amostras com mudança de interpretação", f"{n_mudou}")
    cB.metric("% do total", f"{n_mudou/resumo['n_validos']*100:.1f} %")

    if n_mudou:
        st.error(
            f"⚠️ {n_mudou} amostra(s) mudaram de interpretação entre R1 e R2. "
            "Confira os códigos de barras abaixo — são os pacientes suspeitos."
        )
        tab_mud = base.loc[base["Mudou_interp"],
                           ["ID", "R1", "R2", "Interp_R1", "Interp_R2"]].rename(
            columns={"ID": "Código de barras", "Interp_R1": "Interpretação R1",
                     "Interp_R2": "Interpretação R2"})
        st.dataframe(tab_mud.style.format(precision=3), use_container_width=True)
    else:
        st.success("Nenhuma amostra mudou de interpretação entre R1 e R2.")

    with st.expander("🔀 Matriz de transição (R1 → R2)"):
        st.caption("Quantas amostras foram de cada interpretação em R1 (linhas) para "
                   "cada interpretação em R2 (colunas). A diagonal são as que não mudaram.")
        st.dataframe(pd.crosstab(base["Interp_R1"], base["Interp_R2"],
                                 rownames=["R1"], colnames=["R2"]),
                     use_container_width=True)

# ---- Bloco 5: exportar ---------------------------------------------------- #
st.markdown("### 5 · Exportar resultados")
export = base.rename(columns={"ID": "Codigo_de_barras"}).copy()
d1, d2 = st.columns(2)
with d1:
    st.download_button("⬇️ Baixar tabela (Excel)", data=to_excel(export),
                       file_name="analise_repeticoes.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with d2:
    csv_bytes = export.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("⬇️ Baixar tabela (CSV)", data=csv_bytes,
                       file_name="analise_repeticoes.csv", mime="text/csv")
