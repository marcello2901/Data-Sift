# -*- coding: utf-8 -*-

# Versão 1.3 - Adição de texto de boas-vindas na tela da LGPD
import streamlit as st
import pandas as pd
import numpy as np
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
import io
import uuid
import copy

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Data Sift")

# --- CONSTANTES E DADOS ---
TERMO_LGPD = """
Esta ferramenta foi projetada para processar e filtrar dados de planilhas.
É possível que os arquivos carregados por você contenham dados pessoais sensíveis
(como nome completo, data de nascimento, CPF, informações de saúde, etc.), cujo tratamento é regulado pela
Lei Geral de Proteção de Dados (LGPD - Lei nº 13.709/2018).

É de sua inteira responsabilidade garantir que todos os dados utilizados nesta ferramenta estejam em
conformidade com a LGPD. Recomendamos fortemente que você utilize apenas dados previamente
anonimizados para proteger a privacidade dos titulares dos dados.

A responsabilidade sobre a natureza dos dados processados é exclusivamente sua.

Para prosseguir, você deve confirmar que os dados a serem utilizados foram devidamente tratados e anonimizados.
"""

MANUAL_CONTENT = {
    "Introdução": """**Bem-vindo à Data Sift - Ferramenta de Filtros de Planilhas!**

Este programa foi projetado para otimizar seu trabalho com grandes volumes de dados, oferecendo duas funcionalidades principais:

1.  **Filtragem:** Para limpar sua base de dados, removendo linhas que não são de seu interesse.
2.  **Estratificação:** Para dividir sua base de dados em subgrupos específicos.

Navegue pelos tópicos no menu à esquerda para aprender a usar cada parte da ferramenta.""",
    "1. Configurações Globais": """**1. Configurações Globais**

Esta seção, localizada no topo da janela, contém as configurações essenciais que são compartilhadas entre as duas ferramentas. Configure-as uma vez para usar em ambas as abas.

- **Selecionar Planilha...**
  Abre uma janela para selecionar o arquivo de dados de origem. Suporta os formatos `.xlsx`, `.xls` e `.csv`. Uma vez selecionado, o arquivo fica disponível para ambas as ferramentas.

- **Coluna Idade / Coluna Sexo**
  Campos para especificar o nome **exato** do cabeçalho da coluna em sua planilha. **Atenção:** O nome deve ser idêntico, incluindo maiúsculas e minúsculas (ex: "Idade" é diferente de "idade").

- **Valor para masculino / feminino**
  Campos para definir o valor exato que representa cada sexo na sua planilha (ex: 'M', 'Masculino'). É crucial para o funcionamento correto da "Ferramenta de Estratificação".

- **Formato de Saída**
  Menu de seleção para escolher o formato dos arquivos gerados. O padrão é `.csv`. Escolha `Excel (.xlsx)` para maior compatibilidade com o Microsoft Excel ou `CSV (.csv)` para um formato mais leve e universal.

- **Download do arquivo de saída**
  Após a planilha ser filtrada, um botão de download aparecerá, permitindo que o usuário baixe a planilha filtrada.""",
    "2. Ferramenta de Filtro": """**2. Ferramenta de Filtro**

O objetivo desta ferramenta é **"limpar"** sua planilha, **removendo** linhas que correspondam a critérios específicos. O resultado é um **único arquivo** contendo apenas os dados que "sobreviveram" aos filtros.

**Funcionamento das Regras de Exclusão**
Cada linha que você adiciona é uma condição para **remover** dados. Se uma linha da sua planilha corresponder a uma regra ativa, ela **será excluída** do arquivo final.

- **[✓] (Caixa de Ativação):** Liga ou desliga uma regra sem precisar apagá-la.

- **Coluna:** O nome da coluna onde o filtro será aplicado. **Dica:** você pode aplicar a mesma regra a várias colunas de uma vez, separando seus nomes por ponto e vírgula (`;`).

- **Operador e Valor:** Operadores ">", "<", "≥", "≤", "=", "Não é igual a" definem a lógica da regra. São utilizados para definir os intervalos que serão considerados para que os dados sejam **excluídos**.
**Dica: **a palavra-chave `vazio` é um recurso poderoso:
    - **Cenário 1: Excluir linhas com dados FALTANTES.**
        - **Configuração:** `Coluna: "Exame_X"`, `Operador: "é igual a"`, `Valor: "vazio"`.
    - **Cenário 2: Manter apenas linhas com dados EXISTENTES.**
        - **Configuração:** `Coluna: "Observações"`, `Operador: "Não é igual a"`, `Valor: "vazio"`.

- **Regra Composta (Caixa)** Expande a regra para criar condições `E` / `OU`, quando o usuário quer inserir intervalos para exclusão.

- **Condição:** Permite aplicar um filtro secundário. A regra principal só será aplicada às linhas que também atenderem às condições de sexo e/ou idade.

- **Ações:** O botão de `X` apaga a regra. O botão 'Clonar' duplica a regra.

- **Gerar Planilha Filtrada:** Inicia o processo. Um botão de download aparecerá ao final com o arquivo `Planilha_Filtrada_` com data e hora.""",
    "3. Ferramenta de Estratificação": """**3. Ferramenta de Estratificação**

Diferente do filtro, o objetivo desta ferramenta é **dividir** sua planilha em **vários arquivos menores**, onde cada arquivo representa um subgrupo de interesse (um "estrato").

**Funcionamento da Estratificação**

- **Opções de Estratificação por Sexo:**
  - **[✓] Masculino** e **[✓] Feminino:** Estas caixas controlam a divisão por sexo.

- **Definição das Faixas Etárias:**
  - Esta área serve **exclusivamente** para criar os estratos baseados em idade.

- **Gerar Planilhas Estratificadas:**
  - Inicia o processo de divisão. O número de arquivos gerados será o (`nº de faixas etárias` x `nº de sexos selecionados`).
  - **Confirmação:** Antes de iniciar, o programa perguntará se você está usando uma planilha já filtrada."""
}
DEFAULT_FILTERS = [
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '>', 'p_val2': '50', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '>', 'p_val2': '600', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ultra-PCR.ULTRAPCR', 'p_op1': '>', 'p_val1': '5', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.#HGB', 'p_op1': '<', 'p_val1': '7,0', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.LEUCO', 'p_op1': '>', 'p_val1': '11000', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Creatinina.CRE', 'p_op1': '>', 'p_val1': '1,5', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Creatinina.eTFG2021', 'p_op1': '<', 'p_val1': '60', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'HBGLI.HBGLI', 'p_op1': '>', 'p_val1': '6,5', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'GLICOSE.GLI', 'p_op1': '>', 'p_val1': '200', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '65', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'TSH.TSH', 'p_op1': '>', 'p_val1': '10', 'p_expand': True, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '0,01', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Idade', 'p_op1': '>', 'p_val1': '75', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSSV', 'p_op1': 'Não é igual a', 'p_val1': 'vazio', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSSB', 'p_op1': 'Não é igual a', 'p_val1': 'vazio', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSSP', 'p_op1': 'Não é igual a', 'p_val1': 'vazio', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
]

# --- CLASSES DE PROCESSAMENTO ---

@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '==', 'Não é igual a': '!=', '≥': '>=', '≤': '<='}
    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series): return series
        return pd.to_numeric(series.astype(str).str.replace(',', '.', regex=False), errors='coerce')
    def _build_single_mask(self, series: pd.Series, op: str, val: Any) -> pd.Series:
        if isinstance(val, str) and op == '==': return series.astype(str).str.strip().str.lower() == val.lower().strip()
        return eval(f"series {op} val")
    def _create_main_mask(self, df: pd.DataFrame, f: Dict, col: str) -> pd.Series:
        op1_ui, val1 = f.get('p_op1'), f.get('p_val1')
        op1 = self.OPERATOR_MAP.get(op1_ui, op1_ui)
        if val1 and val1.lower() == 'vazio':
            if op1 == '==': return df[col].isna() | (df[col].astype(str).str.strip() == '')
            if op1 == '!=': return df[col].notna() & (df[col].astype(str).str.strip() != '')
            return pd.Series([False] * len(df), index=df.index)
        try:
            if f.get('p_expand'):
                v1_num = float(str(val1).replace(',', '.'))
                op_central, op2_ui, val2 = f.get('p_op_central'), f.get('p_op2'), f.get('p_val2')
                op2 = self.OPERATOR_MAP.get(op2_ui, op2_ui); v2_num = float(str(val2).replace(',', '.'))
                if op_central == 'ENTRE':
                    min_val, max_val = sorted((v1_num, v2_num)); return df[col].between(min_val, max_val, inclusive='both')
                m1 = self._build_single_mask(df[col], op1, v1_num); m2 = self._build_single_mask(df[col], op2, v2_num)
                if op_central == 'E': return m1 & m2
                if op_central == 'OU': return m1 | m2
            else:
                v1_num = float(str(val1).replace(',', '.')); return self._build_single_mask(df[col], op1, v1_num)
        except (ValueError, TypeError): return pd.Series([False] * len(df), index=df.index)
    def _create_conditional_mask(self, df: pd.DataFrame, f: Dict, global_config: Dict) -> pd.Series:
        mascara_condicional = pd.Series(True, index=df.index)
        if not f.get('c_check'): return mascara_condicional
        col_idade = global_config.get('coluna_idade')
        if f.get('c_idade_check') and col_idade and col_idade in df.columns:
            df[col_idade] = self._safe_to_numeric(df[col_idade])
            try:
                op_idade1_ui, val_idade1 = f.get('c_idade_op1'), f.get('c_idade_val1')
                if op_idade1_ui and val_idade1:
                    op1 = self.OPERATOR_MAP.get(op_idade1_ui, op_idade1_ui); v1 = float(str(val_idade1).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op1, v1)
                op_idade2_ui, val_idade2 = f.get('c_idade_op2'), f.get('c_idade_val2')
                if op_idade2_ui and val_idade2:
                    op2 = self.OPERATOR_MAP.get(op_idade2_ui, op_idade2_ui); v2 = float(str(val_idade2).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op2, v2)
            except (ValueError, TypeError): pass
        col_sexo = global_config.get('coluna_sexo')
        if f.get('c_sexo_check') and col_sexo and col_sexo in df.columns:
            val_sexo_gui = f.get('c_sexo_val', '').lower().strip()
            if val_sexo_gui: mascara_condicional &= self._build_single_mask(df[col_sexo], '==', val_sexo_gui)
        return mascara_condicional
    def apply_filters(self, df: pd.DataFrame, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
        df_processado = df.copy()
        active_filters = [f for f in filters_config if f['p_check']]
        total_filters = len(active_filters)
        for i, f_config in enumerate(active_filters):
            progress = (i + 1) / total_filters
            col_name = f_config.get('p_col', 'Regra desconhecida')
            progress_bar.progress(progress, text=f"Aplicando filtro {i+1}/{total_filters}: '{col_name[:30]}...'")
            col_config_str = f_config.get('p_col', '')
            cols_to_check = [c.strip() for c in col_config_str.split(';')]
            for col in cols_to_check:
                if col in df_processado.columns:
                    is_numeric_filter = f_config.get('p_val1', '').lower() != 'vazio'
                    if is_numeric_filter: df_processado[col] = self._safe_to_numeric(df_processado[col])
            main_mask = pd.Series(True, index=df_processado.index)
            for sub_col in cols_to_check:
                if sub_col not in df_processado.columns:
                    main_mask = pd.Series(False, index=df_processado.index); break
                main_mask &= self._create_main_mask(df_processado, f_config, sub_col)
            conditional_mask = self._create_conditional_mask(df_processado, f_config, global_config)
            final_mask = main_mask & conditional_mask
            df_processado = df_processado[~final_mask]
        progress_bar.progress(1.0, text="Filtragem concluída!")
        return df_processado
    def apply_stratification(self, df: pd.DataFrame, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')
        if not (col_idade and col_idade in df.columns):
            st.error(f"Coluna de idade '{col_idade}' não encontrada na planilha."); return {}
        if not (col_sexo and col_sexo in df.columns):
            st.error(f"Coluna de sexo '{col_sexo}' não encontrada na planilha."); return {}
        df[col_idade] = self._safe_to_numeric(df[col_idade])
        age_strata = strata_config.get('ages', []); sex_strata = strata_config.get('sexes', [])
        final_strata_to_process = []
        if not age_strata and sex_strata:
            for sex_rule in sex_strata: final_strata_to_process.append({'age': None, 'sex': sex_rule})
        elif age_strata and not sex_strata:
            for age_rule in age_strata: final_strata_to_process.append({'age': age_rule, 'sex': None})
        else:
            for sex_rule in sex_strata:
                for age_rule in age_strata: final_strata_to_process.append({'age': age_rule, 'sex': sex_rule})
        total_files = len(final_strata_to_process); generated_dfs = {}
        for i, stratum in enumerate(final_strata_to_process):
            progress = (i + 1) / total_files
            combined_mask = pd.Series(True, index=df.index)
            age_rule = stratum.get('age'); sex_rule = stratum.get('sex')
            if age_rule:
                age_mask = pd.Series(True, index=df.index)
                if age_rule.get('op1') and age_rule.get('val1'):
                    op1 = self.OPERATOR_MAP.get(age_rule['op1'], age_rule['op1']); val1 = float(str(age_rule['val1']).replace(',', '.'))
                    age_mask &= eval(f"df['{col_idade}'] {op1} {val1}")
                if age_rule.get('op2') and age_rule.get('val2'):
                    op2 = self.OPERATOR_MAP.get(age_rule['op2'], age_rule['op2']); val2 = float(str(age_rule['val2']).replace(',', '.'))
                    age_mask &= eval(f"df['{col_idade}'] {op2} {val2}")
                combined_mask &= age_mask
            if sex_rule:
                sex_val = sex_rule.get('value')
                if sex_val: combined_mask &= self._build_single_mask(df[col_sexo], '==', sex_val)
            stratum_df = df[combined_mask]
            filename = self._generate_stratum_name(age_rule, sex_rule)
            progress_bar.progress(progress, text=f"Gerando estrato {i+1}/{total_files}: {filename}...")
            if not stratum_df.empty:
                generated_dfs[filename] = stratum_df
        progress_bar.progress(1.0, text="Estratificação concluída!")
        return generated_dfs
    def _generate_stratum_name(self, age_rule: Optional[Dict], sex_rule: Optional[Dict]) -> str:
        name_parts = []
        if age_rule:
            op1, val1 = age_rule.get('op1'), age_rule.get('val1')
            op2, val2 = age_rule.get('op2'), age_rule.get('val2')
            def get_int(val): return int(float(str(val).replace(',', '.')))
            if op1 and val1 and not (op2 and val2):
                v1 = get_int(val1)
                if op1 == '>': name_parts.append(f"Over_{v1}_years")
                elif op1 == '≥': name_parts.append(f"{v1}_and_over_years")
                elif op1 == '<': name_parts.append(f"Under_{v1}_years")
                elif op1 == '≤': name_parts.append(f"Up_to_{v1}_years")
            elif op1 and val1 and op2 and val2:
                v1_f, v2_f = float(str(val1).replace(',', '.')), float(str(val2).replace(',', '.'))
                ops = sorted([(v1_f, op1), (v2_f, op2)])
                low_val, low_op = ops[0]; high_val, high_op = ops[1]
                low_bound = int(low_val) if low_op == '≥' else int(low_val + 1) if low_op == '>' else int(low_val)
                high_bound = int(high_val) if high_op == '≤' else int(high_val - 1) if high_op == '<' else int(high_val)
                if low_bound > high_bound: name_parts.append("Invalid_range")
                else: name_parts.append(f"{low_bound}_to_{high_bound}_years")
        if sex_rule:
            sex_name = sex_rule.get('name')
            if sex_name: name_parts.append(sex_name)
        return "_".join(part for part in name_parts if part)

@st.cache_data
def load_dataframe(uploaded_file):
    if uploaded_file is None: return None
    try:
        if uploaded_file.name.endswith('.csv'):
            return pd.read_csv(uploaded_file, sep=';', decimal=',', encoding='latin-1')
        else:
            return pd.read_excel(uploaded_file, engine='openpyxl')
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}"); return None

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

def draw_filter_rules():
    st.markdown("""<style>.stButton>button {padding: 0.25rem 0.3rem; font-size: 0.8rem; white-space: nowrap;}</style>""", unsafe_allow_html=True)
    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5]) 
            rule['p_check'] = cols[0].checkbox(" ", value=rule.get('p_check', True), key=f"p_check_{rule['id']}", label_visibility="collapsed")
            rule['p_col'] = cols[1].text_input("Coluna", value=rule.get('p_col', ''), key=f"p_col_{rule['id']}", label_visibility="collapsed")
            ops = ["", ">", "<", "=", "Não é igual a", "≥", "≤"]
            rule['p_op1'] = cols[2].selectbox("Operador 1", ops, index=ops.index(rule['p_op1']) if rule.get('p_op1') in ops else 0, key=f"p_op1_{rule['id']}", label_visibility="collapsed")
            rule['p_val1'] = cols[3].text_input("Valor 1", value=rule.get('p_val1', ''), key=f"p_val1_{rule['id']}", label_visibility="collapsed")
            rule['p_expand'] = cols[4].checkbox("+", value=rule.get('p_expand', False), key=f"p_expand_{rule['id']}", label_visibility="collapsed")
            with cols[5]:
                if rule['p_expand']:
                    exp_cols = st.columns(3)
                    ops_central = ["E", "OU", "ENTRE"]
                    rule['p_op_central'] = exp_cols[0].selectbox("Lógica", ops_central, index=ops_central.index(rule['p_op_central']) if rule.get('p_op_central') in ops_central else 0, key=f"p_op_central_{rule['id']}", label_visibility="collapsed")
                    rule['p_op2'] = exp_cols[1].selectbox("Operador 2", ops, index=ops.index(rule.get('p_op2', '>')) if rule.get('p_op2') in ops else 0, key=f"p_op2_{rule['id']}", label_visibility="collapsed")
                    rule['p_val2'] = exp_cols[2].text_input("Valor 2", value=rule.get('p_val2', ''), key=f"p_val2_{rule['id']}", label_visibility="collapsed")
            with cols[6]:
                rule['c_check'] = st.checkbox("Condição", value=rule.get('c_check', False), key=f"c_check_{rule['id']}")
            action_cols = cols[7].columns(2)
            if action_cols[0].button("Clonar", key=f"clone_{rule['id']}"):
                new_rule = copy.deepcopy(rule)
                new_rule['id'] = str(uuid.uuid4())
                st.session_state.filter_rules.insert(i + 1, new_rule)
                st.rerun()
            if action_cols[1].button("X", key=f"del_filter_{rule['id']}"):
                st.session_state.filter_rules.pop(i)
                st.rerun()

            if rule['c_check']:
                with st.container():
                    cond_cols = st.columns([0.55, 0.5, 1, 3, 1, 3])
                    cond_cols[1].markdown("↳")
                    rule['c_idade_check'] = cond_cols[2].checkbox("Idade", value=rule.get('c_idade_check', False), key=f"c_idade_check_{rule['id']}")
                    with cond_cols[3]:
                        if rule['c_idade_check']:
                            age_cols = st.columns([1,1,0.2,1,1])
                            ops_idade = ["", ">", "<", "≥", "≤", "="]
                            rule['c_idade_op1'] = age_cols[0].selectbox("Op Idade 1", ops_idade, index=ops_idade.index(rule.get('c_idade_op1','>')) if rule.get('c_idade_op1') in ops_idade else 0, key=f"c_idade_op1_{rule['id']}", label_visibility="collapsed")
                            rule['c_idade_val1'] = age_cols[1].text_input("Val Idade 1", value=rule.get('c_idade_val1',''), key=f"c_idade_val1_{rule['id']}", label_visibility="collapsed")
                            age_cols[2].write("E")
                            rule['c_idade_op2'] = age_cols[3].selectbox("Op Idade 2", ops_idade, index=ops_idade.index(rule.get('c_idade_op2','<')) if rule.get('c_idade_op2') in ops_idade else 0, key=f"c_idade_op2_{rule['id']}", label_visibility="collapsed")
                            rule['c_idade_val2'] = age_cols[4].text_input("Val Idade 2", value=rule.get('c_idade_val2',''), key=f"c_idade_val2_{rule['id']}", label_visibility="collapsed")
                    rule['c_sexo_check'] = cond_cols[4].checkbox("Sexo", value=rule.get('c_sexo_check', False), key=f"c_sexo_check_{rule['id']}")
                    with cond_cols[5]:
                        if rule['c_sexo_check']:
                            rule['c_sexo_val'] = st.text_input("Valor Sexo", value=rule.get('c_sexo_val', ''), key=f"c_sexo_val_{rule['id']}", label_visibility="collapsed")
        st.markdown("---")

def draw_stratum_rules():
    st.markdown("""<style>.stButton>button {padding: 0.25rem 0.3rem; font-size: 0.8rem;}</style>""", unsafe_allow_html=True)
    for i, stratum_rule in enumerate(st.session_state.stratum_rules):
        with st.container():
            cols = st.columns([2, 1, 1, 0.5, 1, 1, 1])
            cols[0].write(f"**Faixa Etária {i+1}:**")
            ops = ["", ">", "<", "≥", "≤"]
            stratum_rule['op1'] = cols[1].selectbox("Operador 1", ops, index=ops.index(stratum_rule['op1']) if stratum_rule['op1'] in ops else 0, key=f"s_op1_{stratum_rule['id']}", label_visibility="collapsed")
            stratum_rule['val1'] = cols[2].text_input("Valor 1", value=stratum_rule['val1'], key=f"s_val1_{stratum_rule['id']}", label_visibility="collapsed")
            cols[3].markdown("<p style='text-align: center; margin-top: 25px;'>E</p>", unsafe_allow_html=True)
            stratum_rule['op2'] = cols[4].selectbox("Operador 2", ops, index=ops.index(stratum_rule['op2']) if stratum_rule['op2'] in ops else 0, key=f"s_op2_{stratum_rule['id']}", label_visibility="collapsed")
            stratum_rule['val2'] = cols[5].text_input("Valor 2", value=stratum_rule['val2'], key=f"s_val2_{stratum_rule['id']}", label_visibility="collapsed")
            if cols[6].button("X", key=f"del_stratum_{stratum_rule['id']}"):
                if len(st.session_state.stratum_rules) > 1:
                    st.session_state.stratum_rules.pop(i)
                    st.rerun()
                else:
                    st.warning("Não é possível excluir a última faixa.")
        st.markdown("---")

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    if not st.session_state.lgpd_accepted:
        st.title("Bem-vindo à Data Sift!")
        st.markdown("Este programa foi projetado para otimizar seu trabalho com grandes volumes de dados, oferecendo funcionalidades de exclusão de dados de planilhas a partir de filtros e estratificação da planilha filtrada. Leia os termos abaixo para prosseguir.")
        st.divider()
        st.header("Termos de Uso e Conformidade com a LGPD")
        st.markdown(TERMO_LGPD)
        accepted = st.checkbox("Ao confirmar, garanto que os dados inseridos estão anonimizados e que não há presença de dados sensíveis.")
        if st.button("Continuar", disabled=not accepted):
            st.session_state.lgpd_accepted = True
            st.rerun()
        return

    if 'filter_rules' not in st.session_state: st.session_state.filter_rules = [dict(r) for r in copy.deepcopy(DEFAULT_FILTERS)]
    if 'stratum_rules' not in st.session_state: st.session_state.stratum_rules = [{'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''}]
    
    with st.sidebar:
        st.title("Manual do Usuário")
        topic = st.selectbox("Selecione um tópico", list(MANUAL_CONTENT.keys()), label_visibility="collapsed")
        st.markdown(MANUAL_CONTENT[topic], unsafe_allow_html=True)

    st.title("Data Sift - Ferramenta de Filtros de Planilhas")

    with st.expander("1. Configurações Globais", expanded=True):
        uploaded_file = st.file_uploader("Selecione a planilha", type=['csv', 'xlsx', 'xls'])
        df = load_dataframe(uploaded_file)
        
        c1, c2, c3 = st.columns(3)
        with c1: st.text_input("Coluna Idade", value="Idade", key="col_idade"); st.text_input("Valor para masculino", value="Masculino", key="val_masculino")
        with c2: st.text_input("Coluna Sexo", value="Sexo", key="col_sexo"); st.text_input("Valor para feminino", value="Feminino", key="val_feminino")
        with c3: st.selectbox("Formato de Saída", ["CSV (.csv)", "Excel (.xlsx)"], key="output_format")

    tab_filter, tab_stratify = st.tabs(["2. Ferramenta de Filtro", "3. Ferramenta de Estratificação"])

    with tab_filter:
        st.header("Regras de Exclusão")
        draw_filter_rules()
        if st.button("Adicionar Nova Regra de Filtro"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '<', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OU', 'p_op2': '>', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''})
            st.rerun()
        if st.button("Gerar Planilha Filtrada", type="primary", use_container_width=True):
            if df is None: st.error("Por favor, carregue uma planilha primeiro.")
            else:
                with st.spinner("Aplicando filtros... Aguarde."):
                    progress_bar = st.progress(0, text="Iniciando...")
                    processor = get_data_processor()
                    global_config = {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}
                    filtered_df = processor.apply_filters(df, st.session_state.filter_rules, global_config, progress_bar)
                    st.success(f"Planilha filtrada com sucesso! {len(filtered_df)} linhas restantes.")
                    is_excel = "Excel" in st.session_state.output_format
                    file_bytes = to_excel(filtered_df) if is_excel else to_csv(filtered_df)
                    timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
                    file_name = f"Planilha_Filtrada_{timestamp}.{'xlsx' if is_excel else 'csv'}"
                    st.session_state.filtered_result = (file_bytes, file_name)
        if 'filtered_result' in st.session_state:
            st.download_button("Download da Planilha Filtrada", data=st.session_state.filtered_result[0], file_name=st.session_state.filtered_result[1], use_container_width=True)

    with tab_stratify:
        st.header("Opções de Estratificação")
        c1, c2, c3, c4 = st.columns([2,1,1,6])
        c1.write("Estratificar por sexo:"); stratify_male = c2.checkbox("Masculino", value=True); stratify_female = c3.checkbox("Feminino", value=True)
        st.header("Definição das Faixas Etárias")
        draw_stratum_rules()
        if st.button("Adicionar Faixa Etária"):
            st.session_state.stratum_rules.append({'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''})
            st.rerun()
        if st.button("Gerar Planilhas Estratificadas", type="primary", use_container_width=True):
            st.session_state.confirm_stratify = True
            st.rerun()
        if st.session_state.get('confirm_stratify', False):
            st.warning("Você confirma que a planilha selecionada é a versão FILTRADA?")
            c1, c2 = st.columns(2)
            if c1.button("Sim, continuar", use_container_width=True):
                if df is None: st.error("Por favor, carregue uma planilha primeiro.")
                else:
                    with st.spinner("Gerando estratos... Aguarde."):
                        progress_bar = st.progress(0, text="Iniciando...")
                        processor = get_data_processor()
                        age_rules = [r for r in st.session_state.stratum_rules if r.get('val1')]
                        sex_rules = []
                        if stratify_male:
                            val_m = st.session_state.val_masculino
                            if not val_m: st.error("Defina o valor para 'masculino' nas Configurações Globais."); st.stop()
                            sex_rules.append({'value': val_m, 'name': 'Male'})
                        if stratify_female:
                            val_f = st.session_state.val_feminino
                            if not val_f: st.error("Defina o valor para 'feminino' nas Configurações Globais."); st.stop()
                            sex_rules.append({'value': val_f, 'name': 'Female'})
                        strata_config = {'ages': age_rules, 'sexes': sex_rules}
                        global_config = {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}
                        stratified_dfs = processor.apply_stratification(df.copy(), strata_config, global_config, progress_bar)
                        st.session_state.stratified_results = stratified_dfs
                st.session_state.confirm_stratify = False; st.rerun()
            if c2.button("Não, cancelar", use_container_width=True):
                st.session_state.confirm_stratify = False; st.rerun()
        if st.session_state.get('stratified_results'):
            st.markdown("---"); st.subheader(f"Arquivos para Download ({len(st.session_state.stratified_results)} gerados)")
            is_excel = "Excel" in st.session_state.output_format
            for filename, df_to_download in st.session_state.stratified_results.items():
                file_bytes = to_excel(df_to_download) if is_excel else to_csv(df_to_download)
                file_name = f"{filename}.{'xlsx' if is_excel else 'csv'}"
                st.download_button(f"Download {file_name}", data=file_bytes, file_name=file_name)

if __name__ == "__main__":
    main()