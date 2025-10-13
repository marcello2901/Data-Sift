# -*- coding: utf-8 -*-

# Version 1.6 - Full English UI Translation
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

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Data Sift")

# --- CONSTANTS AND DATA (TRANSLATED) ---
GDPR_TERMS = """
This tool is designed to process and filter data from spreadsheets.
It is possible that the files you upload contain sensitive personal data
(such as full name, date of birth, CPF, health information, etc.), the processing of which is regulated by the
General Data Protection Regulation (GDPR).

It is your sole responsibility to ensure that all data used in this tool complies with the GDPR. We strongly recommend that you only use previously
anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been duly treated and anonymized.
"""

MANUAL_CONTENT = {
    "Introduction": """**Welcome to the Spreadsheet Filter Tool!**

This program is designed to optimize your work with large volumes of data by offering two main functionalities:

1.  **Filtering:** To clean your database by removing rows that are not of interest.
2.  **Stratification:** To divide your database into specific subgroups.

Navigate through the topics in the left menu to learn how to use each part of the tool.""",
    "1. Global Settings": """**1. Global Settings**

This section contains the essential settings that are shared between both tools. Configure them once to use in both tabs.

- **Select Spreadsheet...**
  Opens a window to select the source data file. Supports `.xlsx`, `.xls`, and `.csv` formats. Once selected, the file is available for both tools.

- **Age / Sex/Gender Column**
  Fields to **select** the exact header name of the column in your spreadsheet. The options in the list appear after uploading the file.

- **Sex/Gender (Values)**
  After selecting the "Sex/Gender" column, these two lists will be populated with the unique values from that column. Select which value corresponds to male and which to female for stratification.

- **Output Format**
  A selection menu to choose the format of the generated files. The default is `.csv`. Choose `Excel (.xlsx)` for better compatibility with Microsoft Excel or `CSV (.csv)` for a lighter, universal format.

- **Download output file**
  After the spreadsheet is filtered, a download button will appear, allowing the user to download the filtered sheet.""",
    "2. Filter Tool": """**2. Filter Tool**

The purpose of this tool is to **"clean"** your spreadsheet by **removing** rows that match specific criteria. The result is a **single file** containing only the data that "survived" the filters.

**How Exclusion Rules Work**
Each row you add is a condition to **remove** data. If a row in your spreadsheet matches an active rule, it **will be excluded** from the final file.

- **[✓] (Activation Checkbox):** Toggles a rule on or off without deleting it.

- **Column:** The name of the column where the filter will be applied. **Tip:** you can apply the same rule to multiple columns at once by separating their names with a semicolon (`;`).

- **Operator and Value:** Operators ">", "<", "≥", "≤", "=", "Not equal to" define the rule's logic. They are used to define the ranges that will be considered for data **exclusion**.
**Tip:** the keyword `vazio` (empty) is a powerful feature:
    - **Scenario 1: Exclude rows with MISSING data.**
        - **Configuration:** `Column: "Exam_X"`, `Operator: "is equal to"`, `Value: "vazio"`.
    - **Scenario 2: Keep only rows with EXISTING data.**
        - **Configuration:** `Column: "Observations"`, `Operator: "Not equal to"`, `Value: "vazio"`.

- **Compound Logic (Checkbox)** Expands the rule to create `AND` / `OR` conditions, for when the user wants to set exclusion ranges.

- **Condition:** Allows applying a secondary filter. The main rule will only be applied to rows that also meet the sex and/or age conditions.

- **Actions:** The `X` button deletes the rule. The 'Clone' button duplicates it.

- **Generate Filtered Sheet:** Starts the process. A download button will appear at the end with the file `Filtered_Sheet_` with a timestamp.""",
    "3. Stratification Tool": """**3. Stratification Tool**

Unlike the filter, the purpose of this tool is to **split** your spreadsheet into **multiple smaller files**, where each file represents a subgroup of interest (a "stratum").

**How Stratification Works**

- **Stratification Options by Sex/Gender:**
  - After loading a spreadsheet and selecting the "Sex/Gender" column in the Global Settings, this area will display a checkbox for each unique value found (e.g., Male, Female, etc.). Check the ones you want to include in the stratification.

- **Age Range Definitions:**
  - This area is used **exclusively** to create age-based strata.

- **Generate Stratified Sheets:**
  - Starts the splitting process. The number of generated files will be (`number of age ranges` x `number of selected genders`).
  - **Confirmation:** Before starting, the program will ask if you are using an already filtered spreadsheet."""
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
            sex_name = str(sex_rule.get('value', '')).replace(' ', '_')
            if sex_name: name_parts.append(sex_name)
        return "_".join(part for part in name_parts if part)

# --- FUNÇÕES AUXILIARES ---

@st.cache_data
def load_dataframe(uploaded_file):
    if uploaded_file is None: return None
    try:
        if uploaded_file.name.endswith('.csv'):
            try:
                return pd.read_csv(uploaded_file, sep=';', decimal=',', encoding='latin-1')
            except Exception:
                uploaded_file.seek(0)
                return pd.read_csv(uploaded_file, sep=',', decimal='.', encoding='utf-8')
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

# --- FUNÇÕES DE INTERFACE ---

def draw_filter_rules(sex_column_values):
    st.markdown("""<style>
        .stButton>button { padding: 0.25rem 0.3rem; font-size: 0.8rem; white-space: nowrap; }
        div[data-testid="stTextInput"] input, div[data-testid="stSelectbox"] div[data-baseweb="select"] {
            border: 1px solid rgba(255, 75, 75, 0.15) !important;
            border-radius: 0.25rem;
        }
    </style>""", unsafe_allow_html=True)
    
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5])
    header_cols[1].markdown("**Coluna** <span title='Insira o nome da coluna.'>&#9432;</span>", unsafe_allow_html=True)
    header_cols[2].markdown("**Operador** <span title='Utilize os operadores de comparação para definir o primeiro filtro.'>&#9432;</span>", unsafe_allow_html=True)
    header_cols[3].markdown("**Valor** <span title='Insira o valor que deseja excluir dos dados.'>&#9432;</span>", unsafe_allow_html=True)
    
    tooltip_text = """Selecione mais um operador para definir um intervalo.
Como utilizar:
ENTRE: Exclui valores dentro do intervalo (inclusive). Ex: ENTRE 10 e 20 remove tudo de 10 a 20.
OU: Exclui valores fora de um intervalo. Use para manter os dados que estão no meio. Ex: < 10 OU > 20 remove tudo que for menor que 10 e maior que 20.
E: Exclui valores dentro de um intervalo, sem os extremos. Ex: > 10 E < 20 remove de 11 a 19 (mantém os valores 10 e 20).
"""
    tooltip_text_html = tooltip_text.replace('\n', '&#10;')
    header_cols[5].markdown(f"**Lógica Composta** <span title='{tooltip_text_html}'>&#9432;</span>", unsafe_allow_html=True)
    
    header_cols[6].markdown("**Condição** <span title='Ative a opção de filtrar por idade ou sexo esta coluna em específico'>&#9432;</span>", unsafe_allow_html=True)
    header_cols[7].markdown("**Ações** <span title='Utilize para duplicar ou excluir uma regra'>&#9432;</span>", unsafe_allow_html=True)
    st.markdown("<hr style='margin-top: -0.5rem; margin-bottom: 0.5rem;'>", unsafe_allow_html=True)

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
                            rule['c_sexo_val'] = st.selectbox("Valor Sexo", options=sex_column_values, index=sex_column_values.index(rule.get('c_sexo_val')) if rule.get('c_sexo_val') in sex_column_values else 0, key=f"c_sexo_val_{rule['id']}", label_visibility="collapsed")
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

    st.title("Data Sift")

    with st.expander("1. Configurações Globais", expanded=True):
        uploaded_file = st.file_uploader("Selecione a planilha", type=['csv', 'xlsx', 'xls'])
        df = load_dataframe(uploaded_file)
        
        column_options = []
        if df is not None: column_options = [""] + df.columns.tolist()
        
        c1, c2, c3 = st.columns(3)
        with c1: 
            st.selectbox("Coluna Idade", options=column_options, key="col_idade")
        with c2: 
            st.selectbox("Sexo/Gênero", options=column_options, key="col_sexo")
        with c3: 
            st.selectbox("Formato de Saída", ["CSV (.csv)", "Excel (.xlsx)"], key="output_format")

        sex_column_values = []
        if df is not None and st.session_state.col_sexo:
            try:
                sex_column_values = [""] + df[st.session_state.col_sexo].dropna().unique().tolist()
            except KeyError:
                st.warning(f"Coluna '{st.session_state.col_sexo}' não encontrada. Selecione a coluna correta.")

    tab_filter, tab_stratify = st.tabs(["2. Ferramenta de Filtro", "3. Ferramenta de Estratificação"])

    with tab_filter:
        st.header("Regras de Exclusão")
        draw_filter_rules(sex_column_values)
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
        st.header("Opções de Estratificação por Sexo/Gênero")
        if not sex_column_values:
            st.info("Carregue uma planilha e selecione a coluna 'Sexo/Gênero' nas Configurações Globais para ver as opções.")
        else:
            if 'strat_gender_selection' not in st.session_state:
                st.session_state.strat_gender_selection = {val: True for val in sex_column_values if val}
            
            cols = st.columns(min(len(sex_column_values), 5))
            col_idx = 0
            for gender_val in sex_column_values:
                if not gender_val: continue
                st.session_state.strat_gender_selection[gender_val] = cols[col_idx].checkbox(str(gender_val), value=st.session_state.strat_gender_selection.get(gender_val, True), key=f"strat_check_{gender_val}")
                col_idx = (col_idx + 1) % len(cols)

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
                        for gender_val, is_selected in st.session_state.get('strat_gender_selection', {}).items():
                            if is_selected:
                                sex_rules.append({'value': gender_val, 'name': str(gender_val)})
                        
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