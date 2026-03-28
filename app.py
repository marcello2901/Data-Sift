# -*- coding: utf-8 -*-

# Versão 2.3.0 - Atualização: Relatório Automático de Filtros (Metadata)
# Melhorias: Processamento DuckDB, Preservação de Precisão e Registro Automático de Critérios.

import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
import zipfile
import duckdb
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
import tempfile
import os
import shutil

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Data Sift")
st.markdown("""
    <style>
        /* Remove o esmaecimento da tela ao clicar nos filtros */
        [data-testid="stAppViewBlockContainer"] {
            opacity: 1 !important;
            transition: none !important;
        }
        /* Esconde o aviso "Running..." no canto superior direito */
        [data-testid="stStatusWidget"] {
            visibility: hidden;
        }
    </style>
""", unsafe_allow_html=True)

# --- CONSTANTES E DADOS ---
GDPR_TERMS = """
This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, national ID numbers, health information, etc.), the processing of which is regulated by data protection laws like the General Data Protection Regulation (GDPR or LGPD).

It is your sole responsibility to ensure that all data used in this tool complies with applicable data protection regulations. We strongly recommend that you only use previously anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been properly handled and anonymized.
"""

MANUAL_CONTENT = {
    "Introduction": """**Welcome to Data Sift!**

This program is a spreadsheet filter tool designed to optimize your work with large volumes of data by offering two main functionalities:

1.  **Filtering:** To clean your database by removing rows that are not of interest.
2.  **Stratification:** To divide your database into specific subgroups.

Navigate through the topics in the menu above to learn how to use each part of the tool.""",
    "1. Global Settings": """**1. Global Settings**

This section contains the essential settings that are shared between both tools.

- **Select Spreadsheet:**
  Opens a window to select the source data file. It supports `.xlsx`, `.xls`, and `.csv` formats. Once selected, the file becomes available for both tools.

- **Age Column / Sex/Gender:**
  Fields to **select** the column name in your spreadsheet. The options in the list appear after the file is uploaded.

- **Output Format:**
  A selection menu to choose the format of the generated files. The default is `.csv`. Choose `Excel (.xlsx)` for better compatibility with Microsoft Excel or `CSV Bundle (.zip)` for a lighter format with metadata file included.
  """,
    "2. Filter Tool": """**2. Filter Tool**

The purpose of this tool is to **"clean"** your spreadsheet by **removing** rows that match specific criteria. The result is a **single file** containing only the data that "survived" the filters.

**How Exclusion Rules Work:**
Each row you add is a condition to **remove** data. If a row in your spreadsheet matches an active rule, it **will be excluded** from the final file.

- **[✓] (Activation Checkbox):** Toggles a rule on or off without deleting it.

- **Column:** The name of the column where the filter will be applied. **Tip:** You can apply the rule to multiple columns at once by separating their names with a semicolon (;). When doing so, a row will be excluded only if **all** specified columns meet the condition.

- **Operator and Value:** Operators ">", "<", "≥", "≤", "=", "Not equal to" define the rule's logic. They are used to define the ranges that will be considered for data **exclusion**.
**Tip:** The keyword `empty` is a powerful feature:
    - **Scenario 1: Exclude rows with MISSING data.**
        - **Configuration:** Column: `"Exam_X"`, Operator: `"is equal to"`, Value: `"empty"`.
    - **Scenario 2: Keep only rows with EXISTING data.**
        - **Configuration:** Column: `"Observations"`, Operator: `"Not equal to"`, Value: `"empty"`.

- **Compound Logic:** Expands the rule to create `AND` / `OR` conditions for when the user wants to set exclusion ranges.

- **Condition:** Allows applying a secondary filter. The main rule will only be applied to rows that also meet the specified sex and/or age conditions.

- **Actions:** The `X` button deletes the rule. The 'Clone' button duplicates it.

- **Generate Filtered Sheet:** Starts the process. A download button will appear at the end with the `Filtered_Sheet_` file with a timestamp.""",
    "3. Stratification Tool": """**3. Stratification Tool**

Unlike the filter, the purpose of this tool is to **split** your spreadsheet into **multiple smaller files**, where each file represents a subgroup of interest (a "stratum").

**How Stratification Works:**

- **Stratification Options by Sex/Gender:**
  - After loading a spreadsheet and selecting the "Sex/Gender" column in the Global Settings, this area will display a checkbox for each unique value found (e.g., Male, Female, etc.). Check the ones you want to include in the stratification.

- **Age Range Definitions:**
  - This area is used **exclusively** to create age-based strata.

- **Generate Stratified Sheets:**
  - Starts the splitting process. The number of generated files will be (`number of age ranges` x `number of selected genders`).
  - **Confirmation:** Before starting, the program will ask if you are using an already filtered spreadsheet."""
}

DEFAULT_FILTERS = [
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '50', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '600', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ultra-PCR.ULTRAPCR', 'p_op1': '>', 'p_val1': '5', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Creatinina.CRE', 'p_op1': '>', 'p_val1': '1,5', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Creatinina.eTFG2021', 'p_op1': '<', 'p_val1': '60', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'GLICOSE.GLI', 'p_op1': '<', 'p_val1': '65', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '200', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'HBGLI.HBGLI', 'p_op1': '>', 'p_val1': '6,5', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'TSH.TSH', 'p_op1': '<', 'p_val1': '0,2', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '10', 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'TGP.TGP', 'p_op1': '>', 'p_val1': '41', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'BTF.BTBTF', 'p_op1': '>', 'p_val1': '2,4', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'FALC.FALC', 'p_op1': '>', 'p_val1': '129', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'GGT.GGT', 'p_op1': '>', 'p_val1': '60', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.COL2', 'p_op1': '>', 'p_val1': '190', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.COL2', 'p_op1': '>', 'p_val1': '190', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.TRI2', 'p_op1': '>', 'p_val1': '150', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.TRI2', 'p_op1': '>', 'p_val1': '150', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.LDL2', 'p_op1': '>', 'p_val1': '130', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.LDLD', 'p_op1': '>', 'p_val1': '130', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'LIPIDOGRAMA.HDL5', 'p_op1': '>', 'p_val1': '80', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'COLESTEROL TOTAL E FRACOES.HDL5', 'p_op1': '>', 'p_val1': '80', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSSV', 'p_op1': 'Not equal to', 'p_val1': 'empty', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSSB', 'p_op1': 'Not equal to', 'p_val1': 'empty', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSPLT', 'p_op1': 'Not equal to', 'p_val1': 'empty', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'TGO.TGO', 'p_op1': '>', 'p_val1': '40', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.LEUCO', 'p_op1': '>', 'p_val1': '11000', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.#HGB', 'p_op1': '<', 'p_val1': '7', 'p_expand': False, 'c_check': False},
]

# --- CLASSES DE PROCESSAMENTO (DUCKDB OTIMIZADO) ---

@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '=', '==': '=', 'Não é igual a': '!=', '≥': '>=', '≤': '<=', 'is equal to': '=', 'Not equal to': '!='}

    def _build_single_sql_cond(self, col: str, op: str, val: Any) -> str:
        """Gera a cláusula SQL condicional para um único valor."""
        op = self.OPERATOR_MAP.get(op, op)

        # Trata o cenário de vazio ('empty')
        if str(val).lower() == 'empty':
            if op in ('=', '=='): return f'("{col}" IS NULL OR TRIM(CAST("{col}" AS VARCHAR)) = \'\')'
            if op == '!=': return f'("{col}" IS NOT NULL AND TRIM(CAST("{col}" AS VARCHAR)) != \'\')'
            return "FALSE"

        # Tenta interpretar como número (permite comparar strings que na verdade são números na planilha)
        try:
            v_num = float(str(val).replace(',', '.'))
            safe_cast = f'TRY_CAST(REPLACE(CAST("{col}" AS VARCHAR), \',\', \'.\') AS DOUBLE)'
            return f'({safe_cast} IS NOT NULL AND {safe_cast} {op} {v_num})'
        except ValueError:
            # Tratamento de Strings
            v_str = str(val).replace("'", "''").lower().strip()
            return f'(CAST("{col}" AS VARCHAR) IS NOT NULL AND LOWER(TRIM(CAST("{col}" AS VARCHAR))) {op} \'{v_str}\')'

    def _create_main_sql(self, f: Dict, col: str) -> str:
        """Cria o SQL da regra principal (com suporte a lógica expandida)."""
        op1, val1 = f.get('p_op1'), f.get('p_val1')
        
        if not f.get('p_expand'):
            return self._build_single_sql_cond(col, op1, val1)

        op_central = f.get('p_op_central', '').upper()
        op2, val2 = f.get('p_op2'), f.get('p_val2')

        if op_central == 'BETWEEN':
            try:
                v1_num = float(str(val1).replace(',', '.'))
                v2_num = float(str(val2).replace(',', '.'))
                min_v, max_v = sorted([v1_num, v2_num])
                safe_cast = f'TRY_CAST(REPLACE(CAST("{col}" AS VARCHAR), \',\', \'.\') AS DOUBLE)'
                return f'({safe_cast} IS NOT NULL AND {safe_cast} BETWEEN {min_v} AND {max_v})'
            except ValueError:
                return "FALSE"

        cond1 = self._build_single_sql_cond(col, op1, val1)
        cond2 = self._build_single_sql_cond(col, op2, val2)
        return f"({cond1} {op_central} {cond2})"

    def _create_conditional_sql(self, f: Dict, global_config: Dict) -> str:
        """Cria o SQL das condições secundárias (Idade/Sexo)."""
        if not f.get('c_check'): return "TRUE"
        conds = []

        col_idade = global_config.get('coluna_idade')
        if f.get('c_idade_check') and col_idade:
            op1, val1 = f.get('c_idade_op1'), f.get('c_idade_val1')
            if op1 and val1: conds.append(self._build_single_sql_cond(col_idade, op1, val1))
            
            op2, val2 = f.get('c_idade_op2'), f.get('c_idade_val2')
            if op2 and val2: conds.append(self._build_single_sql_cond(col_idade, op2, val2))

        col_sexo = global_config.get('coluna_sexo')
        if f.get('c_sexo_check') and col_sexo:
            val_sexo = f.get('c_sexo_val')
            if val_sexo:
                conds.append(self._build_single_sql_cond(col_sexo, '=', val_sexo))

        return " AND ".join(conds) if conds else "TRUE"

    def apply_filters(self, df: pd.DataFrame, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
        """Executa a lógica de Exclusão usando o DuckDB para alta performance."""
        active_filters = [f for f in filters_config if f['p_check']]
        
        if not active_filters:
            progress_bar.progress(1.0, text="Nenhum filtro ativo.")
            return df

        exclusion_clauses = []
        
        for i, f_config in enumerate(active_filters):
            progress_bar.progress((i + 1) / len(active_filters), text=f"Mapeando regra SQL {i+1}...")
            
            col_config_str = f_config.get('p_col', '')
            cols_to_check = [c.strip() for c in col_config_str.split(';') if c.strip()]
            
            if not cols_to_check: continue

            main_conds = []
            for sub_col in cols_to_check:
                if sub_col in df.columns:
                    main_conds.append(self._create_main_sql(f_config, sub_col))
                else:
                    main_conds.append("FALSE")

            combined_main_sql = " AND ".join([f"({c})" for c in main_conds]) if main_conds else "FALSE"
            cond_sql = self._create_conditional_sql(f_config, global_config)

            # Lógica de exclusão: NOT (Regra)
            rule_sql = f"({combined_main_sql}) AND ({cond_sql})"
            exclusion_clauses.append(f"NOT ({rule_sql})")

        if not exclusion_clauses:
            return df

        where_clause = " AND ".join(exclusion_clauses)
        query = f"SELECT * FROM df WHERE {where_clause}"

        try:
            progress_bar.progress(0.8, text="Executando Motor DuckDB (SQL)...")
            filtered_df = duckdb.query(query).df()
            progress_bar.progress(1.0, text="Filtering complete!")
            return filtered_df
        except Exception as e:
            st.error(f"Erro no processamento SQL: {e}")
            return df
    
    def apply_stratification(self, df: pd.DataFrame, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        """Divide o banco em sub-planilhas usando DuckDB."""
        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')

        if not (col_idade and col_idade in df.columns) or not (col_sexo and col_sexo in df.columns):
            st.error("Colunas de Idade/Sexo não configuradas."); return {}

        age_strata = strata_config.get('ages', [])
        sex_strata = strata_config.get('sexes', [])

        final_strata_to_process = []
        if not age_strata and sex_strata:
            for sex_rule in sex_strata: final_strata_to_process.append({'age': None, 'sex': sex_rule})
        elif age_strata and not sex_strata:
            for age_rule in age_strata: final_strata_to_process.append({'age': age_rule, 'sex': None})
        else:
            for sex_rule in sex_strata:
                for age_rule in age_strata:
                    final_strata_to_process.append({'age': age_rule, 'sex': sex_rule})

        total_files = len(final_strata_to_process)
        generated_dfs = {}

        for i, stratum in enumerate(final_strata_to_process):
            progress = (i + 1) / total_files
            conditions = []

            age_rule = stratum.get('age')
            if age_rule:
                if age_rule.get('op1') and age_rule.get('val1'):
                    conditions.append(self._build_single_sql_cond(col_idade, age_rule['op1'], age_rule['val1']))
                if age_rule.get('op2') and age_rule.get('val2'):
                    conditions.append(self._build_single_sql_cond(col_idade, age_rule['op2'], age_rule['val2']))

            sex_rule = stratum.get('sex')
            if sex_rule and sex_rule.get('value'):
                conditions.append(self._build_single_sql_cond(col_sexo, '=', sex_rule['value']))

            where_clause = " AND ".join([f"({c})" for c in conditions]) if conditions else "TRUE"
            filename = self._generate_stratum_name(age_rule, sex_rule)
            progress_bar.progress(progress, text=f"Gerando estrato: {filename}...")
            
            try:
                stratum_df = duckdb.query(f"SELECT * FROM df WHERE {where_clause}").df()
                if not stratum_df.empty:
                    generated_dfs[filename] = stratum_df
            except Exception:
                pass

        progress_bar.progress(1.0, text="Stratification complete!")
        return generated_dfs

    def _generate_stratum_name(self, age_rule: Optional[Dict], sex_rule: Optional[Dict]) -> str:
        name_parts = []
        if age_rule:
            def get_int(val): 
                try: return int(float(str(val).replace(',', '.')))
                except: return None
            v1, v2 = get_int(age_rule.get('val1')), get_int(age_rule.get('val2'))
            if age_rule.get('op1') and not age_rule.get('op2'):
                name_parts.append(f"{age_rule.get('op1')}_{v1}_years")
            else:
                name_parts.append(f"{v1}_to_{v2}_years")
        if sex_rule:
            name_parts.append(str(sex_rule.get('value', '')).replace(' ', '_'))
        return "_".join(name_parts)

# --- FUNÇÕES DE METADADOS (REGISTRO AUTOMÁTICO) ---

def generate_filter_description(filters_config: List[Dict]) -> str:
    """Transforma as regras técnicas em um texto legível para o relatório."""
    active = [f for f in filters_config if f['p_check']]
    if not active:
        return "No exclusion filters applied."
    
    descriptions = []
    for f in active:
        col = f.get('p_col', 'Unknown')
        val1 = str(f.get('p_val1'))
        op1 = f.get('p_op1')
        
        # Tradução da lógica principal
        if val1.lower() == 'empty':
            main_text = "is empty" if op1 in ['=', '==', 'is equal to'] else "is not empty"
        elif f.get('p_expand'):
            main_text = f"{op1} {val1} {f.get('p_op_central')} {f.get('p_op2')} {f.get('p_val2')}"
        else:
            main_text = f"{op1} {val1}"
            
        # Tradução da condição (se houver)
        cond_text = ""
        if f.get('c_check'):
            c_parts = []
            if f.get('c_idade_check'):
                c_parts.append(f"Age {f.get('c_idade_op1')} {f.get('c_idade_val1')}")
            if f.get('c_sexo_check'):
                c_parts.append(f"Sex: {f.get('c_sexo_val')}")
            if c_parts:
                cond_text = f" (Applied only if: {' and '.join(c_parts)})"
                
        descriptions.append(f"{col}: {main_text}{cond_text}")
        
    return "; ".join(descriptions)

# --- FUNÇÕES AUXILIARES OTIMIZADAS ---

def fix_column_names(df):
    """Corrige caracteres especiais corrompidos nos nomes das colunas."""
    def clean(t):
        if not isinstance(t, str): return t
        try: return t.encode('latin-1').decode('utf-8')
        except: return t
    df.columns = [clean(c) for c in df.columns]
    return df

@st.cache_data(show_spinner="Lendo arquivo...")
def load_dataframe(uploaded_file):
    if uploaded_file is None: return None
    try:
        file_name = uploaded_file.name.lower()
        uploaded_file.seek(0)
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file_name)[1]) as tmp_file:
            shutil.copyfileobj(uploaded_file, tmp_file)
            tmp_path = tmp_file.name

        df = None
        if file_name.endswith('.zip'):
            with zipfile.ZipFile(tmp_path) as z:
                valid = [f for f in z.namelist() if not f.startswith('__MACOSX/') and f.lower().endswith(('.csv', '.xlsx', '.xls'))]
                if not valid: return None
                with z.open(valid[0]) as f:
                    content = f.read()
                if valid[0].lower().endswith('.csv'):
                    try: df = pd.read_csv(io.BytesIO(content), sep=';', decimal=',', encoding='utf-8', engine='pyarrow')
                    except: df = pd.read_csv(io.BytesIO(content), sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
                else: df = pd.read_excel(io.BytesIO(content), engine='openpyxl')
        elif file_name.endswith('.csv'):
            try: df = pd.read_csv(tmp_path, sep=';', decimal=',', encoding='utf-8', engine='pyarrow')
            except: df = pd.read_csv(tmp_path, sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
        else:
            df = pd.read_excel(tmp_path, engine='openpyxl')

        if os.path.exists(tmp_path): os.remove(tmp_path)
        if df is not None:
            df = fix_column_names(df)
            for col in df.select_dtypes('object').columns:
                if df[col].nunique() / len(df[col]) < 0.5: df[col] = df[col].astype('category')
        return df
    except Exception as e:
        st.error(f"Erro: {e}"); return None

@st.cache_data(show_spinner="Preparando Excel com Metadata...")
def to_excel_with_metadata(df, filters_config):
    """Gera Excel com aba de dados e aba de critérios."""
    output = io.BytesIO()
    description = generate_filter_description(filters_config)
    meta_df = pd.DataFrame({
        "Export Date": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        "Exclusion Criteria Used": [description]
    })
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Filtered Data')
        meta_df.to_excel(writer, index=False, sheet_name='Filter Metadata')
    return output.getvalue()

@st.cache_data(show_spinner="Preparando ZIP com Metadata...")
def to_zip_with_metadata(df, filters_config):
    """Gera ZIP com CSV e arquivo de texto de critérios."""
    output = io.BytesIO()
    description = generate_filter_description(filters_config)
    csv_data = df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig')
    with zipfile.ZipFile(output, 'w') as zf:
        zf.writestr("Filtered_Data.csv", csv_data)
        zf.writestr("Exclusion_Criteria.txt", description)
    return output.getvalue()

# --- FUNÇÕES DE INTERFACE ---

def handle_select_all():
    new_state = st.session_state['select_all_master_checkbox']
    for rule in st.session_state.filter_rules: rule['p_check'] = new_state

def reset_results_on_upload():
    for key in ['filtered_result', 'stratified_results', 'dados_salvos', 'id_arquivo_atual']:
        if key in st.session_state: del st.session_state[key]
    st.session_state.confirm_stratify = False

def draw_filter_rules(sex_column_values, column_options): 
    st.markdown("""<style>
        .stButton>button { padding: 0.25rem 0.3rem; font-size: 0.8rem; white-space: nowrap; }
    </style>""", unsafe_allow_html=True)
    
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium")
    all_checked = all(rule.get('p_check', False) for rule in st.session_state.filter_rules) if st.session_state.filter_rules else False
    header_cols[0].checkbox("All", value=all_checked, key='select_all_master_checkbox', on_change=handle_select_all, label_visibility="collapsed")
    header_cols[1].markdown("**Column**")
    header_cols[2].markdown("**Operator**")
    header_cols[3].markdown("**Value**")
    header_cols[5].markdown("**Compound Logic**")
    header_cols[6].markdown("**Condition**")
    header_cols[7].markdown("**Actions**")
    st.markdown("<hr style='margin-top: -0.5rem; margin-bottom: 0.5rem;'>", unsafe_allow_html=True)

    ops_main = ["", ">", "<", "=", "Not equal to", "≥", "≤"]
    ops_age = ["", ">", "<", "≥", "≤", "="]
    ops_central_logic = ["AND", "OR", "BETWEEN"]

    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium") 
            rule['p_check'] = cols[0].checkbox(f"c_{rule['id']}", value=rule.get('p_check', True), key=f"p_check_{rule['id']}", label_visibility="collapsed")
            rule['p_col'] = cols[1].text_input("Col", value=rule.get('p_col', ''), key=f"p_col_{rule['id']}", label_visibility="collapsed")
            rule['p_op1'] = cols[2].selectbox("O1", ops_main, index=ops_main.index(rule.get('p_op1', '=')) if rule.get('p_op1') in ops_main else 0, key=f"p_op1_{rule['id']}", label_visibility="collapsed")
            rule['p_val1'] = cols[3].text_input("V1", value=rule.get('p_val1', ''), key=f"p_val1_{rule['id']}", label_visibility="collapsed")
            rule['p_expand'] = cols[4].checkbox("+", value=rule.get('p_expand', False), key=f"p_expand_{rule['id']}", label_visibility="collapsed")
            with cols[5]:
                if rule['p_expand']:
                    exp = st.columns([3, 2, 2])
                    rule['p_op_central'] = exp[0].selectbox("L", ops_central_logic, index=0, key=f"p_op_central_{rule['id']}", label_visibility="collapsed")
                    rule['p_op2'] = exp[1].selectbox("O2", ops_main, index=0, key=f"p_op2_{rule['id']}", label_visibility="collapsed")
                    rule['p_val2'] = exp[2].text_input("V2", value=rule.get('p_val2', ''), key=f"p_val2_{rule['id']}", label_visibility="collapsed")
            rule['c_check'] = cols[6].checkbox("Cond", value=rule.get('c_check', False), key=f"c_check_{rule['id']}")
            actions = cols[7].columns(2)
            if actions[0].button("Clone", key=f"cl_{rule['id']}"):
                nr = copy.deepcopy(rule); nr['id'] = str(uuid.uuid4())
                st.session_state.filter_rules.insert(i + 1, nr); st.rerun()
            if actions[1].button("X", key=f"dl_{rule['id']}"):
                st.session_state.filter_rules.pop(i); st.rerun()
            
            if rule['c_check']:
                with st.container():
                    c_cols = st.columns([0.55, 0.5, 1, 3, 1, 3])
                    rule['c_idade_check'] = c_cols[2].checkbox("Age", value=rule.get('c_idade_check', False), key=f"c_idade_check_{rule['id']}")
                    if rule['c_idade_check']:
                        a_c = c_cols[3].columns([2, 2, 1, 2, 2])
                        rule['c_idade_op1'] = a_c[0].selectbox("AO1", ops_age, index=0, key=f"c_idade_op1_{rule['id']}", label_visibility="collapsed")
                        rule['c_idade_val1'] = a_c[1].text_input("AV1", key=f"c_idade_val1_{rule['id']}", label_visibility="collapsed")
                        rule['c_idade_op2'] = a_c[3].selectbox("AO2", ops_age, index=0, key=f"c_idade_op2_{rule['id']}", label_visibility="collapsed")
                        rule['c_idade_val2'] = a_c[4].text_input("AV2", key=f"c_idade_val2_{rule['id']}", label_visibility="collapsed")
                    rule['c_sexo_check'] = c_cols[4].checkbox("Sex", value=rule.get('c_sexo_check', False), key=f"c_sexo_check_{rule['id']}")
                    if rule['c_sexo_check']:
                        s_opts = [v for v in sex_column_values if v]
                        rule['c_sexo_val'] = c_cols[5].selectbox("SV", options=s_opts, key=f"c_sexo_val_{rule['id']}", label_visibility="collapsed")
        st.markdown("---")

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    if not st.session_state.lgpd_accepted:
        st.title("Data Sift")
        st.markdown(GDPR_TERMS)
        if st.checkbox("Confirm data anonymization") and st.button("Continue"):
            st.session_state.lgpd_accepted = True; st.rerun()
        return

    if 'filter_rules' not in st.session_state: st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)
    if 'stratum_rules' not in st.session_state: st.session_state.stratum_rules = [{'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''}]
    
    with st.sidebar:
        topic = st.selectbox("Manual", list(MANUAL_CONTENT.keys()))
        st.markdown(MANUAL_CONTENT[topic], unsafe_allow_html=True)

    st.title("Data Sift")
    with st.expander("1. Global Settings", expanded=True):
        up = st.file_uploader("Select spreadsheet", type=['csv', 'xlsx', 'xls', 'zip'], on_change=reset_results_on_upload)
        if up:
            if st.session_state.get("id_arquivo_atual") != up.file_id:
                st.session_state.dados_salvos = load_dataframe(up)
                st.session_state.id_arquivo_atual = up.file_id
        df = st.session_state.get("dados_salvos")
        opts = df.columns.tolist() if df is not None else []
        c1, c2, c3 = st.columns(3)
        with c1: st.selectbox("Age Column", opts, key="col_idade", index=None)
        with c2: st.selectbox("Sex/Gender Column", opts, key="col_sexo", index=None)
        with c3: st.selectbox("Output Format", ["CSV Bundle (.zip)", "Excel (.xlsx)"], key="output_format")
        s_vals = [""] + list(df[st.session_state.col_sexo].dropna().unique()) if df is not None and st.session_state.col_sexo in opts else []

    t1, t2 = st.tabs(["2. Filter Tool", "3. Stratification Tool"])
    with t1:
        st.header("Exclusion Rules")
        draw_filter_rules(s_vals, opts)
        if st.button("Add New Filter Rule"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '<', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False})
            st.rerun()
        
        if st.button("Generate Filtered Sheet", type="primary", use_container_width=True):
            if df is not None:
                p = st.progress(0, text="Initializing...")
                proc = get_data_processor()
                res = proc.apply_filters(df, st.session_state.filter_rules, {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}, p)
                
                is_ex = "Excel" in st.session_state.output_format
                if is_ex:
                    file_data = to_excel_with_metadata(res, st.session_state.filter_rules)
                    ext = "xlsx"
                else:
                    file_data = to_zip_with_metadata(res, st.session_state.filter_rules)
                    ext = "zip"
                
                st.session_state.filtered_result = (file_data, f"Filtered_Sheet_{datetime.now().strftime('%Y%m%d_%H%M')}.{ext}")
            else: st.error("Please upload a file.")

        if 'filtered_result' in st.session_state:
            st.download_button("Download Filtered Data (With Criteria Report)", st.session_state.filtered_result[0], st.session_state.filtered_result[1], use_container_width=True)

if __name__ == "__main__":
    main()
