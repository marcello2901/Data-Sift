# -*- coding: utf-8 -*-

# Versão 3.1.3
import streamlit as st
import pandas as pd
from scipy import stats
import numpy as np
import io
import uuid
import copy
import zipfile
import duckdb
import time 
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
import tempfile
import os
import shutil
import matplotlib.pyplot as plt
import seaborn as sns
import base64

# --- CONFIGURAÇÃO E TEMA DA PÁGINA ---
st.set_page_config(
    page_title="DataSift",
    page_icon="favicon.png", # Coloque o nome exato do arquivo da imagem que você salvou
    layout="wide" # Opcional, se você quiser que a tela fique mais larga
)

# Paleta de Cores Baseada na Imagem de Referência
COLOR_PRIMARY = "#073B4C"     # Azul Petróleo Escuro
COLOR_SECONDARY = "#00E5FF"   # Ciano Brilhante Neon (Botões e destaques)
COLOR_TERTIARY = "#118AB2"    # Azul Petróleo Médio
COLOR_BG = "#F8F9FA"          # Fundo Off-white
COLOR_CARD_BG = "#FFFFFF"     # Fundo dos Cards Branco puro

# Injeção de CSS para forçar a identidade visual e o Layout em Cards
st.markdown(f"""
    <style>
        /* Ajustes Estruturais e Fundo da Página */
        [data-testid="stAppViewBlockContainer"] {{
            padding-top: 2rem !important;
        }}
        .stApp {{ background-color: {COLOR_BG} !important; }}
        [data-testid="stStatusWidget"] {{ visibility: hidden; }}
        
        /* Oculta títulos padrão para usar HTML customizado */
        .st-emotion-cache-10trblm h1, .st-emotion-cache-10trblm h2, .st-emotion-cache-10trblm h3 {{
            color: {COLOR_PRIMARY} !important;
            font-weight: 800 !important;
        }}

        /* --- ESTILOS DOS CARDS --- */
        .card-container {{
            background-color: {COLOR_CARD_BG};
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            border: 1px solid #E0E0E0;
            padding: 20px;
            margin-bottom: 25px;
        }}
        
        .card-with-header {{
            background-color: {COLOR_CARD_BG};
            border-radius: 12px;
            border: 2px solid {COLOR_PRIMARY};
            overflow: hidden;
            margin-bottom: 20px;
        }}
        .card-header-bar {{
            background-color: {COLOR_PRIMARY};
            color: white;
            padding: 12px 20px;
            font-size: 1.25rem;
            font-weight: bold;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .card-content-area {{ padding: 20px; }}

        /* --- BOTÕES --- */
        button[kind="primary"] {{
            background-color: {COLOR_SECONDARY} !important;
            color: {COLOR_PRIMARY} !important; 
            border-radius: 8px !important;
            border: none !important;
            font-weight: bold !important;
            font-size: 1.1rem !important;
            padding: 0.75rem 1rem !important;
            box-shadow: 0 2px 4px rgba(0, 229, 255, 0.4) !important;
        }}
        button[kind="primary"]:hover {{ opacity: 0.8; }}
        
        button[kind="secondary"] {{
            border-color: {COLOR_TERTIARY} !important;
            color: {COLOR_TERTIARY} !important;
            border-radius: 6px !important;
        }}
        
        /* Mini Botões (Clone/X) */
        .stButton>button {{ padding: 0.15rem 0.5rem; }}

        /* --- INPUTS --- */
        div[data-testid="stTextInput"] input, div[data-testid="stSelectbox"] div[data-baseweb="select"] {{
            background-color: #F0F4F8 !important;
            border: 1px solid #CFD8DC !important; 
            border-radius: 6px;
            color: {COLOR_PRIMARY} !important;
        }}
        div[data-testid="stTextInput"] input:focus, div[data-testid="stSelectbox"] div[data-baseweb="select"]:focus-within {{
            border-color: {COLOR_TERTIARY} !important;
            box-shadow: 0 0 0 1px {COLOR_TERTIARY} !important;
        }}
        label[data-testid="stWidgetLabel"] p {{
            color: {COLOR_PRIMARY} !important;
            font-weight: 600 !important;
            font-size: 0.9rem !important;
        }}
        
        /* Barra de Progresso */
        .stProgress > div > div > div > div {{ background-color: {COLOR_SECONDARY} !important; }}
        
        /* --- MINI CARD LATERAL (Harris-Boyd) --- */
        .mini-card-dark {{
            background-color: {COLOR_PRIMARY};
            color: white;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            margin-top: 15px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .mini-card-dark h4 {{
            color: white !important;
            margin-top: 0;
            margin-bottom: 10px;
            font-size: 1.1rem;
            border-bottom: 1px solid rgba(255,255,255,0.2);
            padding-bottom: 8px;
        }}
        .mini-card-dark p {{ margin: 0; font-size: 0.95rem; }}
        
        /* --- TABS --- */
        div[data-testid="stTabs"] button {{
            font-weight: 600;
            color: {COLOR_PRIMARY} !important;
        }}
        div[data-testid="stTabs"] button[aria-selected="true"] {{
            color: {COLOR_SECONDARY} !important;
            border-bottom-color: {COLOR_SECONDARY} !important;
        }}
    </style>
""", unsafe_allow_html=True)

def get_base64_of_bin_file(bin_file):
    try:
        with open(bin_file, 'rb') as f:
            data = f.read()
        return base64.b64encode(data).decode()
    except FileNotFoundError:
        return None

logo_path = "datasift_logo.png"
logo_base64 = get_base64_of_bin_file(logo_path)

# --- CONSTANTES E DADOS ---
GDPR_TERMS = """
This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, national ID numbers, health information, etc.), the processing of which is regulated by data protection laws like the General Data Protection Regulation (GDPR or LGPD).

It is your sole responsibility to ensure that all data used in this tool complies with applicable data protection regulations. We strongly recommend that you only use previously anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been properly handled and anonymized.
"""

MANUAL_CONTENT = {
    "Introduction": """**Welcome to Data Sift!**\n\nThis program is a spreadsheet filter tool designed to optimize your work with large volumes of data by offering two main functionalities:\n\n1.  **Filtering:** To clean your database by removing rows that are not of interest.\n2.  **Stratification:** To divide your database into specific subgroups.""",
    "1. Global Settings": """**1. Global Settings**\n\nThis section contains the essential settings that are shared between both tools.\n\n- **Select Spreadsheet:**\n  Opens a window to select the source data file. It supports `.xlsx`, `.xls`, and `.csv` formats.\n\n- **Age Column / Sex/Gender / Data Column:**\n  Fields to **select** the column names in your spreadsheet. The **Data Column** is specifically used to automatically run the Harris-Boyd stratification study and generate charts.\n\n- **Output Format:**\n  A selection menu to choose the format of the generated files. Choose `Excel (.xlsx)` for Microsoft Excel or `CSV (.csv)` for a lighter format.""",
    "2. Filter Tool": """**2. Filter Tool**\n\nThe purpose of this tool is to **"clean"** your spreadsheet by **removing** rows that match specific criteria. The result is a **single file** containing only the data that "survived" the filters.\n\n**How Exclusion Rules Work:**\nEach row you add is a condition to **remove** data. If a row in your spreadsheet matches an active rule, it **will be excluded** from the final file.\n\n- **[✓] (Activation Checkbox):** Toggles a rule on or off without deleting it.\n\n- **Column:** The name of the column where the filter will be applied. **Tip:** You can apply the rule to multiple columns at once by separating their names with a semicolon (;).\n\n- **Operator and Value:** Operators define the rule's logic to set exclusion ranges.\n**Tip:** The keyword `empty` is a powerful feature:\n    - **Scenario 1:** Column: `"Exam_X"`, Operator: `"is equal to"`, Value: `"empty"`.\n    - **Scenario 2:** Column: `"Observations"`, Operator: `"Not equal to"`, Value: `"empty"`.\n\n- **Compound Logic:** Expands the rule to create `AND` / `OR` conditions.\n\n- **Condition:** Allows applying a secondary filter based on sex and/or age conditions.\n\n- **Actions:** The `X` button deletes the rule. The 'Clone' button duplicates it.""",
    "3. Stratification Tool": """**3. Stratification Tool**\n\nThis tool splits your spreadsheet into **multiple smaller files**, where each file represents a subgroup of interest.\n\n**Harris-Boyd Study & Charts:**\nAutomatically evaluates the selected Data Column and Age Column to suggest the most statistically relevant age cuts. You can also generate Boxplot charts to visually inspect the data distribution.\n\n**How Stratification Works:**\n- **Stratification Options by Sex/Gender:** Select the genders you want to include.\n- **Age Range Definitions:** Create the specific age boundaries.\n- **Generate Stratified Sheets:** Starts the splitting process."""
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

# --- CLASSES DE PROCESSAMENTO ---
@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '=', '==': '=', 'Não é igual a': '!=', '≥': '>=', '≤': '<=', 'is equal to': '=', 'Not equal to': '!='}

    def _build_single_sql_cond(self, col: str, op: str, val: Any) -> str:
        if not op: return "FALSE"
        op = self.OPERATOR_MAP.get(op, op)
        if str(val).lower() == 'empty':
            if op in ('=', '=='): return f"({col} IS NULL OR TRIM(CAST({col} AS VARCHAR)) = '')"
            if op == '!=': return f"({col} IS NOT NULL AND TRIM(CAST({col} AS VARCHAR)) != '')"
            return "FALSE"
        try:
            v_num = float(str(val).replace(',', '.'))
            safe_cast = f"TRY_CAST(REPLACE(CAST({col} AS VARCHAR), ',', '.') AS DOUBLE)"
            return f"({safe_cast} IS NOT NULL AND {safe_cast} {op} {v_num})"
        except ValueError:
            v_str = str(val).replace("'", "''").lower().strip()
            return f"(CAST({col} AS VARCHAR) IS NOT NULL AND LOWER(TRIM(CAST({col} AS VARCHAR))) {op} '{v_str}')"

    def _create_main_sql(self, f: Dict, col: str) -> str:
        op1, val1 = f.get('p_op1'), f.get('p_val1')
        safe_col = f'"{col}"'
        if not f.get('p_expand'):
            return self._build_single_sql_cond(safe_col, op1, val1)
        op_central = f.get('p_op_central', '').upper()
        op2, val2 = f.get('p_op2'), f.get('p_val2')
        if op_central == 'BETWEEN':
            try:
                v1_num = float(str(val1).replace(',', '.'))
                v2_num = float(str(val2).replace(',', '.'))
                min_v, max_v = sorted([v1_num, v2_num])
                safe_cast = f"TRY_CAST(REPLACE(CAST({safe_col} AS VARCHAR), ',', '.') AS DOUBLE)"
                return f"({safe_cast} IS NOT NULL AND {safe_cast} BETWEEN {min_v} AND {max_v})"
            except ValueError: return "FALSE"
        cond1 = self._build_single_sql_cond(safe_col, op1, val1)
        cond2 = self._build_single_sql_cond(safe_col, op2, val2)
        return f"({cond1} {op_central} {cond2})"

    def _create_conditional_sql(self, f: Dict, global_config: Dict) -> str:
        if not f.get('c_check'): return "TRUE"
        conds = []
        col_idade = global_config.get('coluna_idade')
        if f.get('c_idade_check') and col_idade:
            safe_idade = f'"{col_idade}"'
            op1, val1 = f.get('c_idade_op1'), f.get('c_idade_val1')
            if op1 and val1: conds.append(self._build_single_sql_cond(safe_idade, op1, val1))
            op2, val2 = f.get('c_idade_op2'), f.get('c_idade_val2')
            if op2 and val2: conds.append(self._build_single_sql_cond(safe_idade, op2, val2))
        col_sexo = global_config.get('coluna_sexo')
        if f.get('c_sexo_check') and col_sexo:
            val_sexo = f.get('c_sexo_val')
            if val_sexo:
                safe_sexo = f'"{col_sexo}"'
                conds.append(self._build_single_sql_cond(safe_sexo, '=', val_sexo))
        return " AND ".join(conds) if conds else "TRUE"

    def apply_filters(self, df_input: pd.DataFrame, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
        start_time = time.perf_counter()
        active_filters = [f for f in filters_config if f['p_check']]
        
        if not active_filters:
            end_time = time.perf_counter()
            progress_bar.progress(1.0, text=f"No active filter rules. (Time: {end_time - start_time:.4f}s)")
            return df_input

        exclusion_clauses = []
        for i, f_config in enumerate(active_filters):
            progress_bar.progress((i + 1) / len(active_filters), text=f"Mapping SQL rule {i+1}...")
            col_config_str = f_config.get('p_col', '')
            cols_to_check = [c.strip() for c in col_config_str.split(';') if c.strip()]
            if not cols_to_check: continue

            main_conds = []
            for sub_col in cols_to_check:
                if sub_col in df_input.columns:
                    main_conds.append(self._create_main_sql(f_config, sub_col))
                else:
                    main_conds.append("FALSE")

            combined_main_sql = " AND ".join([f"({c})" for c in main_conds]) if main_conds else "FALSE"
            cond_sql = self._create_conditional_sql(f_config, global_config)
            rule_sql = f"({combined_main_sql}) AND ({cond_sql})"
            exclusion_clauses.append(f"NOT ({rule_sql})")

        if not exclusion_clauses:
            end_time = time.perf_counter()
            progress_bar.progress(1.0, text=f"Processing complete! (Time: {end_time - start_time:.4f}s)")
            return df_input

        where_clause = " AND ".join(exclusion_clauses)
        local_df = df_input.copy()
        local_df['_temp_row_id'] = range(len(local_df))
        
        con = duckdb.connect()
        con.register('local_df', local_df)
        query = f"SELECT _temp_row_id FROM local_df WHERE {where_clause}"

        try:
            progress_bar.progress(0.8, text="Executing DuckDB Engine (SQL)...")
            valid_ids_df = con.execute(query).df()
            filtered_df = local_df[local_df['_temp_row_id'].isin(valid_ids_df['_temp_row_id'])].copy()
            filtered_df.drop(columns=['_temp_row_id'], inplace=True)
            con.close()
            
            end_time = time.perf_counter()
            tempo_execucao = end_time - start_time
            progress_bar.progress(1.0, text=f"Filtering complete! Processing time: {tempo_execucao:.4f} seconds.")
            return filtered_df
        except Exception as e:
            con.close()
            st.session_state.filter_error = f"SQL Processing Error: {e}"
            return df_input
    
    def apply_stratification(self, df_input: pd.DataFrame, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')

        age_strata = strata_config.get('ages', [])
        sex_strata = strata_config.get('sexes', [])

        if age_strata and not (col_idade and col_idade in df_input.columns):
            st.session_state.stratification_error = f"Age column '{col_idade}' not found or not mapped in Global Settings."
            return {}
        if sex_strata and not (col_sexo and col_sexo in df_input.columns):
            st.session_state.stratification_error = f"Sex/Gender column '{col_sexo}' not found or not mapped in Global Settings."
            return {}

        safe_idade = f'"{col_idade}"' if col_idade else ""
        safe_sexo = f'"{col_sexo}"' if col_sexo else ""

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

        local_df = df_input.copy()
        local_df['_temp_row_id'] = range(len(local_df))
        
        con = duckdb.connect()
        con.register('local_df', local_df)

        for i, stratum in enumerate(final_strata_to_process):
            progress = (i + 1) / total_files
            conditions = []
            age_rule = stratum.get('age')
            if age_rule:
                if age_rule.get('op1') and age_rule.get('val1'):
                    conditions.append(self._build_single_sql_cond(safe_idade, age_rule['op1'], age_rule['val1']))
                if age_rule.get('op2') and age_rule.get('val2'):
                    conditions.append(self._build_single_sql_cond(safe_idade, age_rule['op2'], age_rule['val2']))

            sex_rule = stratum.get('sex')
            if sex_rule and sex_rule.get('value'):
                conditions.append(self._build_single_sql_cond(safe_sexo, '=', sex_rule['value']))

            where_clause = " AND ".join([f"({c})" for c in conditions]) if conditions else "TRUE"
            query = f"SELECT _temp_row_id FROM local_df WHERE {where_clause}"

            filename = self._generate_stratum_name(age_rule, sex_rule)
            progress_bar.progress(progress, text=f"Generating stratum {i+1}/{total_files}: {filename}...")
            
            try:
                valid_ids_df = con.execute(query).df()
                if not valid_ids_df.empty:
                    stratum_df = local_df[local_df['_temp_row_id'].isin(valid_ids_df['_temp_row_id'])].copy()
                    stratum_df.drop(columns=['_temp_row_id'], inplace=True)
                    generated_dfs[filename] = stratum_df
            except Exception as e:
                st.session_state.stratification_error = f"SQL error while generating {filename}: {e}"

        con.close()
        progress_bar.progress(1.0, text="Stratification complete!")
        return generated_dfs

    def _generate_stratum_name(self, age_rule: Optional[Dict], sex_rule: Optional[Dict]) -> str:
        name_parts = []
        if age_rule:
            op1, val1 = age_rule.get('op1'), age_rule.get('val1')
            op2, val2 = age_rule.get('op2'), age_rule.get('val2')
            def get_int(val): 
                try: return int(float(str(val).replace(',', '.')))
                except (ValueError, TypeError): return None

            v1_int = get_int(val1)
            v2_int = get_int(val2)

            if op1 and val1 and not (op2 and val2):
                if v1_int is not None:
                    if op1 == '>': name_parts.append(f"Over_{v1_int}_years")
                    elif op1 in ('≥', '>='): name_parts.append(f"{v1_int}_and_over_years")
                    elif op1 == '<': name_parts.append(f"Under_{v1_int}_years")
                    elif op1 in ('≤', '<='): name_parts.append(f"Up_to_{v1_int}_years")
            elif op1 and val1 and op2 and val2:
                if v1_int is not None and v2_int is not None:
                    v1_f, v2_f = float(str(val1).replace(',', '.')), float(str(val2).replace(',', '.'))
                    bounds = []
                    if op1 and val1: bounds.append((v1_f, op1))
                    if op2 and val2: bounds.append((v2_f, op2))
                    bounds.sort(key=lambda x: x[0])
                    low_val_f, low_op = bounds[0]
                    high_val_f, high_op = bounds[1]
                    low_bound = int(low_val_f) if low_op in ('≥', '>=') else int(low_val_f + 1) if low_op == '>' else int(low_val_f)
                    high_bound = int(high_val_f) if high_op in ('≤', '<=') else int(high_val_f - 1) if high_op == '<' else int(high_val_f)
                    
                    if low_bound > high_bound: name_parts.append("Invalid_range")
                    else: name_parts.append(f"{low_bound}_to_{high_bound}_years")
        if sex_rule:
            sex_name = str(sex_rule.get('value', '')).replace(' ', '_')
            if sex_name: name_parts.append(sex_name)
        
        final_name = "_".join(part for part in name_parts if part)
        return final_name if final_name else "Group_All"

# --- FUNÇÕES AUXILIARES OTIMIZADAS ---

@st.cache_data(show_spinner=False)
def run_harris_boyd(df, col_idade, col_dados):
    temp_df = pd.DataFrame()
    temp_df['Age'] = pd.to_numeric(df[col_idade], errors='coerce')

    def clean_val(x):
        if pd.isna(x): return np.nan
        x = str(x).replace(',', '.')
        x = ''.join(c for c in x if c.isdigit() or c == '.' or c == '-')
        try: return float(x)
        except: return np.nan

    temp_df['Data'] = pd.to_numeric(df[col_dados].apply(clean_val), errors='coerce')
    temp_df = temp_df.dropna(subset=['Age', 'Data'])
    temp_df = temp_df[temp_df['Age'] >= 0]
    temp_df = temp_df[temp_df['Data'] > 0].copy()

    if temp_df.empty:
        return "No stratification recommended (Insufficient data).", pd.DataFrame(), []

    max_age = int(temp_df['Age'].max())
    if max_age < 1:
        return "No stratification recommended (Insufficient age variation).", pd.DataFrame(), []

    # =========================================================================
    # FASE 1 — DETECÇÃO DE CORTES CANDIDATOS (Harris-Boyd + Teste Z + Cohen's d)
    # Agora calculando diretamente sobre a escala original (sem Box-Cox)
    # =========================================================================
    valid_cuts = []
    for age_cutoff in range(1, max_age):
        mask_g1 = temp_df['Age'] <= age_cutoff
        mask_g2 = temp_df['Age'] > age_cutoff

        g1 = temp_df[mask_g1]['Data']
        g2 = temp_df[mask_g2]['Data']

        n1, n2 = len(g1), len(g2)
        if n1 < 30 or n2 < 30: continue

        mean1, mean2 = np.mean(g1), np.mean(g2)
        var1,  var2  = np.var(g1, ddof=1), np.var(g2, ddof=1)
        sd1,   sd2   = np.sqrt(var1), np.sqrt(var2)

        sd_ratio = max(sd1, sd2) / min(sd1, sd2) if min(sd1, sd2) > 0 else 0
        den_z = np.sqrt((var1 / n1) + (var2 / n2)) if (var1 / n1) + (var2 / n2) > 0 else 0.0001
        z = abs(mean1 - mean2) / den_z
        z_crit = 3 * np.sqrt((n1 + n2) / 120) if (n1 + n2) < 120 else 3
        den_d = np.sqrt((var1 + var2) / 2) if (var1 + var2) > 0 else 0.0001
        d_value = abs(mean1 - mean2) / den_d

        partition_by_sd   = sd_ratio > 1.5
        partition_by_mean = (z > z_crit) and (d_value > 0.25)
        should_partition  = partition_by_sd or partition_by_mean

        if should_partition:
            just = (
                'Standard Deviation' if partition_by_sd and not partition_by_mean
                else ('Mean' if partition_by_mean and not partition_by_sd else 'Both')
            )
            valid_cuts.append({
                'age': age_cutoff, 'justificativa': just,
                'd_value': d_value, 'sd_ratio': sd_ratio,
                'mean1': mean1, 'mean2': mean2, 'n1': n1, 'n2': n2,
                'Age Cutoff':         f"<= {age_cutoff} vs > {age_cutoff}",
                'Justification':      just,
                'D-value':            round(d_value, 3),
                'SD Ratio':           round(sd_ratio, 3),
                'Mean (<= Cutoff)':   round(mean1, 2),
                'Mean (> Cutoff)':    round(mean2, 2),
            })

    if not valid_cuts:
        return (
            "The statistical model found no clinical necessity or sufficient variance "
            "to recommend age-based reference intervals for this analyte.",
            pd.DataFrame(), []
        )

    valid_cuts = sorted(valid_cuts, key=lambda x: x['age'])
    
    # =========================================================================
    # FASE 2 — CLUSTERIZAÇÃO DINÂMICA (BASEADA NO COEFICIENTE DE VARIAÇÃO)
    #
    # O algoritmo calcula o Coeficiente de Variação (CV) natural do grupo.
    # A margem de equivalência clínica passa a ser desenhada sob medida
    # para cada analito, sem precisar de margens fixas.
    # =========================================================================
    MIN_INTERMEDIATE_N = 10
    
    # FATOR DE RIGOR (Tuning)
    # Ex: 0.50 significa que toleramos uma diferença de médias equivalente a 
    # até 50% do CV natural (variação biológica) daquele analito.
    CV_TOLERANCE_FACTOR = 0.50 

    def is_clinically_different_dynamic(vals_ref, vals_test):
        """
        Retorna True se a diferença entre as médias ultrapassar a margem gerada
        dinamicamente pelo Coeficiente de Variação (CV) dos dados originais.
        """
        if len(vals_ref) < 2 or len(vals_test) < 2:
            return False
        
        mean_ref = np.mean(vals_ref)
        mean_test = np.mean(vals_test)
        sd_ref = np.std(vals_ref, ddof=1)
        
        # 1. Calcula o CV natural do analito (em formato decimal)
        cv_ref = sd_ref / mean_ref if mean_ref > 0 else 0
        
        # 2. A Margem Dinâmica: Adapta a % de corte ao analito atual
        # Sódio terá margem de ~1%, Triglicerídeos terá margem de ~20%
        dynamic_margin = cv_ref * CV_TOLERANCE_FACTOR
        
        # 3. Calcula a diferença absoluta e o limite
        diff_absoluta = abs(mean_ref - mean_test)
        limite_equivalencia = mean_ref * dynamic_margin
        
        return diff_absoluta > limite_equivalencia

    clusters        = []
    current_cluster = [valid_cuts[0]]

    for i in range(1, len(valid_cuts)):
        cut      = valid_cuts[i]
        prev_cut = current_cluster[-1]

        # Extraindo as 3 zonas usando a ESCALA ORIGINAL ('Data')
        mask_mid = (temp_df['Age'] > prev_cut['age']) & (temp_df['Age'] <= cut['age'])
        mid_vals_orig    = temp_df[mask_mid]['Data'].values
        before_vals_orig = temp_df[temp_df['Age'] <= prev_cut['age']]['Data'].values
        after_vals_orig  = temp_df[temp_df['Age'] >  cut['age']]['Data'].values

        if len(mid_vals_orig) < MIN_INTERMEDIATE_N:
            current_cluster.append(cut)
            continue

        # Chama a nova função dinâmica
        is_diff_from_left  = is_clinically_different_dynamic(before_vals_orig, mid_vals_orig)
        is_diff_from_right = is_clinically_different_dynamic(after_vals_orig, mid_vals_orig)

        if is_diff_from_left and is_diff_from_right:
            clusters.append(current_cluster)
            current_cluster = [cut]
        else:
            current_cluster.append(cut)

    if current_cluster:
        clusters.append(current_cluster)

    # =========================================================================
    # FASE 3 — SELEÇÃO DO MELHOR REPRESENTANTE POR CLUSTER
    # O corte com maior Cohen's d é o ponto de separação mais "forte" do cluster.
    # =========================================================================
    best_cuts = [max(cluster, key=lambda x: x['d_value']) for cluster in clusters]
    best_cuts = sorted(best_cuts, key=lambda x: x['age'])

    # =========================================================================
    # FASE 4 — GERAÇÃO DO LAUDO (sem alterações)
    # =========================================================================
    texto_laudo  = "### 💡 Practical Stratification Suggestion\n"
    texto_laudo += (
        f"The algorithm analyzed the means and data dispersion and detected "
        f"**{'1 point' if len(best_cuts) == 1 else str(len(best_cuts)) + ' points'}** "
        f"of significant clinical change across ages:\n\n"
    )

    last_age = 0
    for i, cut in enumerate(best_cuts):
        idade_corte, m1, m2 = cut['age'], cut['mean1'], cut['mean2']
        faixa = (
            f"From {last_age} to {idade_corte} years" if i == 0
            else f"From {last_age + 1} to {idade_corte} years"
        )
        texto_laudo += f"**{i+1}. Group {faixa} (Approx. Mean: {m1:.1f})**\n🔹 *Why separate?* "
        if cut['justificativa'] == 'Mean':
            texto_laudo += (
                f"In this life stage, there is a significant change in central tendency "
                f"compared to the rest of the population (jump to {m2:.1f}). \n\n"
            )
        elif cut['justificativa'] == 'Standard Deviation':
            texto_laudo += (
                "This age group presents a very different variability (data dispersion) "
                "compared to other ages. \n\n"
            )
        else:
            texto_laudo += (
                f"This age group has a unique behavior, both due to a different mean "
                f"(jump to {m2:.1f}) and high data dispersion. \n\n"
            )
        last_age = idade_corte

    texto_laudo += (
        f"**{len(best_cuts)+1}. Group from {last_age + 1} years onwards "
        f"(Approx. Mean: {best_cuts[-1]['mean2']:.1f})**\n"
        f"🔹 From this barrier onwards, the model considers that the results tend to "
        f"stabilize statistically, forming the main reference range for reports.\n"
    )

    idades_sugeridas = [c['age'] for c in best_cuts]
    raw_data_list = [{
        'Recommendation':    '⭐ Suggested' if cut['age'] in idades_sugeridas else '',
        'Age Cutoff':        cut['Age Cutoff'],
        'Justification':     cut['Justification'],
        'D-value':           cut['D-value'],
        'SD Ratio':          cut['SD Ratio'],
        'Mean (<= Cutoff)':  cut['Mean (<= Cutoff)'],
        'Mean (> Cutoff)':   cut['Mean (> Cutoff)'],
        'N (<= Cutoff)':     cut['n1'],
        'N (> Cutoff)':      cut['n2'],
    } for cut in valid_cuts]

    return texto_laudo, pd.DataFrame(raw_data_list), idades_sugeridas

@st.cache_data(show_spinner=False)
def plot_dispersion_chart(df, col_idade, col_dados, col_sexo, intervalo, chart_type, group_by_sex, selected_sexes, show_trendlines):
    temp_df = pd.DataFrame()
    temp_df['Age'] = pd.to_numeric(df[col_idade], errors='coerce')
    def clean_val(x):
        if pd.isna(x): return np.nan
        x = str(x).replace(',', '.')
        x = ''.join(c for c in x if c.isdigit() or c == '.' or c == '-')
        try: return float(x)
        except: return np.nan
    temp_df['Data'] = pd.to_numeric(df[col_dados].apply(clean_val), errors='coerce')
    
    if col_sexo and col_sexo in df.columns: temp_df['Sex'] = df[col_sexo].astype(str)
    else: group_by_sex = False

    temp_df = temp_df.dropna(subset=['Age', 'Data'])
    temp_df = temp_df[temp_df['Age'] >= 0]
    
    if 'Sex' in temp_df.columns and selected_sexes:
        temp_df = temp_df[temp_df['Sex'].isin(selected_sexes)]
        
    if temp_df.empty: return None

    min_age, max_age = int(temp_df['Age'].min()), int(temp_df['Age'].max())

    if intervalo > 1:
        min_bin, max_bin = (min_age // intervalo) * intervalo, (max_age // intervalo) * intervalo
        temp_df['Age_Bin'] = (temp_df['Age'] // intervalo) * intervalo
        temp_df['Age_Label'] = temp_df['Age_Bin'].astype(int).astype(str) + " to " + (temp_df['Age_Bin'] + intervalo - 1).astype(int).astype(str)
        categories = [f"{b} to {b + intervalo - 1}" for b in range(min_bin, max_bin + 1, int(intervalo))]
    else:
        temp_df['Age_Label'] = temp_df['Age'].astype(int).astype(str)
        categories = [str(age) for age in range(min_age, max_age + 1)]
        
    temp_df['Age_Label'] = pd.Categorical(temp_df['Age_Label'], categories=categories, ordered=True)
    x_col = 'Age_Label'

    fig, ax = plt.subplots(figsize=(12, 5))
    hue_col = 'Sex' if group_by_sex and 'Sex' in temp_df.columns else None
    palette_custom = [COLOR_PRIMARY, COLOR_SECONDARY, "#48CAE4", "#06D6A0"] 
    single_color = COLOR_TERTIARY
    
    if chart_type == 'Boxplot':
        if hue_col: sns.boxplot(data=temp_df, x=x_col, y='Data', hue=hue_col, palette=palette_custom, ax=ax, showfliers=False)
        else: sns.boxplot(data=temp_df, x=x_col, y='Data', color=single_color, ax=ax, showfliers=False)
        ax.set_ylabel('Results (Without Extreme Outliers)', fontsize=12, labelpad=10)
    elif chart_type in ['Moving Average', 'Moving Median']:
        metric_func = np.mean if chart_type == 'Moving Average' else np.median
        if hue_col: sns.lineplot(data=temp_df, x=x_col, y='Data', hue=hue_col, palette=palette_custom, estimator=metric_func, marker='o', errorbar=None, ax=ax, linewidth=2, markersize=8)
        else: sns.lineplot(data=temp_df, x=x_col, y='Data', estimator=metric_func, marker='o', color=single_color, errorbar=None, ax=ax, linewidth=2, markersize=8)
        ax.set_ylabel(f'{chart_type} Results', fontsize=12, labelpad=10)

        if show_trendlines:
            metric_str = 'mean' if chart_type == 'Moving Average' else 'median'
            def draw_segments(df_sub, color):
                _, _, cuts = run_harris_boyd(df_sub, 'Age', 'Data')
                starts, ends = [0] + [c + 1 for c in cuts], cuts + [999]
                for s, e in zip(starts, ends):
                    mask = (df_sub['Age'] >= s) & (df_sub['Age'] <= e)
                    if mask.sum() == 0: continue
                    val = df_sub[mask]['Data'].mean() if metric_str == 'mean' else df_sub[mask]['Data'].median()
                    x_positions = [categories.index(lbl) for lbl in df_sub[mask]['Age_Label'].unique() if lbl in categories]
                    if not x_positions: continue
                    ax.hlines(y=val, xmin=min(x_positions)-0.4, xmax=max(x_positions)+0.4, color=color, linestyle='--', linewidth=2.5, alpha=0.8, zorder=10)

            if hue_col:
                palette = sns.color_palette(palette_custom, n_colors=temp_df[hue_col].nunique())
                for i, sex_val in enumerate(temp_df[hue_col].dropna().unique()): draw_segments(temp_df[temp_df[hue_col] == sex_val], palette[i])
            else: draw_segments(temp_df, COLOR_SECONDARY)

    ax.set_xlabel('Age (Years)', fontsize=12, labelpad=10)
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=90 if len(categories) > 30 else 45, ha='center' if len(categories) > 30 else 'right', fontsize=8 if len(categories) > 40 else 10)
    plt.grid(axis='y', linestyle=':', alpha=0.6, color='#CFD8DC')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#CFD8DC')
    ax.spines['bottom'].set_color('#CFD8DC')
    if hue_col: ax.legend(title='Sex/Gender', frameon=True, facecolor='white', edgecolor='#e0e0e0')
    plt.tight_layout()
    return fig

@st.cache_data(show_spinner="Preparing file for export...")
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

@st.cache_data(show_spinner="Preparing CSV for export...")
def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

# --- FUNÇÕES DE INTERFACE ---
def handle_select_all():
    new_state = st.session_state['select_all_master_checkbox']
    for rule in st.session_state.filter_rules: rule['p_check'] = new_state

def reset_results_on_upload():
    if 'filtered_result' in st.session_state: del st.session_state['filtered_result']
    if 'stratified_results' in st.session_state: del st.session_state['stratified_results']
    st.session_state.confirm_stratify = False

def draw_filter_rules(sex_column_values, column_options): 
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="small")
    all_checked = all(rule.get('p_check', False) for rule in st.session_state.filter_rules) if st.session_state.filter_rules else False

    help_icon = "<span style='cursor: help; color: #118AB2; font-size: 0.85em; font-weight: bold; background: #E0F7FA; border-radius: 50%; padding: 0px 5px;'>?</span>"

    header_cols[1].markdown(f"**Column** <span title='Type the exact column name as in the sheet. Separate multiple columns with ;'>{help_icon}</span>", unsafe_allow_html=True)
    header_cols[2].markdown(f"**Operator** <span title='Select the logical operator for the exclusion rule.'>{help_icon}</span>", unsafe_allow_html=True)
    header_cols[3].markdown(f"**Value** <span title='Value to be evaluated. Tip: Use \"empty\" to filter blank cells.'>{help_icon}</span>", unsafe_allow_html=True)
    header_cols[5].markdown(f"**Compound Logic** <span title='Expands the rule with AND, OR, or BETWEEN conditions.'>{help_icon}</span>", unsafe_allow_html=True)
    header_cols[6].markdown(f"**Cond** <span title='Applies this rule conditionally based on Age or Sex.'>{help_icon}</span>", unsafe_allow_html=True)
    header_cols[7].markdown(f"**Action** <span title='Clone (C) or Delete (X) this rule.'>{help_icon}</span>", unsafe_allow_html=True)

    ops_main, ops_age, ops_central_logic = ["", ">", "<", "=", "Not equal to", "≥", "≤"], ["", ">", "<", "≥", "≤", "="], ["AND", "OR", "BETWEEN"]

    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="small") 
            rule['p_check'] = cols[0].checkbox(f"Act {rule['id']}", value=rule.get('p_check', True), key=f"p_check_{rule['id']}", label_visibility="collapsed")
            rule['p_col'] = cols[1].text_input("Column", value=rule.get('p_col', ''), key=f"p_col_{rule['id']}", label_visibility="collapsed", placeholder="Ex: Exam.COL")
            rule['p_op1'] = cols[2].selectbox("Op 1", ops_main, index=ops_main.index(rule.get('p_op1', '=')) if rule.get('p_op1') in ops_main else 0, key=f"p_op1_{rule['id']}", label_visibility="collapsed")
            rule['p_val1'] = cols[3].text_input("Val 1", value=rule.get('p_val1', ''), key=f"p_val1_{rule['id']}", label_visibility="collapsed")
            rule['p_expand'] = cols[4].checkbox("+", value=rule.get('p_expand', False), key=f"p_expand_{rule['id']}", label_visibility="collapsed")
            
            with cols[5]:
                if rule['p_expand']:
                    exp_cols = st.columns([3, 2, 2])
                    rule['p_op_central'] = exp_cols[0].selectbox("Log", ops_central_logic, index=ops_central_logic.index(rule.get('p_op_central', 'OR')) if rule.get('p_op_central') in ops_central_logic else 0, key=f"p_op_central_{rule['id']}", label_visibility="collapsed")
                    rule['p_op2'] = exp_cols[1].selectbox("Op 2", ops_main, index=ops_main.index(rule.get('p_op2', '>')) if rule.get('p_op2') in ops_main else 0, key=f"p_op2_{rule['id']}", label_visibility="collapsed")
                    rule['p_val2'] = exp_cols[2].text_input("Val 2", value=rule.get('p_val2', ''), key=f"p_val2_{rule['id']}", label_visibility="collapsed")

            with cols[6]: rule['c_check'] = st.checkbox("Cond", value=rule.get('c_check', False), key=f"c_check_{rule['id']}", label_visibility="collapsed")
            
            action_cols = cols[7].columns(2)
            if action_cols[0].button("C", key=f"clone_{rule['id']}", help="Clone rule"):
                new_rule = copy.deepcopy(rule)
                new_rule['id'] = str(uuid.uuid4())
                st.session_state.filter_rules.insert(i + 1, new_rule)
                st.rerun()
            if action_cols[1].button("X", key=f"del_filter_{rule['id']}", help="Delete rule"):
                st.session_state.filter_rules.pop(i)
                st.rerun()

            if rule['c_check']:
                with st.container():
                    cond_cols = st.columns([0.55, 0.5, 1, 3, 1, 3])
                    cond_cols[1].markdown("↳")
                    rule['c_idade_check'] = cond_cols[2].checkbox("Age", value=rule.get('c_idade_check', False), key=f"c_idade_check_{rule['id']}")
                    with cond_cols[3]:
                        if rule['c_idade_check']:
                            age_cols = st.columns([2, 2, 1, 2, 2])
                            rule['c_idade_op1'] = age_cols[0].selectbox("Age Op 1", ops_age, index=ops_age.index(rule.get('c_idade_op1','>')) if rule.get('c_idade_op1') in ops_age else 0, key=f"c_idade_op1_{rule['id']}", label_visibility="collapsed")
                            rule['c_idade_val1'] = age_cols[1].text_input("Age Val 1", value=rule.get('c_idade_val1',''), key=f"c_idade_val1_{rule['id']}", label_visibility="collapsed")
                            age_cols[2].markdown("""<div style="display: flex; justify-content: center; align-items: center; height: 38px;">AND</div>""", unsafe_allow_html=True)
                            rule['c_idade_op2'] = age_cols[3].selectbox("Age Op 2", ops_age, index=ops_age.index(rule.get('c_idade_op2','<')) if rule.get('c_idade_op2') in ops_age else 0, key=f"c_idade_op2_{rule['id']}", label_visibility="collapsed")
                            rule['c_idade_val2'] = age_cols[4].text_input("Age Val 2", value=rule.get('c_idade_val2',''), key=f"c_idade_val2_{rule['id']}", label_visibility="collapsed")
                    
                    rule['c_sexo_check'] = cond_cols[4].checkbox("Sex", value=rule.get('c_sexo_check', False), key=f"c_sexo_check_{rule['id']}")
                    with cond_cols[5]:
                        if rule['c_sexo_check']:
                            sex_options = [v for v in sex_column_values if v]
                            current_sex = rule.get('c_sexo_val')
                            sex_index = sex_options.index(current_sex) if current_sex in sex_options else None
                            rule['c_sexo_val'] = st.selectbox("Sex Val", options=sex_options, index=sex_index, placeholder="Select value", key=f"c_sexo_val_{rule['id']}", label_visibility="collapsed")
        st.markdown("<hr style='border-color: rgba(7, 59, 76, 0.1); margin-top: 0.2rem; margin-bottom: 0.5rem;'>", unsafe_allow_html=True)

def draw_stratum_rules():
    ops_stratum = ["", ">", "<", "≥", "≤"]
    for i, stratum_rule in enumerate(st.session_state.stratum_rules):
        with st.container():
            cols = st.columns([2, 1, 1, 0.5, 1, 1, 1])
            cols[0].write(f"**Age Range {i+1}:**")
            stratum_rule['op1'] = cols[1].selectbox("Operator 1", ops_stratum, index=ops_stratum.index(stratum_rule.get('op1', '')) if stratum_rule.get('op1') in ops_stratum else 0, key=f"s_op1_{stratum_rule['id']}", label_visibility="collapsed")
            stratum_rule['val1'] = cols[2].text_input("Value 1", value=stratum_rule.get('val1', ''), key=f"s_val1_{stratum_rule['id']}", label_visibility="collapsed")
            cols[3].markdown("<p style='text-align: center; margin-top: 5px;'>AND</p>", unsafe_allow_html=True)
            stratum_rule['op2'] = cols[4].selectbox("Operator 2", ops_stratum, index=ops_stratum.index(stratum_rule.get('op2', '')) if stratum_rule.get('op2') in ops_stratum else 0, key=f"s_op2_{stratum_rule['id']}", label_visibility="collapsed")
            stratum_rule['val2'] = cols[5].text_input("Value 2", value=stratum_rule.get('val2', ''), key=f"s_val2_{stratum_rule['id']}", label_visibility="collapsed")
            if cols[6].button("X", key=f"del_stratum_{stratum_rule['id']}"):
                if len(st.session_state.stratum_rules) > 1:
                    st.session_state.stratum_rules.pop(i)
                    st.rerun()
                else: st.warning("Cannot delete the last age range.")

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    
    # --- TELA DE ENTRADA (LGPD) ---
    if not st.session_state.lgpd_accepted:
        if logo_base64:
            st.markdown(f'<div style="display: flex; justify-content: center; margin-top: 1rem; margin-bottom: 2rem;"><img src="data:image/png;base64,{logo_base64}" width="220"></div>', unsafe_allow_html=True)
        
        st.title("Welcome to Data Sift!")
        st.markdown("This program is designed to optimize your work with large volumes of data. Please read the terms below.")
        st.divider()

        st.header("Terms of Use and Data Protection Compliance")
        st.markdown(GDPR_TERMS) 
        accepted = st.checkbox("By checking this box, I confirm that the data provided is anonymized.")
        if st.button("Continue", type="primary", disabled=not accepted):
            st.session_state.lgpd_accepted = True
            st.rerun()
        return

    # --- INICIALIZAÇÃO DE VARIÁVEIS ---
    if 'filter_rules' not in st.session_state: 
        st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)
    if 'stratum_rules' not in st.session_state: 
        st.session_state.stratum_rules = [{'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''}]
    
    # --- BARRA LATERAL (Manual) ---
    with st.sidebar:
        if logo_base64:
            st.markdown(f'<div style="display: flex; justify-content: center; margin-bottom: 1rem;"><img src="data:image/png;base64,{logo_base64}" width="150"></div>', unsafe_allow_html=True)
        else:
            st.markdown("<h1 style='text-align: center;'>DataSift</h1>", unsafe_allow_html=True)
        st.markdown("---")
        topic = st.selectbox("User Manual", list(MANUAL_CONTENT.keys()))
        st.markdown(MANUAL_CONTENT[topic], unsafe_allow_html=True)

    # --- LOGO PRINCIPAL ---
    if logo_base64:
        st.markdown(f'<div style="display: flex; justify-content: center; margin-top: 1rem; margin-bottom: 2rem;"><img src="data:image/png;base64,{logo_base64}" width="220"></div>', unsafe_allow_html=True)

    # --- CARD 1: GLOBAL SETTINGS ---
    with st.expander("📁 1. Global Settings (Upload Spreadsheet)", expanded=True):
        uploaded_file = st.file_uploader("Select spreadsheet", type=['csv', 'xlsx', 'xls', 'zip'], on_change=reset_results_on_upload, key="file_uploader_widget", label_visibility="collapsed")

        if "dados_salvos" not in st.session_state: st.session_state.dados_salvos = None
        if "id_arquivo_atual" not in st.session_state: st.session_state.id_arquivo_atual = None

        if uploaded_file is not None:
            if st.session_state.id_arquivo_atual != uploaded_file.file_id:
                st.session_state.dados_salvos = load_dataframe(uploaded_file)
                st.session_state.id_arquivo_atual = uploaded_file.file_id
        else:
            st.session_state.dados_salvos = None
            st.session_state.id_arquivo_atual = None

        df = st.session_state.dados_salvos
        column_options = df.columns.tolist() if df is not None else []
        
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.selectbox("Age Column", options=column_options, key="col_idade", index=None, placeholder="Select Age column")
        with c2: st.selectbox("Sex/Gender Column", options=column_options, key="col_sexo", index=None, placeholder="Select Sex/Gender")
        with c3: st.selectbox("Data Column", options=column_options, key="col_dados", index=None, placeholder="Select Data Column")
        with c4: st.selectbox("Output Format", ["CSV (.csv)", "Excel (.xlsx)"], key="output_format")

        st.session_state.sex_column_is_valid = True
        st.session_state.age_column_is_valid = True
        sex_column_values = []

        if df is not None:
            if st.session_state.col_sexo:
                try:
                    unique_sex_values = df[st.session_state.col_sexo].dropna().unique()
                    if len(unique_sex_values) > 10:
                        st.warning(f"Column '{st.session_state.col_sexo}' has too many unique values.")
                        st.session_state.sex_column_is_valid = False
                    else: sex_column_values = [""] + list(unique_sex_values)
                except KeyError: st.session_state.sex_column_is_valid = False

            if st.session_state.col_idade:
                try:
                    age_col = df[st.session_state.col_idade].dropna()
                    numeric_ages = pd.to_numeric(age_col, errors='coerce')
                    if (numeric_ages.isna().sum() / len(age_col) if len(age_col) > 0 else 0) > 0.2:
                        st.session_state.age_column_is_valid = False
                except KeyError: st.session_state.age_column_is_valid = False

    is_ready_for_processing = st.session_state.age_column_is_valid and st.session_state.sex_column_is_valid
    
    # --- SISTEMA DE ABAS (TABS) ---
    tab_filter, tab_stratify = st.tabs(["2. Filter Tool", "3. Stratification Tool"])

    # --- ABA 1: FILTER TOOL (LAVE) ---
    with tab_filter:
        st.markdown('<div class="card-with-header">', unsafe_allow_html=True)
        st.markdown(f'<div class="card-header-bar">Exclusion Filter Configuration</div>', unsafe_allow_html=True)
        st.markdown('<div class="card-content-area">', unsafe_allow_html=True)
        
        if st.session_state.get('filter_error'):
            st.error(st.session_state.filter_error)
            del st.session_state['filter_error']
            
        draw_filter_rules(sex_column_values, column_options)
        
        col_btn_add, col_space = st.columns([2, 8])
        with col_btn_add:
            if st.button("+ Add New Filter Rule", type="secondary", use_container_width=True):
                st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '<', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''})
                st.rerun()
                
        st.markdown('</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Generate Filtered Sheet", type="primary", use_container_width=True, disabled=not is_ready_for_processing):
            if df is None: st.error("Please upload a spreadsheet in Global Settings first.")
            else:
                with st.spinner("Applying filters..."):
                    progress_bar = st.progress(0, text="Initializing...")
                    processor = get_data_processor()
                    global_config = {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}
                    filtered_df = processor.apply_filters(df, st.session_state.filter_rules, global_config, progress_bar)
                    if not filtered_df.empty:
                        is_excel = "Excel" in st.session_state.output_format
                        file_bytes = to_excel(filtered_df) if is_excel else to_csv(filtered_df)
                        timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
                        st.session_state.filtered_result = (file_bytes, f"Filtered_Sheet_{timestamp}.{'xlsx' if is_excel else 'csv'}")
                    else: st.success("No rows remaining after filters applied.")

        if 'filtered_result' in st.session_state:
            st.download_button("⬇️ Download Final Filtered Sheet", data=st.session_state.filtered_result[0], file_name=st.session_state.filtered_result[1], use_container_width=True, type="secondary")
        st.markdown("<br>", unsafe_allow_html=True)

    # --- ABA 2: ANÁLISE VISUO-ESTATÍSTICA E ESTRATIFICAÇÃO ---
    with tab_stratify:
        st.markdown('<div class="card-with-header">', unsafe_allow_html=True)
        st.markdown(f'<div class="card-header-bar">Visual-Statistical Analysis and Stratification</div>', unsafe_allow_html=True)
        st.markdown('<div class="card-content-area">', unsafe_allow_html=True)

        if df is not None:
            if not st.session_state.col_idade or not st.session_state.col_dados:
                st.info("⚠️ Select the **'Age Column'** and **'Data Column'** in Global Settings to enable visual analysis and Harris-Boyd stratification.")
            else:
                col_grafico, col_hboyd = st.columns([3, 1], gap="large")
                
                # Variáveis capturadas para repasse ao mini-card de Harris Boyd
                group_by_sex_plot = False
                selected_sexes_for_plot = []

                with col_grafico:
                    gc1, gc2, gc3, gc4 = st.columns(4)
                    chart_type = gc1.selectbox("Chart Type", ["Boxplot", "Moving Average", "Moving Median"], label_visibility="collapsed")
                    intervalo_plot = gc2.number_input("Age interval", min_value=1, max_value=20, value=5, step=1, label_visibility="collapsed", help="Age interval in years")
                    
                    show_trendlines = False

                    if chart_type in ['Moving Average', 'Moving Median']:
                        show_trendlines = gc3.checkbox("Plateau Lines", value=True, help="Draws horizontal lines based on Harris-Boyd cuts.")
                        
                    if st.session_state.col_sexo and st.session_state.sex_column_is_valid:
                        group_by_sex_plot = gc4.checkbox("Group by Sex", value=False)
                        sex_options_for_plot = [v for v in sex_column_values if v]
                    
                    # --- NOVA CAIXA DE SELEÇÃO DE SEXO ---
                        selected_sexes_for_plot = st.multiselect(
                        "Filter specific sexes for the chart:", 
                            options=sex_options_for_plot, 
                            default=sex_options_for_plot
                    )
                    
                    # Prevenção: se o usuário apagar todas as opções, volta ao padrão (mostra tudo)
                        if not selected_sexes_for_plot:
                            selected_sexes_for_plot = sex_options_for_plot
                    
                    fig = plot_dispersion_chart(
                        df, st.session_state.col_idade, st.session_state.col_dados, st.session_state.col_sexo,
                        intervalo_plot, chart_type, group_by_sex_plot, selected_sexes_for_plot, show_trendlines
                    )
                    if fig: st.pyplot(fig)
                    else: st.warning("Not enough valid data to generate chart.")

                with col_hboyd:
                    st.markdown('<div class="mini-card-dark">', unsafe_allow_html=True)
                    st.markdown("<h4>Harris-Boyd Study</h4>", unsafe_allow_html=True)
                    
                    with st.spinner("Calculating..."):
                        if group_by_sex_plot and st.session_state.col_sexo:
                            # Loop estratificando por sexo caso a checkbox esteja ativa
                            sex_options_hboyd = [v for v in sex_column_values if v]
                            all_raw_dfs = []
                            for sex_val in sex_options_hboyd:
                                st.markdown(f"<p style='font-size:0.95rem; color:#A6DCEF; margin-bottom:2px; margin-top:10px;'><b>Sex: {sex_val}</b></p>", unsafe_allow_html=True)
                                sub_df = df[df[st.session_state.col_sexo].astype(str) == str(sex_val)]

                                if sub_df.empty:
                                    st.markdown("<p style='font-weight:bold; font-size:1.0rem;'>No data</p>", unsafe_allow_html=True)
                                    continue

                                _, raw_df, cuts = run_harris_boyd(sub_df, st.session_state.col_idade, st.session_state.col_dados)

                                if not cuts:
                                    st.markdown("<p style='font-weight:bold; font-size:0.9rem;'>No stratification needed</p>", unsafe_allow_html=True)
                                else:
                                    for cut in cuts:
                                        st.markdown(f"<p style='font-weight:bold; font-size:1.0rem; color:{COLOR_SECONDARY};'>Barrier at {cut} years</p>", unsafe_allow_html=True)

                                if not raw_df.empty:
                                    raw_df.insert(0, 'Sex', str(sex_val))
                                    all_raw_dfs.append(raw_df)

                            st.markdown("</div>", unsafe_allow_html=True)

                            with st.expander("View Full Statistical Data", expanded=False):
                                if all_raw_dfs:
                                    st.dataframe(pd.concat(all_raw_dfs, ignore_index=True), use_container_width=True, hide_index=True)
                                else:
                                    st.write("Insufficient variance data.")

                        else:
                            # Visão normal/geral, sem estratificar por sexo no mini-card
                            texto_interpretativo, raw_df, cuts = run_harris_boyd(df, st.session_state.col_idade, st.session_state.col_dados)
                            
                            st.markdown("<p style='font-size:0.85rem; color:#A6DCEF; margin-bottom:5px;'>Recommended Age Cuts:</p>", unsafe_allow_html=True)
                            if not cuts:
                                st.markdown("<p style='font-weight:bold; font-size:1.1rem;'>No stratification needed</p>", unsafe_allow_html=True)
                            else:
                                for cut in cuts:
                                    st.markdown(f"<p style='font-weight:bold; font-size:1.2rem; color:{COLOR_SECONDARY};'>Barrier at {cut} years</p>", unsafe_allow_html=True)
                            
                            st.markdown("</div>", unsafe_allow_html=True)
                            
                            with st.expander("View Full Statistical Data", expanded=False):
                                if not raw_df.empty: st.dataframe(raw_df, use_container_width=True, hide_index=True)
                                else: st.write("Insufficient variance data.")

                st.markdown("<hr style='border-color: rgba(7, 59, 76, 0.1); margin: 2rem 0;'>", unsafe_allow_html=True)
                st.markdown(f"<h3 style='color: {COLOR_PRIMARY}; font-size: 1.2rem;'>Generate Stratified Sheets</h3>", unsafe_allow_html=True)
                
                s_col1, s_col2 = st.columns([1, 1])
                with s_col1:
                    st.write("**Age Range Definitions**")
                    draw_stratum_rules()
                    if st.button("Add Age Range", type="secondary"):
                        st.session_state.stratum_rules.append({'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''})
                        st.rerun()

                with s_col2:
                    if st.session_state.sex_column_is_valid and sex_column_values:
                        st.write("**Sex/Gender Filtering**")
                        if 'strat_gender_selection' not in st.session_state: st.session_state.strat_gender_selection = {val: True for val in sex_column_values if val}
                        cols = st.columns(min(len(sex_column_values), 3))
                        col_idx = 0
                        for gender_val in sex_column_values:
                            if not gender_val: continue
                            st.session_state.strat_gender_selection[gender_val] = cols[col_idx].checkbox(str(gender_val), value=st.session_state.strat_gender_selection.get(gender_val, True), key=f"strat_check_{gender_val}")
                            col_idx = (col_idx + 1) % len(cols)
                
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("Execute Stratification Splitting", type="secondary", use_container_width=True):
                    st.session_state.confirm_stratify = True
                    st.rerun()

                if st.session_state.get('confirm_stratify', False):
                    st.warning("Ensure you are stratifying the CORRECT file. Do you wish to proceed?")
                    c1, c2 = st.columns(2)
                    if c1.button("Yes, split data", type="primary"):
                        with st.spinner("Generating strata..."):
                            progress_bar = st.progress(0, text="Initializing...")
                            processor = get_data_processor()
                            age_rules = [r for r in st.session_state.stratum_rules if r.get('val1')]
                            sex_rules = [{'value': gender_val, 'name': str(gender_val)} for gender_val, is_selected in st.session_state.get('strat_gender_selection', {}).items() if is_selected]
                            st.session_state.stratified_results = processor.apply_stratification(df.copy(), {'ages': age_rules, 'sexes': sex_rules}, {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}, progress_bar)
                        st.session_state.confirm_stratify = False
                        st.rerun()
                    if c2.button("Cancel"):
                        st.session_state.confirm_stratify = False
                        st.rerun()

                if st.session_state.get('stratified_results'):
                    st.success(f"Successfully generated {len(st.session_state.stratified_results)} files!")
                    is_excel = "Excel" in st.session_state.output_format
                    for filename, df_to_download in st.session_state.stratified_results.items():
                        file_bytes = to_excel(df_to_download) if is_excel else to_csv(df_to_download)
                        st.download_button(f"📄 Download {filename}", data=file_bytes, file_name=f"{filename}.{'xlsx' if is_excel else 'csv'}", key=f"dl_{filename}", type="secondary")

        else:
            st.info("⚠️ Please upload a spreadsheet to access the analysis and stratification tools.")
            
        st.markdown('</div></div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()
