# -*- coding: utf-8 -*-

# Version 3.5.0 (State Buffer Architecture & Explicit Analysis Processing)
import streamlit as st
import pandas as pd
from scipy import stats
import numpy as np
import math
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

# --- PAGE CONFIGURATION & THEME ---
st.set_page_config(
    page_title="DataSift",
    page_icon="favicon.png", 
    layout="wide" 
)

# Color Palette Based on Reference Image
COLOR_PRIMARY = "#073B4C"     # Dark Teal
COLOR_SECONDARY = "#00E5FF"   # Bright Neon Cyan (Buttons and Highlights)
COLOR_TERTIARY = "#118AB2"    # Medium Teal
COLOR_BG = "#F8F9FA"          # Off-white Background
COLOR_CARD_BG = "#FFFFFF"     # Pure White Card Background

# Função para gerar o Help Option Icon dinamicamente com texto
def make_help_icon(tooltip_text):
    return f"<span style='cursor: help; color: #118AB2; font-size: 0.85em; font-weight: bold; background: #E0F7FA; border-radius: 50%; padding: 0px 5px; margin-left: 5px;' title='{tooltip_text}'>?</span>"

HELP_ICON_PLATEAU = make_help_icon('Draws horizontal lines based on stratification cuts.')

# CSS Injection for Visual Identity and Card-Based Layout
st.markdown(f"""
    <style>
        /* Structural Adjustments and Page Background */
        [data-testid="stAppViewBlockContainer"] {{
            padding-top: 2rem !important;
        }}
        .stApp {{ background-color: {COLOR_BG} !important; }}
        [data-testid="stStatusWidget"] {{ visibility: hidden; }}
        
        /* Força a cor do texto padrão a ser escura para contrastar com o fundo claro */
        p, span, div[data-testid="stMarkdownContainer"], label {{
            color: #212529 !important;
        }}

        /* Hide Default Titles to Use Custom HTML */
        h1, h2, h3, h4, h5, h6 {{
            color: {COLOR_PRIMARY} !important;
            font-weight: 800 !important;
        }}

        /* --- MULTISELECT TAGS (Filtro de Sexo) --- */
        div[data-testid="stMultiSelect"] span[data-baseweb="tag"] {{
            background-color: {COLOR_SECONDARY} !important;
        }}
        div[data-testid="stMultiSelect"] span[data-baseweb="tag"] span {{
            color: #000000 !important;
            font-weight: 700 !important;
        }}
        div[data-testid="stMultiSelect"] span[data-baseweb="tag"] svg {{
            fill: #000000 !important;
            color: #000000 !important;
        }}
        
        /* --- CARD STYLES --- */
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

        /* --- BUTTONS --- */
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
        
        /* Mini Buttons (Clone/X) */
        .stButton>button {{ padding: 0.15rem 0.5rem; }}

        /* --- INPUTS --- */
        div[data-testid="stTextInput"] input, div[data-testid="stSelectbox"] div[data-baseweb="select"], div[data-testid="stNumberInput"] input {{
            background-color: #F0F4F8 !important;
            border: 1px solid #CFD8DC !important; 
            border-radius: 6px;
            color: {COLOR_PRIMARY} !important;
        }}
        div[data-testid="stTextInput"] input:focus, div[data-testid="stSelectbox"] div[data-baseweb="select"]:focus-within, div[data-testid="stNumberInput"] input:focus {{
            border-color: {COLOR_TERTIARY} !important;
            box-shadow: 0 0 0 1px {COLOR_TERTIARY} !important;
        }}
        label[data-testid="stWidgetLabel"] p {{
            color: {COLOR_PRIMARY} !important;
            font-weight: 600 !important;
            font-size: 0.9rem !important;
        }}
        
        /* Progress Bar */
        .stProgress > div > div > div > div {{ background-color: {COLOR_SECONDARY} !important; }}
        
        /* --- MINI CARD SIDEBAR --- */
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

# --- CONSTANTS & DOCUMENTATION ---
GDPR_TERMS = """
This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, national ID numbers, health information, etc.), the processing of which is regulated by data protection laws like the General Data Protection Regulation (GDPR or LGPD).

It is your sole responsibility to ensure that all data used in this tool complies with applicable data protection regulations. We strongly recommend that you only use previously anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been properly handled and anonymized.
"""

MANUAL_CONTENT = {
    "Introduction": """**Welcome to Data Sift!**\n\nThis program is a spreadsheet filter tool designed to optimize your work with large volumes of data by offering two main functionalities:\n\n1.  **Filtering:** To clean your database by removing rows that are not of interest.\n2.  **Stratification:** To divide your database into specific subgroups.""",
    "1. Global Settings": """**1. Global Settings**\n\nThis section contains the essential settings that are shared between both tools.\n\n- **Select Spreadsheet:**\n  Opens a window to select the source data file. It supports `.xlsx`, `.xls`, and `.csv` formats.\n\n- **Age Column / Sex/Gender / Data Column:**\n  Fields to **select** the column names in your spreadsheet. The **Data Column** is specifically used to automatically run the stratification study and generate charts.\n\n- **Output Format:**\n  A selection menu to choose the format of the generated files. Choose `Excel (.xlsx)` for Microsoft Excel or `CSV (.csv)` for a lighter format.""",
    "2. Filter Tool": """**2. Filter Tool**\n\nThe purpose of this tool is to **"clean"** your spreadsheet by **removing** rows that match specific criteria. The result is a **single file** containing only the data that "survived" the filters.\n\n**How Exclusion Rules Work:**\nEach row you add is a condition to **remove** data. If a row in your spreadsheet matches an active rule, it **will be excluded** from the final file.\n\n- **[✓] (Activation Checkbox):** Toggles a rule on or off without deleting it.\n\n- **Column:** The name of the column where the filter will be applied.\n\n- **Operator and Value:** Operators define the rule's logic to set exclusion ranges.\n\n- **Compound Logic:** Expands the rule to create `AND` / `OR` conditions.\n\n- **Condition:** Allows applying a secondary filter based on sex and/or age conditions.\n\n- **Actions:** The `X` button deletes the rule. The 'Clone' button duplicates it.""",
    "3. Stratification Tool": """**3. Stratification Tool**\n\nThis tool splits your spreadsheet into **multiple smaller files**, where each file represents a subgroup of interest.\n\n**Statistical and Practical approaches & Charts:**\nAutomatically evaluates the selected Data Column and Age Column to suggest the most relevant age cuts. If Reference Limits are provided, Haeckel's formula is executed.\n\n**How Stratification Works:**\n- **Stratification Options by Sex/Gender:** Select the genders you want to include.\n- **Age Range Definitions:** Create the specific age boundaries.\n- **Generate Stratified Sheets:** Starts the splitting process."""
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
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.LEUCO', 'p_op1': '>', 'p_val1': '11000', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.#HGB', 'p_op1': '<', 'p_val1': '7', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSSV', 'p_op1': 'Not equal to', 'p_val1': 'empty', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSSB', 'p_op1': 'Not equal to', 'p_val1': 'empty', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Hemo.OBSPLT', 'p_op1': 'Not equal to', 'p_val1': 'empty', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'TGP.TGP', 'p_op1': '>', 'p_val1': '41', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'TGO.TGO', 'p_op1': '>', 'p_val1': '40', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'BTF.BTBTF', 'p_op1': '>', 'p_val1': '2,4', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'FALC.FALC', 'p_op1': '>', 'p_val1': '129', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'GGT.GGT', 'p_op1': '>', 'p_val1': '60', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'LIPIDOGRAMA.COL2', 'p_op1': '>', 'p_val1': '190', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'COLESTEROL TOTAL E FRACOES.COL2', 'p_op1': '>', 'p_val1': '190', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'LIPIDOGRAMA.TRI2', 'p_op1': '>', 'p_val1': '150', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'COLESTEROL TOTAL E FRACOES.TRI2', 'p_op1': '>', 'p_val1': '150', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'LIPIDOGRAMA.LDL2', 'p_op1': '>', 'p_val1': '130', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'COLESTEROL TOTAL E FRACOES.LDLD', 'p_op1': '>', 'p_val1': '130', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'LIPIDOGRAMA.HDL5', 'p_op1': '>', 'p_val1': '80', 'p_expand': False, 'c_check': False},
    {'id': str(uuid.uuid4()), 'p_check': False, 'p_col': 'COLESTEROL TOTAL E FRACOES.HDL5', 'p_op1': '>', 'p_val1': '80', 'p_expand': False, 'c_check': False},
]

# --- PROCESSING ENGINE CLASSES ---
@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '=', '==': '=', 'Is not equal to': '!=', '≥': '>=', '≤': '<=', 'is equal to': '=', 'Not equal to': '!='}

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

# --- CACHED UTILITY FUNCTIONS ---

@st.cache_data(show_spinner="Reading file...")
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
                valid_files = [f for f in z.namelist() if not f.startswith('__MACOSX/') and 
                               (f.lower().endswith('.csv') or f.lower().endswith(('.xlsx', '.xls')))]
                if not valid_files:
                    st.error("The ZIP file contains no valid CSV or Excel files.")
                    os.remove(tmp_path)
                    return None
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(valid_files[0])[1]) as inner_tmp:
                    inner_tmp.write(z.read(valid_files[0]))
                    inner_path = inner_tmp.name
                inner_filename = valid_files[0].lower()
                if inner_filename.endswith('.csv'):
                    try: df = pd.read_csv(inner_path, sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
                    except Exception: df = pd.read_csv(inner_path, sep=',', decimal='.', encoding='utf-8', engine='pyarrow')
                else: df = pd.read_excel(inner_path, engine='openpyxl')
                os.remove(inner_path)
        elif file_name.endswith('.csv'):
            try: df = pd.read_csv(tmp_path, sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
            except Exception: df = pd.read_csv(tmp_path, sep=',', decimal='.', encoding='utf-8', engine='pyarrow')
        else:
            df = pd.read_excel(tmp_path, engine='openpyxl')

        if os.path.exists(tmp_path): os.remove(tmp_path)

        if df is not None:
            for col in df.select_dtypes(include=['object']).columns:
                mask = df[col].notna()
                df.loc[mask, col] = df.loc[mask, col].astype(str)
                try:
                    if col != st.session_state.col_dados and df[col].nunique() / len(df[col]) < 0.5:
                        df[col] = df[col].astype('category')
                except Exception: pass 
        return df
    except Exception as e:
        st.error(f"Error reading file: {e}")
        return None

def remove_outliers_tukey(df, col_dados, iterations=5, multiplier=2.0):
    df_clean = df.copy()
    for _ in range(iterations):
        if df_clean.empty:
            break
        Q1 = df_clean[col_dados].quantile(0.25)
        Q3 = df_clean[col_dados].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - (multiplier * IQR)
        upper_bound = Q3 + (multiplier * IQR)
        
        mask = (df_clean[col_dados] >= lower_bound) & (df_clean[col_dados] <= upper_bound)
        if mask.all(): 
            break 
            
        df_clean = df_clean[mask]
        
    return df_clean

def calcular_limites_haeckel(lri: float, lrs: float):
    # Interceptação para conversão automática do LRI para 15% do LRS
    # se o LRI for vazio (None) ou igual a 0.0, DESDE que o LRS seja um valor válido.
    if lrs is not None and lrs > 0:
        if lri is None or lri <= 0:
            lri = 0.15 * lrs
            
    if lri is None or lrs is None or lri <= 0 or lrs <= lri:
        return None
    
    se_ln = (math.log(lrs) - math.log(lri)) / 3.92
    med_ln_val = (math.log(lri) + math.log(lrs)) / 2
    med = math.exp(med_ln_val)
    
    cve_star = 100 * math.sqrt(math.exp(se_ln**2) - 1)
    val_to_sqrt = cve_star - 0.25
    pcva = math.sqrt(val_to_sqrt) if val_to_sqrt >= 0 else 0
    psa_med = pcva * 0.01 * med
    
    slope = (psa_med - 0.2 * psa_med) / med
    intercept = 0.2 * psa_med
    
    def calc_for_x(x):
        if x <= 0: return {'psa': 0, 'pcva': 0, 'pb': 0}
        psa_x = slope * x + intercept
        pcva_x = (psa_x / x) * 100
        pb_x = pcva_x * 0.70
        return {'psa': psa_x, 'pcva': pcva_x, 'pb': pb_x}
    
    return {
        'lri': lri, 'lrs': lrs, 'cve': cve_star, 'pcva': pcva, 'med': med,
        'psa_med': psa_med, 'slope': slope, 'intercept': intercept,
        'm_lri': calc_for_x(lri), 'm_lrs': calc_for_x(lrs)
    }

def encontrar_limites_casados(idade: float, sexo: str, lista_limites: list) -> Optional[dict]:
    if not lista_limites: return None
    sexo_str = str(sexo).strip().lower() if sexo else ""
    
    filtrados_sexo = []
    for item in lista_limites:
        s_lim = str(item.get('sex', '')).strip().lower()
        if s_lim in ('all', 'todos', '', sexo_str):
            filtrados_sexo.append(item)
            
    if not filtrados_sexo: return None
    
    com_idade = [item for item in filtrados_sexo if item.get('age_min') is not None or item.get('age_max') is not None]
    globais = [item for item in filtrados_sexo if item.get('age_min') is None and item.get('age_max') is None]
    
    if not com_idade:
        for g in globais:
            if str(g.get('sex', '')).strip().lower() == sexo_str: return g
        return globais[0] if globais else None
        
    match_direto = []
    for item in com_idade:
        amin = item.get('age_min') if item.get('age_min') is not None else 0
        amax = item.get('age_max') if item.get('age_max') is not None else 9999
        if amin <= idade <= amax:
            match_direto.append(item)
            
    if match_direto:
        for m in match_direto:
            if str(m.get('sex', '')).strip().lower() == sexo_str: return m
        return match_direto[0]
        
    ordenados_por_min = sorted(com_idade, key=lambda x: x.get('age_min') if x.get('age_min') is not None else 0)
    menor_idade = ordenados_por_min[0].get('age_min', 0) if ordenados_por_min[0].get('age_min') is not None else 0
    
    if idade < menor_idade:
        for o in ordenados_por_min:
            if str(o.get('sex', '')).strip().lower() == sexo_str: return o
        return ordenados_por_min[0]
        
    ordenados_por_max = sorted(com_idade, key=lambda x: x.get('age_max') if x.get('age_max') is not None else 9999, reverse=True)
    maior_idade = ordenados_por_max[0].get('age_max', 9999) if ordenados_por_max[0].get('age_max') is not None else 9999
    
    if idade > maior_idade:
        for o in ordenados_por_max:
            if str(o.get('sex', '')).strip().lower() == sexo_str: return o
        return ordenados_por_max[0]
        
    return globais[0] if globais else None

@st.cache_data(show_spinner=False)
def run_harris_boyd(df, col_idade, col_dados, lista_limites=None, sexo_contexto="All"):
    temp_df = pd.DataFrame()
    temp_df['Age'] = pd.to_numeric(df[col_idade], errors='coerce')

    def clean_val(x):
        if pd.isna(x): return np.nan
        x = str(x).replace(',', '.')
        x = ''.join(c for c in x if c.isdigit() or c == '.' or c == '-')
        try: return float(x)
        except: return np.nan

    temp_df['Data'] = df[col_dados].apply(clean_val).astype('float64')
    temp_df = temp_df.dropna(subset=['Age', 'Data'])
    temp_df = temp_df[temp_df['Age'] >= 0].copy()
    temp_df = remove_outliers_tukey(temp_df, 'Data', iterations=5, multiplier=2.0)

    if temp_df.empty: return pd.DataFrame(), pd.DataFrame(), [], False
    max_age = int(temp_df['Age'].max())
    if max_age < 1: return pd.DataFrame(), pd.DataFrame(), [], False

    # =========================================================================
    # TRACK 1: HARRIS-BOYD — RECURSIVE HIERARCHICAL PARTITIONING
    # Finds the globally best cut, then recurses on each partition.
    # Result: typically 2–5 clinically meaningful cuts instead of 70+.
    # =========================================================================
    MIN_N = 30  # minimum subjects per partition to attempt a cut

    def find_best_cut(df_sub: pd.DataFrame):
        """Return the single most statistically significant cut in df_sub, or None."""
        if len(df_sub) < 2 * MIN_N:
            return None

        min_a = int(df_sub['Age'].min())
        max_a = int(df_sub['Age'].max())
        best_cut = None
        best_z = 0.0

        for age_cutoff in range(min_a, max_a):
            g1 = df_sub[df_sub['Age'] <= age_cutoff]['Data']
            g2 = df_sub[df_sub['Age'] > age_cutoff]['Data']
            n1, n2 = len(g1), len(g2)

            if n1 < MIN_N or n2 < MIN_N:
                continue

            mean1, mean2 = float(np.mean(g1)), float(np.mean(g2))
            var1,  var2  = float(np.var(g1, ddof=1)), float(np.var(g2, ddof=1))
            sd1,   sd2   = np.sqrt(var1), np.sqrt(var2)

            # Fallback seguro: se min(sd1, sd2) for 0, a razão se iguala a 1.0 (não ativando o gatilho falso > 1.5)
            sd_ratio = max(sd1, sd2) / min(sd1, sd2) if min(sd1, sd2) > 0 else 1.0
            
            denom = np.sqrt((var1 / n1) + (var2 / n2))
            if denom == 0:
                continue
                
            z = abs(mean1 - mean2) / denom
            z_crit = 3 * np.sqrt((n1 + n2) / 120) if (n1 + n2) < 120 else 3.0

            is_significant = (sd_ratio > 1.5 or z > z_crit)

            # Keep only the most extreme significant cut in this partition
            if is_significant and z > best_z:
                best_z = z
                best_cut = {
                    'age': age_cutoff,
                    'Age Cutoff': f"<= {age_cutoff} vs > {age_cutoff}",
                    'Z-score': round(z, 2),
                    'SD Ratio': round(sd_ratio, 2),
                    'Mean (<= Cutoff)': round(mean1, 2),
                    'Mean (> Cutoff)': round(mean2, 2),
                }
        return best_cut

    def recursive_partition(df_sub: pd.DataFrame, found: list, depth: int = 0):
        """
        Recursively split df_sub.
        Each call adds at most ONE cut (the best one in this sub-range),
        then dives into the two resulting halves.
        depth cap = 6  →  maximum 2^6 - 1 = 63 cuts, in practice 2–5.
        """
        if depth >= 6:
            return
        best = find_best_cut(df_sub)
        if best is None:
            return
        found.append(best)
        cut_age = best['age']
        recursive_partition(df_sub[df_sub['Age'] <= cut_age], found, depth + 1)
        recursive_partition(df_sub[df_sub['Age'] > cut_age],  found, depth + 1)

    possible_cuts_hb = []
    recursive_partition(temp_df, possible_cuts_hb)
    possible_cuts_hb.sort(key=lambda x: x['age'])

    df_possible = pd.DataFrame(possible_cuts_hb) if possible_cuts_hb else pd.DataFrame()

    # =========================================================================
    # TRACK 2: DYNAMIC CRITICAL BOUNDARY EVALUATION (HAECKEL VS AEDM)
    # =========================================================================
    global_mean = temp_df['Data'].mean()
    global_sd   = temp_df['Data'].std(ddof=1)
    global_cv   = (global_sd / global_mean) if global_mean > 0 else 0.10
    cv_tolerance_margin = global_cv * 0.50

    age_groups = temp_df.groupby('Age')['Data'].agg(['mean', 'count']).reset_index()
    age_groups = age_groups.sort_values(by='Age').to_dict('records')

    clinical_cuts = []
    idades_sugeridas = []
    any_haeckel_applied = False

    if age_groups:
        current_bracket_means = [age_groups[0]['mean']]

        for i in range(1, len(age_groups)):
            current_age_data = age_groups[i]
            reference_mean   = np.mean(current_bracket_means)
            pct_diff = abs(current_age_data['mean'] - reference_mean) / reference_mean if reference_mean > 0 else 0

            is_significant = False
            margin_disp    = 0

            limite_casado = encontrar_limites_casados(current_age_data['Age'], sexo_contexto, lista_limites)

            h_local = None
            if limite_casado and limite_casado.get('lrs') is not None and limite_casado.get('lrs') > 0:
                h_local = calcular_limites_haeckel(limite_casado.get('lri'), limite_casado.get('lrs'))

            if h_local and reference_mean > 0:
                any_haeckel_applied = True
                psa_x        = (h_local['slope'] * reference_mean) + h_local['intercept']
                pd_margin    = 1.645 * psa_x
                diff_absoluta = abs(current_age_data['mean'] - reference_mean)
                is_significant = diff_absoluta > pd_margin
                margin_disp  = round(pd_margin, 3)
            else:
                is_significant = pct_diff > cv_tolerance_margin
                margin_disp  = round(cv_tolerance_margin * 100, 2)

            if is_significant and current_age_data['count'] >= 5:
                cutoff_age = int(age_groups[i - 1]['Age'])
                m_less    = temp_df[temp_df['Age'] <= cutoff_age]['Data'].mean()
                m_greater = temp_df[temp_df['Age'] > cutoff_age]['Data'].mean()

                clinical_cuts.append({
                    'age': cutoff_age,
                    'Age Cutoff': f"<= {cutoff_age} vs > {cutoff_age}",
                    'Diff %':   round(pct_diff * 100, 2),
                    'Limit Threshold': margin_disp,
                    'Mean (<= Cutoff)': round(m_less, 2),
                    'Mean (> Cutoff)':  round(m_greater, 2),
                })
                idades_sugeridas.append(cutoff_age)
                current_bracket_means = [current_age_data['mean']]
            else:
                current_bracket_means.append(current_age_data['mean'])

    df_ideal = pd.DataFrame(clinical_cuts).sort_values(by='age') if clinical_cuts else pd.DataFrame()

    return df_possible, df_ideal, idades_sugeridas, any_haeckel_applied


def plot_dispersion_chart(df, col_idade, col_dados, col_sexo, intervalo, chart_type, group_by_sex, selected_sexes, show_trendlines, lista_limites, age_filter_range):
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
    
    # O Filtro de idade agora funciona pois a função recebeu o age_filter_range
    temp_df = temp_df[(temp_df['Age'] >= age_filter_range[0]) & (temp_df['Age'] <= age_filter_range[1])]
    
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
    elif chart_type in ['Moving Average', 'Moving Median']:
        metric_func = np.mean if chart_type == 'Moving Average' else np.median
        if hue_col: sns.lineplot(data=temp_df, x=x_col, y='Data', hue=hue_col, palette=palette_custom, estimator=metric_func, marker='o', errorbar=None, ax=ax, linewidth=2, markersize=8)
        else: sns.lineplot(data=temp_df, x=x_col, y='Data', estimator=metric_func, marker='o', color=single_color, errorbar=None, ax=ax, linewidth=2, markersize=8)

        if show_trendlines:
            metric_str = 'mean' if chart_type == 'Moving Average' else 'median'
            def draw_segments(df_sub, color, s_context):
                _, _, cuts, _ = run_harris_boyd(df_sub, 'Age', 'Data', lista_limites, s_context)
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
                for i, sex_val in enumerate(temp_df[hue_col].dropna().unique()): 
                    draw_segments(temp_df[temp_df[hue_col] == sex_val], palette[i], str(sex_val))
            else: 
                draw_segments(temp_df, COLOR_SECONDARY, "All")

    # --- NOME DA COLUNA NO EIXO Y ---
    ax.set_ylabel(col_dados, fontsize=12, labelpad=10)
    ax.set_xlabel('Age (Years)', fontsize=12, labelpad=10)
    
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=90 if len(categories) > 30 else 45, ha='center' if len(categories) > 30 else 'right', fontsize=8 if len(categories) > 40 else 10)
    plt.grid(axis='y', linestyle=':', alpha=0.6, color='#CFD8DC')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    if hue_col:
        ax.legend(title='Sex/Gender', frameon=True, facecolor='white', edgecolor='#e0e0e0', loc='upper left', bbox_to_anchor=(1.01, 1))
        plt.subplots_adjust(right=0.85)

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

# --- USER INTERFACE BUILDER FUNCTIONS ---
def draw_filter_rules(sex_column_values, column_options): 
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="small")
    header_cols[1].markdown(f"**Column** {make_help_icon('The name of the column where the filter will be applied.')}", unsafe_allow_html=True)
    header_cols[2].markdown(f"**Operator** {make_help_icon('Defines the rule logic to set exclusion ranges.')}", unsafe_allow_html=True)
    header_cols[3].markdown(f"**Value** {make_help_icon('The target value to trigger the exclusion.')}", unsafe_allow_html=True)
    header_cols[5].markdown(f"**Compound Logic** {make_help_icon('Expands the rule to create AND / OR conditions.')}", unsafe_allow_html=True)
    header_cols[6].markdown(f"**Cond** {make_help_icon('Allows applying a secondary filter based on sex and/or age conditions.')}", unsafe_allow_html=True)
    header_cols[7].markdown(f"**Action** {make_help_icon('Clone (C) or Delete (X) the rule.')}", unsafe_allow_html=True)

    ops_main, ops_age, ops_central_logic = ["", ">", "<", "=", "Not equal to", "≥", "≤"], ["", ">", "<", "≥", "≤", "="], ["AND", "OR", "BETWEEN"]

    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="small") 
            rule['p_check'] = cols[0].checkbox(f"Act {rule['id']}", value=rule.get('p_check', True), key=f"p_check_{rule['id']}", label_visibility="collapsed")
            rule['p_col'] = cols[1].text_input("Column", value=rule.get('p_col', ''), key=f"p_col_{rule['id']}", label_visibility="collapsed")
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
            if action_cols[0].button("C", key=f"clone_{rule['id']}"):
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
                            rule['c_sexo_val'] = st.selectbox("Sex Val", options=sex_options, index=sex_index, key=f"c_sexo_val_{rule['id']}", label_visibility="collapsed")
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

def draw_reference_limits_matrix(sex_options):
    st.markdown("##### 📊 Custom Stratified Reference Intervals Matrix")
    st.markdown("<p style='font-size:0.85rem; color:#555;'>Configure reference criteria for specific sexes and age ranges. Leave ages blank to apply globally to that sex subgroup.</p>", unsafe_allow_html=True)
    
    h_cols = st.columns([2, 1.5, 1.5, 2, 2, 1])
    h_cols[0].markdown("**Sex/Gender**")
    h_cols[1].markdown("**Min Age (Y)**")
    h_cols[2].markdown("**Max Age (Y)**")
    h_cols[3].markdown("**Lower Ref Limit (LRI)**")
    h_cols[4].markdown("**Upper Ref Limit (LRS)**")
    h_cols[5].markdown("**Action**")

    sex_dropdown_options = ["All"] + [x for x in sex_options if x]

    for idx, item in enumerate(st.session_state.ref_limits_list):
        with st.container():
            r_cols = st.columns([2, 1.5, 1.5, 2, 2, 1])
            
            s_idx = sex_dropdown_options.index(item['sex']) if item['sex'] in sex_dropdown_options else 0
            item['sex'] = r_cols[0].selectbox(f"sex_{item['id']}", sex_dropdown_options, index=s_idx, label_visibility="collapsed")
            
            item['age_min'] = r_cols[1].number_input(f"amin_{item['id']}", min_value=0, value=item['age_min'], step=1, label_visibility="collapsed")
            item['age_max'] = r_cols[2].number_input(f"amax_{item['id']}", min_value=0, value=item['age_max'], step=1, label_visibility="collapsed")
            item['lri'] = r_cols[3].number_input(f"lri_{item['id']}", min_value=0.0, format="%.3f", value=item['lri'], label_visibility="collapsed")
            item['lrs'] = r_cols[4].number_input(f"lrs_{item['id']}", min_value=0.0, format="%.3f", value=item['lrs'], label_visibility="collapsed")
            
            if r_cols[5].button("X", key=f"del_ref_{item['id']}", help="Remove this reference range"):
                st.session_state.ref_limits_list.pop(idx)
                st.rerun()

    if st.button("+ Add New Reference Interval Row", type="secondary"):
        st.session_state.ref_limits_list.append({'id': str(uuid.uuid4()), 'sex': 'All', 'age_min': None, 'age_max': None, 'lri': None, 'lrs': None})
        st.rerun()

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    
    # --- ENTER PRIVACY COMPLIANCE SCREEN ---
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

    # --- SESSION STATE INITIALIZATION ---
    if 'filter_rules' not in st.session_state: 
        st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)
    if 'stratum_rules' not in st.session_state: 
        st.session_state.stratum_rules = [{'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''}]
    if 'ref_limits_list' not in st.session_state:
        st.session_state.ref_limits_list = [{'id': str(uuid.uuid4()), 'sex': 'All', 'age_min': None, 'age_max': None, 'lri': None, 'lrs': None}]
    
    with st.sidebar:
        if logo_base64: st.markdown(f'<div style="display: flex; justify-content: center; margin-bottom: 1rem;"><img src="data:image/png;base64,{logo_base64}" width="150"></div>', unsafe_allow_html=True)
        st.markdown("---")
        topic = st.selectbox("Select Screen Mode", list(MANUAL_CONTENT.keys()))
        st.markdown(MANUAL_CONTENT[topic], unsafe_allow_html=True)

    if logo_base64: st.markdown(f'<div style="display: flex; justify-content: center; margin-top: 1rem; margin-bottom: 2rem;"><img src="data:image/png;base64,{logo_base64}" width="220"></div>', unsafe_allow_html=True)

    # --- CARD 1: GLOBAL SETTINGS ---
    with st.expander("📁 1. Global Settings (Upload Spreadsheet)", expanded=True):
        def reset_results_on_upload():
            if 'filtered_result' in st.session_state: del st.session_state['filtered_result']
            if 'stratified_results' in st.session_state: del st.session_state['stratified_results']
            if 'analysis_params' in st.session_state: del st.session_state['analysis_params']
            if 'analysis_results' in st.session_state: del st.session_state['analysis_results']
            st.session_state.confirm_stratify = False
            
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
                    if len(unique_sex_values) > 10: st.session_state.sex_column_is_valid = False
                    else: sex_column_values = [""] + list(unique_sex_values)
                except KeyError: st.session_state.sex_column_is_valid = False

            if st.session_state.col_idade:
                try:
                    age_col = df[st.session_state.col_idade].dropna()
                    numeric_ages = pd.to_numeric(age_col, errors='coerce')
                    if (numeric_ages.isna().sum() / len(age_col) if len(age_col) > 0 else 0) > 0.2: st.session_state.age_column_is_valid = False
                except KeyError: st.session_state.age_column_is_valid = False

    is_ready_for_processing = st.session_state.age_column_is_valid and st.session_state.sex_column_is_valid
    
    # --- NAVIGATION TABS ---
    tab_filter, tab_stratify = st.tabs(["2. Filter Tool", "3. Stratification Tool"])

    # --- TAB 2: FILTER TOOL ---
    with tab_filter:
        st.markdown('<div class="card-with-header">', unsafe_allow_html=True)
        st.markdown(f'<div class="card-header-bar">Exclusion Filter Configuration</div>', unsafe_allow_html=True)
        st.markdown('<div class="card-content-area">', unsafe_allow_html=True)
        if st.session_state.get('filter_error'):
            st.error(st.session_state.filter_error)
            del st.session_state['filter_error']
        draw_filter_rules(sex_column_values, column_options)
        col_btn_add, _ = st.columns([2, 8])
        with col_btn_add:
            if st.button("+ Add New Filter Rule", type="secondary", use_container_width=True):
                st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '<', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''})
                st.rerun()
        st.markdown('</div></div>', unsafe_allow_html=True)

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

    # --- TAB 3: ANALYSIS & STRATIFICATION ---
    with tab_stratify:
        st.markdown('<div class="card-with-header">', unsafe_allow_html=True)
        st.markdown(f'<div class="card-header-bar">Visual-Statistical Analysis and Stratification</div>', unsafe_allow_html=True)
        st.markdown('<div class="card-content-area">', unsafe_allow_html=True)

        if df is not None:
            if not st.session_state.col_idade or not st.session_state.col_dados:
                st.info("⚠️ Select the **'Age Column'** and **'Data Column'** in Global Settings to enable visual analysis and stratification.")
            else:
                st.markdown("#### ⚙️ Reference limits Configuration")
                draw_reference_limits_matrix(sex_column_values)
                st.markdown("<hr style='border-color: rgba(7, 59, 76, 0.1); margin: 15px 0;'>", unsafe_allow_html=True)

                st.markdown("#### 📈 Visual & Analytical Settings")
                
                # --- CÁLCULO SEGURO DOS LIMITES DE IDADE ---
                age_series = pd.to_numeric(df[st.session_state.col_idade], errors='coerce').dropna()
                if not age_series.empty:
                    min_age_data = int(age_series.min())
                    max_age_data = int(age_series.max())
                else:
                    min_age_data, max_age_data = 0, 100
                
                c1, c2, c3, c4, c5 = st.columns([1.5, 1.5, 0.5, 1.5, 2])
                chart_type = c1.selectbox("Chart Type", ["Boxplot", "Moving Average", "Moving Median"], label_visibility="collapsed", key="chart_type_sel")
                intervalo_plot = c2.number_input("Age interval", min_value=1, max_value=20, value=5, step=1, label_visibility="collapsed", key="age_int_num")

                age_zoom = st.slider("Visual Age Zoom (Focus Range)", min_value=min_age_data, max_value=max_age_data, value=(min_age_data, max_age_data))
                
                show_trendlines = False
                if chart_type in ['Moving Average', 'Moving Median']:
                    show_trendlines = c3.checkbox("chk_plateau", value=True, label_visibility="collapsed", key="trend_chk")
                    c4.markdown(f"<div style='font-size: 1rem; color: inherit; margin-top: 5px; margin-left: -15px;'>Plateau Lines {HELP_ICON_PLATEAU}</div>", unsafe_allow_html=True)
                
                group_by_sex_plot = False
                selected_sexes_for_plot = []
                if st.session_state.col_sexo and st.session_state.sex_column_is_valid:
                    group_by_sex_plot = c5.checkbox("Group by Sex", value=False, key="grp_sex_chk")
                    sex_options_for_plot = [v for v in sex_column_values if v]
                    if group_by_sex_plot:
                        selected_sexes_for_plot = st.multiselect("Filter specific sexes:", options=sex_options_for_plot, default=sex_options_for_plot, key="flt_sex_multi")
                        if not selected_sexes_for_plot: selected_sexes_for_plot = sex_options_for_plot
                    else:
                        selected_sexes_for_plot = sex_options_for_plot

                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("🚀 Process Analysis & Generate Charts", type="primary", use_container_width=True):
                    with st.spinner("Processing Visual-Statistical Analysis..."):
                        p = {
                            'chart_type': chart_type,
                            'intervalo_plot': intervalo_plot,
                            'show_trendlines': show_trendlines,
                            'group_by_sex_plot': group_by_sex_plot,
                            'selected_sexes_for_plot': selected_sexes_for_plot,
                            'age_filter_range': age_zoom,
                            'ref_limits_list': copy.deepcopy(st.session_state.ref_limits_list)
                        }

                        # 1. PRÉ-CALCULAR O GRÁFICO
                        age_range_safe = p.get('age_filter_range', (min_age_data, max_age_data))
                        fig = plot_dispersion_chart(df, st.session_state.col_idade, st.session_state.col_dados, st.session_state.col_sexo, p['intervalo_plot'], p['chart_type'], p['group_by_sex_plot'], p['selected_sexes_for_plot'], p['show_trendlines'], p['ref_limits_list'], age_range_safe)

                        # --- NOVA PARTE: CONVERTER PARA IMAGEM FIXA ---
                        img_buffer = None
                        if fig:
                            img_buffer = io.BytesIO()
                            fig.savefig(img_buffer, format='png', bbox_inches='tight', dpi=100)
                            img_buffer.seek(0)
                            plt.close(fig)

                        # 2. PRÉ-CALCULAR ESTUDOS (HARRIS-BOYD)
                        df_possiveis_global_list = []
                        df_ideais_global_list = []
                        any_haeckel_activated_at_all = False
                        hboyd_render_data = []

                        if p['group_by_sex_plot'] and st.session_state.col_sexo:
                            sex_options_hboyd = [v for v in sex_column_values if v]
                            for sex_val in sex_options_hboyd:
                                if sex_val not in p['selected_sexes_for_plot']: continue
                                sub_df = df[df[st.session_state.col_sexo].astype(str) == str(sex_val)].copy()
                                if sub_df.empty: continue

                                df_possiveis, df_ideais, cuts_ideais, h_activated = run_harris_boyd(sub_df, st.session_state.col_idade, st.session_state.col_dados, p['ref_limits_list'], str(sex_val))
                                if h_activated: any_haeckel_activated_at_all = True

                                max_age_sub = int(pd.to_numeric(sub_df[st.session_state.col_idade], errors='coerce').max())
                                titulo_metodo_2 = "EDA Haeckel (Practical approach)" if h_activated else "Empirical Analysis of Dispersion and Means (Empirical approach)"

                                hboyd_render_data.append({
                                    'sex_val': str(sex_val),
                                    'df_possiveis_age': df_possiveis['age'].tolist() if not df_possiveis.empty else [],
                                    'cuts_ideais': cuts_ideais,
                                    'max_age': max_age_sub,
                                    'titulo_metodo_2': titulo_metodo_2,
                                    'sub_df': sub_df
                                })

                                if not df_possiveis.empty:
                                    df_p = df_possiveis.copy(); df_p.insert(0, 'Sex', str(sex_val)); df_possiveis_global_list.append(df_p)
                                if not df_ideais.empty:
                                    df_i = df_ideais.copy(); df_i.insert(0, 'Sex', str(sex_val)); df_ideais_global_list.append(df_i)
                        else:
                            df_possiveis, df_ideais, cuts_ideais, h_activated = run_harris_boyd(df, st.session_state.col_idade, st.session_state.col_dados, p['ref_limits_list'], "All")
                            if h_activated: any_haeckel_activated_at_all = True
                            max_age_full = int(pd.to_numeric(df[st.session_state.col_idade], errors='coerce').max())
                            titulo_metodo_2 = "EDA Haeckel (Practical approach)" if h_activated else "Empirical Analysis of Dispersion and Means (Empirical approach)"

                            hboyd_render_data.append({
                                'sex_val': 'All',
                                'df_possiveis_age': df_possiveis['age'].tolist() if not df_possiveis.empty else [],
                                'cuts_ideais': cuts_ideais,
                                'max_age': max_age_full,
                                'titulo_metodo_2': titulo_metodo_2,
                                'sub_df': df
                            })

                            if not df_possiveis.empty: df_possiveis_global_list.append(df_possiveis)
                            if not df_ideais.empty: df_ideais_global_list.append(df_ideais)

                        valid_haeckel_rows = [r for r in p['ref_limits_list'] if r.get('lrs') is not None and r.get('lrs') > 0]

                        # 3. SALVAR ARTEFATOS FINAIS NO ESTADO DA SESSÃO
                        st.session_state.analysis_results = {
                            'fig': fig,
                            'hboyd_render_data': hboyd_render_data,
                            'group_by_sex_plot': p['group_by_sex_plot'],
                            'valid_haeckel_rows': valid_haeckel_rows,
                            'df_possiveis_global_list': df_possiveis_global_list,
                            'df_ideais_global_list': df_ideais_global_list,
                            'any_haeckel_activated_at_all': any_haeckel_activated_at_all
                        }

                # =========================================================================
                # RENDERING BLOCK (Lê do estado instantaneamente, lag zero no Streamlit)
                # =========================================================================
                if 'analysis_results' in st.session_state:
                    res = st.session_state.analysis_results
                    st.markdown("<hr style='border-color: rgba(7, 59, 76, 0.1); margin: 25px 0;'>", unsafe_allow_html=True)
                    
                    col_grafico, col_hboyd = st.columns([2.8, 1.2], gap="large")

                    with col_grafico:
                        if res['fig']: st.pyplot(res['fig'])

                    with col_hboyd:
                        st.markdown('<div class="card-header-bar" style="margin: -1rem -1rem 1rem -1rem; border-radius: 5px 5px 0 0; padding: 10px 15px; font-size: 1.1rem; text-align: center;">Stratification Studies</div>', unsafe_allow_html=True)
                        
                        for data in res['hboyd_render_data']:
                            if res['group_by_sex_plot'] and st.session_state.col_sexo:
                                st.markdown(f"<hr style='border-color: rgba(7, 59, 76, 0.2); margin: 10px 0;'><p style='font-size:1.0rem; color:{COLOR_PRIMARY}; margin-bottom:2px;'><b>Sex: {data['sex_val']}</b></p>", unsafe_allow_html=True)

                            render_mini_tabela("Harris-Boyd (Statistical approach)", data['df_possiveis_age'], data['max_age'], data['sub_df'], st.session_state.col_idade, st.session_state.col_dados)
                            render_mini_tabela(data['titulo_metodo_2'], data['cuts_ideais'], data['max_age'], data['sub_df'], st.session_state.col_idade, st.session_state.col_dados)
                        st.markdown("</div>", unsafe_allow_html=True)

                    # --- MULTIPARAMETRIC HAECKEL AUDIT TABLES ---
                    st.markdown("<div style='margin-top: 35px;'></div>", unsafe_allow_html=True)
                    if res['valid_haeckel_rows']:
                        with st.expander("🔬 EDA - Haeckel Calculation (State-of-the-Art and Biological Variation)", expanded=True):
                            st.markdown("<p style='font-size:0.9rem; color:#666;'>Verifiable mirror containing the thorough step-by-step math performed to obtain performance limits.</p>", unsafe_allow_html=True)
                            
                            for r_item in res['valid_haeckel_rows']:
                                h = calcular_limites_haeckel(r_item.get('lri'), r_item.get('lrs'))
                                if not h: continue
                                
                                faixa_etaria_label = f"{r_item['age_min']} to {r_item['age_max']} years" if (r_item['age_min'] is not None or r_item['age_max'] is not None) else "Global"
                                st.markdown(f"<div style='background-color:#E0F7FA; padding:5px 10px; font-weight:bold; color:{COLOR_PRIMARY}; margin-top:20px; border-radius:4px; border-left: 5px solid {COLOR_TERTIARY};'>Target Subgroup: Sex [{r_item['sex']}] | Age [{faixa_etaria_label}]</div>", unsafe_allow_html=True)
                                
                                eq_lri_min = h['lri'] - (h['lri'] * h['m_lri']['pb']/100)
                                eq_lri_max = h['lri'] + (h['lri'] * h['m_lri']['pb']/100)
                                eq_lrs_min = h['lrs'] - (h['lrs'] * h['m_lrs']['pb']/100)
                                eq_lrs_max = h['lrs'] + (h['lrs'] * h['m_lrs']['pb']/100)
                                
                                html_table = f"""
                                <table style="width:100%; text-align:center; border-collapse: collapse; font-family: sans-serif; font-size:0.9rem; margin-top:5px; margin-bottom:20px;" border="1">
                                    <tr style="background-color:{COLOR_PRIMARY}; color:white;">
                                        <th colspan="2" style="padding:8px; border: 1px solid #CCC;">Comparative Reference Interval Input</th>
                                        <th colspan="6" style="padding:8px; border: 1px solid #CCC;">Calculation of Analytical Performance Specification Limits (Haeckel)</th>
                                    </tr>
                                    <tr style="background-color:#E0F7FA; font-weight:bold; color:{COLOR_PRIMARY};">
                                        <td style="padding:6px; border: 1px solid #CCC;">LRI</td><td style="padding:6px; border: 1px solid #CCC;">LRS</td>
                                        <td style="border: 1px solid #CCC;">CV<sub>E</sub></td><td style="border: 1px solid #CCC;">pCV<sub>A</sub></td><td style="border: 1px solid #CCC;">Med<sub>ln</sub></td><td style="border: 1px solid #CCC;">pS<sub>A,Med</sub></td><td style="border: 1px solid #CCC;">Slope</td><td style="border: 1px solid #CCC;">Intercept</td>
                                    </tr>
                                    <tr style="background-color:#FFFFFF;">
                                        <td style="padding:8px; border: 1px solid #CCC;"><b>{h['lri']:.3f}</b></td><td style="padding:8px; border: 1px solid #CCC;"><b>{h['lrs']:.3f}</b></td>
                                        <td style="border: 1px solid #CCC;">{h['cve']:.3f}%</td><td style="border: 1px solid #CCC;">{h['pcva']:.3f}%</td>
                                        <td style="border: 1px solid #CCC;">{h['med']:.3f}</td><td style="border: 1px solid #CCC;">{h['psa_med']:.3f}</td>
                                        <td style="border: 1px solid #CCC;">{h['slope']:.4f}</td><td style="border: 1px solid #CCC;">{h['intercept']:.4f}</td>
                                    </tr>
                                    <tr style="background-color:#E0F7FA; font-weight:bold; color:{COLOR_PRIMARY};">
                                        <td colspan="2" style="border:none; background-color:#FFFFFF;"></td>
                                        <td colspan="2" style="padding:6px; border: 1px solid #CCC;">pS<sub>A,LRI</sub></td><td colspan="2" style="border: 1px solid #CCC;">pCV<sub>A,LRI</sub></td><td style="border: 1px solid #CCC;">pB<sub>LRI</sub></td><td style="border: 1px solid #CCC;">LRI Equivalence Bound Interval</td>
                                    </tr>
                                    <tr style="background-color:#FFFFFF;">
                                        <td colspan="2" style="border:none; background-color:#FFFFFF;"></td>
                                        <td colspan="2" style="padding:8px; border: 1px solid #CCC;">{h['m_lri']['psa']:.3f}</td>
                                        <td colspan="2" style="border: 1px solid #CCC;">{h['m_lri']['pcva']:.3f}%</td>
                                        <td style="border: 1px solid #CCC;">{h['m_lri']['pb']:.3f}%</td>
                                        <td style="border: 1px solid #CCC;"><b>{eq_lri_min:.1f} to {eq_lri_max:.1f}</b></td>
                                    </tr>
                                    <tr style="background-color:#E0F7FA; font-weight:bold; color:{COLOR_PRIMARY};">
                                        <td colspan="2" style="border:none; background-color:#FFFFFF;"></td>
                                        <td colspan="2" style="padding:6px; border: 1px solid #CCC;">pS<sub>A,LRS</sub></td><td colspan="2" style="border: 1px solid #CCC;">pCV<sub>A,LRS</sub></td><td style="border: 1px solid #CCC;">pB<sub>LRS</sub></td><td style="border: 1px solid #CCC;">LRS Equivalence Bound Interval</td>
                                    </tr>
                                    <tr style="background-color:#FFFFFF;">
                                        <td colspan="2" style="border:none; background-color:#FFFFFF;"></td>
                                        <td colspan="2" style="padding:8px; border: 1px solid #CCC;">{h['m_lrs']['psa']:.3f}</td>
                                        <td colspan="2" style="border: 1px solid #CCC;">{h['m_lrs']['pcva']:.3f}%</td>
                                        <td style="border: 1px solid #CCC;">{h['m_lrs']['pb']:.3f}%</td>
                                        <td style="border: 1px solid #CCC;"><b>{eq_lrs_min:.1f} to {eq_lrs_max:.1f}</b></td>
                                    </tr>
                                </table>
                                """
                                st.markdown(html_table, unsafe_allow_html=True)

                    # --- SUMMARY DETAILED BOTTOM TABLES ---
                    df_possiveis_global = pd.concat(res['df_possiveis_global_list'], ignore_index=True) if res['df_possiveis_global_list'] else pd.DataFrame()
                    df_ideais_global = pd.concat(res['df_ideais_global_list'], ignore_index=True) if res['df_ideais_global_list'] else pd.DataFrame()

                    if not df_possiveis_global.empty or not df_ideais_global.empty:
                        with st.expander("📊 View Detailed Stratification Data Tables", expanded=False):
                            
                            # Tabela 1: Harris-Boyd
                            if not df_possiveis_global.empty:
                                st.markdown("<h4 style='color: #118AB2; font-size:1.2rem; font-weight:bold;'>Harris-Boyd (Statistical approach)</h4>", unsafe_allow_html=True)
                                cols_to_show_pos = ['Age Cutoff', 'Z-score', 'SD Ratio', 'Mean (<= Cutoff)', 'Mean (> Cutoff)']
                                
                                # Trava de segurança para a coluna Sex
                                if res['group_by_sex_plot'] and 'Sex' in df_possiveis_global.columns: 
                                    cols_to_show_pos.insert(0, 'Sex')
                                    
                                st.dataframe(df_possiveis_global[cols_to_show_pos], use_container_width=True, hide_index=True)

                            # Tabela 2: Haeckel / AEDM
                            if not df_ideais_global.empty:
                                titulo_metodo_2_completo = "EDA Haeckel (Practical approach)" if res['any_haeckel_activated_at_all'] else "Empirical Analysis of Dispersion and Means (Empirical approach)"
                                st.markdown(f"<h4 style='color: #073B4C; font-size:1.2rem; font-weight:bold; margin-top:25px;'>{titulo_metodo_2_completo}</h4>", unsafe_allow_html=True)
                                cols_to_show_ideal = ['Age Cutoff', 'Diff %', 'Limit Threshold', 'Mean (<= Cutoff)', 'Mean (> Cutoff)']

                # --- SHEET PRODUCTION GENERATOR SECTION ---
                st.markdown("<hr style='border-color: rgba(7, 59, 76, 0.1); margin: 2.5rem 0;'>", unsafe_allow_html=True)
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
                        st.session_state.confirm_stratify = False; st.rerun()

                if st.session_state.get('stratified_results'):
                    is_excel = "Excel" in st.session_state.output_format
                    for filename, df_to_download in st.session_state.stratified_results.items():
                        file_bytes = to_excel(df_to_download) if is_excel else to_csv(df_to_download)
                        st.download_button(f"📄 Download {filename}", data=file_bytes, file_name=f"{filename}.{'xlsx' if is_excel else 'csv'}", key=f"dl_{filename}", type="secondary")
        else:
            st.info("⚠️ Please upload a spreadsheet to access the analysis and stratification tools.")
        st.markdown('</div></div>', unsafe_allow_html=True)

# Nova função render_mini_tabela que recebe o df para cálculo da mediana
def render_mini_tabela(titulo, cuts, max_age, df_context, col_idade, col_dados):
    st.markdown(f"<p style='font-size:0.85rem; color:#41A0C4; font-weight: 600; margin-bottom:5px; margin-top:15px; text-transform: uppercase;'>{titulo}:</p>", unsafe_allow_html=True)
    if not cuts:
        st.markdown(f"<p style='font-weight:bold; font-size:0.95rem; color:{COLOR_SECONDARY};'>No stratification needed</p>", unsafe_allow_html=True)
        return
    
    # Limpa e filtra as colunas para o cálculo exato da mediana
    def clean_val(x):
        if pd.isna(x): return np.nan
        x = str(x).replace(',', '.')
        x = ''.join(c for c in x if c.isdigit() or c == '.' or c == '-')
        try: return float(x)
        except: return np.nan
        
    t_age = pd.to_numeric(df_context[col_idade], errors='coerce')
    t_data = pd.to_numeric(df_context[col_dados].apply(clean_val), errors='coerce')

    def get_med_str(amin, amax):
        # Filtra a faixa etária especificada e extrai a mediana
        m = t_data[(t_age >= amin) & (t_age <= amax)].median()
        if pd.isna(m): return "- Mediana: N/A"
        
        # Formata com 2 casas decimais e substitui ponto por vírgula no padrão brasileiro
        val_str = f"{m:.2f}".replace('.', ',')
        return f"- Mediana: {val_str}"

    ranges = []
    last_age = 0
    
    for cut in cuts:
        med_str = get_med_str(last_age, cut)
        ranges.append(f"{last_age} - {cut} years {med_str}")
        last_age = cut + 1
        
    med_str = get_med_str(last_age, max_age)
    ranges.append(f"{last_age} - {max_age} years {med_str}")
    
    for r in ranges[:5]: st.markdown(f"<p style='font-weight:bold; font-size:1.0rem; color:{COLOR_SECONDARY}; margin-bottom:2px;'>{r}</p>", unsafe_allow_html=True)
    if len(ranges) > 5:
        with st.expander(f" (+{len(ranges)-5} groups)"):
            for r in ranges[5:]: st.markdown(f"<p style='font-weight:bold; font-size:0.95rem; color:#073B4C; margin-bottom:2px;'>{r}</p>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
