# -*- coding: utf-8 -*-

# Versão 2.4.2 - Atualização: Internacionalização (i18n)
# Melhorias: Todos os textos de UI, laudos do Harris-Boyd e gráficos do Boxplot foram traduzidos para o Inglês para manter o padrão do aplicativo.

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
import matplotlib.pyplot as plt
import seaborn as sns

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
2.  **Stratification:** To divide your database into specific subgroups.""",
    "1. Global Settings": """**1. Global Settings**

This section contains the essential settings that are shared between both tools.

- **Select Spreadsheet:**
  Opens a window to select the source data file. It supports `.xlsx`, `.xls`, and `.csv` formats.

- **Age Column / Sex/Gender / Data Column:**
  Fields to **select** the column names in your spreadsheet. The **Data Column** is specifically used to automatically run the Harris-Boyd stratification study and generate charts.

- **Output Format:**
  A selection menu to choose the format of the generated files. Choose `Excel (.xlsx)` for Microsoft Excel or `CSV (.csv)` for a lighter format.
  """,
    "2. Filter Tool": """**2. Filter Tool**

The purpose of this tool is to **"clean"** your spreadsheet by **removing** rows that match specific criteria. The result is a **single file** containing only the data that "survived" the filters.

**How Exclusion Rules Work:**
Each row you add is a condition to **remove** data. If a row in your spreadsheet matches an active rule, it **will be excluded** from the final file.

- **[✓] (Activation Checkbox):** Toggles a rule on or off without deleting it.

- **Column:** The name of the column where the filter will be applied. **Tip:** You can apply the rule to multiple columns at once by separating their names with a semicolon (;).

- **Operator and Value:** Operators define the rule's logic to set exclusion ranges.
**Tip:** The keyword `empty` is a powerful feature:
    - **Scenario 1:** Column: `"Exam_X"`, Operator: `"is equal to"`, Value: `"empty"`.
    - **Scenario 2:** Column: `"Observations"`, Operator: `"Not equal to"`, Value: `"empty"`.

- **Compound Logic:** Expands the rule to create `AND` / `OR` conditions.

- **Condition:** Allows applying a secondary filter based on sex and/or age conditions.

- **Actions:** The `X` button deletes the rule. The 'Clone' button duplicates it.""",
    "3. Stratification Tool": """**3. Stratification Tool**

This tool splits your spreadsheet into **multiple smaller files**, where each file represents a subgroup of interest.

**Harris-Boyd Study & Charts:**
Automatically evaluates the selected Data Column and Age Column to suggest the most statistically relevant age cuts. You can also generate Boxplot charts to visually inspect the data distribution.

**How Stratification Works:**
- **Stratification Options by Sex/Gender:** Select the genders you want to include.
- **Age Range Definitions:** Create the specific age boundaries.
- **Generate Stratified Sheets:** Starts the splitting process."""
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
            except ValueError:
                return "FALSE"

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

    def apply_filters(self, df: pd.DataFrame, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
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

            rule_sql = f"({combined_main_sql}) AND ({cond_sql})"
            exclusion_clauses.append(f"NOT ({rule_sql})")

        if not exclusion_clauses:
            progress_bar.progress(1.0, text="Processamento concluído!")
            return df

        where_clause = " AND ".join(exclusion_clauses)
        
        df['_temp_row_id'] = range(len(df))
        query = f"SELECT _temp_row_id FROM df WHERE {where_clause}"

        try:
            progress_bar.progress(0.8, text="Executando Motor DuckDB (SQL)...")
            valid_ids_df = duckdb.query(query).df()
            
            filtered_df = df[df['_temp_row_id'].isin(valid_ids_df['_temp_row_id'])].copy()
            
            filtered_df.drop(columns=['_temp_row_id'], inplace=True)
            df.drop(columns=['_temp_row_id'], inplace=True)
            
            progress_bar.progress(1.0, text="Filtering complete!")
            return filtered_df
        except Exception as e:
            st.error(f"Erro no processamento SQL: {e}")
            if '_temp_row_id' in df.columns:
                df.drop(columns=['_temp_row_id'], inplace=True)
            return df
    
    def apply_stratification(self, df: pd.DataFrame, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')

        if not (col_idade and col_idade in df.columns):
            st.error(f"Age column '{col_idade}' not found in the spreadsheet."); return {}
        if not (col_sexo and col_sexo in df.columns):
            st.error(f"Sex/gender column '{col_sexo}' not found in the spreadsheet."); return {}

        safe_idade = f'"{col_idade}"'
        safe_sexo = f'"{col_sexo}"'

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

        df['_temp_row_id'] = range(len(df))

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
            query = f"SELECT _temp_row_id FROM df WHERE {where_clause}"

            filename = self._generate_stratum_name(age_rule, sex_rule)
            progress_bar.progress(progress, text=f"Gerando estrato {i+1}/{total_files}: {filename}...")
            
            try:
                valid_ids_df = duckdb.query(query).df()
                if not valid_ids_df.empty:
                    stratum_df = df[df['_temp_row_id'].isin(valid_ids_df['_temp_row_id'])].copy()
                    stratum_df.drop(columns=['_temp_row_id'], inplace=True)
                    generated_dfs[filename] = stratum_df
            except Exception as e:
                st.warning(f"Não foi possível gerar o estrato {filename} devido a erro nos valores: {e}")

        df.drop(columns=['_temp_row_id'], inplace=True)
        progress_bar.progress(1.0, text="Estratificação completa!")
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
        return "_".join(part for part in name_parts if part)

# --- FUNÇÕES AUXILIARES OTIMIZADAS ---

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
                valid_files = [f for f in z.namelist() if not f.startswith('__MACOSX/') and 
                               (f.lower().endswith('.csv') or f.lower().endswith(('.xlsx', '.xls')))]
                
                if not valid_files:
                    st.error("O ZIP não contém arquivos CSV ou Excel válidos.")
                    os.remove(tmp_path)
                    return None
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(valid_files[0])[1]) as inner_tmp:
                    inner_tmp.write(z.read(valid_files[0]))
                    inner_path = inner_tmp.name
                
                inner_filename = valid_files[0].lower()
                
                if inner_filename.endswith('.csv'):
                    try:
                        df = pd.read_csv(inner_path, sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
                    except Exception:
                        df = pd.read_csv(inner_path, sep=',', decimal='.', encoding='utf-8', engine='pyarrow')
                else:
                    df = pd.read_excel(inner_path, engine='openpyxl')
                
                os.remove(inner_path)

        elif file_name.endswith('.csv'):
            try: 
                df = pd.read_csv(tmp_path, sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
            except Exception:
                df = pd.read_csv(tmp_path, sep=',', decimal='.', encoding='utf-8', engine='pyarrow')
        else:
            df = pd.read_excel(tmp_path, engine='openpyxl')

        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        if df is not None:
            for col in df.select_dtypes(include=['object']).columns:
                mask = df[col].notna()
                df.loc[mask, col] = df.loc[mask, col].astype(str)
                
                try:
                    if df[col].nunique() / len(df[col]) < 0.5:
                        df[col] = df[col].astype('category')
                except Exception:
                    pass 
        
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        return None

@st.cache_data(show_spinner=False)
def run_harris_boyd(df, col_idade, col_dados):
    temp_df = pd.DataFrame()
    temp_df['Idade'] = pd.to_numeric(df[col_idade], errors='coerce')
    
    def clean_val(x):
        if pd.isna(x): return np.nan
        x = str(x).replace(',', '.')
        x = ''.join(c for c in x if c.isdigit() or c == '.' or c == '-')
        try: return float(x)
        except: return np.nan
        
    temp_df['Data'] = pd.to_numeric(df[col_dados].apply(clean_val), errors='coerce')
    temp_df = temp_df.dropna(subset=['Idade', 'Data'])
    temp_df = temp_df[temp_df['Idade'] >= 0]
    
    if temp_df.empty:
        return "No stratification recommended (Insufficient data).", pd.DataFrame()
        
    max_age = int(temp_df['Idade'].max())
    if max_age < 1:
        return "No stratification recommended (Insufficient age variation).", pd.DataFrame()
        
    valid_cuts = []
    
    for age_cutoff in range(1, max_age):
        g1 = temp_df[temp_df['Idade'] <= age_cutoff]['Data']
        g2 = temp_df[temp_df['Idade'] > age_cutoff]['Data']
        
        n1, n2 = len(g1), len(g2)
        if n1 < 30 or n2 < 30:
            continue
            
        mean1, mean2 = np.mean(g1), np.mean(g2)
        var1, var2 = np.var(g1, ddof=1), np.var(g2, ddof=1)
        sd1, sd2 = np.sqrt(var1), np.sqrt(var2)
        
        sd_ratio = max(sd1, sd2) / min(sd1, sd2) if min(sd1, sd2) > 0 else 0
        
        den_z = np.sqrt((var1/n1) + (var2/n2)) if (var1/n1) + (var2/n2) > 0 else 0.0001
        z = abs(mean1 - mean2) / den_z
        z_crit = 3 * np.sqrt((n1+n2)/120) if (n1+n2) < 120 else 3
        
        den_d = np.sqrt((var1 + var2) / 2) if (var1 + var2) > 0 else 0.0001
        d_value = abs(mean1 - mean2) / den_d
        
        partition_by_sd = sd_ratio > 1.5
        partition_by_mean = (z > z_crit) and (d_value > 0.25)
        should_partition = partition_by_sd or partition_by_mean
        
        if should_partition:
            just = 'Standard Deviation' if partition_by_sd and not partition_by_mean else ('Mean' if partition_by_mean and not partition_by_sd else 'Both')
            valid_cuts.append({
                'age': age_cutoff,
                'justificativa': just,
                'd_value': d_value,
                'sd_ratio': sd_ratio,
                'mean1': mean1,
                'mean2': mean2,
                'n1': n1,
                'n2': n2,
                'Age Cutoff': f"<= {age_cutoff} vs > {age_cutoff}",
                'Justification': just,
                'D-value': round(d_value, 3),
                'SD Ratio': round(sd_ratio, 3),
                'Mean (<= Cutoff)': round(mean1, 2),
                'Mean (> Cutoff)': round(mean2, 2)
            })
            
    if not valid_cuts:
         return "The statistical model found no clinical necessity or sufficient variance to recommend age-based reference intervals for this analyte.", pd.DataFrame()

    valid_cuts = sorted(valid_cuts, key=lambda x: x['age'])

    clusters = []
    current_cluster = []
    
    for i, cut in enumerate(valid_cuts):
        if not current_cluster:
            current_cluster.append(cut)
            continue
            
        prev_cut = valid_cuts[i-1]
        age_gap = cut['age'] - prev_cut['age']
        cluster_max_d = max([c['d_value'] for c in current_cluster])
        
        if age_gap > 3:
            clusters.append(current_cluster)
            current_cluster = [cut]
            continue
        
        drop_from_peak = cluster_max_d - cut['d_value']
        if drop_from_peak > 0.4 and drop_from_peak > (cluster_max_d * 0.25):
            clusters.append(current_cluster)
            current_cluster = [cut]
            continue
            
        d_diff = cut['d_value'] - prev_cut['d_value']
        if d_diff > 0.15 and drop_from_peak > 0.15:
            clusters.append(current_cluster)
            current_cluster = [cut]
            continue
            
        current_cluster.append(cut)
        
    if current_cluster:
        clusters.append(current_cluster)

    best_cuts = []
    for cluster in clusters:
        best = max(cluster, key=lambda x: x['d_value'])
        best_cuts.append(best)

    best_cuts = sorted(best_cuts, key=lambda x: x['age'])
    
    texto_laudo = "### 💡 Practical Stratification Suggestion\n"
    texto_laudo += "The algorithm analyzed the means and data dispersion and detected **"
    texto_laudo += "1 point**" if len(best_cuts) == 1 else f"{len(best_cuts)} points**"
    texto_laudo += " of significant clinical change across ages:\n\n"

    last_age = 0
    for i, cut in enumerate(best_cuts):
        idade_corte = cut['age']
        m1 = cut['mean1']
        m2 = cut['mean2']
        
        if i == 0:
            faixa = f"From {last_age} to {idade_corte} years"
        else:
            faixa = f"From {last_age + 1} to {idade_corte} years"
            
        texto_laudo += f"**{i+1}. Group {faixa} (Approx. Mean: {m1:.1f})**\n"
        texto_laudo += "🔹 *Why separate?* "
        if cut['justificativa'] == 'Mean':
            texto_laudo += f"In this There is a significant change in mean results compared to the rest of the population (jump to {m2:.1f}). "
        elif cut['justificativa'] == 'Standard Deviation':
            texto_laudo += "This age group presents a very different variability (data dispersion) compared to other ages. "
        else:
            texto_laudo += f"This age group has a unique behavior, both due to a different mean (jump to {m2:.1f}) and high data dispersion. "
        texto_laudo += f"\n\n"
        last_age = idade_corte
        
    texto_laudo += f"**{len(best_cuts)+1}. Group from {last_age + 1} years onwards (Approx. Mean: {best_cuts[-1]['mean2']:.1f})**\n"
    texto_laudo += "🔹 From this barrier onwards, the model considers that the results tend to stabilize statistically, forming the main reference range for reports.\n"

    idades_sugeridas = [c['age'] for c in best_cuts]
    raw_data_list = []
    
    for cut in valid_cuts:
        raw_data_list.append({
            'Recommendation': '⭐ Suggested' if cut['age'] in idades_sugeridas else '',
            'Age Cutoff': cut['Age Cutoff'],
            'Justification': cut['Justification'],
            'D-value': cut['D-value'],
            'SD Ratio': cut['SD Ratio'],
            'Mean (<= Cutoff)': cut['Mean (<= Cutoff)'],
            'Mean (> Cutoff)': cut['Mean (> Cutoff)'],
            'N (<= Cutoff)': cut['n1'],
            'N (> Cutoff)': cut['n2']
        })

    raw_df = pd.DataFrame(raw_data_list)
    
    return texto_laudo, raw_df

@st.cache_data(show_spinner=False)
def plot_boxplot_idade(df, col_idade, col_dados, intervalo):
    temp_df = pd.DataFrame()
    temp_df['Idade'] = pd.to_numeric(df[col_idade], errors='coerce')
    
    def clean_val(x):
        if pd.isna(x): return np.nan
        x = str(x).replace(',', '.')
        x = ''.join(c for c in x if c.isdigit() or c == '.' or c == '-')
        try: return float(x)
        except: return np.nan
        
    temp_df['Data'] = pd.to_numeric(df[col_dados].apply(clean_val), errors='coerce')
    temp_df = temp_df.dropna(subset=['Idade', 'Data'])
    temp_df = temp_df[temp_df['Idade'] >= 0]
    
    if temp_df.empty: return None

    if intervalo > 1:
        temp_df['Idade_Bin'] = (temp_df['Idade'] // intervalo) * intervalo
        temp_df['Idade_Label'] = temp_df['Idade_Bin'].astype(int).astype(str) + " to " + (temp_df['Idade_Bin'] + intervalo - 1).astype(int).astype(str)
        temp_df = temp_df.sort_values('Idade_Bin')
        x_col = 'Idade_Label'
    else:
        temp_df['Idade_Label'] = temp_df['Idade'].astype(int)
        temp_df = temp_df.sort_values('Idade')
        x_col = 'Idade_Label'

    fig, ax = plt.subplots(figsize=(16, 6))
    
    sns.boxplot(data=temp_df, x=x_col, y='Data', color='#a2cffe', ax=ax, showfliers=False)
    
    ax.set_title(f'Distribution of {col_dados} by Age', fontsize=16, fontweight='bold', pad=15)
    ax.set_xlabel('Age (Years)', fontsize=14, labelpad=10)
    ax.set_ylabel('Results (Without Extreme Outliers)', fontsize=14, labelpad=10)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    
    return fig

@st.cache_data(show_spinner="Preparing file for export...")
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

@st.cache_data(show_spinner="Preparing CSV for export...")
def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

# --- FUNÇÕES DE INTERFACE ---

def handle_select_all():
    new_state = st.session_state['select_all_master_checkbox']
    for rule in st.session_state.filter_rules:
        rule['p_check'] = new_state

def reset_results_on_upload():
    if 'filtered_result' in st.session_state: del st.session_state['filtered_result']
    if 'stratified_results' in st.session_state: del st.session_state['stratified_results']
    st.session_state.confirm_stratify = False

def draw_filter_rules(sex_column_values, column_options): 
    st.markdown("""<style>
        .stButton>button { padding: 0.25rem 0.3rem; font-size: 0.8rem; white-space: nowrap; }
        div[data-testid="stTextInput"] input, div[data-testid="stSelectbox"] div[data-baseweb="select"] {
            border: 1px solid rgba(255, 75, 75, 0.15) !important;
            border-radius: 0.25rem;
        }
    </style>""", unsafe_allow_html=True)
    
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium")
    
    if st.session_state.filter_rules:
        all_checked = all(rule.get('p_check', False) for rule in st.session_state.filter_rules)
    else:
        all_checked = False

    header_cols[0].checkbox(
        "Select/Unselect all",
        value=all_checked,
        key='select_all_master_checkbox', 
        on_change=handle_select_all,   
        label_visibility="collapsed"
    )
    
    header_cols[1].markdown("**Column** <span title='Type the column name exactly as in the sheet. For multiple columns, separate with ;'>&#9432;</span>", unsafe_allow_html=True)
    header_cols[2].markdown("**Operator**", unsafe_allow_html=True)
    header_cols[3].markdown("**Value**", unsafe_allow_html=True)
    
    tooltip_text = "Select another operator to define an interval.\nBETWEEN: Excludes values within the interval.\nOR: Excludes values outside.\nAND: Excludes values within, without extremes."
    tooltip_text_html = tooltip_text.replace('\n', '&#10;')
    header_cols[5].markdown(f"**Compound Logic** <span title='{tooltip_text_html}'>&#9432;</span>", unsafe_allow_html=True)
    
    header_cols[6].markdown("**Condition**", unsafe_allow_html=True)
    header_cols[7].markdown("**Actions**", unsafe_allow_html=True)
    st.markdown("<hr style='margin-top: -0.5rem; margin-bottom: 0.5rem;'>", unsafe_allow_html=True)

    ops_main = ["", ">", "<", "=", "Not equal to", "≥", "≤"]
    ops_age = ["", ">", "<", "≥", "≤", "="]
    ops_central_logic = ["AND", "OR", "BETWEEN"]

    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium") 
            
            rule['p_check'] = cols[0].checkbox(
                f"Activate rule {rule['id']}", 
                value=rule.get('p_check', True), 
                key=f"p_check_{rule['id']}", 
                label_visibility="collapsed"
            )
            
            rule['p_col'] = cols[1].text_input(
                "Column", 
                value=rule.get('p_col', ''), 
                key=f"p_col_{rule['id']}", 
                label_visibility="collapsed",
                placeholder="Ex: Exam.COL"
            )
            
            rule['p_op1'] = cols[2].selectbox("Operator 1", ops_main, index=ops_main.index(rule.get('p_op1', '=')) if rule.get('p_op1') in ops_main else 0, key=f"p_op1_{rule['id']}", label_visibility="collapsed")
            rule['p_val1'] = cols[3].text_input("Value 1", value=rule.get('p_val1', ''), key=f"p_val1_{rule['id']}", label_visibility="collapsed")
            rule['p_expand'] = cols[4].checkbox("+", value=rule.get('p_expand', False), key=f"p_expand_{rule['id']}", label_visibility="collapsed")
            
            with cols[5]:
                if rule['p_expand']:
                    exp_cols = st.columns([3, 2, 2])
                    rule['p_op_central'] = exp_cols[0].selectbox("Logic", ops_central_logic, index=ops_central_logic.index(rule.get('p_op_central', 'OR')) if rule.get('p_op_central') in ops_central_logic else 0, key=f"p_op_central_{rule['id']}", label_visibility="collapsed")
                    rule['p_op2'] = exp_cols[1].selectbox("Operator 2", ops_main, index=ops_main.index(rule.get('p_op2', '>')) if rule.get('p_op2') in ops_main else 0, key=f"p_op2_{rule['id']}", label_visibility="collapsed")
                    rule['p_val2'] = exp_cols[2].text_input("Value 2", value=rule.get('p_val2', ''), key=f"p_val2_{rule['id']}", label_visibility="collapsed")

            with cols[6]:
                rule['c_check'] = st.checkbox("Condition", value=rule.get('c_check', False), key=f"c_check_{rule['id']}")
            
            action_cols = cols[7].columns(2)
            if action_cols[0].button("Clone", key=f"clone_{rule['id']}"):
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
                    
                    rule['c_sexo_check'] = cond_cols[4].checkbox("Sex/Gender", value=rule.get('c_sexo_check', False), key=f"c_sexo_check_{rule['id']}")
                    with cond_cols[5]:
                        if rule['c_sexo_check']:
                            sex_options = [v for v in sex_column_values if v]
                            current_sex = rule.get('c_sexo_val')
                            sex_index = sex_options.index(current_sex) if current_sex in sex_options else None
                            rule['c_sexo_val'] = st.selectbox("Sex Value", options=sex_options, index=sex_index, placeholder="Select value", key=f"c_sexo_val_{rule['id']}", label_visibility="collapsed")
        st.markdown("---")

def draw_stratum_rules():
    st.markdown("""<style>.stButton>button {padding: 0.25rem 0.3rem; font-size: 0.8rem;}</style>""", unsafe_allow_html=True)
    ops_stratum = ["", ">", "<", "≥", "≤"]

    for i, stratum_rule in enumerate(st.session_state.stratum_rules):
        with st.container():
            cols = st.columns([2, 1, 1, 0.5, 1, 1, 1])
            cols[0].write(f"**Age Range {i+1}:**")
            stratum_rule['op1'] = cols[1].selectbox("Operator 1", ops_stratum, index=ops_stratum.index(stratum_rule.get('op1', '')) if stratum_rule.get('op1') in ops_stratum else 0, key=f"s_op1_{stratum_rule['id']}", label_visibility="collapsed")
            stratum_rule['val1'] = cols[2].text_input("Value 1", value=stratum_rule.get('val1', ''), key=f"s_val1_{stratum_rule['id']}", label_visibility="collapsed")
            cols[3].markdown("<p style='text-align: center; margin-top: 25px;'>AND</p>", unsafe_allow_html=True)
            stratum_rule['op2'] = cols[4].selectbox("Operator 2", ops_stratum, index=ops_stratum.index(stratum_rule.get('op2', '')) if stratum_rule.get('op2') in ops_stratum else 0, key=f"s_op2_{stratum_rule['id']}", label_visibility="collapsed")
            stratum_rule['val2'] = cols[5].text_input("Value 2", value=stratum_rule.get('val2', ''), key=f"s_val2_{stratum_rule['id']}", label_visibility="collapsed")
            if cols[6].button("X", key=f"del_stratum_{stratum_rule['id']}"):
                if len(st.session_state.stratum_rules) > 1:
                    st.session_state.stratum_rules.pop(i)
                    st.rerun()
                else:
                    st.warning("Cannot delete the last age range.")
        st.markdown("---")

def main():
    if 'lgpd_accepted' not in st.session_state: st.session_state.lgpd_accepted = False
    if not st.session_state.lgpd_accepted:
        st.title("Welcome to Data Sift!")
        st.markdown("This program is designed to optimize your work with large volumes of data. Please read the terms below.")
        st.divider()
        st.header("Terms of Use and Data Protection Compliance")
        st.markdown(GDPR_TERMS) 
        accepted = st.checkbox("By checking this box, I confirm that the data provided is anonymized.")
        if st.button("Continue", disabled=not accepted):
            st.session_state.lgpd_accepted = True
            st.rerun()
        return

    if 'filter_rules' not in st.session_state: 
        st.session_state.filter_rules = copy.deepcopy(DEFAULT_FILTERS)

    if 'stratum_rules' not in st.session_state: st.session_state.stratum_rules = [{'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''}]
    
    with st.sidebar:
        st.title("User Manual")
        topic = st.selectbox("Select a topic", list(MANUAL_CONTENT.keys()), label_visibility="collapsed")
        st.markdown(MANUAL_CONTENT[topic], unsafe_allow_html=True)

    st.title("Data Sift")

    with st.expander("1. Global Settings", expanded=True):
        uploaded_file = st.file_uploader(
            "Select spreadsheet", 
            type=['csv', 'xlsx', 'xls', 'zip'],
            on_change=reset_results_on_upload,
            key="file_uploader_widget"
        )

        if "dados_salvos" not in st.session_state:
            st.session_state.dados_salvos = None
        if "id_arquivo_atual" not in st.session_state:
            st.session_state.id_arquivo_atual = None

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
                    else:
                        sex_column_values = [""] + list(unique_sex_values)
                except KeyError: st.session_state.sex_column_is_valid = False

            if st.session_state.col_idade:
                try:
                    age_col = df[st.session_state.col_idade].dropna()
                    numeric_ages = pd.to_numeric(age_col, errors='coerce')
                    if (numeric_ages.isna().sum() / len(age_col) if len(age_col) > 0 else 0) > 0.2:
                        st.session_state.age_column_is_valid = False
                except KeyError: st.session_state.age_column_is_valid = False

    is_ready_for_processing = st.session_state.age_column_is_valid and st.session_state.sex_column_is_valid
    tab_filter, tab_stratify = st.tabs(["2. Filter Tool", "3. Stratification Tool"])

    with tab_filter:
        st.header("Exclusion Rules")
        draw_filter_rules(sex_column_values, column_options)
        if st.button("Add New Filter Rule"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '<', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''})
            st.rerun()
        
        if st.button("Generate Filtered Sheet", type="primary", use_container_width=True, disabled=not is_ready_for_processing):
            if df is None: st.error("Please upload a spreadsheet first.")
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
                    else: st.success("No rows remaining.")

        if 'filtered_result' in st.session_state:
            st.download_button("Download Filtered Sheet", data=st.session_state.filtered_result[0], file_name=st.session_state.filtered_result[1], use_container_width=True)

    with tab_stratify:
        st.header("Harris-Boyd Study (Stratification Suggestion)")
        if df is not None:
            if not st.session_state.col_idade or not st.session_state.col_dados:
                st.info("⚠️ To view the Harris-Boyd study, make sure to fill in the **'Age Column'** and **'Data Column (Harris-Boyd)'** in the **Global Settings** section.")
            else:
                with st.spinner("Calculating and generating interpretative report..."):
                    texto_interpretativo, raw_df = run_harris_boyd(df, st.session_state.col_idade, st.session_state.col_dados)
                    st.markdown(texto_interpretativo)
                    
                    if not raw_df.empty:
                        with st.expander("View full statistical data (Advanced Mode)"):
                            st.dataframe(raw_df, use_container_width=True, hide_index=True)
                            
            st.markdown("---")
            st.header("📊 Visual Dispersion Analysis (Boxplot)")
            st.markdown("Evaluate the variation of medians and boxes by generating the interactive chart below.")
            
            if st.session_state.col_idade and st.session_state.col_dados:
                col1, col2 = st.columns([1, 2])
                intervalo_plot = col1.number_input("Age interval size (e.g., 5 = group every 5 years):", min_value=1, max_value=20, value=5, step=1)
                
                if col2.button("Generate Boxplot Chart", type="primary", use_container_width=True):
                    with st.spinner("Drawing chart..."):
                        fig = plot_boxplot_idade(df, st.session_state.col_idade, st.session_state.col_dados, intervalo_plot)
                        if fig:
                            st.pyplot(fig)
                        else:
                            st.warning("Not enough valid data in the selected column to generate the chart.")
            else:
                st.info("⚠️ Select the Age column and Data column in global settings to enable the chart.")
        else:
            st.info("⚠️ Upload a spreadsheet in 'Global Settings' to use this function.")
        
        st.markdown("---")

        st.header("Stratification Options")
        if st.session_state.sex_column_is_valid and sex_column_values:
            if 'strat_gender_selection' not in st.session_state: st.session_state.strat_gender_selection = {val: True for val in sex_column_values if val}
            cols = st.columns(min(len(sex_column_values), 5))
            col_idx = 0
            for gender_val in sex_column_values:
                if not gender_val: continue
                st.session_state.strat_gender_selection[gender_val] = cols[col_idx].checkbox(str(gender_val), value=st.session_state.strat_gender_selection.get(gender_val, True), key=f"strat_check_{gender_val}")
                col_idx = (col_idx + 1) % len(cols)

        st.header("Age Range Definitions")
        draw_stratum_rules()
        if st.button("Add Age Range"):
            st.session_state.stratum_rules.append({'id': str(uuid.uuid4()), 'op1': '', 'val1': '', 'op2': '', 'val2': ''})
            st.rerun()
        
        if st.button("Generate Stratified Sheets", type="primary", use_container_width=True, disabled=not is_ready_for_processing):
            st.session_state.confirm_stratify = True
            st.rerun()

        if st.session_state.get('confirm_stratify', False):
            st.warning("Do you confirm this is the FILTERED version?")
            c1, c2 = st.columns(2)
            if c1.button("Yes, continue"):
                if df is not None:
                    with st.spinner("Generating strata..."):
                        progress_bar = st.progress(0, text="Initializing...")
                        processor = get_data_processor()
                        age_rules = [r for r in st.session_state.stratum_rules if r.get('val1')]
                        sex_rules = [{'value': gender_val, 'name': str(gender_val)} for gender_val, is_selected in st.session_state.get('strat_gender_selection', {}).items() if is_selected]
                        st.session_state.stratified_results = processor.apply_stratification(df.copy(), {'ages': age_rules, 'sexes': sex_rules}, {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}, progress_bar)
                st.session_state.confirm_stratify = False
                st.rerun()
            if c2.button("No, cancel"):
                st.session_state.confirm_stratify = False
                st.rerun()

        if st.session_state.get('stratified_results'):
            st.markdown("---"); st.subheader(f"Files ({len(st.session_state.stratified_results)} generated)")
            is_excel = "Excel" in st.session_state.output_format
            for filename, df_to_download in st.session_state.stratified_results.items():
                file_bytes = to_excel(df_to_download) if is_excel else to_csv(df_to_download)
                st.download_button(f"Download {filename}", data=file_bytes, file_name=f"{filename}.{'xlsx' if is_excel else 'csv'}", key=f"dl_{filename}")

if __name__ == "__main__":
    main()
