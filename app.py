# -*- coding: utf-8 -*-

# Versão 2.0.0 - Atualização: Otimização com Motor SQL DuckDB
# Melhorias: Processamento de dados extremamente rápido via DuckDB para não travar o Streamlit.

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
  A selection menu to choose the format of the generated files. The default is `.csv`. Choose `Excel (.xlsx)` for better compatibility with Microsoft Excel or `CSV (.csv)` for a lighter, universal format.
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
            if op in ('=', '=='): return f"({col} IS NULL OR TRIM(CAST({col} AS VARCHAR)) = '')"
            if op == '!=': return f"({col} IS NOT NULL AND TRIM(CAST({col} AS VARCHAR)) != '')"
            return "FALSE"

        # Tenta interpretar como número (permite comparar strings que na verdade são números na planilha)
        try:
            v_num = float(str(val).replace(',', '.'))
            safe_cast = f"TRY_CAST(REPLACE(CAST({col} AS VARCHAR), ',', '.') AS DOUBLE)"
            return f"({safe_cast} IS NOT NULL AND {safe_cast} {op} {v_num})"
        except ValueError:
            # Tratamento de Strings
            v_str = str(val).replace("'", "''").lower().strip()
            return f"(CAST({col} AS VARCHAR) IS NOT NULL AND LOWER(TRIM(CAST({col} AS VARCHAR))) {op} '{v_str}')"

    def _create_main_sql(self, f: Dict, col: str) -> str:
        """Cria o SQL da regra principal (com suporte a lógica expandida)."""
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
        """Cria o SQL das condições secundárias (Idade/Sexo)."""
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

            # Lógica de exclusão: se a linha satisfaz a regra principal E a condicional, ela deve ser excluída.
            # Portanto, nós queremos manter as linhas onde NOT(regra_principal AND regra_condicional)
            rule_sql = f"({combined_main_sql}) AND ({cond_sql})"
            exclusion_clauses.append(f"NOT ({rule_sql})")

        if not exclusion_clauses:
            progress_bar.progress(1.0, text="Processamento concluído!")
            return df

        where_clause = " AND ".join(exclusion_clauses)
        query = f"SELECT * FROM df WHERE {where_clause}"

        try:
            progress_bar.progress(0.8, text="Executando Motor DuckDB (SQL)...")
            filtered_df = duckdb.query(query).df()
            progress_bar.progress(1.0, text="Filtragem completa!")
            return filtered_df
        except Exception as e:
            st.error(f"Erro no processamento SQL: {e}")
            return df
    
    def apply_stratification(self, df: pd.DataFrame, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        """Divide o banco em sub-planilhas usando DuckDB."""
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
            query = f"SELECT * FROM df WHERE {where_clause}"

            filename = self._generate_stratum_name(age_rule, sex_rule)
            progress_bar.progress(progress, text=f"Gerando estrato {i+1}/{total_files}: {filename}...")
            
            try:
                stratum_df = duckdb.query(query).df()
                if not stratum_df.empty:
                    generated_dfs[filename] = stratum_df
            except Exception as e:
                st.warning(f"Não foi possível gerar o estrato {filename} devido a erro nos valores: {e}")

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

@st.cache_data(show_spinner="Lendo arquivo (Modo Econômico de Memória)...")
def load_dataframe(uploaded_file):
    if uploaded_file is None: return None
    try:
        file_name = uploaded_file.name.lower()
        
        # 1. STREAMING PARA O DISCO: Evita clonar o arquivo gigante na memória RAM
        uploaded_file.seek(0)
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file_name)[1]) as tmp_file:
            shutil.copyfileobj(uploaded_file, tmp_file)
            tmp_path = tmp_file.name

        df = None

        # --- LÓGICA DE TRATAMENTO DE ZIP ---
        if file_name.endswith('.zip'):
            with zipfile.ZipFile(tmp_path) as z:
                valid_files = [f for f in z.namelist() if not f.startswith('__MACOSX/') and 
                               (f.lower().endswith('.csv') or f.lower().endswith(('.xlsx', '.xls')))]
                
                if not valid_files:
                    st.error("O ZIP não contém arquivos CSV ou Excel válidos.")
                    os.remove(tmp_path)
                    return None
                
                # Extrai o arquivo para o disco temporário
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(valid_files[0])[1]) as inner_tmp:
                    inner_tmp.write(z.read(valid_files[0]))
                    inner_path = inner_tmp.name
                
                inner_filename = valid_files[0].lower()
                
                if inner_filename.endswith('.csv'):
                    try:
                        # 2. MOTOR PYARROW: Leitura super rápida com metade do custo de memória
                        df = pd.read_csv(inner_path, sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
                    except Exception:
                        df = pd.read_csv(inner_path, sep=',', decimal='.', encoding='utf-8', engine='pyarrow')
                else:
                    df = pd.read_excel(inner_path, engine='openpyxl')
                
                os.remove(inner_path)

        # --- LÓGICA PARA ARQUIVOS DIRETOS ---
        elif file_name.endswith('.csv'):
            try: 
                df = pd.read_csv(tmp_path, sep=';', decimal=',', encoding='latin-1', engine='pyarrow')
            except Exception:
                df = pd.read_csv(tmp_path, sep=',', decimal='.', encoding='utf-8', engine='pyarrow')
        else:
            df = pd.read_excel(tmp_path, engine='openpyxl')

        # Limpa o arquivo temporário do disco para liberar espaço
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        # --- DOWNCASTING (Mantido da sua versão original) ---
        if df is not None:
            fcols = df.select_dtypes('float').columns
            icols = df.select_dtypes('integer').columns
            df[fcols] = df[fcols].apply(pd.to_numeric, downcast='float')
            df[icols] = df[icols].apply(pd.to_numeric, downcast='integer')

            for col in df.select_dtypes('object').columns:
                if df[col].nunique() / len(df[col]) < 0.5:
                    df[col] = df[col].astype('category')
        
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        return None

@st.cache_data(show_spinner="Preparando arquivo para exportação...")
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

@st.cache_data(show_spinner="Preparando CSV para exportação...")
def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

# --- FUNÇÕES DE INTERFACE ---

def handle_select_all():
    """Lógica para marcar/desmarcar todos os filtros baseado no estado da master checkbox."""
    new_state = st.session_state['select_all_master_checkbox']
    for rule in st.session_state.filter_rules:
        rule['p_check'] = new_state

def reset_results_on_upload():
    """Limpa os resultados anteriores quando um novo arquivo é carregado."""
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
        "Selecionar/Desmarcar tudo",
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
                f"Ativar regra {rule['id']}", 
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
        df = load_dataframe(uploaded_file)
        column_options = df.columns.tolist() if df is not None else []
        c1, c2, c3 = st.columns(3)
        with c1: st.selectbox("Age Column", options=column_options, key="col_idade", index=None, placeholder="Select the Age column")
        with c2: st.selectbox("Sex/Gender Column", options=column_options, key="col_sexo", index=None, placeholder="Select the Sex/Gender column")
        with c3: st.selectbox("Output Format", ["CSV (.csv)", "Excel (.xlsx)"], key="output_format")

        st.session_state.sex_column_is_valid = True
        st.session_state.age_column_is_valid = True
        sex_column_values = []

        if df is not None:
            if st.session_state.col_sexo:
                try:
                    unique_sex_values = df[st.session_state.col_sexo].dropna().unique()
                    if len(unique_sex_values) > 10:
                        st.warning(f"Coluna '{st.session_state.col_sexo}' possui muitos valores únicos.")
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
