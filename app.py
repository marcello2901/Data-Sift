# -*- coding: utf-8 -*-

# Versão 1.9 - Correção definitiva do marcador "Selecionar Todos" e implementação de placeholders
import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

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
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'CAPA.IST', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '50', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ferritina.FERRI', 'p_op1': '<', 'p_val1': '15', 'p_expand': True, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '600', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
    {'id': str(uuid.uuid4()), 'p_check': True, 'p_col': 'Ultra-PCR.ULTRAPCR', 'p_op1': '>', 'p_val1': '5', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '<', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''},
]

# --- CLASSES DE PROCESSAMENTO ---

@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '==', 'Não é igual a': '!=', '≥': '>=', '≤': '<=', 'is equal to': '==', 'Not equal to': '!='}

    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series): return series
        return pd.to_numeric(series.astype(str).str.replace(',', '.', regex=False), errors='coerce')

    def _build_single_mask(self, series: pd.Series, op: str, val: Any) -> pd.Series:
        if isinstance(val, str):
            val_lower_strip = val.lower().strip()
            series_lower_strip = series.astype(str).str.strip().str.lower()
            if op == '==': return series_lower_strip == val_lower_strip
            elif op == '!=': return series_lower_strip != val_lower_strip
        return eval(f"series {op} val")

    def _create_main_mask(self, df: pd.DataFrame, f: Dict, col: str) -> pd.Series:
        op1_ui, val1 = f.get('p_op1'), f.get('p_val1')
        op1 = self.OPERATOR_MAP.get(op1_ui, op1_ui)

        if val1 and val1.lower() == 'empty':
            if op1 == '==': return df[col].isna() | (df[col].astype(str).str.strip() == '')
            if op1 == '!=': return df[col].notna() & (df[col].astype(str).str.strip() != '')
            return pd.Series([False] * len(df), index=df.index)

        try:
            if f.get('p_expand'):
                v1_num = float(str(val1).replace(',', '.'))
                op_central_ui, op2_ui, val2 = f.get('p_op_central'), f.get('p_op2'), f.get('p_val2')
                op2 = self.OPERATOR_MAP.get(op2_ui, op2_ui)
                v2_num = float(str(val2).replace(',', '.'))

                if op_central_ui.upper() == 'BETWEEN':
                    min_val, max_val = sorted((v1_num, v2_num))
                    return df[col].between(min_val, max_val, inclusive='both')
                m1 = self._build_single_mask(df[col], op1, v1_num)
                m2 = self._build_single_mask(df[col], op2, v2_num)
                if op_central_ui.upper() == 'AND': return m1 & m2
                if op_central_ui.upper() == 'OR': return m1 | m2
            else:
                v1_num = float(str(val1).replace(',', '.'))
                return self._build_single_mask(df[col], op1, v1_num)
        except (ValueError, TypeError):
            return pd.Series([False] * len(df), index=df.index)

    def _create_conditional_mask(self, df: pd.DataFrame, f: Dict, global_config: Dict) -> pd.Series:
        mascara_condicional = pd.Series(True, index=df.index)
        if not f.get('c_check'): return mascara_condicional

        col_idade = global_config.get('coluna_idade')
        if f.get('c_idade_check') and col_idade and col_idade in df.columns:
            df[col_idade] = self._safe_to_numeric(df[col_idade])
            try:
                op_idade1_ui, val_idade1 = f.get('c_idade_op1'), f.get('c_idade_val1')
                if op_idade1_ui and val_idade1:
                    op1 = self.OPERATOR_MAP.get(op_idade1_ui, op_idade1_ui)
                    v1 = float(str(val_idade1).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op1, v1)
                
                op_idade2_ui, val_idade2 = f.get('c_idade_op2'), f.get('c_idade_val2')
                if op_idade2_ui and val_idade2:
                    op2 = self.OPERATOR_MAP.get(op_idade2_ui, op_idade2_ui)
                    v2 = float(str(val_idade2).replace(',', '.'))
                    mascara_condicional &= self._build_single_mask(df[col_idade], op2, v2)
            except (ValueError, TypeError):
                pass

        col_sexo = global_config.get('coluna_sexo')
        if f.get('c_sexo_check') and col_sexo and col_sexo in df.columns:
            val_sexo_gui = f.get('c_sexo_val', '').lower().strip()
            if val_sexo_gui:
                mascara_condicional &= self._build_single_mask(df[col_sexo], '==', val_sexo_gui)
        return mascara_condicional

    def apply_filters(self, df: pd.DataFrame, filters_config: List[Dict], global_config: Dict, progress_bar) -> pd.DataFrame:
        df_processado = df.copy()
        active_filters = [f for f in filters_config if f['p_check']]
        total_filters = len(active_filters)

        for i, f_config in enumerate(active_filters):
            progress = (i + 1) / total_filters
            col_name = f_config.get('p_col', 'Unknown Rule')
            progress_bar.progress(progress, text=f"Applying filter {i+1}/{total_filters}: '{col_name[:30]}...'")

            col_config_str = f_config.get('p_col', '')
            # A lógica para múltiplas colunas (separadas por ';') foi mantida.
            # Se for usado selectbox (uma coluna), este código ainda funciona.
            # Para manter a funcionalidade original de múltiplas colunas, o campo teria que ser um text_input.
            # Como a solicitação foi para usar o mesmo funcionamento (selectbox), esta lógica se aplicará a uma única coluna por regra.
            cols_to_check = [c.strip() for c in col_config_str.split(';') if c.strip()]

            is_numeric_filter = f_config.get('p_val1', '').lower() != 'empty'
            for col in cols_to_check:
                if col in df_processado.columns and is_numeric_filter:
                    df_processado[col] = self._safe_to_numeric(df_processado[col])

            combined_mask = pd.Series(True, index=df_processado.index)
            if not cols_to_check:
                combined_mask = pd.Series(False, index=df_processado.index)
            else:
                for sub_col in cols_to_check:
                    if sub_col in df_processado.columns:
                        combined_mask &= self._create_main_mask(df_processado, f_config, sub_col)
                    else:
                        combined_mask = pd.Series(False, index=df_processado.index)
                        break
            
            conditional_mask = self._create_conditional_mask(df_processado, f_config, global_config)
            final_mask_to_exclude = combined_mask & conditional_mask
            
            df_processado = df_processado[~final_mask_to_exclude]
        
        progress_bar.progress(1.0, text="Filtering complete!")
        return df_processado
    
    def apply_stratification(self, df: pd.DataFrame, strata_config: Dict, global_config: Dict, progress_bar) -> Dict[str, pd.DataFrame]:
        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')

        if not (col_idade and col_idade in df.columns):
            st.error(f"Age column '{col_idade}' not found in the spreadsheet."); return {}
        if not (col_sexo and col_sexo in df.columns):
            st.error(f"Sex/gender column '{col_sexo}' not found in the spreadsheet."); return {}

        df[col_idade] = self._safe_to_numeric(df[col_idade])

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
            combined_mask = pd.Series(True, index=df.index)

            age_rule = stratum.get('age')
            sex_rule = stratum.get('sex')

            if age_rule:
                age_mask = pd.Series(True, index=df.index)
                try:
                    if age_rule.get('op1') and age_rule.get('val1'):
                        op1 = self.OPERATOR_MAP.get(age_rule['op1'], age_rule['op1'])
                        val1 = float(str(age_rule['val1']).replace(',', '.'))
                        age_mask &= eval(f"df['{col_idade}'] {op1} {val1}")
                    if age_rule.get('op2') and age_rule.get('val2'):
                        op2 = self.OPERATOR_MAP.get(age_rule['op2'], age_rule['op2'])
                        val2 = float(str(age_rule['val2']).replace(',', '.'))
                        age_mask &= eval(f"df['{col_idade}'] {op2} {val2}")
                    combined_mask &= age_mask
                except (ValueError, TypeError):
                    st.warning(f"Could not apply age rule due to invalid values: {age_rule}")
                    continue

            if sex_rule:
                sex_val = sex_rule.get('value')
                if sex_val:
                    combined_mask &= self._build_single_mask(df[col_sexo], '==', sex_val)
            
            stratum_df = df[combined_mask]
            filename = self._generate_stratum_name(age_rule, sex_rule)
            progress_bar.progress(progress, text=f"Generating stratum {i+1}/{total_files}: {filename}...")
            
            if not stratum_df.empty:
                generated_dfs[filename] = stratum_df
        
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
                    elif op1 == '≥': name_parts.append(f"{v1_int}_and_over_years")
                    elif op1 == '<': name_parts.append(f"Under_{v1_int}_years")
                    elif op1 == '≤': name_parts.append(f"Up_to_{v1_int}_years")
            elif op1 and val1 and op2 and val2:
                if v1_int is not None and v2_int is not None:
                    v1_f, v2_f = float(str(val1).replace(',', '.')), float(str(val2).replace(',', '.'))
                    
                    bounds = []
                    if op1 and val1: bounds.append((v1_f, op1))
                    if op2 and val2: bounds.append((v2_f, op2))
                    bounds.sort(key=lambda x: x[0])

                    low_val_f, low_op = bounds[0]
                    high_val_f, high_op = bounds[1]

                    low_bound = int(low_val_f) if low_op == '≥' else int(low_val_f + 1) if low_op == '>' else int(low_val_f)
                    high_bound = int(high_val_f) if high_op == '≤' else int(high_val_f - 1) if high_op == '<' else int(high_val_f)
                    
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
        uploaded_file.seek(0)
        if uploaded_file.name.endswith('.csv'):
            try: return pd.read_csv(io.BytesIO(uploaded_file.getvalue()), sep=';', decimal=',', encoding='latin-1')
            except Exception:
                uploaded_file.seek(0)
                return pd.read_csv(io.BytesIO(uploaded_file.getvalue()), sep=',', decimal='.', encoding='utf-8')
        else:
            return pd.read_excel(io.BytesIO(uploaded_file.getvalue()), engine='openpyxl')
    except Exception as e:
        st.error(f"Error reading file: {e}"); return None

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')

# --- FUNÇÕES DE INTERFACE ---

def handle_select_all():
    new_state = st.session_state.get('select_all_master_checkbox', False)
    for rule in st.session_state.filter_rules:
        rule['p_check'] = new_state

# ######### INÍCIO DAS ALTERAÇÕES #########
def draw_filter_rules(sex_column_values, column_options): # Parâmetro adicionado
    st.markdown("""<style>
        .stButton>button { padding: 0.25rem 0.3rem; font-size: 0.8rem; white-space: nowrap; }
        div[data-testid="stTextInput"] input, div[data-testid="stSelectbox"] div[data-baseweb="select"] {
            border: 1px solid rgba(255, 75, 75, 0.15) !important;
            border-radius: 0.25rem;
        }
        /* Estilo para o placeholder do selectbox */
        div[data-baseweb="select"] input::placeholder {
            color: black !important;
            opacity: 0.2 !important;
        }
    </style>""", unsafe_allow_html=True)
    
    header_cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium")
    
    if st.session_state.filter_rules:
        all_checked = all(rule.get('p_check', True) for rule in st.session_state.filter_rules)
    else:
        all_checked = False

    header_cols[0].checkbox(
        "Select/Deselect all",
        value=all_checked,
        key='select_all_master_checkbox', 
        on_change=handle_select_all,   
        label_visibility="collapsed",
        help="Select/Deselect all rules"
    )
    
    header_cols[1].markdown("**Column** <span title='Select the column to apply the filter to.'>&#9432;</span>", unsafe_allow_html=True)
    header_cols[2].markdown("**Operator** <span title='Use comparison operators to define the first filter.'>&#9432;</span>", unsafe_allow_html=True)
    header_cols[3].markdown("**Value** <span title='Enter the value you want to exclude from the data.'>&#9432;</span>", unsafe_allow_html=True)
    
    tooltip_text = """Select another operator to define an interval.
How to use:
BETWEEN: Excludes values within the interval (inclusive). Ex: BETWEEN 10 and 20 removes everything from 10 to 20.
OR: Excludes values outside an interval. Use to keep the data in between. Ex: < 10 OR > 20 removes everything less than 10 and greater than 20.
AND: Excludes values within an interval, without the extremes. Ex: > 10 AND < 20 removes from 11 to 19 (keeps the values 10 and 20).
"""
    tooltip_text_html = tooltip_text.replace('\n', '&#10;')
    header_cols[5].markdown(f"**Compound Logic** <span title='{tooltip_text_html}'>&#9432;</span>", unsafe_allow_html=True)
    
    header_cols[6].markdown("**Condition** <span title='Restricts the main rule to a specific subgroup. The exclusion will only affect rows that also satisfy the age and/or sex/gender criteria defined here.'>&#9432;</span>", unsafe_allow_html=True)
    header_cols[7].markdown("**Actions** <span title='Use to duplicate or delete a rule.'>&#9432;</span>", unsafe_allow_html=True)
    st.markdown("<hr style='margin-top: -0.5rem; margin-bottom: 0.5rem;'>", unsafe_allow_html=True)

    ops_main = ["", ">", "<", "=", "Not equal to", "≥", "≤"]
    ops_age = ["", ">", "<", "≥", "≤", "="]
    ops_central_logic = ["AND", "OR", "BETWEEN"]

    for i, rule in enumerate(st.session_state.filter_rules):
        with st.container():
            cols = st.columns([0.5, 3, 2, 2, 0.5, 3, 1.2, 1.5], gap="medium") 
            rule['p_check'] = cols[0].checkbox(" ", value=rule.get('p_check', True), key=f"p_check_{rule['id']}", label_visibility="collapsed")
            
            # --- CAMPO DE TEXTO ALTERADO PARA SELECTBOX ---
            current_col = rule.get('p_col')
            current_index = None
            if current_col and column_options:
                try:
                    current_index = column_options.index(current_col)
                except ValueError:
                    current_index = None # Coluna salva não existe nas opções atuais
            
            rule['p_col'] = cols[1].selectbox(
                "Column", 
                options=column_options, 
                index=current_index,
                placeholder="Select column to filter", # Placeholder adicionado
                key=f"p_col_{rule['id']}", 
                label_visibility="collapsed"
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
                            
                            centered_and_html = """
                            <div style="display: flex; justify-content: center; align-items: center; height: 38px;">
                                AND
                            </div>
                            """
                            age_cols[2].markdown(centered_and_html, unsafe_allow_html=True)
                            
                            rule['c_idade_op2'] = age_cols[3].selectbox("Age Op 2", ops_age, index=ops_age.index(rule.get('c_idade_op2','<')) if rule.get('c_idade_op2') in ops_age else 0, key=f"c_idade_op2_{rule['id']}", label_visibility="collapsed")
                            rule['c_idade_val2'] = age_cols[4].text_input("Age Val 2", value=rule.get('c_idade_val2',''), key=f"c_idade_val2_{rule['id']}", label_visibility="collapsed")
                    
                    rule['c_sexo_check'] = cond_cols[4].checkbox("Sex/Gender", value=rule.get('c_sexo_check', False), key=f"c_sexo_check_{rule['id']}")
                    with cond_cols[5]:
                        if rule['c_sexo_check']:
                            # Adicionado placeholder aqui também, por consistência, embora o comportamento seja um pouco diferente
                            sex_options = [v for v in sex_column_values if v] # Remove o valor vazio se houver
                            current_sex = rule.get('c_sexo_val')
                            sex_index = None
                            if current_sex and sex_options:
                                try:
                                    sex_index = sex_options.index(current_sex)
                                except ValueError:
                                    sex_index = None
                            
                            rule['c_sexo_val'] = st.selectbox("Sex Value", 
                                options=sex_options, 
                                index=sex_index,
                                placeholder="Select value",
                                key=f"c_sexo_val_{rule['id']}", 
                                label_visibility="collapsed")
        st.markdown("---")
# ######### FIM DAS ALTERAÇÕES #########

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
        st.markdown("This program is designed to optimize your work with large volumes of data, offering features to exclude data from spreadsheets using filters and to stratify the filtered spreadsheet. Please read the terms below to proceed.")
        st.divider()
        st.header("Terms of Use and Data Protection Compliance")
        st.markdown(GDPR_TERMS) 
        accepted = st.checkbox("By checking this box, I confirm that the data provided is anonymized and contains no sensitive personal data.")
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
        uploaded_file = st.file_uploader("Select spreadsheet", type=['csv', 'xlsx', 'xls'])
        df = load_dataframe(uploaded_file)
        
        # ######### INÍCIO DA ALTERAÇÃO #########
        # Opções de coluna sem o item vazio para permitir o placeholder
        column_options = df.columns.tolist() if df is not None else []
        
        c1, c2, c3 = st.columns(3)
        with c1: 
            st.selectbox(
                "Age Column", 
                options=column_options, 
                key="col_idade", 
                index=None, # Define o padrão como não selecionado
                placeholder="Select the Age column" # Adiciona o placeholder
            )
        with c2: 
            st.selectbox(
                "Sex/Gender Column", 
                options=column_options, 
                key="col_sexo", 
                index=None, # Define o padrão como não selecionado
                placeholder="Select the Sex/Gender column" # Adiciona o placeholder
            )
        with c3: 
            st.selectbox("Output Format", ["CSV (.csv)", "Excel (.xlsx)"], key="output_format")
        # ######### FIM DA ALTERAÇÃO #########

        st.session_state.sex_column_is_valid = True
        st.session_state.age_column_is_valid = True
        sex_column_values = []

        if df is not None:
            if st.session_state.col_sexo:
                try:
                    unique_sex_values = df[st.session_state.col_sexo].dropna().unique()
                    if len(unique_sex_values) > 10:
                        st.warning(f"A coluna '{st.session_state.col_sexo}' possui {len(unique_sex_values)} valores únicos, excedendo o limite de 10. A estratificação por gênero foi desativada.")
                        st.session_state.sex_column_is_valid = False
                    else:
                        sex_column_values = [""] + list(unique_sex_values) # Mantido para compatibilidade com partes existentes
                except KeyError:
                    st.warning(f"Coluna '{st.session_state.col_sexo}' não encontrada."); st.session_state.sex_column_is_valid = False

            if st.session_state.col_idade:
                try:
                    age_col = df[st.session_state.col_idade].dropna()
                    numeric_ages = pd.to_numeric(age_col, errors='coerce')
                    non_numeric_ratio = numeric_ages.isna().sum() / len(age_col) if len(age_col) > 0 else 0

                    if non_numeric_ratio > 0.2:
                        st.warning(f"A coluna '{st.session_state.col_idade}' não parece conter dados de idade válidos (mais de 20% não são números). As funções que dependem da idade estão desativadas.")
                        st.session_state.age_column_is_valid = False
                    elif age_col.nunique() > 120:
                        st.warning(f"A coluna '{st.session_state.col_idade}' possui {age_col.nunique()} valores únicos, excedendo o limite de 120.")
                except KeyError:
                    st.warning(f"Coluna '{st.session_state.col_idade}' não encontrada."); st.session_state.age_column_is_valid = False

    is_ready_for_processing = st.session_state.age_column_is_valid and st.session_state.sex_column_is_valid
    
    tab_filter, tab_stratify = st.tabs(["2. Filter Tool", "3. Stratification Tool"])

    with tab_filter:
        st.header("Exclusion Rules")
        # ######### INÍCIO DA ALTERAÇÃO #########
        draw_filter_rules(sex_column_values, column_options) # Passa as opções de coluna
        # ######### FIM DA ALTERAÇÃO #########
        if st.button("Add New Filter Rule"):
            st.session_state.filter_rules.append({'id': str(uuid.uuid4()), 'p_check': True, 'p_col': '', 'p_op1': '<', 'p_val1': '', 'p_expand': False, 'p_op_central': 'OR', 'p_op2': '>', 'p_val2': '', 'c_check': False, 'c_idade_check': False, 'c_idade_op1': '>', 'c_idade_val1': '', 'c_idade_op2': '<', 'c_idade_val2': '', 'c_sexo_check': False, 'c_sexo_val': ''})
            st.rerun()
        
        if st.button("Generate Filtered Sheet", type="primary", use_container_width=True, disabled=not is_ready_for_processing):
            if df is None: st.error("Please upload a spreadsheet first.")
            else:
                with st.spinner("Applying filters... Please wait."):
                    progress_bar = st.progress(0, text="Initializing...")
                    processor = get_data_processor()
                    global_config = {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}
                    filtered_df = processor.apply_filters(df, st.session_state.filter_rules, global_config, progress_bar)
                    
                    if filtered_df.empty:
                        st.success("Filtros aplicados com sucesso! Nenhuma linha corresponde aos critérios para permanecer na planilha.")
                        if 'filtered_result' in st.session_state: del st.session_state['filtered_result']
                    else:
                        st.success(f"Spreadsheet filtered successfully! {len(filtered_df)} rows remaining.")
                        is_excel = "Excel" in st.session_state.output_format
                        file_bytes = to_excel(filtered_df) if is_excel else to_csv(filtered_df)
                        timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
                        file_name = f"Filtered_Sheet_{timestamp}.{'xlsx' if is_excel else 'csv'}"
                        st.session_state.filtered_result = (file_bytes, file_name)

        if 'filtered_result' in st.session_state:
            st.download_button("Download Filtered Sheet", data=st.session_state.filtered_result[0], file_name=st.session_state.filtered_result[1], use_container_width=True)

    with tab_stratify:
        st.header("Stratification Options by Sex/Gender")
        
        if not st.session_state.sex_column_is_valid:
            st.info("Select a valid 'Sex/Gender Column' in Global Settings with 5 or fewer unique values to see options.")
        elif not sex_column_values:
            st.info("Upload a spreadsheet and select the 'Sex/Gender Column' in Global Settings to see options.")
        else:
            if 'strat_gender_selection' not in st.session_state:
                st.session_state.strat_gender_selection = {val: True for val in sex_column_values if val}
            
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
            age_rules_count = len([r for r in st.session_state.stratum_rules if r.get('val1')])
            sex_rules_count = sum(1 for val, selected in st.session_state.get('strat_gender_selection', {}).items() if selected)
            total_files = age_rules_count * sex_rules_count if age_rules_count > 0 and sex_rules_count > 0 else age_rules_count + sex_rules_count
            
            warning_msg = f"Atenção: Esta operação irá gerar {total_files} arquivos." if total_files > 30 else ""
            st.warning(f"Do you confirm that the selected spreadsheet is the FILTERED version? {warning_msg}")

            c1, c2 = st.columns(2)
            if c1.button("Yes, continue", use_container_width=True):
                if df is None: st.error("Please upload a spreadsheet first.")
                else:
                    with st.spinner("Generating strata... Please wait."):
                        progress_bar = st.progress(0, text="Initializing...")
                        processor = get_data_processor()
                        age_rules = [r for r in st.session_state.stratum_rules if r.get('val1')]
                        sex_rules = [{'value': gender_val, 'name': str(gender_val)} for gender_val, is_selected in st.session_state.get('strat_gender_selection', {}).items() if is_selected]
                        
                        strata_config = {'ages': age_rules, 'sexes': sex_rules}
                        global_config = {"coluna_idade": st.session_state.col_idade, "coluna_sexo": st.session_state.col_sexo}
                        stratified_dfs = processor.apply_stratification(df.copy(), strata_config, global_config, progress_bar)
                        st.session_state.stratified_results = stratified_dfs
                st.session_state.confirm_stratify = False; st.rerun()
            if c2.button("No, cancel", use_container_width=True):
                st.session_state.confirm_stratify = False; st.rerun()
        if st.session_state.get('stratified_results'):
            st.markdown("---"); st.subheader(f"Files to Download ({len(st.session_state.stratified_results)} generated)")
            is_excel = "Excel" in st.session_state.output_format
            for filename, df_to_download in st.session_state.stratified_results.items():
                file_bytes = to_excel(df_to_download) if is_excel else to_csv(df_to_download)
                file_name = f"{filename}.{'xlsx' if is_excel else 'csv'}"
                st.download_button(f"Download {file_name}", data=file_bytes, file_name=file_name)

if __name__ == "__main__":
    main()