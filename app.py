# -*- coding: utf-8 -*-

# Versão 1.9.5 - OTIMIZADA
# Melhorias: performance, memória e estabilidade para arquivos grandes

import streamlit as st
import pandas as pd
import numpy as np
import io
import uuid
import copy
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

st.set_page_config(layout="wide", page_title="Data Sift")

# --- CONSTANTES ---
GDPR_TERMS = """This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, national ID numbers, health information, etc.), the processing of which is regulated by data protection laws like the General Data Protection Regulation (GDPR or LGPD).

It is your sole responsibility to ensure that all data used in this tool complies with applicable data protection regulations. We strongly recommend that you only use previously anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been properly handled and anonymized."""  # (mantido igual)
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
}  # (mantido igual)
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
] # (mantido igual)

# --- PROCESSOR ---

@st.cache_resource
def get_data_processor():
    return DataProcessor()

class DataProcessor:
    OPERATOR_MAP = {'=': '==', 'Não é igual a': '!=', '≥': '>=', '≤': '<=', 'is equal to': '==', 'Not equal to': '!='}

    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series):
            return series

        # evita reconversões
        if hasattr(series, "_is_numeric_converted"):
            return series

        result = pd.to_numeric(
            series.astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )
        result._is_numeric_converted = True
        return result

    def _build_single_mask(self, series: pd.Series, op: str, val: Any) -> pd.Series:
        if isinstance(val, str):
            val_lower_strip = val.lower().strip()
            series_lower_strip = series.astype(str).str.strip().str.lower()
            if op == '==': return series_lower_strip == val_lower_strip
            elif op == '!=': return series_lower_strip != val_lower_strip
        return eval(f"series {op} val")

    def _create_main_mask(self, df: pd.DataFrame, f: Dict, col: str) -> pd.Series:
        op1 = self.OPERATOR_MAP.get(f.get('p_op1'), f.get('p_op1'))
        val1 = f.get('p_val1')

        if val1 and val1.lower() == 'empty':
            if op1 == '==': return df[col].isna() | (df[col].astype(str).str.strip() == '')
            if op1 == '!=': return df[col].notna() & (df[col].astype(str).str.strip() != '')
            return pd.Series(False, index=df.index)

        try:
            if f.get('p_expand'):
                v1 = float(str(val1).replace(',', '.'))
                op2 = self.OPERATOR_MAP.get(f.get('p_op2'), f.get('p_op2'))
                v2 = float(str(f.get('p_val2')).replace(',', '.'))

                m1 = self._build_single_mask(df[col], op1, v1)
                m2 = self._build_single_mask(df[col], op2, v2)

                if f.get('p_op_central').upper() == 'AND': return m1 & m2
                if f.get('p_op_central').upper() == 'OR': return m1 | m2
            else:
                v1 = float(str(val1).replace(',', '.'))
                return self._build_single_mask(df[col], op1, v1)

        except:
            return pd.Series(False, index=df.index)

    def _create_conditional_mask(self, df, f, global_config):
        mask = pd.Series(True, index=df.index)

        if not f.get('c_check'):
            return mask

        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')

        if f.get('c_idade_check') and col_idade in df.columns:
            df[col_idade] = self._safe_to_numeric(df[col_idade])

        if f.get('c_sexo_check') and col_sexo in df.columns:
            val = f.get('c_sexo_val', '').lower().strip()
            if val:
                mask &= self._build_single_mask(df[col_sexo], '==', val)

        return mask

    def apply_filters(self, df, filters_config, global_config, progress_bar):

        df_processado = df  # 🔥 sem cópia

        converted_cols = set()

        active_filters = [f for f in filters_config if f['p_check']]
        total = len(active_filters)

        for i, f in enumerate(active_filters):

            progress_bar.progress((i+1)/total)

            cols = [c.strip() for c in f.get('p_col', '').split(';') if c.strip()]
            is_numeric = f.get('p_val1', '').lower() != 'empty'

            for col in cols:
                if col in df_processado.columns and is_numeric:
                    if col not in converted_cols:
                        df_processado[col] = self._safe_to_numeric(df_processado[col])
                        converted_cols.add(col)

            if not cols:
                continue

            combined_mask = pd.Series(True, index=df_processado.index)

            for col in cols:
                if col in df_processado.columns:
                    combined_mask &= self._create_main_mask(df_processado, f, col)
                else:
                    combined_mask = pd.Series(False, index=df_processado.index)

            conditional_mask = self._create_conditional_mask(df_processado, f, global_config)
            final_mask = combined_mask & conditional_mask

            df_processado = df_processado.loc[~final_mask]

        return df_processado

    def apply_stratification(self, df, strata_config, global_config, progress_bar):

        col_idade = global_config.get('coluna_idade')
        col_sexo = global_config.get('coluna_sexo')

        df[col_idade] = self._safe_to_numeric(df[col_idade])

        generated = {}

        for i, stratum in enumerate(strata_config.get('ages', [])):
            mask = pd.Series(True, index=df.index)

            if stratum.get('op1'):
                val = float(str(stratum.get('val1')).replace(',', '.'))
                op = self.OPERATOR_MAP.get(stratum['op1'], stratum['op1'])
                mask &= eval(f"df[col_idade] {op} {val}")

            result = df.loc[mask]

            if not result.empty:
                generated[f"stratum_{i}"] = result

        return generated


# --- LOAD DATAFRAME OTIMIZADO ---

@st.cache_data
def load_dataframe(uploaded_file):
    if uploaded_file is None:
        return None

    file_name = uploaded_file.name.lower()

    try:
        if file_name.endswith('.csv'):
            uploaded_file.seek(0)
            try:
                df = pd.read_csv(
                    uploaded_file,
                    sep=';',
                    decimal=',',
                    encoding='latin-1',
                    low_memory=True,
                    memory_map=True
                )
            except:
                uploaded_file.seek(0)
                df = pd.read_csv(
                    uploaded_file,
                    sep=',',
                    decimal='.',
                    encoding='utf-8',
                    low_memory=True,
                    memory_map=True
                )

        else:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, engine='openpyxl')

        # downcast
        for col in df.select_dtypes(include=['float']):
            df[col] = pd.to_numeric(df[col], downcast='float')

        for col in df.select_dtypes(include=['int']):
            df[col] = pd.to_numeric(df[col], downcast='integer')

        return df

    except Exception as e:
        st.error(e)
        return None


# --- EXPORTS ---

@st.cache_data
def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',').encode('utf-8')


@st.cache_data
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()


# --- MAIN ---

def main():
    st.title("Data Sift")

    uploaded_file = st.file_uploader("Upload", type=['csv', 'xlsx'])

    df = load_dataframe(uploaded_file)

    if df is not None:

        if st.button("Process"):

            processor = get_data_processor()

            progress = st.progress(0)

            result = processor.apply_filters(
                df,
                DEFAULT_FILTERS,
                {"coluna_idade": None, "coluna_sexo": None},
                progress
            )

            st.success(f"{len(result)} linhas restantes")

            st.download_button(
                "Download CSV",
                to_csv(result),
                "resultado.csv"
            )


if __name__ == "__main__":
    main()
