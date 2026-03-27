# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import io
import uuid
import duckdb
import tempfile

st.set_page_config(layout="wide", page_title="Data Sift")

# --- CONSTANTES ---
GDPR_TERMS = """This tool is designed to process and filter data from spreadsheets. The files you upload may contain sensitive personal data (such as full name, date of birth, national ID numbers, health information, etc.), the processing of which is regulated by data protection laws like the General Data Protection Regulation (GDPR or LGPD).

It is your sole responsibility to ensure that all data used in this tool complies with applicable data protection regulations. We strongly recommend that you only use previously anonymized data to protect the privacy of data subjects.

The responsibility for the nature of the processed data is exclusively yours.

To proceed, you must confirm that the data to be used has been properly handled and anonymized."""


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


# --- DUCKDB SETUP ---
@st.cache_resource
def get_duckdb_conn():
    return duckdb.connect()

def save_uploaded_file(uploaded_file):
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.write(uploaded_file.getbuffer())
    temp.close()
    return temp.name

# --- FILTER ENGINE ---
def apply_filters_duckdb(file_path, filters_config):
    con = get_duckdb_conn()
    query = "SELECT * FROM read_csv_auto(?) WHERE TRUE"
    params = [file_path]

    for f in filters_config:
        if not f['p_check']:
            continue

        col = f['p_col']
        op = f['p_op1']
        val = f['p_val1']

        if val.lower() == "empty":
            if op in ["=", "is equal to"]:
                query += f" AND ({col} IS NULL OR {col} = '')"
            elif op in ["Not equal to"]:
                query += f" AND ({col} IS NOT NULL AND {col} != '')"
            continue

        try:
            val = float(str(val).replace(",", "."))
        except:
            continue

        if op == ">":
            query += f" AND {col} <= {val}"
        elif op == "<":
            query += f" AND {col} >= {val}"
        elif op == "=":
            query += f" AND {col} != {val}"

    return con.execute(query, params).df()

# --- EXPORT ---
@st.cache_data
def to_csv(df):
    return df.to_csv(index=False, sep=';', decimal=',').encode('utf-8')

# --- MAIN ---
def main():

    # Sidebar Manual
    with st.sidebar:
        st.title("User Manual")
        topic = st.selectbox("Select topic", list(MANUAL_CONTENT.keys()))
        st.markdown(MANUAL_CONTENT[topic])

    if 'accepted' not in st.session_state:
        st.session_state.accepted = False

    if not st.session_state.accepted:
        st.title("Data Sift")
        st.markdown(GDPR_TERMS)
        if st.button("Accept"):
            st.session_state.accepted = True
            st.rerun()
        return

    st.title("Data Sift")

    uploaded_file = st.file_uploader("Upload CSV", type=['csv'])

    if uploaded_file is not None:
        file_path = save_uploaded_file(uploaded_file)

        if st.button("Process"):
            result = apply_filters_duckdb(file_path, DEFAULT_FILTERS)

            st.success(f"{len(result)} rows remaining")

            st.download_button(
                "Download CSV",
                to_csv(result),
                "resultado.csv"
            )

if __name__ == "__main__":
    main()
