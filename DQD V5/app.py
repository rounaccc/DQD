# ============================================================================
# FILE 1: app.py - Main Streamlit Application
# ============================================================================

import streamlit as st
import pandas as pd
import yaml
import pyodbc
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import io
import os
import sys

# ============================================================================
# Helper function to find default column match
# ============================================================================

def find_default_match(logical_name, available_columns, preselected=None):
    """Find best matching column from available columns.
    If preselected is provided (from config), use that first."""
    options = [''] + available_columns

    # If config pre-selected a value, use it
    if preselected and preselected in available_columns:
        return available_columns.index(preselected) + 1  # +1 for empty option

    # Default mappings to try
    mappings = {
        'policy_number':    ['Policy Number', 'Policy_Number', 'PolicyNumber', 'Policy_Num', 'PolNum'],
        'claim_number':     ['Claim Number', 'Claim_Number', 'ClaimNumber', 'Claim_Num', 'ClmNum'],
        'accident_date':    ['Accident Date', 'Accident_Date', 'AccidentDate', 'Date of Loss', 'Loss Date', 'DOL'],
        'claim_status':     ['Status of Claim', 'Claim Status', 'claim_status', 'Status', 'ClaimStatus'],
        'uw_date':          ['UW Date', 'Policy Inception Date', 'Policy Start Date', 'Inception Date', 'UW_Date'],
        'policy_start_date':['Policy Start Date', 'Policy_Start_Date', 'Inception Date', 'Start Date'],
        'policy_end_date':  ['Policy End Date', 'Policy_End_Date', 'End Date', 'Expiry Date'],
        'total_incurred':   ['Total Incurred', 'Total_Incurred', 'Incurred', 'Incurred Amount'],
        'total_paid':       ['Total Paid', 'Total_Paid', 'Paid', 'Paid Amount'],
        'total_os':         ['Total OS', 'Total Outstanding', 'Total Reserve', 'Total Estimate', 'Outstanding', 'Reserve', 'OS'],
        'policy':           ['Policy Number', 'Policy_Number', 'PolicyNumber']
    }

    possible = mappings.get(logical_name, [])
    for option in possible:
        if option in available_columns:
            return available_columns.index(option) + 1
    return 0


def filter_enabled_checks(_config, _enabled_checks):
    return _config


# Ensure we import modules from this folder first
sys.path.insert(0, os.path.dirname(__file__))

# Page config
st.set_page_config(
    page_title="Data Quality Validation System",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import custom modules
from streamlit_utils import (
    test_db_connection,
    get_tables_list,
    get_table_preview,
    get_bdx_months,
    get_columns_from_table,
    load_data_from_db,
    get_record_count_for_bdx,
    get_previous_bdx_value,
    create_schema_in_db,
    check_existing_validations,
    load_default_config,
    run_validation_pipeline,
    save_results_to_sql,
    create_excel_report,
    create_errors_excel,
    prepare_final_config
)

# Initialize session state
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'config' not in st.session_state:
    st.session_state.config = None
if 'validation_results' not in st.session_state:
    st.session_state.validation_results = None
if 'column_mappings' not in st.session_state:
    st.session_state.column_mappings = {}

# Title
st.title("✅ Data Quality Validation System")
st.markdown("---")

# Sidebar - Progress Tracker
with st.sidebar:
    st.header("📋 Progress")
    steps = [
        "1. Basic Info",
        "2. Input Data",
        "3. Output Setup",
        "4. Configuration",
        "5. Column Mapping",
        "6. Check Settings",
        "7. Review & Run",
        "8. Results"
    ]
    for i, step_name in enumerate(steps, 1):
        if i < st.session_state.step:
            st.success(f"✓ {step_name}")
        elif i == st.session_state.step:
            st.info(f"→ {step_name}")
        else:
            st.text(f"  {step_name}")

# ============================================================================
# STEP 1: BASIC INFORMATION
# ============================================================================

if st.session_state.step == 1:
    st.header("📝 Step 1: Basic Information")

    st.subheader("Optional: Load Configuration File")
    uploaded_config_early = st.file_uploader(
        "Upload Config YAML to auto-fill all steps",
        type=["yaml", "yml"],
        help="Values will be pre-filled in every step. You can review and edit before running.",
        key="uploaded_config_early",
    )

    if uploaded_config_early:
        try:
            loaded = yaml.safe_load(uploaded_config_early)
            if isinstance(loaded, dict) and ("metadata" in loaded) and ("column_mappings" in loaded) and ("checks" in loaded):
                meta = loaded.get("metadata", {})
                # Load everything into session state — widgets below will read from these
                st.session_state.business_line   = meta.get("business_line", "")
                st.session_state.quarter         = meta.get("quarter", "")
                st.session_state.data_source     = meta.get("data_source", "Database")
                st.session_state.server          = meta.get("server", "EMEUKHC4DB15VT")
                st.session_state.username        = meta.get("username", "")
                st.session_state.password        = meta.get("password", "")

                bdx_raw = meta.get("bdx_month")
                prev_bdx_raw = meta.get("prev_bdx_month")
                st.session_state.bdx_month      = pd.to_datetime(bdx_raw, errors="coerce") if bdx_raw else None
                st.session_state.prev_bdx_month = pd.to_datetime(prev_bdx_raw, errors="coerce") if prev_bdx_raw else None
                st.session_state.closed_status_values = meta.get("closed_status_values", ["Closed"])

                if meta.get("data_source", "Database") == "Database":
                    st.session_state.claims_db    = meta.get("claims_db", "")
                    st.session_state.claims_table = meta.get("claims_table", "")

                st.session_state.use_risk_db = bool(meta.get("use_risk_db", False))
                if st.session_state.use_risk_db:
                    st.session_state.risk_db    = meta.get("risk_db", "")
                    st.session_state.risk_table = meta.get("risk_table", "")

                st.session_state.column_mappings = loaded.get("column_mappings", {})
                st.session_state.config = {
                    "checks": loaded.get("checks", []),
                    "scoring_thresholds": loaded.get("scoring_thresholds", {})
                }
                st.session_state.config_from_file = True
                st.success("✓ Config loaded — all steps pre-filled. Walk through to review or edit, then run.")
            else:
                st.error("❌ Config file missing required keys: metadata, column_mappings, checks")
        except Exception as e:
            st.error(f"Error reading config: {str(e)}")

    col1, col2 = st.columns(2)

    with col1:
        business_line = st.text_input(
            "Business Line Name*",
            value=st.session_state.get('business_line', ''),
            placeholder="e.g., Auto, Workers_Comp, General_Liability",
        )

        current_year = datetime.now().year
        current_month = datetime.now().month
        current_quarter = (current_month - 1) // 3 + 1
        default_quarter = f"{current_year}_Q{current_quarter}"

        quarter = st.text_input(
            "Quarter*",
            value=st.session_state.get('quarter', default_quarter),
            placeholder="e.g., 2024_Q3",
        )

    with col2:
        st.subheader("Database Server")
        server = st.text_input(
            "Server Name*",
            value=st.session_state.get('server', 'EMEUKHC4DB15VT'),
        )

        use_password = st.checkbox(
            "Use SQL Authentication (otherwise Windows Auth)",
            value=bool(st.session_state.get('username', ''))
        )

        if use_password:
            username = st.text_input("Username", value=st.session_state.get('username', ''))
            password = st.text_input("Password", type="password", value=st.session_state.get('password', ''))
        else:
            username = ""
            password = ""

    st.markdown("---")

    if st.button("Next →", type="primary", disabled=not (business_line and quarter)):
        st.session_state.business_line = business_line
        st.session_state.quarter = quarter
        st.session_state.server = server
        st.session_state.username = username
        st.session_state.password = password
        st.session_state.step = 2
        st.rerun()

# ============================================================================
# STEP 2: INPUT DATA CONFIGURATION
# ============================================================================

elif st.session_state.step == 2:
    st.header("📥 Step 2: Input Data Configuration")

    data_source = st.radio(
        "Select Data Source",
        ["Database", "Excel File"],
        index=0 if st.session_state.get('data_source', 'Database') == 'Database' else 1,
        horizontal=True
    )
    st.session_state.data_source = data_source

    if data_source == "Database":
        st.subheader("Claims Database Connection")

        col1, col2 = st.columns([3, 1])
        with col1:
            claims_db = st.text_input(
                "Database Name*",
                value=st.session_state.get('claims_db', ''),
                placeholder="e.g., ClaimsDB",
                key="claims_db_input"
            )
        with col2:
            if st.button("Test Connection", key="test_claims"):
                if claims_db:
                    with st.spinner("Testing..."):
                        success, message = test_db_connection(st.session_state.server, claims_db, st.session_state.username, st.session_state.password)
                        st.success(message) if success else st.error(message)
                else:
                    st.warning("Please enter database name")

        if claims_db:
            tables = get_tables_list(st.session_state.server, claims_db, st.session_state.username, st.session_state.password)

            if tables:
                # Pre-select from config if available
                saved_table = st.session_state.get('claims_table', '')
                table_index = tables.index(saved_table) if saved_table in tables else 0

                claims_table = st.selectbox(
                    "Select Claims Table*",
                    options=tables,
                    index=table_index,
                    key="claims_table_select"
                )

                if st.checkbox("Preview data"):
                    preview_df = get_table_preview(st.session_state.server, claims_db, claims_table, st.session_state.username, st.session_state.password)
                    st.dataframe(preview_df.head(5))
                    st.info(f"Total columns: {len(preview_df.columns)} | Sample rows: 5")

                st.markdown("---")
                st.subheader("📅 BDX Month Selection")

                bdx_months = get_bdx_months(st.session_state.server, claims_db, claims_table, st.session_state.username, st.session_state.password)

                if bdx_months:
                    # Pre-select bdx_month from config if available
                    saved_bdx = st.session_state.get('bdx_month')
                    bdx_index = 0
                    if saved_bdx is not None:
                        saved_bdx_str = str(saved_bdx)
                        for i, m in enumerate(bdx_months):
                            if str(m) == saved_bdx_str:
                                bdx_index = i
                                break

                    bdx_month = st.selectbox("Select bdx_month*", options=bdx_months, index=bdx_index, key="bdx_month_select")
                    prev_bdx = get_previous_bdx_value(bdx_months, bdx_month)

                    if prev_bdx is None:
                        st.warning("No previous bdx_month available (month-over-month checks will be skipped).")
                    else:
                        st.info(f"Previous available bdx_month: **{prev_bdx}**")

                    record_count = get_record_count_for_bdx(st.session_state.server, claims_db, claims_table, bdx_month, st.session_state.username, st.session_state.password)
                    st.info(f"📊 Records in selected bdx_month: **{record_count:,}**")
                    if record_count == 0:
                        st.warning("No records found for selected bdx_month")
            else:
                st.warning("No tables found in database")
                claims_table = ""
                bdx_month = None
                prev_bdx = None
        else:
            claims_table = ""
            bdx_month = None
            prev_bdx = None

    else:  # Excel File
        st.subheader("Upload Excel File")
        uploaded_file = st.file_uploader("Choose Excel file", type=['xlsx', 'xls'])

        if uploaded_file:
            df = pd.read_excel(uploaded_file)
            st.success(f"✓ File uploaded: {uploaded_file.name}")
            st.info(f"Rows: {len(df):,} | Columns: {len(df.columns)}")

            if st.checkbox("Preview data", key="preview_excel"):
                st.dataframe(df.head(5))

            bdx_col_options = [col for col in df.columns if 'bdx' in col.lower() or 'date' in col.lower()]
            if bdx_col_options:
                bdx_column = st.selectbox("Select bdx_month column", options=bdx_col_options)
                bdx_series = pd.to_datetime(df[bdx_column], errors="coerce")
                bdx_values = sorted([v for v in bdx_series.dropna().unique()])

                saved_bdx = st.session_state.get('bdx_month')
                bdx_index = len(bdx_values) - 1
                if saved_bdx is not None:
                    for i, v in enumerate(bdx_values):
                        if str(v) == str(saved_bdx):
                            bdx_index = i
                            break

                bdx_month = st.selectbox("Select bdx_month", options=bdx_values, index=bdx_index)
                prev_bdx = get_previous_bdx_value(bdx_values, bdx_month)
                if prev_bdx is None:
                    st.warning("No previous bdx_month available (month-over-month checks will be skipped).")
                else:
                    st.info(f"Previous available bdx_month: **{prev_bdx}**")
                filtered_df = df[bdx_series == bdx_month]
                st.info(f"📊 Records in selected bdx_month: **{len(filtered_df):,}**")
                st.session_state.excel_data = filtered_df
                st.session_state.excel_prev_data = df[bdx_series == prev_bdx] if prev_bdx is not None else None
                st.session_state.excel_bdx_column = bdx_column
                st.session_state.bdx_month = bdx_month
                st.session_state.prev_bdx_month = prev_bdx
            else:
                st.warning("Could not find bdx_month column. All data will be used.")
                st.session_state.excel_data = df

    # Optional: Risk Database
    st.markdown("---")
    st.subheader("🔧 Optional: Risk Database (for Check 11)")

    use_risk_db = st.checkbox(
        "Use Risk Database for additional validation",
        value=st.session_state.get('use_risk_db', False)
    )

    if use_risk_db:
        col1, col2 = st.columns([3, 1])
        with col1:
            risk_db = st.text_input(
                "Risk Database Name",
                value=st.session_state.get('risk_db', ''),
                placeholder="e.g., RiskDB",
                key="risk_db_input"
            )
        with col2:
            if st.button("Test Connection", key="test_risk"):
                if risk_db:
                    with st.spinner("Testing..."):
                        success, message = test_db_connection(st.session_state.server, risk_db, st.session_state.username, st.session_state.password)
                        st.success(message) if success else st.error(message)

        if risk_db:
            risk_tables = get_tables_list(st.session_state.server, risk_db, st.session_state.username, st.session_state.password)
            if risk_tables:
                saved_risk_table = st.session_state.get('risk_table', '')
                risk_table_index = risk_tables.index(saved_risk_table) if saved_risk_table in risk_tables else 0
                risk_table = st.selectbox("Select Risk Table*", options=risk_tables, index=risk_table_index, key="risk_table_select")
            else:
                st.warning("No tables found in risk database")
                risk_table = ""
        else:
            risk_table = ""
    else:
        risk_db = ""
        risk_table = ""

    st.markdown("---")
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("← Back"):
            st.session_state.step = 1
            st.rerun()
    with col2:
        can_proceed = (data_source == "Excel File") or bool(claims_db and claims_table)
        if st.button("Next →", type="primary", disabled=not can_proceed):
            st.session_state.data_source = data_source
            if data_source == "Database":
                st.session_state.claims_db_selected = claims_db
                st.session_state.claims_table_selected = claims_table
                st.session_state.claims_db = claims_db
                st.session_state.claims_table = claims_table
                st.session_state.bdx_month = bdx_month
                st.session_state.prev_bdx_month = prev_bdx
            st.session_state.use_risk_db = use_risk_db
            if use_risk_db:
                st.session_state.risk_db_selected = risk_db
                st.session_state.risk_table_selected = risk_table
                st.session_state.risk_db = risk_db
                st.session_state.risk_table = risk_table
            st.session_state.step = 3
            st.rerun()

# ============================================================================
# STEP 3: OUTPUT SETUP
# ============================================================================

elif st.session_state.step == 3:
    st.header("💾 Step 3: Output Setup")

    col1, col2 = st.columns([3, 1])
    with col1:
        output_db = st.text_input(
            "Output Database Name*",
            value=st.session_state.get('output_db', ''),
            placeholder="e.g., DataQualityDB",
        )
    with col2:
        if st.button("Test Connection", key="test_output"):
            if output_db:
                with st.spinner("Testing..."):
                    success, message = test_db_connection(st.session_state.server, output_db, st.session_state.username, st.session_state.password)
                    st.success(message) if success else st.error(message)

    if output_db:
        st.markdown("---")
        st.subheader("Schema Creation")
        schema_name = st.session_state.business_line.replace(" ", "_")
        st.info(f"Schema will be created as: **{schema_name}**")

        if st.button("Create Schema Now", type="secondary"):
            with st.spinner("Creating schema..."):
                success, message = create_schema_in_db(st.session_state.server, output_db, schema_name, st.session_state.username, st.session_state.password)
                if success:
                    st.success(message)
                    st.session_state.schema_created = True
                else:
                    st.warning(message)

        st.markdown("---")
        st.subheader("📋 Existing Validations")
        existing = check_existing_validations(st.session_state.server, output_db, st.session_state.business_line, st.session_state.quarter, st.session_state.username, st.session_state.password)
        if existing:
            st.info(f"Found existing validation for **{st.session_state.business_line}** - **{st.session_state.quarter}**")
        else:
            st.info("No existing validation found for this business line and quarter")

    st.markdown("---")
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("← Back"):
            st.session_state.step = 2
            st.rerun()
    with col2:
        if st.button("Next →", type="primary", disabled=not output_db):
            st.session_state.output_db = output_db
            st.session_state.schema_name = schema_name
            st.session_state.step = 4
            st.rerun()

# ============================================================================
# STEP 4: CONFIGURATION SELECTION
# ============================================================================

elif st.session_state.step == 4:
    st.header("⚙️ Step 4: Configuration")

    if st.session_state.get('config_from_file') and st.session_state.config:
        st.success("✓ Configuration loaded from uploaded config file. You can adjust check settings in the next step.")
    else:
        st.info("✓ Using the default check set (you can enable/disable and set points in the next step).")
        st.session_state.config = load_default_config()

    st.markdown("---")
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("← Back"):
            st.session_state.step = 3
            st.rerun()
    with col2:
        if st.button("Next →", type="primary", disabled=st.session_state.config is None):
            st.session_state.step = 5
            st.rerun()

# ============================================================================
# STEP 5: COLUMN MAPPING
# ============================================================================

elif st.session_state.step == 5:
    st.header("🔗 Step 5: Column Mapping")
    st.info("Map logical column names to actual columns in your data. Values pre-filled from config where available.")

    if st.session_state.data_source == "Database":
        available_columns = get_columns_from_table(
            st.session_state.server,
            st.session_state.claims_db_selected,
            st.session_state.claims_table_selected,
            st.session_state.username,
            st.session_state.password
        )
    else:
        available_columns = list(st.session_state.excel_data.columns)

    st.success(f"Found {len(available_columns)} columns in your data")

    required_columns = {
        'policy_number':    'Policy Number',
        'claim_number':     'Claim Number',
        'accident_date':    'Accident Date / Date of Loss',
        'claim_status':     'Status of Claim',
        'uw_date':          'UW Date / Policy Inception Date / Policy Start Date',
        'policy_start_date':'Policy Start Date',
        'policy_end_date':  'Policy End Date',
        'total_incurred':   'Total Incurred',
        'total_paid':       'Total Paid',
        'total_os':         'Total Reserve / Total Outstanding (OS)',
    }

    # Risk column mapping if needed
    if st.session_state.get('use_risk_db'):
        risk_columns = get_columns_from_table(
            st.session_state.server,
            st.session_state.risk_db_selected,
            st.session_state.risk_table_selected,
            st.session_state.username,
            st.session_state.password
        )
        st.markdown("---")
        st.subheader("Risk Data Columns")
        saved_risk_policy = st.session_state.column_mappings.get('risk_policy_number', '')
        st.session_state.column_mappings['risk_policy_number'] = st.selectbox(
            "Policy Number (Risk Table)",
            options=[''] + risk_columns,
            index=find_default_match('policy', risk_columns, preselected=saved_risk_policy),
        )

    st.markdown("---")
    st.subheader("Claims Data Columns")
    st.info("ℹ️ All mappings are optional. Checks that require an unmapped column will be automatically disabled.")

    col1, col2 = st.columns(2)
    current_mappings = {}

    for i, (logical_name, display_name) in enumerate(required_columns.items()):
        col = col1 if i % 2 == 0 else col2
        with col:
            # Use config pre-selected value if available
            saved_val = st.session_state.column_mappings.get(logical_name, '')
            default_index = find_default_match(logical_name, available_columns, preselected=saved_val)

            selected = st.selectbox(
                display_name,
                options=[''] + available_columns,
                index=default_index,
                key=f"map_{logical_name}",
                help=f"Map to column containing {display_name}. Leave blank to auto-disable dependent checks."
            )
            if selected:
                current_mappings[logical_name] = selected

    # Closed status values
    st.markdown("---")
    st.subheader("🔧 Claim Status Settings")
    saved_closed = st.session_state.get('closed_status_values', ['Closed'])
    closed_status_input = st.text_input(
        "Closed Status Values (comma-separated)",
        value=", ".join(saved_closed) if isinstance(saved_closed, list) else str(saved_closed),
        help="Values that indicate a claim is closed, e.g. Closed, CLOSED, C"
    )
    closed_status_values = [v.strip() for v in closed_status_input.split(",") if v.strip()]

    st.markdown("---")
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("← Back"):
            st.session_state.step = 4
            st.rerun()
    with col2:
        if st.button("Next →", type="primary"):
            st.session_state.column_mappings = current_mappings
            st.session_state.closed_status_values = closed_status_values
            st.session_state.step = 6
            st.rerun()

# ============================================================================
# STEP 6: CHECK SETTINGS
# ============================================================================

elif st.session_state.step == 6:
    st.header("⚙️ Step 6: Check Settings")
    st.info("Enable/disable checks and adjust points. Values pre-filled from config where available.")

    config = st.session_state.config
    checks = config.get('checks', [])

    # Group by category
    categories = {}
    for check in checks:
        cat = check.get('category', 'Other')
        categories.setdefault(cat, []).append(check)

    updated_checks = []
    total_points = 0

    for category, cat_checks in categories.items():
        st.subheader(f"📂 {category}")
        cols = st.columns([3, 1, 1])
        cols[0].markdown("**Check**")
        cols[1].markdown("**Enabled**")
        cols[2].markdown("**Points**")

        for check in cat_checks:
            cols = st.columns([3, 1, 1])
            with cols[0]:
                st.write(f"{check['id']} — {check['name']}")
            with cols[1]:
                enabled = st.checkbox(
                    "Enabled",
                    value=check.get('enabled', True),
                    key=f"chk_enabled_{check['id']}",
                    label_visibility="collapsed"
                )
            with cols[2]:
                points = st.number_input(
                    "Points",
                    value=float(check.get('points', 5)),
                    min_value=0.0,
                    max_value=100.0,
                    step=1.0,
                    key=f"chk_points_{check['id']}",
                    label_visibility="collapsed"
                )
            updated_check = {**check, 'enabled': enabled, 'points': points}
            updated_checks.append(updated_check)
            if enabled:
                total_points += points

    st.markdown("---")
    if abs(total_points - 100) > 0.01:
        st.warning(f"⚠️ Total enabled points: **{total_points:.0f}** (should equal 100 for a clean percentage score)")
    else:
        st.success(f"✓ Total enabled points: **{total_points:.0f}**")

    st.markdown("---")
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("← Back"):
            st.session_state.step = 5
            st.rerun()
    with col2:
        if st.button("Next →", type="primary"):
            st.session_state.config['checks'] = updated_checks
            st.session_state.step = 7
            st.rerun()

# ============================================================================
# STEP 7: REVIEW & RUN
# ============================================================================

elif st.session_state.step == 7:
    st.header("🚀 Step 7: Review & Run")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📋 Configuration Summary")
        st.write(f"**Business Line:** {st.session_state.business_line}")
        st.write(f"**Quarter:** {st.session_state.quarter}")
        st.write(f"**Data Source:** {st.session_state.data_source}")
        if st.session_state.data_source == "Database":
            st.write(f"**Claims DB:** {st.session_state.get('claims_db_selected', '')}")
            st.write(f"**Claims Table:** {st.session_state.get('claims_table_selected', '')}")
        st.write(f"**BDX Month:** {st.session_state.get('bdx_month', 'N/A')}")
        st.write(f"**Prev BDX Month:** {st.session_state.get('prev_bdx_month', 'N/A')}")

    with col2:
        st.subheader("🔗 Column Mappings")
        if st.session_state.column_mappings:
            for logical, physical in st.session_state.column_mappings.items():
                st.write(f"**{logical}** → {physical}")
        else:
            st.warning("No column mappings set")

    st.markdown("---")
    st.subheader("✅ Enabled Checks")
    enabled = [c for c in st.session_state.config.get('checks', []) if c.get('enabled', True)]
    disabled = [c for c in st.session_state.config.get('checks', []) if not c.get('enabled', True)]
    st.write(f"**{len(enabled)} enabled** / {len(disabled)} disabled")
    for c in enabled:
        st.write(f"  • {c['id']} {c['name']} ({c.get('points', 0)} pts)")

    # Download config
    st.markdown("---")
    final_config = prepare_final_config(st.session_state.config, st.session_state.column_mappings, st.session_state)
    config_yaml = yaml.dump(final_config, default_flow_style=False, allow_unicode=True)
    st.download_button(
        label="💾 Download Config YAML",
        data=config_yaml,
        file_name=f"config_{st.session_state.business_line}_{st.session_state.quarter}.yaml",
        mime="text/yaml"
    )

    st.markdown("---")
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("← Back"):
            st.session_state.step = 6
            st.rerun()
    with col2:
        if st.button("▶️ Run Validation", type="primary"):
            with st.spinner("Loading data and running validation..."):
                try:
                    # Load data
                    if st.session_state.data_source == "Database":
                        current_data = load_data_from_db(
                            st.session_state.server,
                            st.session_state.claims_db_selected,
                            st.session_state.claims_table_selected,
                            st.session_state.bdx_month,
                            st.session_state.username,
                            st.session_state.password
                        )
                        prev_data = None
                        if st.session_state.get('prev_bdx_month'):
                            try:
                                prev_data = load_data_from_db(
                                    st.session_state.server,
                                    st.session_state.claims_db_selected,
                                    st.session_state.claims_table_selected,
                                    st.session_state.prev_bdx_month,
                                    st.session_state.username,
                                    st.session_state.password
                                )
                            except:
                                pass
                    else:
                        current_data = st.session_state.excel_data
                        prev_data = st.session_state.get('excel_prev_data')

                    # Load risk data if needed
                    risk_data = None
                    if st.session_state.get('use_risk_db') and st.session_state.get('risk_db_selected'):
                        try:
                            risk_data = load_data_from_db(
                                st.session_state.server,
                                st.session_state.risk_db_selected,
                                st.session_state.risk_table_selected,
                                None,
                                st.session_state.username,
                                st.session_state.password
                            )
                        except:
                            st.warning("Could not load risk data")

                    # Store raw data for errors excel
                    st.session_state.raw_data_for_errors = current_data.copy()

                    # Build config with settings
                    run_config = dict(st.session_state.config)
                    run_config['_prev_data'] = prev_data
                    run_config['_settings'] = {
                        'closed_status_values': st.session_state.get('closed_status_values', ['Closed'])
                    }

                    quarter_id = f"{st.session_state.business_line}_{st.session_state.quarter}"

                    results = run_validation_pipeline(
                        current_data,
                        run_config,
                        st.session_state.column_mappings,
                        quarter_id,
                        risk_data=risk_data
                    )

                    st.session_state.validation_results = results
                    st.session_state.step = 8
                    st.rerun()

                except Exception as e:
                    st.error(f"❌ Validation failed: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

# ============================================================================
# STEP 8: RESULTS
# ============================================================================

elif st.session_state.step == 8:
    st.header("📊 Step 8: Results")

    results = st.session_state.validation_results
    if results is None:
        st.error("No results found. Please run validation first.")
        st.stop()

    # Overall Score
    st.subheader("🎯 Overall Score")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Overall Score", f"{results['overall_score']:.1f}%")
    with col2:
        st.metric("Status", results['status'])
    with col3:
        st.metric("Total Records", f"{results['total_records']:,}")
    with col4:
        st.metric("Quarter", results['quarter_id'])

    # Gauge chart
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=results['overall_score'],
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Data Quality Score"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 65], 'color': "lightcoral"},
                {'range': [65, 75], 'color': "lightyellow"},
                {'range': [75, 85], 'color': "lightblue"},
                {'range': [85, 100], 'color': "lightgreen"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 85
            }
        }
    ))
    st.plotly_chart(fig_gauge, use_container_width=True)

    # Category Scores
    st.markdown("---")
    st.subheader("📊 Category Scores")

    category_data = []
    for category, scores in results['category_scores'].items():
        category_data.append({
            'Category': category.replace('_', ' ').title(),
            'Score': scores['percentage'],
            'Points': f"{scores['points_earned']}/{scores['points_possible']}"
        })

    category_df = pd.DataFrame(category_data)
    fig_categories = px.bar(
        category_df, x='Category', y='Score', text='Points',
        title="Score by Category", color='Score',
        color_continuous_scale=['red', 'yellow', 'green']
    )
    fig_categories.update_traces(textposition='outside')
    fig_categories.update_layout(showlegend=False)
    st.plotly_chart(fig_categories, use_container_width=True)

    # Detailed Results Table
    st.markdown("---")
    st.subheader("📋 Detailed Check Results")

    detailed_results = []
    for category, subcategories in results['detailed_results'].items():
        for subcategory, checks in subcategories.items():
            for check in checks:
                status_icon = '✅' if check['points_earned'] == check['points_possible'] else \
                             '⚠️' if check['points_earned'] > 0 else '❌'
                detailed_results.append({
                    'Status': status_icon,
                    'Check ID': check['check_id'],
                    'Check Name': check['check_name'],
                    'Category': category.replace('_', ' ').title(),
                    'Pass Rate': f"{check.get('pass_rate', 0):.1f}%",
                    'Failed': check.get('failed', 0),
                    'Points': f"{check['points_earned']}/{check['points_possible']}"
                })

    results_df = pd.DataFrame(detailed_results)

    col1, col2 = st.columns(2)
    with col1:
        filter_status = st.multiselect(
            "Filter by Status",
            options=['✅ Pass', '⚠️ Warning', '❌ Fail'],
            default=['✅ Pass', '⚠️ Warning', '❌ Fail']
        )
    with col2:
        filter_category = st.multiselect(
            "Filter by Category",
            options=results_df['Category'].unique(),
            default=results_df['Category'].unique()
        )

    status_map = {'✅': '✅', '⚠️': '⚠️', '❌': '❌'}
    filtered_df = results_df[
        (results_df['Status'].isin([status_map[s.split()[0]] for s in filter_status])) &
        (results_df['Category'].isin(filter_category))
    ]
    st.dataframe(filtered_df, use_container_width=True, height=400)

    # Issues Summary
    st.markdown("---")
    st.subheader("🚨 Issues Summary")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Passed", results['summary']['passed'])
    with col2:
        st.metric("Warnings", results['summary']['warnings'])
    with col3:
        st.metric("Failed", results['summary']['failed'])

    if results.get("flags", {}).get("risk_match_ok"):
        st.success("Claims data matches with risk data")

    # Export Options
    st.markdown("---")
    st.subheader("💾 Export Results")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        excel_buffer = create_excel_report(results)
        st.download_button(
            label="📥 Download Excel Report",
            data=excel_buffer,
            file_name=f"DQ_Report_{st.session_state.business_line}_{st.session_state.quarter}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with col2:
        raw_data = st.session_state.get('raw_data_for_errors')
        if raw_data is not None:
            errors_buffer = create_errors_excel(results, raw_data, st.session_state.column_mappings)
            st.download_button(
                label="🚨 Download Errors Excel",
                data=errors_buffer,
                file_name=f"DQ_Errors_{st.session_state.business_line}_{st.session_state.quarter}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.button("🚨 Download Errors Excel", disabled=True, help="Raw data not available")

    with col3:
        json_str = json.dumps(results, indent=2, default=str)
        st.download_button(
            label="📥 Download JSON Results",
            data=json_str,
            file_name=f"DQ_Results_{st.session_state.business_line}_{st.session_state.quarter}.json",
            mime="application/json"
        )

    with col4:
        if st.button("💾 Save to SQL Database", type="primary"):
            with st.spinner("Saving to database..."):
                success = save_results_to_sql(
                    results,
                    st.session_state.server,
                    st.session_state.output_db,
                    st.session_state.schema_name,
                    st.session_state.username,
                    st.session_state.password
                )
                if success:
                    st.success("✅ Results saved to database successfully!")
                    st.info(f"""
                    **Tables updated:**
                    - {st.session_state.schema_name}.dq_scores
                    - {st.session_state.schema_name}.dq_check_results
                    - {st.session_state.schema_name}.dq_issues
                    """)
                else:
                    st.error("❌ Error saving to database")

    st.markdown("---")
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("🔄 New Validation"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.step = 1
            st.rerun()
    with col2:
        if st.button("📊 Connect to Power BI"):
            st.info("""
            **Power BI Connection Details:**

            1. Open Power BI Desktop
            2. Get Data → SQL Server
            3. Server: `{}`
            4. Database: `{}`
            5. Import tables:
               - `{}.dq_scores`
               - `{}.dq_check_results`
               - `{}.dq_issues`
            """.format(
                st.session_state.server,
                st.session_state.output_db,
                st.session_state.schema_name,
                st.session_state.schema_name,
                st.session_state.schema_name
            ))