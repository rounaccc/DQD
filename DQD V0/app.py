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

def find_default_match(logical_name, available_columns):
    """Find best matching column from available columns"""
    # Default mappings to try
    mappings = {
        'policy_number': ['Policy Number', 'Policy_Number', 'PolicyNumber', 'Policy_Num', 'PolNum'],
        'claim_number': ['Claim Number', 'Claim_Number', 'ClaimNumber', 'Claim_Num', 'ClmNum'],
        'accident_date': ['Accident Date', 'Accident_Date', 'AccidentDate', 'Date of Loss', 'Loss Date', 'DOL'],
        'claim_status': ['Status of Claim', 'Claim Status', 'claim_status', 'Status', 'ClaimStatus'],
        'uw_date': ['UW Date', 'Policy Inception Date', 'Policy Start Date', 'Inception Date', 'UW_Date'],
        'policy_start_date': ['Policy Start Date', 'Policy_Start_Date', 'Inception Date', 'Start Date'],
        'policy_end_date': ['Policy End Date', 'Policy_End_Date', 'End Date', 'Expiry Date'],
        'total_incurred': ['Total Incurred', 'Total_Incurred', 'Incurred', 'Incurred Amount'],
        'total_paid': ['Total Paid', 'Total_Paid', 'Paid', 'Paid Amount'],
        'total_os': ['Total OS', 'Total Outstanding', 'Total Reserve', 'Total Estimate', 'Outstanding', 'Reserve', 'OS'],
        'policy': ['Policy Number', 'Policy_Number', 'PolicyNumber']
    }
    
    # Get possible matches for this logical name
    possible = mappings.get(logical_name, [])
    
    # Find first match
    for option in possible:
        if option in available_columns:
            return available_columns.index(option) + 1  # +1 for empty option
    
    return 0  # Return empty option


def filter_enabled_checks(_config, _enabled_checks):
    # Legacy function retained for compatibility; no longer used.
    return _config


# Ensure we import modules from this folder (DQD V1/) first
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
    
    st.subheader("Optional: Load Configuration File (recommended)")
    uploaded_config_early = st.file_uploader(
        "Upload Config YAML to auto-fill everything",
        type=["yaml", "yml"],
        help="If you upload a config here, the app will load metadata, mappings, and check settings automatically.",
        key="uploaded_config_early",
    )
    
    if uploaded_config_early:
        try:
            loaded = yaml.safe_load(uploaded_config_early)
            if isinstance(loaded, dict) and ("metadata" in loaded) and ("column_mappings" in loaded) and ("checks" in loaded):
                meta = loaded.get("metadata", {})
                st.session_state.business_line = meta.get("business_line", "")
                st.session_state.quarter = meta.get("quarter", "")
                st.session_state.data_source = meta.get("data_source", "Database")
                st.session_state.bdx_month = pd.to_datetime(meta.get("bdx_month"), errors="coerce")
                st.session_state.prev_bdx_month = pd.to_datetime(meta.get("prev_bdx_month"), errors="coerce")
                st.session_state.closed_status_values = meta.get("closed_status_values", ["Closed"])
                
                # server/auth may still be provided by user; keep defaults if missing
                st.session_state.server = meta.get("server", "EMEUKHC4DB15VT") if isinstance(meta, dict) else "EMEUKHC4DB15VT"
                st.session_state.username = meta.get("username", "")
                st.session_state.password = meta.get("password", "")
                
                if st.session_state.data_source == "Database":
                    st.session_state.claims_db = meta.get("claims_db", "")
                    st.session_state.claims_table = meta.get("claims_table", "")
                
                st.session_state.use_risk_db = bool(meta.get("use_risk_db", False))
                if st.session_state.use_risk_db:
                    st.session_state.risk_db = meta.get("risk_db", "")
                    st.session_state.risk_table = meta.get("risk_table", "")
                
                st.session_state.column_mappings = loaded.get("column_mappings", {})
                st.session_state.config = {"checks": loaded.get("checks", []), "scoring_thresholds": loaded.get("scoring_thresholds", {})}
                
                st.success("✓ Config loaded. You can review and run.")
                if st.button("Jump to Review →", type="primary"):
                    st.session_state.step = 7
                    st.rerun()
            else:
                st.error("❌ Config file missing required keys: metadata, column_mappings, checks")
        except Exception as e:
            st.error(f"Error reading config: {str(e)}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        business_line = st.text_input(
            "Business Line Name*",
            placeholder="e.g., Auto, Workers_Comp, General_Liability",
            help="Enter the business line for this validation"
        )
        
        # Auto-detect current quarter
        current_year = datetime.now().year
        current_month = datetime.now().month
        current_quarter = (current_month - 1) // 3 + 1
        default_quarter = f"{current_year}_Q{current_quarter}"
        
        quarter = st.text_input(
            "Quarter*",
            value=default_quarter,
            placeholder="e.g., 2024_Q3",
            help="Format: YYYY_QX"
        )
    
    with col2:
        st.subheader("Database Server")
        server = st.text_input(
            "Server Name*",
            value="EMEUKHC4DB15VT",
            help="Default server for connections"
        )
        
        use_password = st.checkbox("Use SQL Authentication (otherwise Windows Auth)")
        
        if use_password:
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
        else:
            username = ""
            password = ""
    
    st.markdown("---")
    
    # Validation
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
    
    # Data source selection
    data_source = st.radio(
        "Select Data Source",
        ["Database", "Excel File"],
        horizontal=True
    )
    
    if data_source == "Database":
        st.subheader("Claims Database Connection")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            claims_db = st.text_input(
                "Database Name*",
                placeholder="e.g., ClaimsDB",
                key="claims_db"
            )
        
        with col2:
            if st.button("Test Connection", key="test_claims"):
                if claims_db:
                    with st.spinner("Testing connection..."):
                        success, message = test_db_connection(
                            st.session_state.server,
                            claims_db,
                            st.session_state.username,
                            st.session_state.password
                        )
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
                else:
                    st.warning("Please enter database name")
        
        # Table selection
        if claims_db:
            tables = get_tables_list(
                st.session_state.server,
                claims_db,
                st.session_state.username,
                st.session_state.password
            )
            
            if tables:
                claims_table = st.selectbox(
                    "Select Claims Table*",
                    options=tables,
                    key="claims_table"
                )
                
                # Preview data
                if st.checkbox("Preview data"):
                    preview_df = get_table_preview(
                        st.session_state.server,
                        claims_db,
                        claims_table,
                        st.session_state.username,
                        st.session_state.password
                    )
                    st.dataframe(preview_df.head(5))
                    st.info(f"Total columns: {len(preview_df.columns)} | Sample rows: 5")
                
                # bdx_month selection (single month)
                st.markdown("---")
                st.subheader("📅 BDX Month Selection")
                
                bdx_months = get_bdx_months(
                    st.session_state.server,
                    claims_db,
                    claims_table,
                    st.session_state.username,
                    st.session_state.password
                )
                
                if bdx_months:
                    bdx_month = st.selectbox(
                        "Select bdx_month*",
                        options=bdx_months,
                        index=0,  # list is DESC from helper
                        key="bdx_month",
                    )
                    prev_bdx = get_previous_bdx_value(bdx_months, bdx_month)
                    if prev_bdx is None:
                        st.warning("No previous bdx_month available (month-over-month checks will be skipped).")
                    else:
                        st.info(f"Previous available bdx_month: **{prev_bdx}**")

                    record_count = get_record_count_for_bdx(
                        st.session_state.server,
                        claims_db,
                        claims_table,
                        bdx_month,
                        st.session_state.username,
                        st.session_state.password,
                    )
                    st.info(f"📊 Records in selected bdx_month: **{record_count:,}**")
                    if record_count == 0:
                        st.warning("No records found for selected bdx_month")
            else:
                st.warning("No tables found in database")
    
    else:  # Excel File
        st.subheader("Upload Excel File")
        
        uploaded_file = st.file_uploader(
            "Choose Excel file",
            type=['xlsx', 'xls'],
            help="Upload your claims data file"
        )
        
        if uploaded_file:
            # Load Excel
            df = pd.read_excel(uploaded_file)
            
            st.success(f"✓ File uploaded: {uploaded_file.name}")
            st.info(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
            
            # Preview
            if st.checkbox("Preview data", key="preview_excel"):
                st.dataframe(df.head(5))
            
            # Check for bdx_month column
            bdx_col_options = [col for col in df.columns if 'bdx' in col.lower() or 'date' in col.lower()]
            
            if bdx_col_options:
                bdx_column = st.selectbox(
                    "Select bdx_month column",
                    options=bdx_col_options
                )
                
                bdx_series = pd.to_datetime(df[bdx_column], errors="coerce")
                bdx_values = sorted([v for v in bdx_series.dropna().unique()])
                bdx_month = st.selectbox("Select bdx_month", options=bdx_values, index=len(bdx_values)-1)
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
    
    use_risk_db = st.checkbox("Use Risk Database for additional validation")
    
    if use_risk_db:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            risk_db = st.text_input(
                "Risk Database Name",
                placeholder="e.g., RiskDB",
                key="risk_db"
            )
        
        with col2:
            if st.button("Test Connection", key="test_risk"):
                if risk_db:
                    with st.spinner("Testing connection..."):
                        success, message = test_db_connection(
                            st.session_state.server,
                            risk_db,
                            st.session_state.username,
                            st.session_state.password
                        )
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
        
        if risk_db:
            risk_tables = get_tables_list(
                st.session_state.server,
                risk_db,
                st.session_state.username,
                st.session_state.password
            )
            
            if risk_tables:
                risk_table = st.selectbox(
                    "Select Risk Table",
                    options=risk_tables,
                    key="risk_table"
                )
                
                if st.checkbox("Preview risk data"):
                    risk_preview = get_table_preview(
                        st.session_state.server,
                        risk_db,
                        risk_table,
                        st.session_state.username,
                        st.session_state.password
                    )
                    st.dataframe(risk_preview.head(5))
    
    st.markdown("---")
    
    # Navigation
    col1, col2 = st.columns([1, 5])
    
    with col1:
        if st.button("← Back"):
            st.session_state.step = 1
            st.rerun()
    
    with col2:
        # Validation for next button
        can_proceed = False
        
        if data_source == "Database":
            can_proceed = (
                claims_db and 
                claims_table and 
                bdx_month is not None
            )
            
            # If risk DB selected, validate it too
            if use_risk_db:
                can_proceed = can_proceed and risk_db and risk_table
        else:
            can_proceed = uploaded_file is not None
        
        if st.button("Next →", type="primary", disabled=not can_proceed):
            # Store all data in session state
            st.session_state.data_source = data_source
            
            if data_source == "Database":
                st.session_state.claims_db_selected = claims_db
                st.session_state.claims_table_selected = claims_table
                st.session_state.bdx_month_selected = bdx_month
                st.session_state.prev_bdx_month = prev_bdx
            
            st.session_state.use_risk_db = use_risk_db
            if use_risk_db:
                st.session_state.risk_db_selected = risk_db
                st.session_state.risk_table_selected = risk_table
            
            st.session_state.step = 3
            st.rerun()

# ============================================================================
# STEP 3: OUTPUT DATABASE SETUP
# ============================================================================

elif st.session_state.step == 3:
    st.header("💾 Step 3: Output Database Setup")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        output_db = st.text_input(
            "Output Database Name*",
            placeholder="e.g., DataQualityDB",
            help="Database where validation results will be stored"
        )
    
    with col2:
        if st.button("Test Connection", key="test_output"):
            if output_db:
                with st.spinner("Testing connection..."):
                    success, message = test_db_connection(
                        st.session_state.server,
                        output_db,
                        st.session_state.username,
                        st.session_state.password
                    )
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
    
    if output_db:
        st.markdown("---")
        st.subheader("Schema Creation")
        
        schema_name = st.session_state.business_line.replace(" ", "_")
        st.info(f"Schema will be created as: **{schema_name}**")
        
        if st.button("Create Schema Now", type="secondary"):
            with st.spinner("Creating schema..."):
                success, message = create_schema_in_db(
                    st.session_state.server,
                    output_db,
                    schema_name,
                    st.session_state.username,
                    st.session_state.password
                )
                
                if success:
                    st.success(message)
                    st.session_state.schema_created = True
                else:
                    st.warning(message)
                    st.session_state.schema_created = False
                    
                    # Ask user if they want to proceed anyway
                    if st.button("⚠️ Proceed Anyway"):
                        st.session_state.schema_created = True
                        st.rerun()
    
    st.markdown("---")
    
    # Check for existing validations
    if output_db:
        st.subheader("📋 Existing Validations")
        
        existing = check_existing_validations(
            st.session_state.server,
            output_db,
            st.session_state.business_line,
            st.session_state.quarter,
            st.session_state.username,
            st.session_state.password
        )
        
        if existing:
            st.info(f"Found existing validation for **{st.session_state.business_line}** - **{st.session_state.quarter}**")
            
            if st.button("📂 Load Existing Validation"):
                st.session_state.load_existing = True
                # Load configuration and results
                st.success("Existing validation loaded!")
        else:
            st.info("No existing validation found for this business line and quarter")
    
    st.markdown("---")
    
    # Navigation
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
    
    st.info("✓ Using the configured check set for this build (you can enable/disable and set points in the next step).")
    st.session_state.config = load_default_config()
    
    st.markdown("---")
    
    # Navigation
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
# CONTINUATION OF app.py - Steps 5-8
# ============================================================================

# ============================================================================
# STEP 5: COLUMN MAPPING
# ============================================================================

elif st.session_state.step == 5:
    st.header("🔗 Step 5: Column Mapping")
    
    st.info("Map logical column names to actual columns in your data")
    
    # Get available columns from data
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
    
    # Define required columns
    required_columns = {
        'policy_number': 'Policy Number',
        'claim_number': 'Claim Number',
        'accident_date': 'Accident Date / Date of Loss',
        'claim_status': 'Status of Claim',
        'uw_date': 'UW Date / Policy Inception Date / Policy Start Date',
        'policy_start_date': 'Policy Start Date',
        'policy_end_date': 'Policy End Date',
        'total_incurred': 'Total Incurred',
        'total_paid': 'Total Paid',
        'total_os': 'Total Reserve / Total Outstanding (OS)',
    }
    
    # Add risk column if needed
    if st.session_state.use_risk_db:
        # Get risk table columns
        risk_columns = get_columns_from_table(
            st.session_state.server,
            st.session_state.risk_db_selected,
            st.session_state.risk_table_selected,
            st.session_state.username,
            st.session_state.password
        )
        
        st.markdown("---")
        st.subheader("Risk Data Columns")
        
        col1, col2 = st.columns(2)
        
        st.session_state.column_mappings['risk_policy_number'] = st.selectbox(
            "Policy Number (Risk Table)",
            options=[''] + risk_columns,
            index=find_default_match('policy', risk_columns),
            help="Policy number column in risk table",
        )
    
    st.markdown("---")
    st.subheader("Claims Data Columns")
    st.info("ℹ️ All mappings are optional. Checks that require an unmapped column will be automatically disabled.")
    
    # Create column mapping UI
    col1, col2 = st.columns(2)
    
    current_mappings = {}

    for i, (logical_name, display_name) in enumerate(required_columns.items()):
        col = col1 if i % 2 == 0 else col2
        
        with col:
            # Try to find default match
            default_index = find_default_match(logical_name, available_columns)
            
            selected = st.selectbox(
                display_name,  # no asterisk — not required
                options=[''] + available_columns,
                index=default_index,
                key=f"map_{logical_name}",
                help=f"Map to column containing {display_name}. Leave blank to auto-disable dependent checks."
            )
            
            if selected:
                current_mappings[logical_name] = selected
            else:
                st.caption(f"⚠️ Not mapped — dependent checks will be disabled")

    # policy_end_date fallback notice
    if 'policy_end_date' not in current_mappings and 'policy_start_date' in current_mappings:
        st.info("📅 **Policy End Date** not mapped — will be inferred as Policy Start Date + 1 year for check 3.2.")

    # Determine which checks will be auto-disabled based on missing mappings
    _field_to_checks = {
        'policy_number': ['1.1'],
        'claim_number':  ['1.2', '4.1', '6.1', '7.1'],
        'accident_date': ['1.3', '5.1', '3.2'],
        'claim_status':  ['1.4', '3.1'],
        'uw_date':       ['1.5', '5.2'],
        'policy_start_date': ['3.2'],
        'policy_end_date':   [],          # handled via fallback, not auto-disabled
        'total_incurred':    ['2.1', '3.3'],
        'total_paid':        ['2.2', '3.3', '7.1'],
        'total_os':          ['2.3', '3.1', '3.3'],
    }
    auto_disabled_check_ids = set()
    for logical_name in required_columns:
        if logical_name not in current_mappings:
            for cid in _field_to_checks.get(logical_name, []):
                auto_disabled_check_ids.add(cid)

    if auto_disabled_check_ids:
        st.warning(f"⚠️ The following checks will be **automatically disabled** due to unmapped columns: `{'`, `'.join(sorted(auto_disabled_check_ids))}`")

    # Show mapping summary
    with st.expander("📋 View Mapping Summary"):
        rows = []
        for logical_name, display_name in required_columns.items():
            mapped_to = current_mappings.get(logical_name, "— not mapped —")
            rows.append({"Logical Name": logical_name, "Display Name": display_name, "Mapped To": mapped_to})
        mapping_df = pd.DataFrame(rows)
        st.dataframe(mapping_df, use_container_width=True)
    
    st.markdown("---")
    
    # Navigation — always enabled (at least one mapped column required)
    col1, col2 = st.columns([1, 5])
    
    with col1:
        if st.button("← Back"):
            st.session_state.step = 4
            st.rerun()
    
    with col2:
        can_proceed_mapping = len(current_mappings) > 0
        if not can_proceed_mapping:
            st.warning("Map at least one column to proceed.")
        if st.button("Next →", type="primary", disabled=not can_proceed_mapping):
            st.session_state.column_mappings = current_mappings
            st.session_state.auto_disabled_check_ids = auto_disabled_check_ids
            st.session_state.step = 6
            st.rerun()

# ============================================================================
# STEP 6: CHECK SETTINGS
# ============================================================================

elif st.session_state.step == 6:
    st.header("⚙️ Step 6: Validation Check Settings")
    st.info("Enable/disable checks and adjust points. Enabled check points must sum to 100.")
    config = st.session_state.config
    checks = config.get("checks", [])

    if "closed_status_values" not in st.session_state:
        st.session_state.closed_status_values = ["Closed"]

    st.subheader("Closed status values")
    closed_csv = st.text_input("Closed status values (comma-separated)", value="Closed")
    st.session_state.closed_status_values = [v.strip() for v in closed_csv.split(",") if v.strip()]

    st.markdown("---")
    st.subheader("Checks & Points (must total 100)")

    auto_disabled_ids = getattr(st.session_state, "auto_disabled_check_ids", set())

    total_enabled_points = 0
    enabled_count = 0

    for c in checks:
        cid = c.get("id")
        is_auto_disabled = str(cid) in auto_disabled_ids

        # Force-disable checks whose required columns aren't mapped
        if is_auto_disabled:
            c["enabled"] = False

        label = f"{'✅' if c.get('enabled', True) and not is_auto_disabled else '❌'} {cid}: {c.get('name')}"
        if is_auto_disabled:
            label += " 🔕 (auto-disabled: column not mapped)"

        with st.expander(label):
            col_a, col_b = st.columns([1, 1])
            with col_a:
                if is_auto_disabled:
                    st.checkbox("Enabled", value=False, key=f"en_{cid}", disabled=True,
                                help="Cannot enable — required column is not mapped in Step 5")
                    c["enabled"] = False
                else:
                    c["enabled"] = st.checkbox("Enabled", value=bool(c.get("enabled", True)), key=f"en_{cid}")
            with col_b:
                c["points"] = st.number_input("Points", value=int(c.get("points", 0)), min_value=0, max_value=100, step=1, key=f"pt_{cid}",
                                               disabled=is_auto_disabled)

        if c.get("enabled", True):
            enabled_count += 1
            total_enabled_points += int(c.get("points", 0))

    st.markdown("---")
    st.metric("Enabled checks", enabled_count)
    st.metric("Total enabled points", total_enabled_points)
    if enabled_count == 0:
        st.error("⚠️ You must enable at least one check")
    if enabled_count > 0 and total_enabled_points != 100:
        st.warning("Points must sum to 100 to proceed. If you disabled checks, adjust points to keep total 100.")
    
    # Navigation
    col1, col2 = st.columns([1, 5])
    
    with col1:
        if st.button("← Back"):
            st.session_state.step = 5
            st.rerun()
    
    with col2:
        if st.button("Next →", type="primary", disabled=(enabled_count == 0 or total_enabled_points != 100)):
            st.session_state.config = {"checks": checks, "scoring_thresholds": config.get("scoring_thresholds", {})}
            st.session_state.step = 7
            st.rerun()

# ============================================================================
# STEP 7: REVIEW & RUN
# ============================================================================

elif st.session_state.step == 7:
    st.header("🔍 Step 7: Review & Confirm")
    
    st.info("Review all settings before running validation")
    
    # Summary of all settings
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📋 Configuration Summary")
        
        st.markdown(f"""
        **Business Line:** {st.session_state.business_line}  
        **Quarter:** {st.session_state.quarter}  
        **Server:** {st.session_state.server}
        """)
        
        st.markdown("---")
        
        st.markdown("**Input Data:**")
        if st.session_state.data_source == "Database":
            st.markdown(f"""
            - Source: Database  
            - Database: {st.session_state.claims_db_selected}  
            - Table: {st.session_state.claims_table_selected}  
            - bdx_month: {st.session_state.bdx_month_selected}
            - prev_bdx_month: {st.session_state.prev_bdx_month}
            """)
        else:
            st.markdown(f"""
            - Source: Excel File  
            - Rows: {len(st.session_state.excel_data):,}  
            - Columns: {len(st.session_state.excel_data.columns)}
            """)
        
        if st.session_state.use_risk_db:
            st.markdown(f"""
            **Risk Data:**  
            - Database: {st.session_state.risk_db_selected}  
            - Table: {st.session_state.risk_table_selected}
            """)
    
    with col2:
        st.subheader("📊 Validation Details")
        
        enabled_count = sum(1 for c in st.session_state.config.get("checks", []) if c.get("enabled", True))
        
        st.markdown(f"""
        **Checks Enabled:** {enabled_count}  
        **Columns Mapped:** {len(st.session_state.column_mappings)}  
        **Output Database:** {st.session_state.output_db}  
        **Schema:** {st.session_state.schema_name}
        """)
        
        st.markdown("---")
        
        # Save configuration option
        st.subheader("💾 Save Configuration")
        
        config_filename = f"{st.session_state.business_line}_{st.session_state.quarter}_config.yaml"
        
        st.text_input(
            "Config Filename",
            value=config_filename,
            key="config_filename"
        )
        
        if st.button("💾 Download Configuration File"):
            # Prepare config for download
            final_config = prepare_final_config(
                st.session_state.config,
                st.session_state.column_mappings,
                st.session_state
            )
            
            config_yaml = yaml.dump(final_config, default_flow_style=False)
            
            st.download_button(
                label="⬇️ Download Config YAML",
                data=config_yaml,
                file_name=st.session_state.config_filename,
                mime="text/yaml"
            )
            
            st.success("✓ Configuration ready for download")
    
    st.markdown("---")
    st.markdown("---")
    
    # Final confirmation
    st.subheader("🚀 Ready to Run Validation")
    
    st.warning("""
    **Before proceeding:**
    - ✓ All column mappings are correct
    - ✓ Check settings are configured
    - ✓ Output database is accessible
    - ✓ You have saved the configuration file (optional)
    """)
    
    # Run button
    col1, col2, col3 = st.columns([1, 2, 2])
    
    with col1:
        if st.button("← Back"):
            st.session_state.step = 6
            st.rerun()
    
    with col2:
        if st.button("🔄 Edit Settings", type="secondary"):
            st.session_state.step = 5
            st.rerun()
    
    with col3:
        if st.button("▶️ START VALIDATION", type="primary"):
            st.session_state.step = 8
            st.rerun()

# ============================================================================
# STEP 8: RESULTS & EXPORT
# ============================================================================

elif st.session_state.step == 8:
    st.header("📊 Step 8: Validation Results")
    
    # Run validation if not already done
    if st.session_state.validation_results is None:
        with st.spinner("🔍 Running validation checks..."):
            
            # Progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Load data
            status_text.text("Loading data...")
            progress_bar.progress(10)
            
            if st.session_state.data_source == "Database":
                data = load_data_from_db(
                    st.session_state.server,
                    st.session_state.claims_db_selected,
                    st.session_state.claims_table_selected,
                    st.session_state.bdx_month_selected,
                    st.session_state.username,
                    st.session_state.password
                )
                prev_data = None
                if st.session_state.prev_bdx_month is not None and str(st.session_state.prev_bdx_month) != "NaT":
                    prev_data = load_data_from_db(
                        st.session_state.server,
                        st.session_state.claims_db_selected,
                        st.session_state.claims_table_selected,
                        st.session_state.prev_bdx_month,
                        st.session_state.username,
                        st.session_state.password,
                    )
            else:
                data = st.session_state.excel_data
                prev_data = getattr(st.session_state, "excel_prev_data", None)
            
            progress_bar.progress(20)
            
            # Load risk data if needed
            risk_data = None
            if st.session_state.use_risk_db:
                status_text.text("Loading risk data...")
                progress_bar.progress(30)
                
                risk_data = load_data_from_db(
                    st.session_state.server,
                    st.session_state.risk_db_selected,
                    st.session_state.risk_table_selected,
                    None,
                    st.session_state.username,
                    st.session_state.password
                )
            
            # Run validation
            status_text.text("Running validation checks...")
            progress_bar.progress(40)
            
            # Inject previous month + settings into config for pipeline call
            st.session_state.config["_prev_data"] = prev_data
            st.session_state.config["_settings"] = {
                "closed_status_values": getattr(st.session_state, "closed_status_values", ["Closed"]),
            }
            # If risk policy column mapped, tell validator which column to use
            risk_policy_col = st.session_state.column_mappings.get("risk_policy_number")
            if risk_policy_col and risk_data is not None and risk_policy_col in risk_data.columns:
                for c in st.session_state.config.get("checks", []):
                    if c.get("type") == "risk_policy_match":
                        c["risk_policy_col"] = risk_policy_col

            results = run_validation_pipeline(
                data,
                st.session_state.config,
                st.session_state.column_mappings,
                st.session_state.quarter,
                risk_data,
                progress_callback=lambda p: progress_bar.progress(40 + int(p * 0.5))
            )
            
            progress_bar.progress(100)
            status_text.text("✓ Validation complete!")
            
            st.session_state.validation_results = results
    
    results = st.session_state.validation_results
    
    # Display results
    st.success("✅ Validation Complete!")
    
    # Overall Score
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        score = results['overall_score']
        st.metric(
            "Overall Score",
            f"{score:.1f}/100",
            delta=None
        )
    
    with col2:
        status = results['status']
        status_color = {
            'EXCELLENT': '🟢',
            'GOOD': '🟢',
            'FAIR': '🟡',
            'POOR': '🟠',
            'CRITICAL': '🔴'
        }
        st.metric("Status", f"{status_color.get(status, '⚪')} {status}")
    
    with col3:
        st.metric("Total Records", f"{results['total_records']:,}")
    
    with col4:
        st.metric("Checks Run", results['summary']['total_checks'])
    
    # Score gauge
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Data Quality Score"},
        delta={'reference': 85},
        gauge={
            'axis': {'range': [None, 100]},
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
    
    # Bar chart
    fig_categories = px.bar(
        category_df,
        x='Category',
        y='Score',
        text='Points',
        title="Score by Category",
        color='Score',
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
    
    # Filter options
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
    
    # Apply filters
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
        st.metric("Passed", results['summary']['passed'], delta=None)
    with col2:
        st.metric("Warnings", results['summary']['warnings'], delta=None)
    with col3:
        st.metric("Failed", results['summary']['failed'], delta=None)
    
    # Critical Issues
    if results.get("flags", {}).get("risk_match_ok"):
        st.success("Claims data matches with risk data")
    
    # Export Options
    st.markdown("---")
    st.subheader("💾 Export Results")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Download Excel Report
        excel_buffer = create_excel_report(results)
        
        st.download_button(
            label="📥 Download Excel Report",
            data=excel_buffer,
            file_name=f"DQ_Report_{st.session_state.business_line}_{st.session_state.quarter}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    with col2:
        # Download JSON Results
        json_str = json.dumps(results, indent=2, default=str)
        
        st.download_button(
            label="📥 Download JSON Results",
            data=json_str,
            file_name=f"DQ_Results_{st.session_state.business_line}_{st.session_state.quarter}.json",
            mime="application/json"
        )
    
    with col3:
        # Save to SQL Database
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
    
    # Navigation
    col1, col2 = st.columns([1, 5])
    
    with col1:
        if st.button("🔄 New Validation"):
            # Reset session state
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

