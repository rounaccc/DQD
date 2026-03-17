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
    create_schema_in_db,
    load_default_config,
    save_config_file,
    run_validation_pipeline,
    save_results_to_sql
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
            value="14324u15VT",
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
                
                # bdx_month selection
                st.markdown("---")
                st.subheader("📅 Date Range Selection")
                
                bdx_months = get_bdx_months(
                    st.session_state.server,
                    claims_db,
                    claims_table,
                    st.session_state.username,
                    st.session_state.password
                )
                
                if bdx_months:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        from_bdx = st.selectbox(
                            "From bdx_month*",
                            options=bdx_months,
                            index=len(bdx_months)-1,  # Default to latest
                            key="from_bdx"
                        )
                    
                    with col2:
                        to_bdx = st.selectbox(
                            "To bdx_month*",
                            options=bdx_months,
                            index=len(bdx_months)-1,  # Default to latest
                            key="to_bdx"
                        )
                    
                    # Validate date range
                    if from_bdx > to_bdx:
                        st.error("'From' date must be before or equal to 'To' date")
                    else:
                        # Show record count for selected range
                        record_count = get_record_count(
                            st.session_state.server,
                            claims_db,
                            claims_table,
                            from_bdx,
                            to_bdx,
                            st.session_state.username,
                            st.session_state.password
                        )
                        st.info(f"📊 Records in selected range: **{record_count:,}**")
                        
                        if record_count == 0:
                            st.warning("No records found in selected date range")
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
                
                # Get unique values
                bdx_values = sorted(df[bdx_column].dropna().unique())
                
                col1, col2 = st.columns(2)
                
                with col1:
                    from_bdx = st.selectbox(
                        "From bdx_month",
                        options=bdx_values,
                        index=len(bdx_values)-1
                    )
                
                with col2:
                    to_bdx = st.selectbox(
                        "To bdx_month",
                        options=bdx_values,
                        index=len(bdx_values)-1
                    )
                
                # Filter data
                filtered_df = df[(df[bdx_column] >= from_bdx) & (df[bdx_column] <= to_bdx)]
                st.info(f"📊 Records in selected range: **{len(filtered_df):,}**")
                
                # Store in session state
                st.session_state.excel_data = filtered_df
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
                from_bdx and 
                to_bdx and 
                from_bdx <= to_bdx
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
                st.session_state.claims_db = claims_db
                st.session_state.claims_table = claims_table
                st.session_state.from_bdx = from_bdx
                st.session_state.to_bdx = to_bdx
            
            st.session_state.use_risk_db = use_risk_db
            if use_risk_db:
                st.session_state.risk_db = risk_db
                st.session_state.risk_table = risk_table
            
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
    
    config_option = st.radio(
        "Choose configuration approach:",
        ["Use Default Configuration", "Upload Existing Configuration"],
        help="Default config has predefined checks. Upload if you have a saved config from previous run."
    )
    
    if config_option == "Use Default Configuration":
        st.info("✓ Using default configuration with 10 standard checks")
        
        # Load default config
        config = load_default_config()
        
        # Show summary
        st.subheader("Default Configuration Summary")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Checks", "10")
        with col2:
            st.metric("Categories", "5")
        with col3:
            st.metric("Columns Required", "10" if not st.session_state.use_risk_db else "11")
        
        # Show check list
        with st.expander("📋 View All Checks"):
            st.markdown("""
            **Completeness (2 checks):**
            - 1.1: Policy Number Complete
            - 1.2: Claim Number Complete
            
            **Consistency (2 checks):**
            - 2.1: Gross Incurred Formula
            - 2.3: Net ≤ Gross Relationship
            
            **Reasonableness (2 checks):**
            - 3.1: Non-Negative Financial Values
            - 3.2: Accident Year Range
            
            **Data Quality (2 checks):**
            - 4.1: Unique Claim Numbers
            - 4.3: Valid Date Formats
            
            **Temporal Logic (2 checks):**
            - 5.2: Accident Before Notification
            - 5.6: Accident Year Matches Date
            """)
            
            if st.session_state.use_risk_db:
                st.markdown("""
                **Additional (1 check):**
                - 11.1: Total Incurred vs GWP Reconciliation
                """)
        
        st.session_state.config = config
    
    else:  # Upload existing config
        uploaded_config = st.file_uploader(
            "Upload Configuration File",
            type=['yaml', 'yml'],
            help="Upload a previously saved configuration file"
        )
        
        if uploaded_config:
            try:
                config = yaml.safe_load(uploaded_config)
                
                # Validate config structure
                required_keys = ['column_mappings', 'validation_checks', 'scoring_thresholds']
                if all(key in config for key in required_keys):
                    st.success("✓ Valid configuration file loaded")
                    
                    # Show summary
                    total_checks = sum(
                        len(checks) 
                        for category in config['validation_checks'].values() 
                        for checks in category.values()
                    )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Checks", total_checks)
                    with col2:
                        st.metric("Categories", len(config['validation_checks']))
                    
                    st.session_state.config = config
                else:
                    st.error("❌ Invalid configuration file structure")
            
            except Exception as e:
                st.error(f"Error loading configuration: {str(e)}")
    
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
            st.session_state.claims_db,
            st.session_state.claims_table,
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
        'accident_date': 'Accident Date',
        'accident_year': 'Accident Year',
        'notification_date': 'Notification Date',
        'gross_paid': 'Gross Paid',
        'gross_incurred': 'Gross Incurred',
        'gross_estimate': 'Gross Estimate',
        'net_paid': 'Net Paid',
        'net_incurred': 'Net Incurred'
    }
    
    # Add risk column if needed
    if st.session_state.use_risk_db:
        # Get risk table columns
        risk_columns = get_columns_from_table(
            st.session_state.server,
            st.session_state.risk_db,
            st.session_state.risk_table,
            st.session_state.username,
            st.session_state.password
        )
        
        st.markdown("---")
        st.subheader("Risk Data Columns")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.session_state.column_mappings['gwp'] = st.selectbox(
                "GWP (Gross Written Premium)*",
                options=[''] + risk_columns,
                index=find_default_match('gwp', risk_columns),
                help="Map to GWP column in risk table"
            )
        
        with col2:
            st.session_state.column_mappings['risk_policy_number'] = st.selectbox(
                "Policy Number (Risk Table)*",
                options=[''] + risk_columns,
                index=find_default_match('policy', risk_columns),
                help="Policy number to join with claims table"
            )
    
    st.markdown("---")
    st.subheader("Claims Data Columns")
    
    # Create column mapping UI
    col1, col2 = st.columns(2)
    
    mappings_complete = True
    
    for i, (logical_name, display_name) in enumerate(required_columns.items()):
        col = col1 if i % 2 == 0 else col2
        
        with col:
            # Try to find default match
            default_index = find_default_match(logical_name, available_columns)
            
            selected = st.selectbox(
                f"{display_name}*",
                options=[''] + available_columns,
                index=default_index,
                key=f"map_{logical_name}",
                help=f"Map to column containing {display_name}"
            )
            
            if selected:
                st.session_state.column_mappings[logical_name] = selected
            else:
                mappings_complete = False
    
    # Show mapping summary
    if mappings_complete:
        with st.expander("📋 View Mapping Summary"):
            mapping_df = pd.DataFrame([
                {"Logical Name": k, "Mapped To": v}
                for k, v in st.session_state.column_mappings.items()
            ])
            st.dataframe(mapping_df, use_container_width=True)
    else:
        st.warning("⚠️ Please map all required columns to proceed")
    
    st.markdown("---")
    
    # Navigation
    col1, col2 = st.columns([1, 5])
    
    with col1:
        if st.button("← Back"):
            st.session_state.step = 4
            st.rerun()
    
    with col2:
        if st.button("Next →", type="primary", disabled=not mappings_complete):
            st.session_state.step = 6
            st.rerun()

# ============================================================================
# STEP 6: CHECK SETTINGS
# ============================================================================

elif st.session_state.step == 6:
    st.header("⚙️ Step 6: Validation Check Settings")
    
    st.info("Review and modify scoring thresholds for each check")
    
    config = st.session_state.config
    
    # Allow enabling/disabling checks
    st.subheader("Enable/Disable Checks")
    
    if 'enabled_checks' not in st.session_state:
        st.session_state.enabled_checks = {}
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        enable_all = st.checkbox("Enable All", value=True)
    
    with col2:
        if enable_all:
            st.caption("All checks are enabled")
    
    # Display checks by category
    for category, subcategories in config['validation_checks'].items():
        st.markdown("---")
        st.subheader(f"📂 {category.replace('_', ' ').title()}")
        
        for subcategory, checks in subcategories.items():
            st.markdown(f"**{subcategory.replace('_', ' ').title()}**")
            
            for check in checks:
                check_id = check['id']
                
                # Initialize if not exists
                if check_id not in st.session_state.enabled_checks:
                    st.session_state.enabled_checks[check_id] = enable_all
                
                # Create expandable section for each check
                with st.expander(f"{'✅' if st.session_state.enabled_checks[check_id] else '❌'} {check_id}: {check['name']}"):
                    col1, col2, col3 = st.columns([1, 2, 2])
                    
                    with col1:
                        st.session_state.enabled_checks[check_id] = st.checkbox(
                            "Enabled",
                            value=st.session_state.enabled_checks[check_id],
                            key=f"enable_{check_id}"
                        )
                    
                    with col2:
                        st.metric("Points", check['points'])
                        st.caption(f"Priority: {check['priority']}")
                    
                    with col3:
                        st.caption("Scoring Thresholds:")
                        for threshold, points in check['scoring'].items():
                            if threshold != 'default':
                                st.text(f"≥{threshold}% → {points} pts")
                            else:
                                st.text(f"<threshold → {points} pts")
                    
                    # Allow editing scoring
                    if st.checkbox("Edit Scoring", key=f"edit_{check_id}"):
                        st.markdown("**Modify Scoring Thresholds:**")
                        
                        new_scoring = {}
                        scoring_items = [k for k in check['scoring'].keys() if k != 'default']
                        
                        for i, threshold in enumerate(scoring_items):
                            col_a, col_b = st.columns(2)
                            
                            with col_a:
                                new_threshold = st.number_input(
                                    f"Threshold {i+1} (%)",
                                    value=float(threshold),
                                    min_value=0.0,
                                    max_value=100.0,
                                    step=1.0,
                                    key=f"threshold_{check_id}_{i}"
                                )
                            
                            with col_b:
                                new_points = st.number_input(
                                    f"Points {i+1}",
                                    value=check['scoring'][threshold],
                                    min_value=0,
                                    max_value=check['points'],
                                    step=1,
                                    key=f"points_{check_id}_{i}"
                                )
                            
                            new_scoring[new_threshold] = new_points
                        
                        # Default points
                        new_scoring['default'] = st.number_input(
                            "Default Points (below all thresholds)",
                            value=check['scoring']['default'],
                            min_value=0,
                            max_value=check['points'],
                            step=1,
                            key=f"default_{check_id}"
                        )
                        
                        # Update config
                        check['scoring'] = new_scoring
    
    # Summary
    st.markdown("---")
    st.subheader("📊 Summary")
    
    enabled_count = sum(1 for v in st.session_state.enabled_checks.values() if v)
    total_count = len(st.session_state.enabled_checks)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Checks", total_count)
    with col2:
        st.metric("Enabled", enabled_count)
    with col3:
        st.metric("Disabled", total_count - enabled_count)
    
    if enabled_count == 0:
        st.error("⚠️ You must enable at least one check")
    
    st.markdown("---")
    
    # Navigation
    col1, col2 = st.columns([1, 5])
    
    with col1:
        if st.button("← Back"):
            st.session_state.step = 5
            st.rerun()
    
    with col2:
        if st.button("Next →", type="primary", disabled=enabled_count == 0):
            # Filter config to only include enabled checks
            filtered_config = filter_enabled_checks(config, st.session_state.enabled_checks)
            st.session_state.config = filtered_config
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
            - Database: {st.session_state.claims_db}  
            - Table: {st.session_state.claims_table}  
            - Date Range: {st.session_state.from_bdx} to {st.session_state.to_bdx}
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
            - Database: {st.session_state.risk_db}  
            - Table: {st.session_state.risk_table}
            """)
    
    with col2:
        st.subheader("📊 Validation Details")
        
        enabled_count = sum(1 for v in st.session_state.enabled_checks.values() if v)
        
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
                    st.session_state.claims_db,
                    st.session_state.claims_table,
                    st.session_state.from_bdx,
                    st.session_state.to_bdx,
                    st.session_state.username,
                    st.session_state.password
                )
            else:
                data = st.session_state.excel_data
            
            progress_bar.progress(20)
            
            # Load risk data if needed
            risk_data = None
            if st.session_state.use_risk_db:
                status_text.text("Loading risk data...")
                progress_bar.progress(30)
                
                risk_data = load_data_from_db(
                    st.session_state.server,
                    st.session_state.risk_db,
                    st.session_state.risk_table,
                    None,
                    None,
                    st.session_state.username,
                    st.session_state.password
                )
            
            # Run validation
            status_text.text("Running validation checks...")
            progress_bar.progress(40)
            
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
    if results['summary']['critical_issues_count'] > 0:
        st.error(f"⚠️ {results['summary']['critical_issues_count']} Critical Issues Found")
        
        with st.expander("View Critical Issues"):
            for issue in results['summary']['critical_issues']:
                st.markdown(f"""
                **{issue['check_id']}: {issue['check_name']}**  
                Failed Records: {issue['failed_count']}
                """)
    
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


# ============================================================================
# Helper function to find default column match
# ============================================================================

def find_default_match(logical_name, available_columns):
    """Find best matching column from available columns"""
    # Default mappings to try
    mappings = {
        'policy_number': ['Policy Number', 'Policy_Number', 'PolicyNumber', 'Policy_Num', 'PolNum'],
        'claim_number': ['Claim Number', 'Claim_Number', 'ClaimNumber', 'Claim_Num', 'ClmNum'],
        'accident_date': ['Accident Date', 'Accident_Date', 'AccidentDate', 'Acc_Date'],
        'accident_year': ['Accident Year', 'Accident_Year', 'AccidentYear', 'AY'],
        'notification_date': ['Notification Date', 'Notification_Date', 'NotificationDate'],
        'gross_paid': ['Gross Paid', 'Gross_Paid', 'GrossPaid'],
        'gross_incurred': ['Gross Incurred', 'Gross_Incurred', 'GrossIncurred'],
        'gross_estimate': ['Gross Estimate', 'Gross_Estimate', 'GrossEstimate'],
        'net_paid': ['Net Paid', 'Net_Paid', 'NetPaid'],
        'net_incurred': ['Net Incurred', 'Net_Incurred', 'NetIncurred'],
        'gwp': ['GWP', 'Gross Written Premium', 'Gross_Written_Premium'],
        'policy': ['Policy Number', 'Policy_Number', 'PolicyNumber']
    }
    
    # Get possible matches for this logical name
    possible = mappings.get(logical_name, [])
    
    # Find first match
    for option in possible:
        if option in available_columns:
            return available_columns.index(option) + 1  # +1 for empty option
    
    return 0  # Return empty option


def filter_enabled_checks(config, enabled_checks):
    """Filter config to only include enabled checks"""
    filtered_config = config.copy()
    
    for category, subcategories in config['validation_checks'].items():
        for subcategory, checks in subcategories.items():
            filtered_checks = [
                check for check in checks 
                if enabled_checks.get(check['id'], True)
            ]
            filtered_config['validation_checks'][category][subcategory] = filtered_checks
    
    return filtered_config