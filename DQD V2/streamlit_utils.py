# ============================================================================
# FILE: streamlit_utils.py  
# FINAL helper functions with alternate column name support
# ============================================================================

import pyodbc
import pandas as pd
import yaml
import json
from datetime import datetime
import io

# ============================================================================
# ALTERNATE COLUMN NAMES MAPPING
# ============================================================================

ALTERNATE_COLUMNS = {
    'accident_date': ['Accident Date', 'Date of Loss', 'Loss Date', 'Accident_Date', 'DateOfLoss'],
    'uw_date': ['UW Date', 'Policy Inception Date', 'Policy Start Date', 'Underwriting Date', 'UW_Date', 'Policy_Inception_Date', 'Policy_Start_Date'],
    'policy_start_date': ['Policy Start Date', 'Policy Inception Date', 'Inception Date', 'Policy_Start_Date', 'Start_Date'],
    'policy_end_date': ['Policy End Date', 'Expiry Date', 'Policy_End_Date', 'End_Date', 'Expiration_Date'],
    'total_reserve': ['Total Reserve', 'Total Estimate', 'Total Outstanding', 'Total OS', 'Total_Reserve', 'Total_Estimate', 'Total_Outstanding', 'Total_OS', 'Reserve', 'Outstanding'],
    'policy_number': ['Policy Number', 'Policy_Number', 'PolicyNumber', 'Policy_Num', 'PolNum'],
    'claim_number': ['Claim Number', 'Claim_Number', 'ClaimNumber', 'Claim_Num', 'ClmNum'],
    'status': ['Status of Claim', 'Status_of_Claim', 'Claim_Status', 'Status'],
    'total_incurred': ['Total Incurred', 'Total_Incurred', 'TotalIncurred', 'Incurred'],
    'total_paid': ['Total Paid', 'Total_Paid', 'TotalPaid', 'Paid']
}

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def get_connection_string(server, database, username="", password=""):
    """Build connection string"""
    if username and password:
        return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    else:
        return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"

def test_db_connection(server, database, username="", password=""):
    """Test database connection"""
    try:
        conn = pyodbc.connect(get_connection_string(server, database, username, password), timeout=5)
        conn.close()
        return True, f"✓ Connected to {database}"
    except Exception as e:
        return False, f"✗ Failed: {str(e)}"

def get_tables_list(server, database, username="", password=""):
    """Get list of tables"""
    try:
        conn = pyodbc.connect(get_connection_string(server, database, username, password))
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
        cursor = conn.cursor()
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except:
        return []

def get_columns_from_table(server, database, table, username="", password=""):
    """Get columns from table"""
    try:
        conn = pyodbc.connect(get_connection_string(server, database, username, password))
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
        conn.close()
        return columns
    except:
        return []

def get_table_preview(server, database, table, username="", password="", limit=5):
    """Preview table data"""
    try:
        query = f"SELECT TOP {limit} * FROM {table}"
        return pd.read_sql(query, get_connection_string(server, database, username, password))
    except:
        return pd.DataFrame()

def get_bdx_months(server, database, table, username="", password=""):
    """Get unique bdx_month values"""
    try:
        conn = pyodbc.connect(get_connection_string(server, database, username, password))
        cursor = conn.cursor()
        
        for col in ['bdx_month', 'Bdx_Month', 'BDX_MONTH', 'evaluation_date']:
            try:
                query = f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL ORDER BY {col} DESC"
                cursor.execute(query)
                months = [row[0] for row in cursor.fetchall()]
                conn.close()
                return months
            except:
                continue
        
        conn.close()
        return []
    except:
        return []

def get_record_count(server, database, table, bdx_month, username="", password=""):
    """Get record count for single bdx_month"""
    try:
        conn = pyodbc.connect(get_connection_string(server, database, username, password))
        
        columns = get_columns_from_table(server, database, table, username, password)
        bdx_col = next((col for col in columns if 'bdx' in col.lower()), 'bdx_month')
        
        query = f"SELECT COUNT(*) FROM {table} WHERE {bdx_col} = ?"
        cursor = conn.cursor()
        cursor.execute(query, (bdx_month,))
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except:
        return 0

def load_data_from_db(server, database, table, bdx_month, username="", password=""):
    """Load data for specific bdx_month"""
    try:
        columns = get_columns_from_table(server, database, table, username, password)
        bdx_col = next((col for col in columns if 'bdx' in col.lower()), 'bdx_month')
        
        query = f"SELECT * FROM {table} WHERE {bdx_col} = ?"
        return pd.read_sql(query, get_connection_string(server, database, username, password), params=[bdx_month])
    except Exception as e:
        raise Exception(f"Error loading data: {str(e)}")

def load_previous_month_data(server, database, table, bdx_month, username="", password=""):
    """Load previous month data for historical checks"""
    try:
        # Get all bdx_months
        all_months = get_bdx_months(server, database, table, username, password)
        
        if bdx_month not in all_months:
            return None, "Current bdx_month not found"
        
        # Find previous month
        current_index = all_months.index(bdx_month)
        if current_index >= len(all_months) - 1:
            return None, "No previous month data available"
        
        prev_month = all_months[current_index + 1]
        
        # Load previous month data
        df_prev = load_data_from_db(server, database, table, prev_month, username, password)
        return df_prev, f"Loaded previous month: {prev_month}"
        
    except Exception as e:
        return None, f"Error loading previous month: {str(e)}"

def create_schema_in_db(server, database, schema_name, username="", password=""):
    """Create schema"""
    try:
        conn = pyodbc.connect(get_connection_string(server, database, username, password))
        cursor = conn.cursor()
        
        check_query = f"SELECT COUNT(*) FROM sys.schemas WHERE name = '{schema_name}'"
        cursor.execute(check_query)
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            cursor.execute(f"CREATE SCHEMA {schema_name}")
            conn.commit()
            conn.close()
            return True, f"✓ Schema '{schema_name}' created"
        else:
            conn.close()
            return True, f"ℹ️ Schema '{schema_name}' exists"
    except Exception as e:
        return False, f"✗ Error: {str(e)}"

# ============================================================================
# COLUMN MAPPING HELPERS
# ============================================================================

def find_column_match(logical_name, available_columns):
    """Find best matching column from alternates"""
    alternates = ALTERNATE_COLUMNS.get(logical_name, [logical_name])
    
    for alt in alternates:
        if alt in available_columns:
            return alt
    
    return None

def get_default_column_index(logical_name, available_columns):
    """Get dropdown index for default match"""
    match = find_column_match(logical_name, available_columns)
    if match and match in available_columns:
        return available_columns.index(match) + 1  # +1 for empty option
    return 0

# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

def load_config_from_file(uploaded_file):
    """Load configuration from uploaded file"""
    try:
        config = yaml.safe_load(uploaded_file)
        return config, "✓ Config loaded successfully"
    except Exception as e:
        return None, f"✗ Error loading config: {str(e)}"

def save_config_to_yaml(config, filename):
    """Save configuration to YAML"""
    try:
        config_yaml = yaml.dump(config, default_flow_style=False, sort_keys=False)
        return config_yaml
    except Exception as e:
        return f"# Error: {str(e)}"

def create_config_dict(session_state, column_mappings, enabled_checks, check_points, closed_values):
    """Create configuration dictionary"""
    config = {
        'metadata': {
            'business_line': session_state.business_line,
            'quarter': session_state.quarter,
            'created_date': datetime.now().isoformat(),
            'data_source': session_state.data_source
        },
        'database': {
            'server': session_state.server,
            'claims_db': session_state.get('claims_db', ''),
            'claims_table': session_state.get('claims_table', ''),
            'output_db': session_state.output_db,
            'schema_name': session_state.schema_name
        },
        'bdx_month': session_state.get('bdx_month', ''),
        'column_mappings': column_mappings,
        'enabled_checks': enabled_checks,
        'check_points': check_points,
        'closed_values': closed_values or ['Closed'],
        'use_risk_db': session_state.get('use_risk_db', False)
    }
    
    if session_state.get('use_risk_db'):
        config['risk_db'] = {
            'database': session_state.get('risk_db', ''),
            'table': session_state.get('risk_table', '')
        }
    
    return config

# ============================================================================
# VALIDATION PIPELINE
# ============================================================================

def run_validation_pipeline(data, column_mappings, enabled_checks, check_points, quarter_id, 
                           prev_data=None, risk_data=None, closed_values=None, progress_callback=None):
    """Run validation pipeline"""
    from validator import DataQualityValidator
    
    validator = DataQualityValidator(enabled_checks, check_points)
    
    results = validator.validate(
        data, 
        column_mappings, 
        quarter_id,
        prev_df=prev_data,
        risk_df=risk_data,
        closed_values=closed_values
    )
    
    if progress_callback:
        progress_callback(100)
    
    return results

# ============================================================================
# RESULTS EXPORT
# ============================================================================

def save_results_to_sql(results, server, database, schema_name, username="", password=""):
    """Save results to SQL"""
    try:
        from db_utils import DatabaseManager
        
        db = DatabaseManager(server, database, schema_name, username, password)
        
        if db.connect():
            success = db.save_results(results)
            db.disconnect()
            return success
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def create_excel_report(results):
    """Create Excel report"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Summary
        summary_df = pd.DataFrame([{
            'Quarter': results['quarter_id'],
            'Overall Score': f"{results['overall_score']:.0f}/100",
            'Status': results['status'],
            'Total Records': results['total_records'],
            'Checks Passed': results['summary']['passed'],
            'Checks Failed': results['summary']['failed']
        }])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Detailed Results
        detailed = []
        for check_id, check in results['detailed_results'].items():
            if not check.get('is_risk_check', False):  # Skip risk check in main summary
                detailed.append({
                    'Check ID': check['check_id'],
                    'Check Name': check['check_name'],
                    'Pass Rate': f"{check['pass_rate']:.1f}%",
                    'Passed': check['passed'],
                    'Failed': check['failed'],
                    'Total': check['total'],
                    'Points': f"{check['points_earned']}/{check['points_possible']}"
                })
        
        pd.DataFrame(detailed).to_excel(writer, sheet_name='Check Results', index=False)
        
        # Risk Check (if exists)
        risk_check = results['detailed_results'].get('6.1')
        if risk_check and risk_check.get('is_risk_check'):
            risk_df = pd.DataFrame([{
                'Check': risk_check['check_name'],
                'Result': 'PASS - Claims data matches with risk data' if risk_check.get('matches_risk') else 'FAIL',
                'Pass Rate': f"{risk_check['pass_rate']:.1f}%",
                'Matched': risk_check['passed'],
                'Unmatched': risk_check['failed']
            }])
            risk_df.to_excel(writer, sheet_name='Risk Reconciliation', index=False)
    
    output.seek(0)
    return output