# ============================================================================
# FILE: streamlit_utils.py
# SIMPLIFIED helper functions for Streamlit app
# ============================================================================

import pyodbc
import pandas as pd
import yaml
import json
from datetime import datetime
import io

# ============================================================================
# DATABASE CONNECTION FUNCTIONS
# ============================================================================

def get_connection_string(server, database, username="", password=""):
    """Build connection string for SQL Server"""
    if username and password:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
    return conn_str


def test_db_connection(server, database, username="", password=""):
    """Test database connection"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str, timeout=5)
        conn.close()
        return True, f"✓ Connected to {database} successfully"
    except Exception as e:
        return False, f"✗ Connection failed: {str(e)}"


def get_tables_list(server, database, username="", password=""):
    """Get list of tables in database"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str)
        
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return tables
    except:
        return []


def get_columns_from_table(server, database, table, username="", password=""):
    """Get list of columns from a table"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str)
        
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return columns
    except:
        return []


def get_table_preview(server, database, table, username="", password="", limit=5):
    """Get preview of table data"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str)
        query = f"SELECT TOP {limit} * FROM {table}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


def get_bdx_months(server, database, table, username="", password=""):
    """Get unique bdx_month values from table"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        
        # Try common column names
        possible_columns = ['bdx_month', 'Bdx_Month', 'bdx_Month', 'BDX_MONTH', 'evaluation_date']
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Find which column exists
        for col in possible_columns:
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


def get_record_count_for_bdx(server, database, table, bdx_value, username="", password=""):
    """Get record count for a single bdx month"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        
        # Find bdx column name
        columns = get_columns_from_table(server, database, table, username, password)
        bdx_col = next((col for col in columns if 'bdx' in col.lower()), 'bdx_month')
        
        query = f"""
        SELECT COUNT(*) 
        FROM {table} 
        WHERE {bdx_col} = ?
        """
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query, (bdx_value,))
        count = cursor.fetchone()[0]
        conn.close()
        
        return count
    except:
        return 0


def load_data_from_db(server, database, table, bdx_value=None, username="", password=""):
    """Load data from database. If bdx_value provided, loads that single month."""
    try:
        import pyodbc

        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str)

        # Find bdx column
        columns = get_columns_from_table(server, database, table, username, password)
        bdx_col = next((col for col in columns if 'bdx' in col.lower()), 'bdx_month')

        if bdx_value is not None:
            query = f"SELECT * FROM {table} WHERE {bdx_col} = ?"
            df = pd.read_sql(query, conn, params=[bdx_value])
        else:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, conn)

        conn.close()
        return df
    except Exception as e:
        raise Exception(f"Error loading data: {str(e)}")


def get_previous_bdx_value(all_bdx_values, selected_bdx):
    """Given a list of bdx values (datetimes), return the previous available value before selected."""
    try:
        values = [v for v in all_bdx_values if v is not None]
        values_sorted = sorted(values)
        if selected_bdx not in values_sorted:
            return None
        idx = values_sorted.index(selected_bdx)
        if idx == 0:
            return None
        return values_sorted[idx - 1]
    except Exception:
        return None


# ============================================================================
# SCHEMA & TABLE MANAGEMENT
# ============================================================================

def create_schema_in_db(server, database, schema_name, username="", password=""):
    """Create schema in database"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Check if schema exists
        check_query = f"""
        SELECT COUNT(*) 
        FROM sys.schemas 
        WHERE name = '{schema_name}'
        """
        cursor.execute(check_query)
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            # Create schema
            create_query = f"CREATE SCHEMA {schema_name}"
            cursor.execute(create_query)
            conn.commit()
            conn.close()
            return True, f"✓ Schema '{schema_name}' created successfully"
        else:
            conn.close()
            return True, f"ℹ️ Schema '{schema_name}' already exists"
    
    except Exception as e:
        return False, f"✗ Error creating schema: {str(e)}"


def check_existing_validations(server, database, business_line, quarter, username="", password=""):
    """Check if validation already exists for this business line and quarter"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str)
        
        schema_name = business_line.replace(" ", "_")
        
        query = f"""
        SELECT COUNT(*) 
        FROM {schema_name}.dq_scores 
        WHERE quarter_id = ?
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (quarter,))
        count = cursor.fetchone()[0]
        conn.close()
        
        return count > 0
    except:
        return False


# ============================================================================
# VALIDATION PIPELINE
# ============================================================================

def run_validation_pipeline(data, config, column_mappings, quarter_id, risk_data=None, progress_callback=None):
    """Run complete validation pipeline - SIMPLIFIED VERSION"""
    from validator import DataQualityValidator
    
    # Initialize validator with config dict
    validator = DataQualityValidator(config)
    
    # Run validation
    prev_data = config.get("_prev_data") if isinstance(config, dict) else None
    settings = config.get("_settings", {}) if isinstance(config, dict) else {}
    results = validator.validate(
        data,
        column_mappings,
        quarter_id=quarter_id,
        risk_df=risk_data,
        prev_df=prev_data,
        settings=settings,
    )
    
    if progress_callback:
        progress_callback(100)
    
    return results


# ============================================================================
# RESULTS EXPORT
# ============================================================================

def save_results_to_sql(results, server, database, schema_name, username="", password=""):
    """Save validation results to SQL database"""
    try:
        from db_utils import DatabaseManager
        
        db = DatabaseManager(server, database, schema_name, username, password)
        
        if db.connect():
            success = db.save_results(results)
            db.disconnect()
            return success
        else:
            return False
    
    except Exception as e:
        print(f"Error saving to SQL: {str(e)}")
        return False


def create_excel_report(results):
    """Create Excel report from validation results"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Sheet 1: Summary
        summary_df = pd.DataFrame([{
            'Quarter': results['quarter_id'],
            'Overall Score': results['overall_score'],
            'Status': results['status'],
            'Total Records': results['total_records'],
            'Total Checks': results['summary']['total_checks'],
            'Passed': results['summary']['passed'],
            'Warnings': results['summary']['warnings'],
            'Failed': results['summary']['failed'],
            'Critical Issues': results['summary']['critical_issues_count']
        }])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Sheet 2: Category Scores
        category_data = []
        for cat, scores in results['category_scores'].items():
            category_data.append({
                'Category': cat.replace('_', ' ').title(),
                'Percentage': scores['percentage'],
                'Points Earned': scores['points_earned'],
                'Points Possible': scores['points_possible']
            })
        category_df = pd.DataFrame(category_data)
        category_df.to_excel(writer, sheet_name='Category Scores', index=False)
        
        # Sheet 3: Detailed Results
        detailed_data = []
        for category, subcategories in results['detailed_results'].items():
            for subcategory, checks in subcategories.items():
                for check in checks:
                    detailed_data.append({
                        'Check ID': check['check_id'],
                        'Check Name': check['check_name'],
                        'Category': category.replace('_', ' ').title(),
                        'Pass Rate': f"{check.get('pass_rate', 0):.1f}%",
                        'Passed': check.get('passed', 0),
                        'Failed': check.get('failed', 0),
                        'Total': check.get('total', 0),
                        'Points Earned': check['points_earned'],
                        'Points Possible': check['points_possible']
                    })
        detailed_df = pd.DataFrame(detailed_data)
        detailed_df.to_excel(writer, sheet_name='Detailed Results', index=False)
        
        # Sheet 4: Failed Checks Only
        failed_data = [d for d in detailed_data if d['Points Earned'] < d['Points Possible']]
        if failed_data:
            failed_df = pd.DataFrame(failed_data)
            failed_df.to_excel(writer, sheet_name='Failed Checks', index=False)
    
    output.seek(0)
    return output


def prepare_final_config(config, column_mappings, session_state):
    """Prepare final configuration with all settings"""
    final_config = {
        'metadata': {
            'business_line': session_state.business_line,
            'quarter': session_state.quarter,
            'created_date': datetime.now().isoformat(),
            'data_source': session_state.data_source,
            'bdx_month': str(getattr(session_state, "bdx_month", "")),
            'prev_bdx_month': str(getattr(session_state, "prev_bdx_month", "")),
            'closed_status_values': getattr(session_state, "closed_status_values", ["Closed"]),
        },
        'column_mappings': column_mappings,
        'checks': config.get('checks', []),
        'scoring_thresholds': {
            'thresh_full': getattr(session_state, 'thresh_full', 95),
            'thresh_mid': getattr(session_state, 'thresh_mid', 90),
            'mid_points': getattr(session_state, 'mid_points', 3),
        },
    }
    
    if session_state.data_source == "Database":
        final_config['metadata']['claims_db'] = session_state.claims_db
        final_config['metadata']['claims_table'] = session_state.claims_table
    
    if session_state.use_risk_db:
        final_config['metadata']['risk_db'] = session_state.risk_db
        final_config['metadata']['risk_table'] = session_state.risk_table
    
    return final_config


def load_default_config():
    """Load default configuration - returns simple dict"""
    config = {
        'checks': [
            # --- Not Null ---
            {'id': '1.1', 'name': 'Policy Number Not Null', 'type': 'not_null', 'field': 'policy_number', 'category': 'Not Null', 'subcategory': 'Core', 'points': 5, 'enabled': True},
            {'id': '1.2', 'name': 'Claim Number Not Null', 'type': 'not_null', 'field': 'claim_number', 'category': 'Not Null', 'subcategory': 'Core', 'points': 5, 'enabled': True},
            {'id': '1.3', 'name': 'Accident Date / Date of Loss Not Null', 'type': 'not_null', 'field': 'accident_date', 'category': 'Not Null', 'subcategory': 'Core', 'points': 5, 'enabled': True},
            {'id': '1.4', 'name': 'Status of Claim Not Null', 'type': 'not_null', 'field': 'claim_status', 'category': 'Not Null', 'subcategory': 'Core', 'points': 5, 'enabled': True},
            {'id': '1.5', 'name': 'UW Date Not Null and Not Year 1900', 'type': 'not_null_not_year_1900', 'field': 'uw_date', 'category': 'Not Null', 'subcategory': 'Core', 'points': 5, 'enabled': True},

            # --- Non-Negative ---
            {'id': '2.1', 'name': 'Total Incurred Non-Negative', 'type': 'non_negative', 'field': 'total_incurred', 'category': 'Non-Negative', 'subcategory': 'Financial', 'points': 7, 'enabled': True},
            {'id': '2.2', 'name': 'Total Paid Non-Negative', 'type': 'non_negative', 'field': 'total_paid', 'category': 'Non-Negative', 'subcategory': 'Financial', 'points': 7, 'enabled': True},
            {'id': '2.3', 'name': 'Total OS Non-Negative', 'type': 'non_negative', 'field': 'total_os', 'category': 'Non-Negative', 'subcategory': 'Financial', 'points': 7, 'enabled': True},

            # --- Logical ---
            {'id': '3.1', 'name': 'Closed Claim => Total OS = 0', 'type': 'closed_os_zero', 'category': 'Logical', 'subcategory': 'Rules', 'points': 10, 'enabled': True},
            {'id': '3.2', 'name': 'Date of Loss within Policy Start/End', 'type': 'loss_within_policy', 'category': 'Logical', 'subcategory': 'Rules', 'points': 10, 'enabled': True},
            {'id': '3.3', 'name': 'Total Incurred = Total Paid + Total OS', 'type': 'incurred_equals_paid_plus_os', 'tolerance': 5, 'category': 'Logical', 'subcategory': 'Rules', 'points': 10, 'enabled': True},

            # --- Uniqueness ---
            {'id': '4.1', 'name': 'Claim Number Unique', 'type': 'unique', 'field': 'claim_number', 'category': 'Uniqueness', 'subcategory': 'Keys', 'points': 8, 'enabled': True},

            # --- Valid Date Format ---
            {'id': '5.1', 'name': 'Accident Date Valid Format', 'type': 'valid_date', 'field': 'accident_date', 'category': 'Valid Date', 'subcategory': 'Dates', 'points': 4, 'enabled': True},
            {'id': '5.2', 'name': 'UW Date Valid Format', 'type': 'valid_date', 'field': 'uw_date', 'category': 'Valid Date', 'subcategory': 'Dates', 'points': 4, 'enabled': True},

            # --- Missing Claims ---
            {'id': '6.1', 'name': 'Missing Claims vs Previous BDX Month', 'type': 'missing_claims_from_prev_month', 'category': 'Continuity', 'subcategory': 'Month over Month', 'points': 4, 'enabled': True},

            # --- Total Paid MoM ---
            {'id': '7.1', 'name': 'Total Paid Non-Decreasing vs Previous BDX Month', 'type': 'total_paid_non_decreasing_vs_prev', 'category': 'Continuity', 'subcategory': 'Month over Month', 'points': 4, 'enabled': True},

            # --- Optional Risk Table ---
            {'id': '8.1', 'name': 'Claims Policy Numbers exist in Risk Table', 'type': 'risk_policy_match', 'category': 'Risk', 'subcategory': 'Cross Table', 'points': 10, 'enabled': False},
        ],
        'scoring_thresholds': {
            'excellent': 95,
            'good': 85,
            'fair': 75,
            'poor': 65
        }
    }
    
    return config