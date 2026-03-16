# ============================================================================
# FILE: streamlit_utils.py
# Helper functions for Streamlit app
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
        query = f"SELECT TOP {limit} * FROM {table}"
        df = pd.read_sql(query, conn_str)
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


def get_record_count(server, database, table, from_bdx, to_bdx, username="", password=""):
    """Get record count for date range"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        
        # Find bdx column name
        columns = get_columns_from_table(server, database, table, username, password)
        bdx_col = next((col for col in columns if 'bdx' in col.lower()), 'bdx_month')
        
        query = f"""
        SELECT COUNT(*) 
        FROM {table} 
        WHERE {bdx_col} >= ? AND {bdx_col} <= ?
        """
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query, (from_bdx, to_bdx))
        count = cursor.fetchone()[0]
        conn.close()
        
        return count
    except:
        return 0


def load_data_from_db(server, database, table, from_bdx, to_bdx, username="", password=""):
    """Load data from database with date filter"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        
        # Find bdx column
        columns = get_columns_from_table(server, database, table, username, password)
        bdx_col = next((col for col in columns if 'bdx' in col.lower()), 'bdx_month')
        
        if from_bdx and to_bdx:
            query = f"SELECT * FROM {table} WHERE {bdx_col} >= ? AND {bdx_col} <= ?"
            df = pd.read_sql(query, conn_str, params=[from_bdx, to_bdx])
        else:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, conn_str)
        
        return df
    except Exception as e:
        raise Exception(f"Error loading data: {str(e)}")


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


def create_output_tables(server, database, schema_name, username="", password=""):
    """Create output tables if they don't exist"""
    try:
        conn_str = get_connection_string(server, database, username, password)
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Table 1: Scores
        cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dq_scores' AND schema_id = SCHEMA_ID('{schema_name}'))
        BEGIN
            CREATE TABLE {schema_name}.dq_scores (
                id INT IDENTITY(1,1) PRIMARY KEY,
                quarter_id VARCHAR(10) NOT NULL,
                run_timestamp DATETIME NOT NULL,
                total_records INT NOT NULL,
                overall_score DECIMAL(5,2) NOT NULL,
                status VARCHAR(20) NOT NULL,
                completeness_score DECIMAL(5,2),
                completeness_points INT,
                consistency_score DECIMAL(5,2),
                consistency_points INT,
                reasonableness_score DECIMAL(5,2),
                reasonableness_points INT,
                data_quality_score DECIMAL(5,2),
                data_quality_points INT,
                temporal_logic_score DECIMAL(5,2),
                temporal_logic_points INT,
                total_checks INT,
                passed_checks INT,
                warning_checks INT,
                failed_checks INT,
                critical_issues_count INT,
                created_date DATETIME DEFAULT GETDATE()
            )
        END
        """)
        
        # Table 2: Check Results
        cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dq_check_results' AND schema_id = SCHEMA_ID('{schema_name}'))
        BEGIN
            CREATE TABLE {schema_name}.dq_check_results (
                id INT IDENTITY(1,1) PRIMARY KEY,
                quarter_id VARCHAR(10) NOT NULL,
                run_timestamp DATETIME NOT NULL,
                category VARCHAR(50) NOT NULL,
                subcategory VARCHAR(50) NOT NULL,
                check_id VARCHAR(10) NOT NULL,
                check_name VARCHAR(200) NOT NULL,
                priority VARCHAR(20) NOT NULL,
                pass_rate DECIMAL(5,2),
                passed_count INT,
                failed_count INT,
                total_count INT,
                points_possible INT,
                points_earned INT,
                status VARCHAR(10),
                failed_records_json NVARCHAR(MAX),
                created_date DATETIME DEFAULT GETDATE()
            )
        END
        """)
        
        # Table 3: Issues
        cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dq_issues' AND schema_id = SCHEMA_ID('{schema_name}'))
        BEGIN
            CREATE TABLE {schema_name}.dq_issues (
                id INT IDENTITY(1,1) PRIMARY KEY,
                quarter_id VARCHAR(10) NOT NULL,
                run_timestamp DATETIME NOT NULL,
                category VARCHAR(50) NOT NULL,
                subcategory VARCHAR(50) NOT NULL,
                check_id VARCHAR(10) NOT NULL,
                check_name VARCHAR(200) NOT NULL,
                priority VARCHAR(20) NOT NULL,
                pass_rate DECIMAL(5,2),
                failed_count INT,
                total_count INT,
                failed_records_json NVARCHAR(MAX),
                resolved BIT DEFAULT 0,
                resolved_date DATETIME NULL,
                resolved_by VARCHAR(100) NULL,
                resolution_notes NVARCHAR(MAX) NULL,
                created_date DATETIME DEFAULT GETDATE()
            )
        END
        """)
        
        conn.commit()
        conn.close()
        return True
    
    except Exception as e:
        return False


# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

def load_default_config():
    """Load default configuration with 10 checks"""
    config = {
        'column_mappings': {
            'policy_number': ['Policy Number', 'Policy_Number', 'PolicyNumber'],
            'claim_number': ['Claim Number', 'Claim_Number', 'ClaimNumber'],
            'accident_date': ['Accident Date', 'Accident_Date', 'AccidentDate'],
            'accident_year': ['Accident Year', 'Accident_Year', 'AccidentYear'],
            'notification_date': ['Notification Date', 'Notification_Date'],
            'gross_paid': ['Gross Paid', 'Gross_Paid'],
            'gross_incurred': ['Gross Incurred', 'Gross_Incurred'],
            'gross_estimate': ['Gross Estimate', 'Gross_Estimate'],
            'net_paid': ['Net Paid', 'Net_Paid'],
            'net_incurred': ['Net Incurred', 'Net_Incurred']
        },
        
        'validation_checks': {
            'completeness': {
                'critical_fields': [
                    {
                        'id': '1.1',
                        'name': 'Policy Number Complete',
                        'field': 'policy_number',
                        'check_type': 'not_null',
                        'priority': 'critical',
                        'points': 3,
                        'scoring': {100: 3, 'default': 0}
                    },
                    {
                        'id': '1.2',
                        'name': 'Claim Number Complete',
                        'field': 'claim_number',
                        'check_type': 'not_null',
                        'priority': 'critical',
                        'points': 3,
                        'scoring': {100: 3, 'default': 0}
                    }
                ]
            },
            
            'consistency': {
                'financial': [
                    {
                        'id': '2.1',
                        'name': 'Gross Incurred Formula',
                        'check_type': 'formula',
                        'formula': 'gross_incurred = gross_paid + gross_estimate',
                        'tolerance': 1,
                        'priority': 'critical',
                        'points': 3,
                        'scoring': {98: 3, 95: 2, 90: 1, 'default': 0}
                    },
                    {
                        'id': '2.3',
                        'name': 'Net <= Gross Relationship',
                        'check_type': 'comparison',
                        'comparisons': ['net_paid <= gross_paid', 'net_incurred <= gross_incurred'],
                        'priority': 'critical',
                        'points': 3,
                        'scoring': {99: 3, 97: 2, 95: 1, 'default': 0}
                    }
                ]
            },
            
            'reasonableness': {
                'value_ranges': [
                    {
                        'id': '3.1',
                        'name': 'Non-Negative Financial Values',
                        'check_type': 'range',
                        'fields': ['gross_paid', 'gross_incurred', 'net_paid', 'net_incurred'],
                        'min_value': 0,
                        'priority': 'critical',
                        'points': 3,
                        'scoring': {99.5: 3, 99: 2, 98: 1, 'default': 0}
                    },
                    {
                        'id': '3.2',
                        'name': 'Accident Year Reasonable Range',
                        'check_type': 'range',
                        'field': 'accident_year',
                        'min_value': 'current_year - 15',
                        'max_value': 'current_year',
                        'priority': 'high',
                        'points': 2,
                        'scoring': {99.9: 2, 99: 1, 'default': 0}
                    }
                ]
            },
            
            'data_quality': {
                'duplicates': [
                    {
                        'id': '4.1',
                        'name': 'Unique Claim Numbers',
                        'check_type': 'duplicates',
                        'field': 'claim_number',
                        'priority': 'critical',
                        'points': 3,
                        'scoring': {100: 3, 99: 2, 97: 1, 'default': 0}
                    }
                ],
                'format': [
                    {
                        'id': '4.3',
                        'name': 'Valid Date Formats',
                        'check_type': 'date_format',
                        'fields': ['accident_date', 'notification_date'],
                        'priority': 'high',
                        'points': 2,
                        'scoring': {100: 2, 99: 1, 'default': 0}
                    }
                ]
            },
            
            'temporal_logic': {
                'date_sequence': [
                    {
                        'id': '5.2',
                        'name': 'Accident Before Notification',
                        'check_type': 'date_comparison',
                        'comparison': 'accident_date <= notification_date',
                        'priority': 'critical',
                        'points': 3,
                        'scoring': {98: 3, 95: 2, 92: 1, 'default': 0}
                    },
                    {
                        'id': '5.6',
                        'name': 'Accident Year Matches Date',
                        'check_type': 'custom',
                        'function': 'check_ay_matches_date',
                        'priority': 'high',
                        'points': 2,
                        'scoring': {99.9: 2, 99: 1, 'default': 0}
                    }
                ]
            }
        },
        
        'scoring_thresholds': {
            'excellent': 95,
            'good': 85,
            'fair': 75,
            'poor': 65
        }
    }
    
    return config


def prepare_final_config(config, column_mappings, session_state):
    """Prepare final configuration with all settings"""
    final_config = config.copy()
    
    # Update column mappings with user selections
    final_config['column_mappings'] = column_mappings
    
    # Add metadata
    final_config['metadata'] = {
        'business_line': session_state.business_line,
        'quarter': session_state.quarter,
        'created_date': datetime.now().isoformat(),
        'data_source': session_state.data_source
    }
    
    if session_state.data_source == "Database":
        final_config['metadata']['claims_db'] = session_state.claims_db
        final_config['metadata']['claims_table'] = session_state.claims_table
        final_config['metadata']['date_range'] = {
            'from': session_state.from_bdx,
            'to': session_state.to_bdx
        }
    
    if session_state.use_risk_db:
        final_config['metadata']['risk_db'] = session_state.risk_db
        final_config['metadata']['risk_table'] = session_state.risk_table
    
    return final_config


# ============================================================================
# VALIDATION PIPELINE
# ============================================================================

def run_validation_pipeline(data, config, column_mappings, quarter_id, risk_data=None, progress_callback=None):
    """Run complete validation pipeline"""
    from validator import DataQualityValidator
    from column_mapper import ColumnMapper
    
    # Create temporary config with mappings
    temp_config = config.copy()
    temp_config['column_mappings'] = {
        k: [v] for k, v in column_mappings.items()
    }
    
    # Save temporary config
    with open('temp_config.yaml', 'w') as f:
        yaml.dump(temp_config, f)
    
    # Initialize validator
    validator = DataQualityValidator('temp_config.yaml')
    
    # Run validation
    results = validator.validate(data, quarter_id=quarter_id)
    
    if progress_callback:
        progress_callback(100)
    
    return results


# ============================================================================
# RESULTS EXPORT
# ============================================================================

def save_results_to_sql(results, server, database, schema_name, username="", password=""):
    """Save validation results to SQL database"""
    try:
        # Create tables if they don't exist
        create_output_tables(server, database, schema_name, username, password)
        
        # Use DatabaseManager from db_utils.py
        from db_utils import DatabaseManager
        
        config = {
            'database': {
                'server': server,
                'database': database,
                'driver': 'ODBC Driver 17 for SQL Server',
                'username': username,
                'password': password
            },
            'database_tables': {
                'scores': f'{schema_name}.dq_scores',
                'check_results': f'{schema_name}.dq_check_results',
                'issues': f'{schema_name}.dq_issues'
            }
        }
        
        db = DatabaseManager(config)
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
                        'Priority': check['priority'],
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