# ============================================================================
# FILE: db_utils.py
# Simplified database operations
# ============================================================================

import pyodbc
import pandas as pd
import json
from datetime import datetime


class DatabaseManager:
    """Handles database operations for validation results"""

    def __init__(self, server, database, schema_name, username="", password=""):
        """Initialize database connection parameters"""
        self.server = server
        self.database = database
        self.schema_name = schema_name
        self.username = username
        self.password = password
        self.conn = None
        self.cursor = None

    def get_connection_string(self):
        """Build connection string"""
        if self.username and self.password:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password}"
            )
        else:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        return conn_str

    def connect(self):
        """Connect to database"""
        try:
            conn_str = self.get_connection_string()
            self.conn = pyodbc.connect(conn_str)
            self.cursor = self.conn.cursor()
            print("✓ Connected to database")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {str(e)}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")

    def create_tables_if_not_exist(self):
        """Create output tables if they don't exist"""
        try:
            # Table 1: Scores
            self.cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dq_scores' AND schema_id = SCHEMA_ID('{self.schema_name}'))
            BEGIN
                CREATE TABLE {self.schema_name}.dq_scores (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    quarter_id VARCHAR(20) NOT NULL,
                    run_timestamp DATETIME NOT NULL,
                    source_database VARCHAR(200),
                    source_table VARCHAR(200),
                    bdx_month VARCHAR(50),
                    total_records INT NOT NULL,
                    overall_score DECIMAL(5,2) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    not_null_score DECIMAL(5,2),
                    not_null_points INT,
                    non_negative_score DECIMAL(5,2),
                    non_negative_points INT,
                    logical_score DECIMAL(5,2),
                    logical_points INT,
                    uniqueness_score DECIMAL(5,2),
                    uniqueness_points INT,
                    valid_date_score DECIMAL(5,2),
                    valid_date_points INT,
                    continuity_score DECIMAL(5,2),
                    continuity_points INT,
                    risk_score DECIMAL(5,2),
                    risk_points INT,
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
            self.cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dq_check_results' AND schema_id = SCHEMA_ID('{self.schema_name}'))
            BEGIN
                CREATE TABLE {self.schema_name}.dq_check_results (
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
            self.cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dq_issues' AND schema_id = SCHEMA_ID('{self.schema_name}'))
            BEGIN
                CREATE TABLE {self.schema_name}.dq_issues (
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
                    created_date DATETIME DEFAULT GETDATE()
                )
            END
            """)

            self.conn.commit()
            return True

        except Exception as e:
            print(f"Error creating tables: {str(e)}")
            return False

    def save_results(self, validation_results):
        """Save validation results to database"""
        try:
            # Create tables if needed
            self.create_tables_if_not_exist()

            # Save scores
            self._save_scores(validation_results)

            # Save check results
            self._save_check_results(validation_results)

            # Save issues
            self._save_issues(validation_results)

            self.conn.commit()
            print("✓ All results saved successfully")
            return True

        except Exception as e:
            self.conn.rollback()
            print(f"✗ Error saving results: {str(e)}")
            return False

    def _save_scores(self, results):
        """Save overall scores"""
        quarter_id = results['quarter_id']
        timestamp = results['timestamp']
        total_records = results['total_records']
        overall_score = results['overall_score']
        status = results['status']
        category_scores = results['category_scores']
        summary = results['summary']

        # Source info passed in via results metadata (optional, defaults to None)
        meta = results.get('metadata', {})
        source_database = meta.get('source_database')
        source_table = meta.get('source_table')
        bdx_month = meta.get('bdx_month')

        # Helper to safely get category score values
        def cat(name, key):
            return category_scores.get(name, {}).get(key)

        query = f"""
        INSERT INTO {self.schema_name}.dq_scores (
            quarter_id, run_timestamp, source_database, source_table, bdx_month,
            total_records, overall_score, status,
            not_null_score, not_null_points,
            non_negative_score, non_negative_points,
            logical_score, logical_points,
            uniqueness_score, uniqueness_points,
            valid_date_score, valid_date_points,
            continuity_score, continuity_points,
            risk_score, risk_points,
            total_checks, passed_checks, warning_checks, failed_checks, critical_issues_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.cursor.execute(query, (
            quarter_id, timestamp, source_database, source_table, bdx_month,
            total_records, overall_score, status,
            cat('Not Null', 'percentage'),      cat('Not Null', 'points_earned'),
            cat('Non-Negative', 'percentage'),   cat('Non-Negative', 'points_earned'),
            cat('Logical', 'percentage'),        cat('Logical', 'points_earned'),
            cat('Uniqueness', 'percentage'),     cat('Uniqueness', 'points_earned'),
            cat('Valid Date', 'percentage'),     cat('Valid Date', 'points_earned'),
            cat('Continuity', 'percentage'),     cat('Continuity', 'points_earned'),
            cat('Risk', 'percentage'),           cat('Risk', 'points_earned'),
            summary['total_checks'], summary['passed'], summary['warnings'],
            summary['failed'], summary['critical_issues_count']
        ))

    def _save_check_results(self, results):
        """Save detailed check results"""
        quarter_id = results['quarter_id']
        timestamp = results['timestamp']

        rows = []
        for category, subcategories in results['detailed_results'].items():
            for subcategory, checks in subcategories.items():
                for check in checks:
                    status = 'PASS' if check['points_earned'] == check['points_possible'] else \
                            'WARN' if check['points_earned'] > 0 else 'FAIL'

                    rows.append((
                        quarter_id, timestamp, category, subcategory,
                        check['check_id'], check['check_name'], check['priority'],
                        check.get('pass_rate', 0), check.get('passed', 0),
                        check.get('failed', 0), check.get('total', 0),
                        check['points_possible'], check['points_earned'], status,
                        json.dumps(check.get('failed_records', [])[:100])
                    ))

        query = f"""
        INSERT INTO {self.schema_name}.dq_check_results (
            quarter_id, run_timestamp, category, subcategory,
            check_id, check_name, priority, pass_rate, passed_count, failed_count, total_count,
            points_possible, points_earned, status, failed_records_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.cursor.executemany(query, rows)

    def _save_issues(self, results):
        """Save issues (failed checks)"""
        quarter_id = results['quarter_id']
        timestamp = results['timestamp']

        rows = []
        for category, subcategories in results['detailed_results'].items():
            for subcategory, checks in subcategories.items():
                for check in checks:
                    if check['points_earned'] < check['points_possible']:
                        rows.append((
                            quarter_id, timestamp, category, subcategory,
                            check['check_id'], check['check_name'], check['priority'],
                            check.get('pass_rate', 0), check.get('failed', 0), check.get('total', 0),
                            json.dumps(check.get('failed_records', [])[:100])
                        ))

        if not rows:
            return

        query = f"""
        INSERT INTO {self.schema_name}.dq_issues (
            quarter_id, run_timestamp, category, subcategory,
            check_id, check_name, priority, pass_rate, failed_count, total_count,
            failed_records_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.cursor.executemany(query, rows)