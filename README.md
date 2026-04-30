# ============================================================================
# FILE: requirements.txt
# Dependencies for Streamlit App
# ============================================================================

"""
streamlit==1.29.0
pandas==2.1.3
pyodbc==5.0.1
pyyaml==6.0.1
plotly==5.18.0
openpyxl==3.1.2
xlsxwriter==3.1.9
"""

# ============================================================================
# FILE: run_app.bat (Windows)
# Easy launcher for Windows users
# ============================================================================

"""
@echo off
echo Starting Data Quality Validation System...
echo.
streamlit run app.py
pause
"""

# ============================================================================
# FILE: run_app.sh (Linux/Mac)
# Easy launcher for Linux/Mac users
# ============================================================================

"""
#!/bin/bash
echo "Starting Data Quality Validation System..."
streamlit run app.py
"""

# ============================================================================
# INSTALLATION INSTRUCTIONS
# ============================================================================

"""
# SETUP INSTRUCTIONS

## 1. Install Python
- Download Python 3.8 or higher from python.org
- Make sure to check "Add Python to PATH" during installation

## 2. Install Dependencies
Open command prompt/terminal and run:

```bash
pip install -r requirements.txt
```

## 3. Install ODBC Driver
Download and install:
- ODBC Driver 17 for SQL Server
- Link: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

## 4. File Structure
Make sure you have these files in same folder:

data_quality_system/
├── app.py                    # Main Streamlit app
├── streamlit_utils.py        # Helper functions
├── validator.py              # From previous code
├── column_mapper.py          # From previous code
├── checks.py                 # From previous code
├── db_utils.py               # From previous code
├── requirements.txt          # Dependencies
├── run_app.bat              # Windows launcher
└── run_app.sh               # Linux/Mac launcher

## 5. Run the Application

### Windows:
Double-click run_app.bat
OR
Open command prompt and type: streamlit run app.py

### Linux/Mac:
chmod +x run_app.sh
./run_app.sh
OR
streamlit run app.py

## 6. Access the App
- Browser will open automatically
- If not, go to: http://localhost:8501

## 7. First Time Setup
1. Enter business line name and quarter
2. Connect to your database (default: 14324u15VT)
3. Select input table
4. Choose date range
5. Map columns
6. Review checks
7. Run validation
8. Save results to SQL

## TROUBLESHOOTING

### Error: "No module named 'streamlit'"
Solution: pip install streamlit

### Error: "Can't connect to database"
Solution: 
- Check server name (14324u15VT)
- Verify Windows Authentication is enabled
- Install ODBC Driver 17

### Error: "Column mapping not found"
Solution:
- Check that column names in your data match the mappings
- Use the dropdown to manually select correct columns

### Error: "Permission denied to create schema"
Solution:
- Make sure you have CREATE SCHEMA permission
- Or use existing schema
- Or click "Proceed Anyway"

## SUPPORT
For issues, contact: [Your Email/Team]
"""

# ============================================================================
# FILE: README_STREAMLIT.md
# User guide for the Streamlit app
# ============================================================================

"""
# Data Quality Validation System - User Guide

## Overview
This Streamlit application provides a user-friendly interface for running data quality validation checks on claims data before LDF projection modeling.

## Features
- ✅ Automatic data quality scoring (0-100)
- ✅ 10 configurable validation checks
- ✅ Support for database and Excel input
- ✅ Date range filtering
- ✅ Column mapping with auto-detection
- ✅ Customizable scoring thresholds
- ✅ Interactive results dashboard
- ✅ Export to Excel and SQL
- ✅ Save/load configurations for reuse

## Step-by-Step Guide

### Step 1: Basic Information
- Enter your business line name (e.g., "Auto", "Workers_Comp")
- Enter quarter (auto-populated with current quarter)
- Verify server name (default: 14324u15VT)
- Choose authentication method

### Step 2: Input Data
**Option A: Database**
- Enter database name
- Test connection
- Select table from dropdown
- Select date range (from/to bdx_month)
- Optionally: Add risk database for check #11

**Option B: Excel File**
- Upload .xlsx or .xls file
- Preview data
- Select date column if available

### Step 3: Output Setup
- Enter output database name
- Test connection
- Create schema (named after business line)
- Check for existing validations

### Step 4: Configuration
**Option A: Use Default**
- 10 pre-configured checks
- Standard scoring thresholds
- Best for first-time users

**Option B: Upload Existing**
- Upload previously saved .yaml config
- Reuse settings from prior validations
- Modify as needed

### Step 5: Column Mapping
- System auto-detects column matches
- Review all mappings
- Use dropdowns to correct any mismatches
- All required columns must be mapped

### Step 6: Check Settings
- Enable/disable specific checks
- Modify scoring thresholds
- Adjust point values
- View check descriptions

### Step 7: Review & Confirm
- Review all settings
- Save configuration file for future use
- Confirm and start validation

### Step 8: Results
- View overall score and status
- Explore category breakdowns
- Review detailed check results
- Filter by status or category
- Download Excel report
- Save results to SQL database
- Connect to Power BI

## The 10 Standard Checks

### Completeness (2 checks)
1. **Policy Number Complete** - All records have policy numbers
2. **Claim Number Complete** - All records have claim numbers

### Consistency (2 checks)
3. **Gross Incurred Formula** - Incurred = Paid + Estimate
4. **Net ≤ Gross Relationship** - Net values don't exceed gross

### Reasonableness (2 checks)
5. **Non-Negative Values** - Financial fields are positive
6. **Accident Year Range** - Years are within valid range

### Data Quality (2 checks)
7. **Unique Claim Numbers** - No duplicate claims
8. **Valid Date Formats** - Dates parse correctly

### Temporal Logic (2 checks)
9. **Accident Before Notification** - Logical date sequence
10. **Accident Year Matches Date** - Consistency check

### Optional (1 check)
11. **GWP Reconciliation** - Total incurred vs risk GWP (requires risk DB)

## Scoring System

**Overall Score = Sum of all check points (max 100)**

### Score Interpretation:
- **95-100 (EXCELLENT):** ✅ Proceed with full confidence
- **85-94 (GOOD):** ✅ Proceed with minor caveats
- **75-84 (FAIR):** ⚠️ Review issues before proceeding
- **65-74 (POOR):** ⚠️ Address significant issues
- **0-64 (CRITICAL):** ❌ Do not proceed

## Saving & Reusing Configurations

### Save Configuration
1. Complete steps 1-6
2. In Step 7, click "Download Configuration File"
3. Save .yaml file to your computer
4. File name: `{BusinessLine}_{Quarter}_config.yaml`

### Reuse Configuration
1. In Step 4, choose "Upload Existing Configuration"
2. Upload your saved .yaml file
3. Review and modify settings if needed
4. Proceed to validation

## Power BI Integration

### After Saving Results to SQL:
1. Open Power BI Desktop
2. Get Data → SQL Server
3. Server: `14324u15VT`
4. Database: `[Your Output DB]`
5. Import tables:
   - `[BusinessLine].dq_scores`
   - `[BusinessLine].dq_check_results`
   - `[BusinessLine].dq_issues`
6. Create visualizations

### Recommended Visuals:
- Overall score gauge
- Category scores bar chart
- Trend line (if multiple quarters)
- Issues table with filters
- Pass/fail/warning breakdown

## Tips & Best Practices

### For Better Performance:
- Use database input for large datasets (>100K rows)
- Excel is good for smaller datasets or testing
- Filter date range to only what you need

### For Accurate Results:
- Verify column mappings carefully
- Test with sample data first
- Review failed checks in detail
- Document any overrides or exceptions

### For Reusability:
- Save configuration after first successful run
- Use consistent naming: `BusinessLine_Quarter_config.yaml`
- Store configs in shared folder for team access
- Update config when data structure changes

## Common Workflows

### First Time for New Business Line:
1. Use default configuration
2. Map columns
3. Run validation
4. Save configuration
5. Use saved config for future quarters

### Quarterly Validation:
1. Upload previous quarter's config
2. Update quarter name
3. Select new date range
4. Verify mappings (usually unchanged)
5. Run validation
6. Compare to previous quarter

### Troubleshooting Data Issues:
1. Run validation
2. Review failed checks
3. Download Excel report
4. Investigate specific failed records
5. Fix data issues
6. Re-run validation

## FAQ

**Q: Can I run multiple business lines at once?**
A: No, run separately and save different configs for each.

**Q: What if my column names change?**
A: Update the column mappings in Step 5. System will auto-detect if possible.

**Q: Can I add custom checks?**
A: Not in the UI, but you can modify the code (checks.py) and config.

**Q: How long does validation take?**
A: Typically 30 seconds to 2 minutes depending on data size.

**Q: Can I skip the risk database?**
A: Yes, it's optional. Only needed for check #11 (GWP reconciliation).

**Q: What if validation fails?**
A: Review the failed checks, fix data issues, and re-run.

**Q: Can I change scoring thresholds?**
A: Yes, in Step 6 you can modify all scoring rules.

**Q: Where are results stored?**
A: In SQL database under schema: `[BusinessLine].dq_scores`

**Q: Can I export results?**
A: Yes, download Excel report or query SQL tables.

**Q: How do I compare quarters?**
A: Query SQL database for historical data or use Power BI.
"""

### how to run
# 1. Install dependencies
pip install streamlit pandas pyodbc pyyaml plotly openpyxl xlsxwriter

# 2. Run the app
streamlit run app.py

# 3. App opens in browser at http://localhost:8501


Saving Results to SQL Server — Implementation Guide
This section describes how the SQL output would be implemented. The code for this already exists in db_utils.py and streamlit_utils.py — it just needs to be connected and tested.

What the Code Already Has
db_utils.py has a fully written DatabaseManager class that:

Connects to SQL Server using PyODBC
Auto-creates three output tables if they don't exist
Inserts validation results into those tables

streamlit_utils.py has save_results_to_sql() which instantiates DatabaseManager and calls save_results().
app.py Step 8 already has the Save to SQL Database button wired up to call save_results_to_sql().
So the plumbing is all there — it was not tested end-to-end due to time constraints.

The Three Output Tables
All three tables are created under the business line schema (e.g. Auto.dq_scores). They are created automatically on first save — no manual table creation needed.
dq_scores — one row per validation run. Stores the headline numbers.
ColumnDescriptionquarter_idBusiness line + quarter e.g. Auto_2024_Q3run_timestampWhen the validation was runtotal_recordsTotal records in the BDX monthoverall_scoreFinal score out of 100statusEXCELLENT / GOOD / FAIR / POOR / CRITICALcompleteness_scoreCategory score %consistency_scoreCategory score %reasonableness_scoreCategory score %data_quality_scoreCategory score %temporal_logic_scoreCategory score %total_checksTotal checks runpassed_checksChecks that got full pointswarning_checksChecks that got partial pointsfailed_checksChecks that got zero points
dq_check_results — one row per check per run. Full detail.
ColumnDescriptionquarter_idBusiness line + quarterrun_timestampWhen the validation was runcategorye.g. Completeness, Consistencysubcategorye.g. Required Fields, Logical Rulescheck_ide.g. 1.1, 3.3check_nameFull check namepriorityHigh / Medium / Lowpass_rate% of records that passedpassed_countNumber of records passedfailed_countNumber of records failedpoints_possibleMax points for this checkpoints_earnedActual points earnedstatusPASS / WARN / FAILfailed_records_jsonFirst 100 failed records stored as JSON
dq_issues — same as dq_check_results but only rows where points_earned < points_possible. This is the table to use for tracking and resolving issues over time.

How to Implement and Test
Step 1 — Create the output database
On the SQL Server instance, create a database to store results:
sqlCREATE DATABASE DataQualityDB
Step 2 — Create the schema
The app does this automatically in Step 3 when the user clicks "Create Schema Now". Alternatively run manually:
sqlCREATE SCHEMA Auto
Step 3 — Verify ODBC connectivity
On the machine running the app, confirm the ODBC Driver 17 for SQL Server is installed. Open Anaconda prompt and test:
pythonimport pyodbc
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=your_server;"
    "DATABASE=DataQualityDB;"
    "Trusted_Connection=yes;"
)
print("Connected")
Step 4 — Run a validation and click Save to SQL
In Step 8 of the app, after a successful validation run, click the Save to SQL Database button. The app will:

Call save_results_to_sql() in streamlit_utils.py
Which calls DatabaseManager.connect() in db_utils.py
Then calls create_tables_if_not_exist() — creates tables on first run
Then calls save_results() — inserts into all three tables
Returns success or failure message
