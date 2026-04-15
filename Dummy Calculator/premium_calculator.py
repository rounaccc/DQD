# ============================================================================
# FILE: premium_calculator.py
# Insurance Premium Calculator - Streamlit App
# ============================================================================

import streamlit as st
import pandas as pd
import pyodbc
import io

st.set_page_config(
    page_title="Premium Calculator",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

.stApp {
    background: #f4f1ec;
}

h1, h2, h3 {
    font-family: 'DM Serif Display', serif;
}

/* Header */
.app-header {
    background: #1a1a2e;
    color: #f4f1ec;
    padding: 2.5rem 3rem;
    border-radius: 16px;
    margin-bottom: 2rem;
    display: flex;
    align-items: center;
    gap: 1.5rem;
}
.app-header h1 {
    font-family: 'DM Serif Display', serif;
    font-size: 2.2rem;
    margin: 0;
    color: #f4f1ec;
}
.app-header p {
    margin: 0.3rem 0 0;
    color: #a0a0c0;
    font-size: 0.95rem;
    font-weight: 300;
}

/* Cards */
.card {
    background: white;
    border-radius: 12px;
    padding: 1.8rem;
    margin-bottom: 1.5rem;
    border: 1px solid #e8e4dd;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}
.card-title {
    font-family: 'DM Serif Display', serif;
    font-size: 1.15rem;
    color: #1a1a2e;
    margin-bottom: 1.2rem;
    padding-bottom: 0.8rem;
    border-bottom: 2px solid #f4f1ec;
}

/* Result table */
.result-header {
    background: #1a1a2e;
    color: white;
    padding: 1.5rem 2rem;
    border-radius: 12px 12px 0 0;
    font-family: 'DM Serif Display', serif;
    font-size: 1.2rem;
}
.result-row {
    background: white;
    padding: 1rem 2rem;
    border-left: 1px solid #e8e4dd;
    border-right: 1px solid #e8e4dd;
    border-bottom: 1px solid #e8e4dd;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.result-row:last-child {
    border-radius: 0 0 12px 12px;
}
.referral-badge {
    background: #ff4757;
    color: white;
    padding: 0.25rem 0.8rem;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}
.none-badge {
    background: #2ed573;
    color: white;
    padding: 0.25rem 0.8rem;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 600;
    letter-spacing: 0.05em;
}
.metric-value {
    font-size: 1.1rem;
    font-weight: 600;
    color: #1a1a2e;
}
.metric-label {
    font-size: 0.8rem;
    color: #888;
    font-weight: 300;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

/* Step badge */
.step-badge {
    background: #1a1a2e;
    color: #f4f1ec;
    width: 28px;
    height: 28px;
    border-radius: 50%;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 0.85rem;
    font-weight: 600;
    margin-right: 0.6rem;
}

div[data-testid="stSelectbox"] label,
div[data-testid="stMultiSelect"] label {
    font-weight: 500;
    color: #1a1a2e;
}

.stButton > button {
    background: #1a1a2e;
    color: white;
    border: none;
    border-radius: 8px;
    padding: 0.6rem 1.8rem;
    font-family: 'DM Sans', sans-serif;
    font-weight: 500;
    font-size: 0.95rem;
    cursor: pointer;
    transition: opacity 0.2s;
}
.stButton > button:hover {
    opacity: 0.85;
    color: white;
}
</style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
    <div>🏠</div>
    <div>
        <h1>Insurance Premium Calculator</h1>
        <p>Calculate final building & contents premiums with referral flagging</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Helper functions ──────────────────────────────────────────────────────────

def get_connection_string(server, database, username="", password=""):
    if username and password:
        return (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};DATABASE={database};"
                f"UID={username};PWD={password}")
    return (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;")

def get_tables(server, database, username="", password=""):
    try:
        conn = pyodbc.connect(get_connection_string(server, database, username, password))
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")
        tables = [r[0] for r in cursor.fetchall()]
        conn.close()
        return tables
    except:
        return []

def load_table(server, database, table, username="", password=""):
    conn = pyodbc.connect(get_connection_string(server, database, username, password))
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    return df

def calculate_premiums(df, mappings):
    """Apply calculations to the mapped dataframe."""
    r = df.copy()

    bp   = pd.to_numeric(r[mappings['building_prem']], errors='coerce')
    cp   = pd.to_numeric(r[mappings['contents_prem']], errors='coerce')
    fl   = pd.to_numeric(r[mappings['flood_loading']], errors='coerce')
    tl   = pd.to_numeric(r[mappings['theft_loading']], errors='coerce')
    sl   = pd.to_numeric(r[mappings['subsidence_loading']], errors='coerce')
    subs = pd.to_numeric(r[mappings['subsidence']], errors='coerce')

    r['Final Building Premium'] = (bp * fl * tl * sl).round(3)
    r['Final Contents Premium'] = (cp * fl * tl * sl).round(3)
    r['Referral Flag'] = subs.apply(lambda x: 'Referral' if pd.notna(x) and x > 8 else 'None')

    return r

# ── Session state ─────────────────────────────────────────────────────────────
for key in ['df', 'mappings']:
    if key not in st.session_state:
        st.session_state[key] = None

# ============================================================================
# STEP 1 — Data Source
# ============================================================================
st.markdown('<div class="card"><div class="card-title"><span class="step-badge">1</span>Data Source</div>', unsafe_allow_html=True)

source = st.radio("Select data source", ["SQL Server Database", "Excel File"], horizontal=True, label_visibility="collapsed")

df_raw = None

if source == "SQL Server Database":
    col1, col2 = st.columns([2, 1])
    with col1:
        server = st.text_input("Server Name", value="EMEUKHC4DB15VT", placeholder="e.g. MYSERVER\\SQLEXPRESS")
    with col2:
        use_sql_auth = st.checkbox("SQL Authentication")

    username = password = ""
    if use_sql_auth:
        c1, c2 = st.columns(2)
        with c1:
            username = st.text_input("Username")
        with c2:
            password = st.text_input("Password", type="password")

    database = st.text_input("Database Name", placeholder="e.g. InsuranceDB")

    if database and server:
        tables = get_tables(server, database, username, password)
        if tables:
            table = st.selectbox("Select Table", options=tables)
            if st.button("Load Table"):
                try:
                    with st.spinner("Loading data..."):
                        st.session_state.df = load_table(server, database, table, username, password)
                        st.session_state.mappings = None
                    st.success(f"✓ Loaded {len(st.session_state.df):,} rows from **{table}**")
                except Exception as e:
                    st.error(f"Error loading table: {e}")
        elif database:
            st.warning("No tables found — check database name and connection.")

else:  # Excel
    uploaded = st.file_uploader("Upload Excel file", type=['xlsx', 'xls'])
    if uploaded:
        try:
            st.session_state.df = pd.read_excel(uploaded)
            st.session_state.mappings = None
            st.success(f"✓ Loaded {len(st.session_state.df):,} rows from **{uploaded.name}**")
        except Exception as e:
            st.error(f"Error reading file: {e}")

st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# STEP 2 — Column Mapping
# ============================================================================
if st.session_state.df is not None:
    df = st.session_state.df
    cols = [''] + list(df.columns)

    st.markdown('<div class="card"><div class="card-title"><span class="step-badge">2</span>Column Mapping</div>', unsafe_allow_html=True)
    st.caption("Map the logical fields to your table's actual column names.")

    # Auto-match helpers
    def best_match(candidates):
        for c in candidates:
            for col in df.columns:
                if c.lower() in col.lower():
                    return col
        return ''

    def selectbox_idx(val):
        return cols.index(val) if val in cols else 0

    logical_fields = {
        'postcode':            ('Postcode',            ['postcode', 'post_code', 'postal']),
        'address':             ('Address',             ['address', 'addr']),
        'flood_score':         ('Flood Score',         ['flood score', 'flood_score', 'floodscore']),
        'theft':               ('Theft Score',         ['theft']),
        'subsidence':          ('Subsidence Score',    ['subsidence', 'subsid']),
        'building_prem':       ('Building Premium',    ['building prem', 'building_prem', 'bldg']),
        'contents_prem':       ('Contents Premium',    ['contents prem', 'contents_prem', 'cont']),
        'flood_loading':       ('Flood Loading',       ['flood loading', 'flood_loading']),
        'theft_loading':       ('Theft Loading',       ['theft loading', 'theft_loading']),
        'subsidence_loading':  ('Subsidence Loading',  ['subsidence loading', 'subsidence_load']),
    }

    defaults = {k: best_match(v[1]) for k, v in logical_fields.items()}

    col1, col2 = st.columns(2)
    mappings = {}
    for i, (key, (label, _)) in enumerate(logical_fields.items()):
        with col1 if i % 2 == 0 else col2:
            mappings[key] = st.selectbox(label, options=cols, index=selectbox_idx(defaults[key]), key=f"map_{key}")

    mapped_required = all(mappings[k] for k in logical_fields)
    if not mapped_required:
        st.warning("⚠️ Please map all fields to proceed.")
    else:
        st.session_state.mappings = mappings
        st.caption("✓ All fields mapped")

    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# STEP 3 — Postcode & Address Selection
# ============================================================================
if st.session_state.df is not None and st.session_state.mappings is not None:
    df = st.session_state.df
    mappings = st.session_state.mappings

    st.markdown('<div class="card"><div class="card-title"><span class="step-badge">3</span>Select Postcode & Address</div>', unsafe_allow_html=True)

    postcodes = sorted(df[mappings['postcode']].dropna().unique().tolist())
    selected_postcode = st.selectbox("Postcode", options=postcodes)

    filtered = df[df[mappings['postcode']] == selected_postcode]
    addresses = sorted(filtered[mappings['address']].dropna().unique().tolist())

    selected_addresses = st.multiselect(
        "Address(es)",
        options=addresses,
        default=addresses[:1] if addresses else [],
        help="Select one or more addresses to calculate premiums for"
    )

    st.markdown('</div>', unsafe_allow_html=True)

    # ============================================================================
    # STEP 4 — Results
    # ============================================================================
    if selected_addresses:
        subset = filtered[filtered[mappings['address']].isin(selected_addresses)].copy()
        result_df = calculate_premiums(subset, mappings)

        st.markdown('<div class="card"><div class="card-title"><span class="step-badge">4</span>Results</div>', unsafe_allow_html=True)

        # Summary metrics
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Addresses Selected", len(selected_addresses))
        with m2:
            total_bp = result_df['Final Building Premium'].sum()
            st.metric("Total Building Premium", f"£{total_bp:,.2f}")
        with m3:
            total_cp = result_df['Final Contents Premium'].sum()
            st.metric("Total Contents Premium", f"£{total_cp:,.2f}")

        referrals = (result_df['Referral Flag'] == 'Referral').sum()
        if referrals > 0:
            st.warning(f"⚠️ {referrals} address(es) flagged for referral (Subsidence Score > 8)")

        st.markdown("---")

        # Results table
        display_cols = [
            mappings['postcode'],
            mappings['address'],
            mappings['flood_score'],
            mappings['theft'],
            mappings['subsidence'],
            mappings['building_prem'],
            mappings['contents_prem'],
            'Final Building Premium',
            'Final Contents Premium',
            'Referral Flag'
        ]

        display_df = result_df[display_cols].copy()
        display_df = display_df.rename(columns={
            mappings['postcode']: 'Postcode',
            mappings['address']: 'Address',
            mappings['flood_score']: 'Flood Score',
            mappings['theft']: 'Theft',
            mappings['subsidence']: 'Subsidence',
            mappings['building_prem']: 'Bldg Prem',
            mappings['contents_prem']: 'Cont Prem',
        })

        # Highlight referral rows
        def highlight_referral(row):
            if row['Referral Flag'] == 'Referral':
                return ['background-color: #fff5f5'] * len(row)
            return [''] * len(row)

        styled = display_df.style.apply(highlight_referral, axis=1)\
            .format({
                'Final Building Premium': '£{:,.3f}',
                'Final Contents Premium': '£{:,.3f}',
            })

        st.dataframe(styled, use_container_width=True, hide_index=True)

        # Export
        st.markdown("---")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            display_df.to_excel(writer, index=False, sheet_name='Premium Results')
        output.seek(0)

        st.download_button(
            label="📥 Download Results (Excel)",
            data=output,
            file_name=f"premiums_{selected_postcode.replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.markdown('</div>', unsafe_allow_html=True)