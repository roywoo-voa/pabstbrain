import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PabstBrain — Production",
    page_icon="🏭",
    layout="wide"
)

# ── BigQuery connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials)

client = get_client()

# ── Data loader ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_gold():
    query = """
        SELECT *
        FROM `pabst_mis.gold_production_summary`
        ORDER BY production_month_key, brand
    """
    return client.query(query).to_dataframe()

df = load_gold()
# ── Header ───────────────────────────────────────────────────────────────────
st.title("🏭 Production Intelligence")
st.caption("Pabst Labs · Materials cost incurred · Data current Jan 2024 – Sep 2025 (Oct 2025–present pending Roshi correction)")

st.divider()

# ── Global filters ────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    brands = ["All Brands"] + sorted(df["brand"].dropna().unique().tolist())
    selected_brand = st.selectbox("Brand", brands)

with col2:
    years = ["All Years"] + sorted(df["production_year"].dropna().unique().tolist(), reverse=True)
    selected_year = st.selectbox("Year", years)

with col3:
    product_lines = ["All Product Lines"] + sorted(df["product_line"].dropna().unique().tolist())
    selected_line = st.selectbox("Product Line", product_lines)

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df.copy()
if selected_brand != "All Brands":
    filtered = filtered[filtered["brand"] == selected_brand]
if selected_year != "All Years":
    filtered = filtered[filtered["production_year"] == selected_year]
if selected_line != "All Product Lines":
    filtered = filtered[filtered["product_line"] == selected_line]

st.divider()
