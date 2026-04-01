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
