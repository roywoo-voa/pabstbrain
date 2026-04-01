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
# ── KPI Cards ─────────────────────────────────────────────────────────────────
st.subheader("Production Overview")

total_batches = int(filtered["batch_count"].sum())
total_units = int(filtered["total_units"].sum())
costed_units = int(filtered["clean_units"].sum())
total_cost = filtered["total_materials_cost"].sum()
avg_cpu = filtered["avg_cost_per_unit"].mean()

k1, k2, k3, k4, k5 = st.columns(5)

k1.metric("Batches", f"{total_batches:,}")
k2.metric("Units Produced", f"{total_units:,.0f}")
k3.metric("Costed Units", f"{costed_units:,.0f}")
k4.metric("Materials Cost Incurred", f"${total_cost:,.0f}")
k5.metric("Avg Production Cost / Unit", f"${avg_cpu:.4f}")

st.divider()
# ── Production Trend ──────────────────────────────────────────────────────────
st.subheader("Units Produced by Month")

trend = (
    filtered.groupby(["production_month_key", "brand"])["total_units"]
    .sum()
    .reset_index()
    .sort_values("production_month_key")
)

if not trend.empty:
    import plotly.express as px
    fig = px.bar(
        trend,
        x="production_month_key",
        y="total_units",
        color="brand",
        labels={
            "production_month_key": "Month",
            "total_units": "Units Produced",
            "brand": "Brand"
        },
        color_discrete_map={
            "St. Ides": "#C8102E",
            "Pabst": "#003087",
            "NYF": "#F5A800"
        }
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend_title="Brand"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data for selected filters.")

st.divider()
