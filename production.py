import streamlit as st
import pandas as pd
import plotly.express as px
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
avg_cpu = (
    filtered["total_materials_cost"].sum() / filtered["clean_units"].sum()
    if filtered["clean_units"].sum() > 0 else 0
)

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
# ── Cost Trend ────────────────────────────────────────────────────────────────
st.subheader("Avg Production Cost / Unit by Month")

cost_trend = (
    filtered[filtered["clean_units"] > 0]
    .groupby(["production_month_key", "brand"])
    .agg(
        total_cost=("total_materials_cost", "sum"),
        total_units=("clean_units", "sum")
    )
    .reset_index()
    .sort_values("production_month_key")
)
cost_trend["avg_cost_per_unit"] = (
    cost_trend["total_cost"] / cost_trend["total_units"]
)

if not cost_trend.empty:
    fig2 = px.line(
        cost_trend,
        x="production_month_key",
        y="avg_cost_per_unit",
        color="brand",
        labels={
            "production_month_key": "Month",
            "avg_cost_per_unit": "Avg Cost / Unit ($)",
            "brand": "Brand"
        },
        color_discrete_map={
            "St. Ides": "#C8102E",
            "Pabst": "#003087",
            "NYF": "#F5A800"
        }
    )
    fig2.update_layout(
        xaxis_tickangle=-45,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend_title="Brand"
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No cost data for selected filters.")

st.divider()

# ── Brand Breakdown ───────────────────────────────────────────────────────────
st.subheader("Production by Brand & Product Line")

brand_summary = (
    filtered.groupby(["brand", "product_line"])
    .agg(
        batches=("batch_count", "sum"),
        units=("total_units", "sum"),
        cost=("total_materials_cost", "sum")
    )
    .reset_index()
    .sort_values("units", ascending=False)
)

brand_summary["cost"] = brand_summary["cost"].map("${:,.0f}".format)
brand_summary["units"] = brand_summary["units"].map("{:,.0f}".format)
brand_summary.columns = ["Brand", "Product Line", "Batches", "Units Produced", "Materials Cost"]

st.dataframe(brand_summary, use_container_width=True, hide_index=True)

st.divider()
# ── Batch Detail ──────────────────────────────────────────────────────────────
st.subheader("Batch Detail")

@st.cache_data(ttl=3600)
def load_silver():
    query = """
        SELECT
            Batch_Number,
            Item_Name,
            brand,
            product_line,
            completed_date,
            units_produced,
            cost_per_unit,
            materials_cost,
            cost_flag,
            Location
        FROM `pabst_mis.silver_production`
        ORDER BY completed_date DESC
    """
    return client.query(query).to_dataframe()

silver = load_silver()

# Apply same filters to silver
silver_filtered = silver.copy()
if selected_brand != "All Brands":
    silver_filtered = silver_filtered[silver_filtered["brand"] == selected_brand]
if selected_line != "All Product Lines":
    silver_filtered = silver_filtered[silver_filtered["product_line"] == selected_line]
if selected_year != "All Years":
    silver_filtered = silver_filtered[
        silver_filtered["completed_date"].astype(str).str[:4] == str(selected_year)
    ]

# Format for display
display = silver_filtered[[
    "Batch_Number", "Item_Name", "brand", "product_line",
    "completed_date", "units_produced", "cost_per_unit", "materials_cost", "cost_flag"
]].copy()

display.columns = [
    "Batch", "Product", "Brand", "Product Line",
    "Completed", "Units", "Cost/Unit", "Materials Cost", "Flag"
]

display["Units"] = display["Units"].map("{:,.0f}".format)
display["Cost/Unit"] = display["Cost/Unit"].map("${:.4f}".format)
display["Materials Cost"] = display["Materials Cost"].map("${:,.0f}".format)

st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ── Data Quality Panel ────────────────────────────────────────────────────────
with st.expander("📊 Data Quality & Audit Trail"):
    st.markdown("**Source:** Roshi `ProductionBatchPerformanceDetail`")
    st.markdown("**Deduplication:** One row per Package Tag (state compliance ID), most recent export kept")
    st.markdown("**Corrections applied:** 33 batches corrected per Jason West (Roshi CTO), March 11–13 2026")
    st.markdown("**Destroyed batch excluded:** PL-MN036 (1 unit produced, immediately destroyed per Mario)")
    st.markdown("**Estimated cost batch:** PL-CL021 — Cherry Limeade Soda, $0.4917/unit (avg of 21 clean batches, pending Jason confirmation)")
    st.markdown("**Cost data reliable:** Jan 2024 – Sep 2025. Oct 2025–present pending Roshi correction.")
    st.markdown("**Zero cost batches:** 101 pre-2024 batches — units visible, cost not available")

    flag_summary = silver.groupby("cost_flag").agg(
        batches=("Batch_Number", "count"),
        units=("units_produced", "sum")
    ).reset_index()
    flag_summary.columns = ["Cost Flag", "Batches", "Units"]
    flag_summary["Units"] = flag_summary["Units"].map("{:,.0f}".format)
    st.dataframe(flag_summary, use_container_width=True, hide_index=True)
