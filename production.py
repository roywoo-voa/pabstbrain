import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery

# ── Page config ───────────────────────────────────────────────────────────────
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

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_gold():
    query = """
        SELECT *
        FROM `pabst_mis.gold_production_summary`
        ORDER BY production_month_key, brand
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_silver():
    query = """
        SELECT
            Batch_Number, Item_Name, brand, product_line,
            completed_date, units_produced, cost_per_unit,
            materials_cost, cost_flag, Location
        FROM `pabst_mis.silver_production`
        ORDER BY completed_date DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_sku_cost():
    query = """
        SELECT *
        FROM `pabst_mis.gold_sku_cost_monthly`
        ORDER BY month_date DESC, materials_cost DESC
    """
    return client.query(query).to_dataframe()

df = load_gold()
silver = load_silver()
sku_df = load_sku_cost()
silver["year"] = pd.to_datetime(silver["completed_date"]).dt.year

# ── Header ────────────────────────────────────────────────────────────────────
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

silver_filtered = silver.copy()
if selected_brand != "All Brands":
    silver_filtered = silver_filtered[silver_filtered["brand"] == selected_brand]
if selected_line != "All Product Lines":
    silver_filtered = silver_filtered[silver_filtered["product_line"] == selected_line]
if selected_year != "All Years":
    silver_filtered = silver_filtered[silver_filtered["year"] == int(selected_year)]

st.divider()

# ── KPI Cards ─────────────────────────────────────────────────────────────────
st.subheader("Production Overview")

total_batches = int(filtered["batch_count"].sum())
total_units = int(filtered["total_units"].sum())
costed_units = int(filtered["clean_units"].sum())
total_cost = filtered["total_materials_cost"].sum()
avg_cpu = total_cost / costed_units if costed_units > 0 else 0

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
        labels={"production_month_key": "Month", "total_units": "Units Produced", "brand": "Brand"},
        color_discrete_map={"St. Ides": "#C8102E", "Pabst": "#003087", "NYF": "#F5A800"}
    )
    fig.update_layout(xaxis_tickangle=-45, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data for selected filters.")

st.divider()

# ── Cost Trend ────────────────────────────────────────────────────────────────
st.subheader("Avg Production Cost / Unit by Month (Weighted)")

cost_trend = (
    filtered[filtered["clean_units"] > 0]
    .groupby(["production_month_key", "brand"])
    .agg(total_cost=("total_materials_cost", "sum"), total_units=("clean_units", "sum"))
    .reset_index()
    .sort_values("production_month_key")
)
cost_trend["avg_cost_per_unit"] = cost_trend["total_cost"] / cost_trend["total_units"]

if not cost_trend.empty:
    fig2 = px.line(
        cost_trend,
        x="production_month_key",
        y="avg_cost_per_unit",
        color="brand",
        labels={"production_month_key": "Month", "avg_cost_per_unit": "Avg Cost / Unit ($)", "brand": "Brand"},
        color_discrete_map={"St. Ides": "#C8102E", "Pabst": "#003087", "NYF": "#F5A800"}
    )
    fig2.update_layout(xaxis_tickangle=-45, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No cost data for selected filters.")

st.divider()

# ── Brand Breakdown ───────────────────────────────────────────────────────────
st.subheader("Production by Brand & Product Line")

brand_summary = (
    filtered.groupby(["brand", "product_line"])
    .agg(batches=("batch_count", "sum"), units=("total_units", "sum"), cost=("total_materials_cost", "sum"))
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

display = silver_filtered[[
    "Batch_Number", "Item_Name", "brand", "product_line",
    "completed_date", "units_produced", "cost_per_unit", "materials_cost", "cost_flag"
]].copy()
display.columns = ["Batch", "Product", "Brand", "Product Line", "Completed", "Units", "Cost/Unit", "Materials Cost", "Flag"]
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

st.divider()

# ── SKU Month-over-Month Cost Comparison ──────────────────────────────────────
st.subheader("SKU Cost — Month over Month")

available_months = sorted(sku_df["month"].unique().tolist(), reverse=True)
col_m1, col_m2, col_m3 = st.columns(3)
with col_m1:
    current_month = st.selectbox("Current Month", available_months, index=0)
with col_m2:
    prior_index = 1 if len(available_months) > 1 else 0
    prior_month = st.selectbox("Prior Month", available_months, index=prior_index)
with col_m3:
    min_units = st.slider("Min Units Filter", 0, 500, 100)

if current_month == prior_month:
    st.warning("Current Month and Prior Month are the same. Select different months for a meaningful comparison.")

current = sku_df[sku_df["month"] == current_month].copy()
prior = sku_df[sku_df["month"] == prior_month].copy()

# Build historical baseline for z-score
history = sku_df.groupby("sku").agg(
    cpu_mean=("weighted_cpu", "mean"),
    cpu_std=("weighted_cpu", "std")
).reset_index()

merged = current.merge(
    prior[["sku", "brand", "product_line", "clean_units", "materials_cost", "weighted_cpu"]],
    on=["sku", "brand", "product_line"],
    suffixes=("_current", "_prior"),
    how="left"
)

merged = merged.merge(history, on="sku", how="left")

merged = merged[
    (merged["clean_units_current"] >= min_units) |
    (merged["clean_units_prior"] >= min_units)
]

merged["delta_cpu"] = merged["weighted_cpu_current"] - merged["weighted_cpu_prior"]
merged["delta_pct"] = np.where(
    merged["weighted_cpu_prior"] > 0,
    (merged["delta_cpu"] / merged["weighted_cpu_prior"] * 100),
    np.nan
)

merged["z_score"] = np.where(
    merged["cpu_std"] > 0,
    (merged["weighted_cpu_current"] - merged["cpu_mean"]) / merged["cpu_std"],
    0
)

merged["cost_flag_anomaly"] = np.where(
    merged["z_score"] > 2, "🚨 Spike",
    np.where(merged["z_score"] < -2, "✅ Drop", "Normal")
)

merged["impact"] = merged["delta_cpu"] * merged["clean_units_current"]

if selected_brand != "All Brands":
    merged = merged[merged["brand"] == selected_brand]
if selected_line != "All Product Lines":
    merged = merged[merged["product_line"] == selected_line]

mom_display = merged[[
    "sku", "brand", "product_line",
    "clean_units_current", "weighted_cpu_current",
    "clean_units_prior", "weighted_cpu_prior",
    "materials_cost_current", "materials_cost_prior",
    "delta_cpu", "delta_pct", "impact", "cost_flag_anomaly"
]].copy()

mom_display = mom_display.sort_values(by="impact", ascending=False)

mom_display.columns = [
    "SKU", "Brand", "Product Line",
    "Units (Current)", "CPU (Current)",
    "Units (Prior)", "CPU (Prior)",
    "Cost (Current)", "Cost (Prior)",
    "Δ CPU ($)", "Δ %", "$ Impact", "Flag"
]

mom_display["Units (Current)"] = mom_display["Units (Current)"].map("{:,.0f}".format)
mom_display["Units (Prior)"] = mom_display["Units (Prior)"].map("{:,.0f}".format)
mom_display["CPU (Current)"] = mom_display["CPU (Current)"].map("${:.4f}".format)
mom_display["CPU (Prior)"] = mom_display["CPU (Prior)"].map("${:.4f}".format)
mom_display["Cost (Current)"] = mom_display["Cost (Current)"].map("${:,.0f}".format)
mom_display["Cost (Prior)"] = mom_display["Cost (Prior)"].map("${:,.0f}".format)
mom_display["Δ CPU ($)"] = mom_display["Δ CPU ($)"].map("${:+.4f}".format)
mom_display["Δ %"] = mom_display["Δ %"].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "NEW")
mom_display["$ Impact"] = mom_display["$ Impact"].map("${:+,.0f}".format)

st.dataframe(mom_display, use_container_width=True, hide_index=True)

valid_prior = merged[
    merged["weighted_cpu_prior"].notna() &
    (merged["weighted_cpu_prior"] > 0)
].copy()

st.markdown("**Top 5 Cost Increases (by $ Impact)**")
top_increases = valid_prior.nlargest(5, "impact")[["sku", "brand", "weighted_cpu_current", "weighted_cpu_prior", "delta_pct", "impact"]].copy()
top_increases.columns = ["SKU", "Brand", "CPU (Current)", "CPU (Prior)", "Δ %", "$ Impact"]
top_increases["CPU (Current)"] = top_increases["CPU (Current)"].map("${:.4f}".format)
top_increases["CPU (Prior)"] = top_increases["CPU (Prior)"].map("${:.4f}".format)
top_increases["Δ %"] = top_increases["Δ %"].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "NEW")
top_increases["$ Impact"] = top_increases["$ Impact"].map("${:+,.0f}".format)
st.dataframe(top_increases, use_container_width=True, hide_index=True)

st.markdown("**Top 5 Cost Decreases (by $ Impact)**")
top_decreases = valid_prior.nsmallest(5, "impact")[["sku", "brand", "weighted_cpu_current", "weighted_cpu_prior", "delta_pct", "impact"]].copy()
top_decreases.columns = ["SKU", "Brand", "CPU (Current)", "CPU (Prior)", "Δ %", "$ Impact"]
top_decreases["CPU (Current)"] = top_decreases["CPU (Current)"].map("${:.4f}".format)
top_decreases["CPU (Prior)"] = top_decreases["CPU (Prior)"].map("${:.4f}".format)
top_decreases["Δ %"] = top_decreases["Δ %"].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "NEW")
top_decreases["$ Impact"] = top_decreases["$ Impact"].map("${:+,.0f}".format)
st.dataframe(top_decreases, use_container_width=True, hide_index=True)

st.divider()

# ── Cost Anomaly Detection ────────────────────────────────────────────────────
st.subheader("🚨 Cost Anomalies — Statistical Detection")
st.caption("SKUs where current cost is statistically abnormal vs their own history. Z-score > 2 = spike. Z-score < -2 = drop.")

anomalies = merged[merged["cost_flag_anomaly"] != "Normal"].copy()
anomalies = anomalies.sort_values(by="z_score", ascending=False)

if not anomalies.empty:
    anomalies_display = anomalies[[
        "sku", "brand",
        "weighted_cpu_current",
        "cpu_mean",
        "z_score",
        "cost_flag_anomaly"
    ]].copy()

    anomalies_display.columns = [
        "SKU", "Brand",
        "CPU (Current)", "CPU (Historical Avg)",
        "Z-Score", "Flag"
    ]

    anomalies_display["CPU (Current)"] = anomalies_display["CPU (Current)"].map("${:.4f}".format)
    anomalies_display["CPU (Historical Avg)"] = anomalies_display["CPU (Historical Avg)"].map("${:.4f}".format)
    anomalies_display["Z-Score"] = anomalies_display["Z-Score"].map("{:.2f}".format)

    st.dataframe(anomalies_display, use_container_width=True, hide_index=True)
else:
    st.success("No cost anomalies detected for the selected period and filters.")

st.divider()

# ── FIFO / Transfer Readiness (Placeholder) ───────────────────────────────────
with st.expander("📦 FG Transfer Readiness — Coming Soon"):
    st.info("FIFO inventory layer and finished goods transfer readiness are pending inventory system integration. This section will show units available by SKU, oldest batch first, and estimated transfer value once the inventory layer is complete.")
