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

# ── Helper functions ──────────────────────────────────────────────────────────
def fmt_pct(x):
    return f"{x*100:.1f}%" if pd.notnull(x) else "-"

def fmt_usd(x, decimals=0):
    if pd.isnull(x):
        return "-"
    return f"${x:,.{decimals}f}"

def fmt_num(x):
    return f"{x:,.0f}" if pd.notnull(x) else "-"

def highlight_variance_flag(row):
    styles = [""] * len(row)
    try:
        flag_idx = row.index.get_loc("Variance Flag")
        flag = row.iloc[flag_idx]
        if flag == "Outlier":
            styles[flag_idx] = "background-color: #ff4444; color: white; font-weight: bold"
        elif flag == "Increase vs Prior":
            styles[flag_idx] = "background-color: #ff9900; color: white; font-weight: bold"
        elif flag == "Decrease vs Prior":
            styles[flag_idx] = "background-color: #00aa44; color: white; font-weight: bold"
    except (KeyError, TypeError):
        pass
    return styles

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_gold():
    return client.query("""
        SELECT *
        FROM `pabst_mis.gold_production_summary`
        ORDER BY production_month_key, brand
    """).to_dataframe()

@st.cache_data(ttl=3600)
def load_silver():
    return client.query("""
        SELECT
            Batch_Number, Item_Name, brand, product_line,
            completed_date, units_produced, cost_per_unit,
            materials_cost, cost_flag, Location
        FROM `pabst_mis.silver_production`
        ORDER BY completed_date DESC
    """).to_dataframe()

@st.cache_data(ttl=3600)
def load_sku_cost():
    return client.query("""
        SELECT *
        FROM `pabst_mis.gold_sku_cost_monthly`
        ORDER BY month_date DESC, materials_cost DESC
    """).to_dataframe()

@st.cache_data(ttl=3600)
def load_batch_detail():
    return client.query("""
        SELECT *
        FROM `pabst_mis.gold_batch_cost_detail`
        ORDER BY completed_date DESC
    """).to_dataframe()

# ── Cached SKU MoM builder — shared by Tab 3 and Tab 4 ───────────────────────
@st.cache_data(ttl=3600)
def build_sku_mom(sku_df, current_month, prior_month, min_units):
    current = sku_df[sku_df["month"] == current_month].copy()
    prior = sku_df[sku_df["month"] == prior_month].copy()

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
    return merged

# ── Load data ─────────────────────────────────────────────────────────────────
df = load_gold()
silver = load_silver()
sku_df = load_sku_cost()
batch_df = load_batch_detail()

silver["year"] = pd.to_datetime(silver["completed_date"]).dt.year
batch_df["completed_date"] = pd.to_datetime(batch_df["completed_date"], errors="coerce")

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏭 Production Intelligence")
st.caption("Pabst Labs · Materials cost incurred · Data current Jan 2024 – Sep 2025 (Oct 2025–present pending Roshi correction)")
st.caption("ℹ️ Historical baselines (SKU avg CPU, z-scores) are computed across all dates. Year filter affects display rows only.")

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

# ── Apply global filters ──────────────────────────────────────────────────────
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

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Overview",
    "🔬 Batch Variance",
    "📈 SKU Cost MoM",
    "🚨 Cost Anomalies",
    "📦 Raw Data",
    "🚚 FG Transfer Readiness"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
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

    st.subheader("Units Produced by Month")
    trend = (
        filtered.groupby(["production_month_key", "brand"])["total_units"]
        .sum().reset_index().sort_values("production_month_key")
    )
    if not trend.empty:
        fig = px.bar(
            trend, x="production_month_key", y="total_units", color="brand",
            labels={"production_month_key": "Month", "total_units": "Units Produced", "brand": "Brand"},
            color_discrete_map={"St. Ides": "#C8102E", "Pabst": "#003087", "NYF": "#F5A800"}
        )
        fig.update_layout(xaxis_tickangle=-45, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

    st.divider()

    st.subheader("Avg Production Cost / Unit by Month (Weighted)")
    cost_trend = (
        filtered[filtered["clean_units"] > 0]
        .groupby(["production_month_key", "brand"])
        .agg(total_cost=("total_materials_cost", "sum"), total_units=("clean_units", "sum"))
        .reset_index().sort_values("production_month_key")
    )
    cost_trend["avg_cost_per_unit"] = cost_trend["total_cost"] / cost_trend["total_units"]
    if not cost_trend.empty:
        fig2 = px.line(
            cost_trend, x="production_month_key", y="avg_cost_per_unit", color="brand",
            labels={"production_month_key": "Month", "avg_cost_per_unit": "Avg Cost / Unit ($)", "brand": "Brand"},
            color_discrete_map={"St. Ides": "#C8102E", "Pabst": "#003087", "NYF": "#F5A800"}
        )
        fig2.update_layout(xaxis_tickangle=-45, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No cost data for selected filters.")

    st.divider()

    st.subheader("Production by Brand & Product Line")
    brand_summary = (
        filtered.groupby(["brand", "product_line"])
        .agg(batches=("batch_count", "sum"), units=("total_units", "sum"), cost=("total_materials_cost", "sum"))
        .reset_index().sort_values("units", ascending=False)
    )
    brand_summary["cost"] = brand_summary["cost"].map("${:,.0f}".format)
    brand_summary["units"] = brand_summary["units"].map("{:,.0f}".format)
    brand_summary.columns = ["Brand", "Product Line", "Batches", "Units Produced", "Materials Cost"]
    st.dataframe(brand_summary, use_container_width=True, hide_index=True)

    st.divider()

    with st.expander("📊 Data Quality & Audit Trail"):
        st.markdown("**Source:** Roshi `ProductionBatchPerformanceDetail`")
        st.markdown("**Deduplication:** One row per Package Tag (state compliance ID), most recent export kept")
        st.markdown("**Corrections applied:** 33 batches corrected per Jason West (Roshi CTO), March 11–13 2026")
        st.markdown("**Destroyed batch excluded:** PL-MN036 (1 unit produced, immediately destroyed per Mario)")
        st.markdown("**Estimated cost batch:** PL-CL021 — Cherry Limeade Soda, $0.4917/unit (avg of 21 clean batches, pending Jason confirmation)")
        st.markdown("**Cost data reliable:** Jan 2024 – Sep 2025. Oct 2025–present pending Roshi correction.")
        st.markdown("**Zero cost batches:** 101 pre-2024 batches — units visible, cost not available")
        flag_summary = silver.groupby("cost_flag").agg(
            batches=("Batch_Number", "count"), units=("units_produced", "sum")
        ).reset_index()
        flag_summary.columns = ["Cost Flag", "Batches", "Units"]
        flag_summary["Units"] = flag_summary["Units"].map("{:,.0f}".format)
        st.dataframe(flag_summary, use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — BATCH VARIANCE
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Batch Cost Variance")
    st.caption("Every batch compared to its prior batch and SKU historical average. Sorted by financial impact.")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        batch_brand = st.selectbox("Brand", ["All"] + sorted(batch_df["brand"].dropna().unique().tolist()), key="batch_brand")
    with col2:
        batch_line = st.selectbox("Product Line", ["All"] + sorted(batch_df["product_line"].dropna().unique().tolist()), key="batch_line")
    with col3:
        batch_sku = st.selectbox("SKU", ["All"] + sorted(batch_df["sku"].dropna().unique().tolist()), key="batch_sku")
    with col4:
        batch_flag = st.selectbox("Variance Flag", ["All", "Outlier", "Increase vs Prior", "Decrease vs Prior", "Normal"], key="batch_flag")

    col_a, col_b = st.columns(2)
    with col_a:
        min_units_batch = st.slider("Min Units", 0, int(batch_df["units_produced"].max()), 0, key="batch_min_units")
    with col_b:
        min_impact = st.slider("Min |Impact vs Prior| ($)", 0, 50000, 0, step=500, key="batch_min_impact")

    show_outliers_only = st.checkbox("Show Outliers Only", key="batch_outliers_only")

    batch_filtered = batch_df.copy()
    if batch_brand != "All":
        batch_filtered = batch_filtered[batch_filtered["brand"] == batch_brand]
    if batch_line != "All":
        batch_filtered = batch_filtered[batch_filtered["product_line"] == batch_line]
    if batch_sku != "All":
        batch_filtered = batch_filtered[batch_filtered["sku"] == batch_sku]
    if batch_flag != "All":
        batch_filtered = batch_filtered[batch_filtered["variance_flag"] == batch_flag]
    batch_filtered = batch_filtered[batch_filtered["units_produced"] >= min_units_batch]
    if min_impact > 0:
        batch_filtered = batch_filtered[batch_filtered["impact_vs_prior"].abs() >= min_impact]
    if show_outliers_only:
        batch_filtered = batch_filtered[batch_filtered["variance_flag"] == "Outlier"]
    if selected_year != "All Years":
        batch_filtered = batch_filtered[
            batch_filtered["completed_date"].dt.year == int(selected_year)
        ]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Batches", fmt_num(len(batch_filtered)))
    k2.metric("Avg % vs Prior", fmt_pct(batch_filtered["delta_pct_vs_prior"].mean()) if batch_filtered["delta_pct_vs_prior"].notna().any() else "-")
    k3.metric("Avg % vs SKU Avg", fmt_pct(batch_filtered["delta_pct_vs_avg"].mean()) if batch_filtered["delta_pct_vs_avg"].notna().any() else "-")
    k4.metric("Total Impact vs Prior", fmt_usd(batch_filtered["impact_vs_prior"].sum()))

    batch_table = batch_filtered[[
        "Batch_Number", "sku", "brand", "product_line", "completed_date",
        "units_produced", "cost_per_unit", "prior_batch_cpu", "delta_pct_vs_prior",
        "sku_avg_cpu", "delta_pct_vs_avg", "impact_vs_prior", "z_score_cpu",
        "variance_flag", "cost_flag"
    ]].copy()

    batch_table = batch_table.sort_values(by="impact_vs_prior", ascending=False, na_position="last")
    batch_table["units_produced"] = batch_table["units_produced"].apply(fmt_num)
    batch_table["cost_per_unit"] = batch_table["cost_per_unit"].apply(lambda x: fmt_usd(x, 4))
    batch_table["prior_batch_cpu"] = batch_table["prior_batch_cpu"].apply(lambda x: fmt_usd(x, 4))
    batch_table["delta_pct_vs_prior"] = batch_table["delta_pct_vs_prior"].apply(fmt_pct)
    batch_table["sku_avg_cpu"] = batch_table["sku_avg_cpu"].apply(lambda x: fmt_usd(x, 4))
    batch_table["delta_pct_vs_avg"] = batch_table["delta_pct_vs_avg"].apply(fmt_pct)
    batch_table["impact_vs_prior"] = batch_table["impact_vs_prior"].apply(fmt_usd)
    batch_table["z_score_cpu"] = batch_table["z_score_cpu"].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "-")
    batch_table.columns = [
        "Batch", "SKU", "Brand", "Product Line", "Completed",
        "Units", "CPU", "Prior Batch CPU", "% vs Prior",
        "SKU Avg CPU", "% vs Avg", "Impact vs Prior", "Z-Score",
        "Variance Flag", "Cost Flag"
    ]

    st.dataframe(
        batch_table.style.apply(highlight_variance_flag, axis=1),
        use_container_width=True, height=500
    )

    st.divider()
    st.subheader("🔎 SKU Batch Drilldown")

    selected_sku_drill = st.selectbox(
        "Select SKU to investigate",
        sorted(batch_df["sku"].dropna().unique().tolist()),
        key="sku_drill"
    )

    drill_df = batch_df[batch_df["sku"] == selected_sku_drill].sort_values("completed_date")

    if not drill_df.empty:
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("SKU Avg CPU", fmt_usd(drill_df["sku_avg_cpu"].iloc[0], 4))
        d2.metric("Latest CPU", fmt_usd(drill_df["cost_per_unit"].iloc[-1], 4))
        d3.metric("Latest Z-Score", f"{drill_df['z_score_cpu'].iloc[-1]:.2f}" if pd.notnull(drill_df['z_score_cpu'].iloc[-1]) else "-")
        d4.metric("Latest Flag", drill_df["variance_flag"].iloc[-1])

        fig_drill = px.line(
            drill_df.dropna(subset=["cost_per_unit"]),
            x="completed_date", y="cost_per_unit",
            markers=True,
            labels={"completed_date": "Date", "cost_per_unit": "Cost/Unit ($)"},
            title=f"{selected_sku_drill} — Batch Cost Trend"
        )
        fig_drill.add_hline(
            y=drill_df["sku_avg_cpu"].iloc[0],
            line_dash="dash", line_color="gray",
            annotation_text="SKU Avg"
        )
        fig_drill.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_drill, use_container_width=True)

        drill_table = drill_df[[
            "Batch_Number", "completed_date", "units_produced", "cost_per_unit",
            "prior_batch_cpu", "delta_pct_vs_prior", "sku_avg_cpu",
            "delta_pct_vs_avg", "impact_vs_prior", "variance_flag", "cost_flag"
        ]].copy().sort_values("completed_date", ascending=False)

        drill_table["units_produced"] = drill_table["units_produced"].apply(fmt_num)
        drill_table["cost_per_unit"] = drill_table["cost_per_unit"].apply(lambda x: fmt_usd(x, 4))
        drill_table["prior_batch_cpu"] = drill_table["prior_batch_cpu"].apply(lambda x: fmt_usd(x, 4))
        drill_table["delta_pct_vs_prior"] = drill_table["delta_pct_vs_prior"].apply(fmt_pct)
        drill_table["sku_avg_cpu"] = drill_table["sku_avg_cpu"].apply(lambda x: fmt_usd(x, 4))
        drill_table["delta_pct_vs_avg"] = drill_table["delta_pct_vs_avg"].apply(fmt_pct)
        drill_table["impact_vs_prior"] = drill_table["impact_vs_prior"].apply(fmt_usd)
        drill_table.columns = [
            "Batch", "Completed", "Units", "CPU", "Prior CPU",
            "% vs Prior", "SKU Avg CPU", "% vs Avg",
            "Impact vs Prior", "Variance Flag", "Cost Flag"
        ]
        st.dataframe(
            drill_table.style.apply(highlight_variance_flag, axis=1),
            use_container_width=True, height=350
        )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — SKU COST MOM
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("SKU Cost — Month over Month")
    st.caption("$ Impact = Δ CPU × current clean units. Baselines computed across full history.")

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

    merged = build_sku_mom(sku_df, current_month, prior_month, min_units)

    merged_display = merged.copy()
    if selected_brand != "All Brands":
        merged_display = merged_display[merged_display["brand"] == selected_brand]
    if selected_line != "All Product Lines":
        merged_display = merged_display[merged_display["product_line"] == selected_line]

    show_flagged_only = st.checkbox("Show only Spike / Drop flagged SKUs", key="mom_flagged_only")
    if show_flagged_only:
        merged_display = merged_display[merged_display["cost_flag_anomaly"] != "Normal"]

    mom_display = merged_display[[
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

    valid_prior = merged_display[
        merged_display["weighted_cpu_prior"].notna() &
        (merged_display["weighted_cpu_prior"] > 0)
    ].copy()

    col_inc, col_dec = st.columns(2)
    with col_inc:
        st.markdown("**Top 5 Cost Increases (by $ Impact)**")
        top_increases = valid_prior.nlargest(5, "impact")[["sku", "brand", "weighted_cpu_current", "weighted_cpu_prior", "delta_pct", "impact"]].copy()
        top_increases.columns = ["SKU", "Brand", "CPU (Current)", "CPU (Prior)", "Δ %", "$ Impact"]
        top_increases["CPU (Current)"] = top_increases["CPU (Current)"].map("${:.4f}".format)
        top_increases["CPU (Prior)"] = top_increases["CPU (Prior)"].map("${:.4f}".format)
        top_increases["Δ %"] = top_increases["Δ %"].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "NEW")
        top_increases["$ Impact"] = top_increases["$ Impact"].map("${:+,.0f}".format)
        st.dataframe(top_increases, use_container_width=True, hide_index=True)

    with col_dec:
        st.markdown("**Top 5 Cost Decreases (by $ Impact)**")
        top_decreases = valid_prior.nsmallest(5, "impact")[["sku", "brand", "weighted_cpu_current", "weighted_cpu_prior", "delta_pct", "impact"]].copy()
        top_decreases.columns = ["SKU", "Brand", "CPU (Current)", "CPU (Prior)", "Δ %", "$ Impact"]
        top_decreases["CPU (Current)"] = top_decreases["CPU (Current)"].map("${:.4f}".format)
        top_decreases["CPU (Prior)"] = top_decreases["CPU (Prior)"].map("${:.4f}".format)
        top_decreases["Δ %"] = top_decreases["Δ %"].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "NEW")
        top_decreases["$ Impact"] = top_decreases["$ Impact"].map("${:+,.0f}".format)
        st.dataframe(top_decreases, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🔍 SKU Drilldown")

    drill_source = st.radio(
        "Select SKU from:",
        ["Top 5 Increases", "Top 5 Decreases", "All SKUs"],
        horizontal=True
    )

    if drill_source == "Top 5 Increases":
        drill_options = valid_prior.nlargest(5, "impact")["sku"].tolist()
    elif drill_source == "Top 5 Decreases":
        drill_options = valid_prior.nsmallest(5, "impact")["sku"].tolist()
    else:
        drill_options = sorted(merged_display["sku"].unique().tolist())

    if drill_options:
        selected_sku = st.selectbox("Select SKU", drill_options)
        sku_row_df = merged_display[merged_display["sku"] == selected_sku]

        if not sku_row_df.empty:
            sku_row = sku_row_df.iloc[0]
            d1, d2, d3, d4, d5 = st.columns(5)
            d1.metric("Brand", sku_row["brand"])
            d2.metric("CPU (Current)", f"${sku_row['weighted_cpu_current']:.4f}")
            d3.metric("CPU (Prior)", f"${sku_row['weighted_cpu_prior']:.4f}" if pd.notna(sku_row['weighted_cpu_prior']) else "N/A")
            d4.metric("Z-Score", f"{sku_row['z_score']:.2f}")
            d5.metric("Anomaly", sku_row["cost_flag_anomaly"])

            sku_history = sku_df[sku_df["sku"] == selected_sku].sort_values("month")
            if not sku_history.empty:
                fig_sku = px.line(
                    sku_history, x="month", y="weighted_cpu",
                    markers=True,
                    labels={"month": "Month", "weighted_cpu": "Weighted CPU ($)"},
                    title=f"Monthly Cost Trend — {selected_sku}"
                )
                fig_sku.update_layout(xaxis_tickangle=-45, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_sku, use_container_width=True)

            # Use batch_df for drilldown — richer than silver
            sku_batches = batch_df[
                (batch_df["sku"] == selected_sku) &
                (batch_df["completed_date"].dt.strftime("%Y-%m").isin([current_month, prior_month]))
            ].copy()

            if not sku_batches.empty:
                sku_batches["month"] = sku_batches["completed_date"].dt.strftime("%Y-%m")
                drill_batch = sku_batches[[
                    "Batch_Number", "month", "completed_date", "units_produced",
                    "cost_per_unit", "prior_batch_cpu", "delta_pct_vs_prior",
                    "sku_avg_cpu", "delta_pct_vs_avg", "impact_vs_prior",
                    "variance_flag", "cost_flag"
                ]].copy().sort_values("completed_date", ascending=False)

                drill_batch["units_produced"] = drill_batch["units_produced"].apply(fmt_num)
                drill_batch["cost_per_unit"] = drill_batch["cost_per_unit"].apply(lambda x: fmt_usd(x, 4))
                drill_batch["prior_batch_cpu"] = drill_batch["prior_batch_cpu"].apply(lambda x: fmt_usd(x, 4))
                drill_batch["delta_pct_vs_prior"] = drill_batch["delta_pct_vs_prior"].apply(fmt_pct)
                drill_batch["sku_avg_cpu"] = drill_batch["sku_avg_cpu"].apply(lambda x: fmt_usd(x, 4))
                drill_batch["delta_pct_vs_avg"] = drill_batch["delta_pct_vs_avg"].apply(fmt_pct)
                drill_batch["impact_vs_prior"] = drill_batch["impact_vs_prior"].apply(fmt_usd)

                drill_batch.columns = [
                    "Batch", "Month", "Completed", "Units", "CPU",
                    "Prior CPU", "% vs Prior", "SKU Avg CPU", "% vs Avg",
                    "Impact vs Prior", "Variance Flag", "Cost Flag"
                ]
                st.markdown(f"**Batch Detail — {selected_sku} ({prior_month} & {current_month})**")
                st.dataframe(
                    drill_batch.style.apply(highlight_variance_flag, axis=1),
                    use_container_width=True, hide_index=True
                )
            else:
                st.info("No batch detail available for this SKU in the selected months.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — COST ANOMALIES
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("🚨 Cost Anomalies — Statistical Detection")
    st.caption("SKUs where current cost is statistically abnormal vs their own history. Z-score > 2 = spike. Z-score < -2 = drop.")

    available_months_t4 = sorted(sku_df["month"].unique().tolist(), reverse=True)
    col_t4a, col_t4b, col_t4c = st.columns(3)
    with col_t4a:
        anomaly_current = st.selectbox("Current Month", available_months_t4, index=0, key="anomaly_current")
    with col_t4b:
        anomaly_prior_idx = 1 if len(available_months_t4) > 1 else 0
        anomaly_prior = st.selectbox("Prior Month", available_months_t4, index=anomaly_prior_idx, key="anomaly_prior")
    with col_t4c:
        anomaly_min_units = st.slider("Min Units", 0, 500, 100, key="anomaly_min_units")

    anomaly_merged = build_sku_mom(sku_df, anomaly_current, anomaly_prior, anomaly_min_units)

    if selected_brand != "All Brands":
        anomaly_merged = anomaly_merged[anomaly_merged["brand"] == selected_brand]
    if selected_line != "All Product Lines":
        anomaly_merged = anomaly_merged[anomaly_merged["product_line"] == selected_line]

    anomalies = anomaly_merged[anomaly_merged["cost_flag_anomaly"] != "Normal"].copy()
    anomalies = anomalies.sort_values(by="z_score", ascending=False)

    if not anomalies.empty:
        anomalies_display = anomalies[[
            "sku", "brand", "weighted_cpu_current", "cpu_mean", "z_score", "cost_flag_anomaly"
        ]].copy()
        anomalies_display.columns = ["SKU", "Brand", "CPU (Current)", "CPU (Historical Avg)", "Z-Score", "Flag"]
        anomalies_display["CPU (Current)"] = anomalies_display["CPU (Current)"].map("${:.4f}".format)
        anomalies_display["CPU (Historical Avg)"] = anomalies_display["CPU (Historical Avg)"].map("${:.4f}".format)
        anomalies_display["Z-Score"] = anomalies_display["Z-Score"].map("{:.2f}".format)
        st.dataframe(anomalies_display, use_container_width=True, hide_index=True)
    else:
        st.success("No cost anomalies detected for the selected period and filters.")

    st.divider()
    st.subheader("Batch-Level Outliers")
    st.caption("Batches flagged as statistical outliers vs their SKU historical average.")

    batch_outliers = batch_df[batch_df["variance_flag"] == "Outlier"].copy()
    if selected_brand != "All Brands":
        batch_outliers = batch_outliers[batch_outliers["brand"] == selected_brand]
    if selected_line != "All Product Lines":
        batch_outliers = batch_outliers[batch_outliers["product_line"] == selected_line]
    if selected_year != "All Years":
        batch_outliers = batch_outliers[batch_outliers["completed_date"].dt.year == int(selected_year)]

    if not batch_outliers.empty:
        bo_display = batch_outliers[[
            "Batch_Number", "sku", "brand", "completed_date",
            "units_produced", "cost_per_unit", "sku_avg_cpu",
            "delta_pct_vs_avg", "z_score_cpu", "impact_vs_prior"
        ]].copy().sort_values("z_score_cpu", ascending=False)

        bo_display["units_produced"] = bo_display["units_produced"].apply(fmt_num)
        bo_display["cost_per_unit"] = bo_display["cost_per_unit"].apply(lambda x: fmt_usd(x, 4))
        bo_display["sku_avg_cpu"] = bo_display["sku_avg_cpu"].apply(lambda x: fmt_usd(x, 4))
        bo_display["delta_pct_vs_avg"] = bo_display["delta_pct_vs_avg"].apply(fmt_pct)
        bo_display["z_score_cpu"] = bo_display["z_score_cpu"].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "-")
        bo_display["impact_vs_prior"] = bo_display["impact_vs_prior"].apply(fmt_usd)
        bo_display.columns = ["Batch", "SKU", "Brand", "Completed", "Units", "CPU", "SKU Avg CPU", "% vs Avg", "Z-Score", "Impact vs Prior"]
        st.dataframe(bo_display, use_container_width=True, hide_index=True)
    else:
        st.success("No batch-level outliers for selected filters.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — RAW DATA
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("Raw Data")

    raw_choice = st.selectbox(
        "Select Dataset",
        ["Production Batches (Silver)", "Batch Variance Detail", "SKU Monthly Cost"],
        key="raw_choice"
    )

    if raw_choice == "Production Batches (Silver)":
        raw_data = silver_filtered.copy()
        filename = "production_batches.csv"
    elif raw_choice == "Batch Variance Detail":
        raw_data = batch_df.copy()
        if selected_brand != "All Brands":
            raw_data = raw_data[raw_data["brand"] == selected_brand]
        if selected_year != "All Years":
            raw_data = raw_data[raw_data["completed_date"].dt.year == int(selected_year)]
        filename = "batch_variance.csv"
    else:
        raw_data = sku_df.copy()
        filename = "sku_cost_monthly.csv"

    st.caption(f"{len(raw_data):,} rows")
    st.dataframe(raw_data, use_container_width=True, height=500)
    st.download_button("⬇️ Download CSV", raw_data.to_csv(index=False), filename, "text/csv")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — FG TRANSFER READINESS
# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.subheader("📦 FG Transfer Readiness")
    st.info(
        "FIFO inventory layer and finished goods transfer readiness are pending inventory system integration. "
        "This section will show units available by SKU, oldest batch first, and estimated transfer value "
        "once the inventory layer is complete."
    )
    with st.expander("🏗️ What This Will Show"):
        st.markdown("""
        **When built, this tab will display:**
        - Finished goods on hand by SKU (units produced minus units transferred to Nabis)
        - Oldest available batch first (FIFO sequencing)
        - Units available for transfer by SKU
        - Estimated transfer value at standard cost
        - Age of finished goods in days
        - Blocked or flagged inventory

        **Data dependencies:**
        - Roshi batch transfer records
        - Nabis transfer confirmation data
        - IES inventory valuation (once live)
        """)
