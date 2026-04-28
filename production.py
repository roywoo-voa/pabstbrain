import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PabstBrain -- Production",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── STYLES ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #0f1117;
    color: #e8e8e8;
}

.stApp { background-color: #0f1117; }

/* Header */
.pb-header {
    display: flex;
    align-items: baseline;
    gap: 12px;
    padding: 24px 0 8px 0;
    border-bottom: 1px solid #2a2a2a;
    margin-bottom: 24px;
}
.pb-title {
    font-family: 'DM Mono', monospace;
    font-size: 22px;
    font-weight: 500;
    color: #ffffff;
    letter-spacing: -0.5px;
}
.pb-subtitle {
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #666;
    letter-spacing: 1px;
    text-transform: uppercase;
}

/* KPI cards */
.kpi-row {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 12px;
    margin-bottom: 24px;
}
.kpi-card {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 8px;
    padding: 16px 20px;
}
.kpi-label {
    font-size: 11px;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 6px;
    font-family: 'DM Mono', monospace;
}
.kpi-value {
    font-size: 24px;
    font-weight: 600;
    color: #ffffff;
    font-family: 'DM Mono', monospace;
    letter-spacing: -1px;
}
.kpi-sub {
    font-size: 11px;
    color: #8b949e;
    margin-top: 4px;
}

/* Coverage badges */
.badge-good     { background:#0d3a1e; color:#3fb950; border:1px solid #238636; border-radius:4px; padding:2px 8px; font-size:11px; font-family:'DM Mono',monospace; }
.badge-moderate { background:#2d2006; color:#d29922; border:1px solid #9e6a03; border-radius:4px; padding:2px 8px; font-size:11px; font-family:'DM Mono',monospace; }
.badge-low      { background:#2d1b00; color:#f0883e; border:1px solid #bd561d; border-radius:4px; padding:2px 8px; font-size:11px; font-family:'DM Mono',monospace; }
.badge-critical { background:#3d0c0c; color:#f85149; border:1px solid #da3633; border-radius:4px; padding:2px 8px; font-size:11px; font-family:'DM Mono',monospace; }

/* Exception badges */
.exc-corrupted  { background:#3d1a6e; color:#d2a8ff; border:1px solid #8b5cf6; border-radius:4px; padding:1px 6px; font-size:10px; font-family:'DM Mono',monospace; }
.exc-missing    { background:#1a2332; color:#79c0ff; border:1px solid #388bfd; border-radius:4px; padding:1px 6px; font-size:10px; font-family:'DM Mono',monospace; }
.exc-zero       { background:#2d1b00; color:#f0883e; border:1px solid #bd561d; border-radius:4px; padding:1px 6px; font-size:10px; font-family:'DM Mono',monospace; }
.exc-variance   { background:#3d0c0c; color:#f85149; border:1px solid #da3633; border-radius:4px; padding:1px 6px; font-size:10px; font-family:'DM Mono',monospace; }
.exc-nopo       { background:#2d2006; color:#d29922; border:1px solid #9e6a03; border-radius:4px; padding:1px 6px; font-size:10px; font-family:'DM Mono',monospace; }
.exc-ok         { color:#3fb950; font-size:10px; font-family:'DM Mono',monospace; }

/* Section headers */
.section-header {
    font-family: 'DM Mono', monospace;
    font-size: 11px;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    margin: 20px 0 12px 0;
    padding-bottom: 6px;
    border-bottom: 1px solid #21262d;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: transparent;
    border-bottom: 1px solid #21262d;
    gap: 0;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #8b949e;
    padding: 10px 20px;
    border-bottom: 2px solid transparent;
    background: transparent;
}
.stTabs [aria-selected="true"] {
    color: #ffffff;
    border-bottom: 2px solid #f78166;
    background: transparent;
}

/* Dataframe */
.stDataFrame { border: 1px solid #21262d; border-radius: 8px; }

/* Selectbox / filters */
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 6px;
    color: #e8e8e8;
}

/* Metric delta */
.stMetric { background: #161b22; border-radius: 8px; padding: 12px; border: 1px solid #21262d; }

/* Drilldown ingredient table */
.ingredient-row {
    display: grid;
    grid-template-columns: 2fr 1fr 1fr 1fr 1fr 1fr 1fr;
    gap: 0;
    padding: 10px 12px;
    border-bottom: 1px solid #21262d;
    align-items: center;
    font-size: 13px;
}
.ingredient-row:hover { background: #161b22; }
.ingredient-header {
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    background: #0d1117;
    border-bottom: 2px solid #21262d;
    border-radius: 6px 6px 0 0;
}
.pos-var { color: #f85149; }
.neg-var { color: #3fb950; }
.neu-var { color: #8b949e; }

/* Divider */
hr { border-color: #21262d; }

button[kind="primary"] {
    background: #238636;
    border: 1px solid #2ea043;
    color: white;
    border-radius: 6px;
    font-family: 'DM Mono', monospace;
    font-size: 12px;
}
</style>
""", unsafe_allow_html=True)

# ── BIGQUERY CONNECTION ───────────────────────────────────────────────────────
PROJECT = "amplified-name-490015-e0"
DATASET = "pabst_mis"

@st.cache_resource
def get_bq_client():
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        return bigquery.Client(credentials=creds, project=PROJECT)
    return bigquery.Client(project=PROJECT)

@st.cache_data(ttl=300)
def load_gold():
    client = get_bq_client()
    q = f"""
    SELECT
        Batch_Number, Product_Name, batch_date, batch_sequence,
        actual_yield, yield_units,
        roshi_stated_materials_cost, roshi_cost_per_unit,
        total_recorded_material_cost, total_estimated_missing_cost,
        total_material_cost_blended,
        recorded_cost_per_unit, blended_cost_per_unit,
        prior_batch_number, prior_batch_date,
        prior_batch_blended_cost, prior_batch_cost_per_unit,
        pct_vs_prior_batch, dollar_vs_prior_batch,
        pct_cpu_vs_prior_batch, dollar_cpu_vs_prior_batch,
        trailing_6_avg_material_cost, trailing_6_avg_cost_per_unit,
        trailing_6_batch_count, pct_material_cost_vs_trailing_6, pct_cpu_vs_trailing_6,
        costed_line_count, zero_cost_line_count,
        total_line_count, line_cost_coverage_pct, dollar_coverage_pct,
        zero_cost_high_value_line_count, has_zero_cost_high_value_input,
        max_abs_dollar_var,
        variance_exception_count, no_po_match_count, variance_flag_count,
        stale_cost_count, exact_match_count,
        coverage_status, low_dollar_coverage_flag, low_line_coverage_flag
    FROM `{PROJECT}.{DATASET}.gold_batch_cost_summary`
    ORDER BY batch_date DESC NULLS LAST, Batch_Number DESC
    """
    return client.query(q).to_dataframe()

@st.cache_data(ttl=300)
def load_silver(batch_number: str):
    client = get_bq_client()
    q = f"""
    SELECT
        s.rm_item_name, s.Item_Category, s.rm_lot_number,
        s.qty_consumed, s.uom, s.batch_unit_cost, s.batch_extended_cost,
        s.effective_last_po_cost, s.last_po_date, s.last_po_supplier, s.last_po_order_number,
        s.avg_cost_90d, s.match_status,
        s.pct_var_vs_last_po, s.dollar_var_vs_last_po,
        s.pct_var_vs_avg_90d, s.dollar_var_vs_avg_90d,
        s.days_since_last_po, s.stale_cost_flag,
        s.exception_flag, s.variance_flag,
        s.corrupted_unit_cost_flag, s.extended_cost_mismatch_flag,
        r.recipe_rate_per_unit, r.recipe_uom, r.batches_used AS recipe_batches_used,
        r.status AS recipe_status, r.avg_qty_per_unit AS recipe_avg_qty_per_unit
    FROM `{PROJECT}.{DATASET}.silver_batch_material_detail` s
    LEFT JOIN `{PROJECT}.{DATASET}.gold_recipe_rates` r
      ON s.Product_Name = r.Product_Name
     AND s.rm_item_name = r.rm_item_name
    WHERE s.Batch_Number = @batch
    ORDER BY s.batch_extended_cost DESC NULLS LAST
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("batch", "STRING", batch_number)]
    )
    return client.query(q, job_config=job_config).to_dataframe()

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fmt_currency(v, decimals=0):
    if pd.isna(v): return "--"
    if decimals == 0:
        return f"${v:,.0f}"
    return f"${v:,.{decimals}f}"

def fmt_pct(v, decimals=1):
    if pd.isna(v): return "--"
    sign = "+" if v > 0 else ""
    return f"{sign}{v*100:.{decimals}f}%"

def fmt_num(v, decimals=0):
    if pd.isna(v): return "--"
    return f"{v:,.{decimals}f}"

def coverage_badge(status):
    badges = {
        "good":     '<span class="badge-good">GOOD</span>',
        "moderate": '<span class="badge-moderate">MODERATE</span>',
        "low":      '<span class="badge-low">LOW</span>',
        "critical": '<span class="badge-critical">CRITICAL</span>',
    }
    return badges.get(status, status)

def exception_badge(exc):
    if pd.isna(exc) or exc == "":
        return '<span class="exc-ok">✓</span>'
    badges = {
        "corrupted_unit_cost":    '<span class="exc-corrupted">CORRUPT</span>',
        "missing_batch_cost":     '<span class="exc-missing">MISSING</span>',
        "zero_or_negative_cost":  '<span class="exc-zero">ZERO COST</span>',
        "variance_above_threshold":'<span class="exc-variance">VARIANCE</span>',
        "no_po_match":            '<span class="exc-nopo">NO PO</span>',
    }
    return badges.get(exc, f'<span class="exc-missing">{exc}</span>')

def var_color_class(v):
    if pd.isna(v): return "neu-var"
    return "pos-var" if v > 0 else ("neg-var" if v < 0 else "neu-var")

# ── VERIFICATION HELPERS (Commit B) ───────────────────────────────────────────

def format_delta_pct(current, average, decimals=1, lower_is_better=True):
    """Format a percent-difference vs an average with color hint.
    Returns (text, color_hex). Lower-is-better flips color logic for cost-type metrics."""
    if pd.isna(current) or pd.isna(average) or average == 0:
        return ("--", "#8b949e")
    pct = (current - average) / average
    sign = "+" if pct > 0 else ""
    text = f"{sign}{pct*100:.{decimals}f}% vs avg"
    if abs(pct) < 0.05:
        color = "#8b949e"
    elif lower_is_better:
        color = "#f85149" if pct > 0 else "#3fb950"
    else:
        color = "#3fb950" if pct > 0 else "#f85149"
    return (text, color)

def compute_anomalies(silver_df, batch_row):
    """Detect data quality issues on a batch. Returns list of issue strings."""
    issues = []
    if silver_df.empty:
        return issues
    no_po = silver_df[silver_df["exception_flag"] == "no_po_match"]
    for _, r in no_po.iterrows():
        issues.append(f"{r['rm_item_name']}: no PO match")
    high_var = silver_df[silver_df["exception_flag"] == "variance_above_threshold"]
    for _, r in high_var.iterrows():
        pct = r.get("pct_var_vs_last_po", None)
        pct_str = f" ({pct*100:+.1f}% vs PO)" if pd.notna(pct) else ""
        issues.append(f"{r['rm_item_name']}: variance flag{pct_str}")
    zero_thc = silver_df[
        (silver_df["Item_Category"].isin(["Extract (weight)", "Other Concentrate (weight)"]))
        & ((silver_df["batch_unit_cost"].isna()) | (silver_df["batch_unit_cost"] == 0))
    ]
    for _, r in zero_thc.iterrows():
        issues.append(f"{r['rm_item_name']}: zero cost on high-value input")
    return issues

def compute_yield_reconciliation(silver_df, batch_row):
    """Compute theoretical max yield from binding constraint.
    Returns dict: theo_max, binding_ingredient, actual, variance, status."""
    actual_yield = batch_row.get("actual_yield", None)
    if silver_df.empty or pd.isna(actual_yield) or actual_yield <= 0:
        return {"theo_max": None, "binding_ingredient": None,
                "actual": actual_yield, "variance": None, "status": "no_data"}
    candidates = silver_df[
        (silver_df["recipe_rate_per_unit"].notna())
        & (silver_df["recipe_rate_per_unit"] > 0)
        & (silver_df["qty_consumed"].notna())
        & (silver_df["qty_consumed"] > 0)
        & (silver_df["recipe_status"].isin(["reliable", "single_batch"]))
    ].copy()
    if candidates.empty:
        return {"theo_max": None, "binding_ingredient": None,
                "actual": actual_yield, "variance": None, "status": "no_reliable_rates"}
    candidates["theo_yield"] = candidates["qty_consumed"] / candidates["recipe_rate_per_unit"]
    candidates = candidates[candidates["theo_yield"] < actual_yield * 5]
    if candidates.empty:
        return {"theo_max": None, "binding_ingredient": None,
                "actual": actual_yield, "variance": None, "status": "all_outliers"}
    binding_idx = candidates["theo_yield"].idxmin()
    theo_max = candidates.loc[binding_idx, "theo_yield"]
    binding_ingredient = candidates.loc[binding_idx, "rm_item_name"]
    variance = actual_yield - theo_max
    return {
        "theo_max": int(round(theo_max)),
        "binding_ingredient": binding_ingredient,
        "actual": int(round(actual_yield)),
        "variance": int(round(variance)),
        "status": "computed"
    }

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
try:
    gold = load_gold()
except Exception as e:
    st.error(f"BigQuery connection failed: {e}")
    st.stop()

# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="pb-header">
    <span class="pb-title">PabstBrain</span>
    <span class="pb-subtitle">Production Intelligence</span>
</div>
""", unsafe_allow_html=True)

# ── SIDEBAR FILTERS ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="section-header">Filters</div>', unsafe_allow_html=True)

    products = ["All Products"] + sorted(gold["Product_Name"].dropna().unique().tolist())
    sel_product = st.selectbox("Product", products)

    coverage_options = ["All", "good", "moderate", "low", "critical"]
    sel_coverage = st.selectbox("Coverage Status", coverage_options)

    date_range = st.date_input(
        "Completion Date Range",
        value=[],
        help="Leave empty to show all dates"
    )

    show_exceptions_only = st.checkbox("Exceptions only", value=False)

    st.markdown("---")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# ── FILTER DATA ───────────────────────────────────────────────────────────────
df = gold.copy()

if sel_product != "All Products":
    df = df[df["Product_Name"] == sel_product]

if sel_coverage != "All":
    df = df[df["coverage_status"] == sel_coverage]

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    df["batch_date"] = pd.to_datetime(df["batch_date"])
    df = df[
        (df["batch_date"] >= pd.Timestamp(date_range[0])) &
        (df["batch_date"] <= pd.Timestamp(date_range[1]))
    ]

if show_exceptions_only:
    df = df[
        (df["variance_exception_count"] > 0) |
        (df["low_dollar_coverage_flag"] == True) |
        (df["has_zero_cost_high_value_input"] == True)
    ]

# ── KPI ROW ───────────────────────────────────────────────────────────────────
total_batches   = len(df)
total_cost      = df["total_material_cost_blended"].sum()
avg_cpu         = df["blended_cost_per_unit"].median()
pct_good        = (df["coverage_status"] == "good").mean() * 100
total_var_flags = df["variance_exception_count"].sum()

st.markdown(f"""
<div class="kpi-row">
    <div class="kpi-card">
        <div class="kpi-label">Batches</div>
        <div class="kpi-value">{total_batches:,}</div>
        <div class="kpi-sub">in view</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Total Material Cost</div>
        <div class="kpi-value">{fmt_currency(total_cost)}</div>
        <div class="kpi-sub">blended</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Median CPU</div>
        <div class="kpi-value">{fmt_currency(avg_cpu, 3)}</div>
        <div class="kpi-sub">blended cost / unit</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Good Coverage</div>
        <div class="kpi-value">{pct_good:.0f}%</div>
        <div class="kpi-sub">of batches</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Variance Flags</div>
        <div class="kpi-value">{int(total_var_flags):,}</div>
        <div class="kpi-sub">ingredients >10% vs PO</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── TABS ──────────────────────────────────────────────────────────────────────
tab_summary, tab_drilldown, tab_exceptions, tab_product = st.tabs([
    "📋  Batch Summary",
    "🔬  Batch Drilldown",
    "⚠️  Exceptions",
    "🧪  Product View"
])

# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 -- BATCH SUMMARY
# ════════════════════════════════════════════════════════════════════════════════
with tab_summary:
    st.markdown('<div class="section-header">All Batches</div>', unsafe_allow_html=True)

    sort_col = st.selectbox(
"Sort by",
        ["batch_date", "total_material_cost_blended", "pct_cpu_vs_prior_batch",
         "blended_cost_per_unit", "dollar_coverage_pct"],
        format_func=lambda x: {
            "batch_date": "Completion Date",
            "total_material_cost_blended": "Total Material Cost",
            "pct_cpu_vs_prior_batch": "% vs Prior Batch (CPU)",
            "blended_cost_per_unit": "Cost Per Unit",
            "dollar_coverage_pct": "Coverage %",
        }[x],
    )

    sort_asc = st.checkbox("Ascending", value=False)
    df_sorted = df.sort_values(sort_col, ascending=sort_asc, na_position="last")

    # Build display table
    rows = []
    for _, r in df_sorted.iterrows():
        pct_var = r["pct_cpu_vs_prior_batch"]
        var_class = var_color_class(pct_var)
        pct_str = fmt_pct(pct_var) if not pd.isna(pct_var) else "--"

        rows.append({
            "Batch": r["Batch_Number"],
            "Product": r["Product_Name"],
            "Date": str(r["batch_date"])[:10] if pd.notna(r["batch_date"]) else "--",
            "Yield": f"{fmt_num(r['actual_yield'])} {r['yield_units'] or ''}".strip(),
            "Material Cost": fmt_currency(r["total_material_cost_blended"]),
            "CPU (Blended)": fmt_currency(r["blended_cost_per_unit"], 3),
            "vs Prior Batch (CPU)": pct_str,
            "$ vs Prior (CPU)": fmt_currency(r["dollar_cpu_vs_prior_batch"], 4),
            "Coverage": r["coverage_status"],
            "Costed Lines": f"{int(r['costed_line_count'])}/{int(r['total_line_count'])}",
            "Var Flags": int(r["variance_exception_count"]),
            "THC Zero Cost": "⚠️" if r["has_zero_cost_high_value_input"] else "",
        })

    display_df = pd.DataFrame(rows)

    def color_coverage(val):
        colors = {
            "good": "color: #3fb950",
            "moderate": "color: #d29922",
            "low": "color: #f0883e",
            "critical": "color: #f85149",
        }
        return colors.get(val, "")

    def color_variance(val):
        if val == "--": return ""
        try:
            num = float(val.replace("+","").replace("%",""))
            if num > 10: return "color: #f85149"
            if num > 5: return "color: #f0883e"
            if num < -5: return "color: #3fb950"
        except: pass
        return ""

    styled = display_df.style\
        .map(color_coverage, subset=["Coverage"])\
        .map(color_variance, subset=["vs Prior Batch (CPU)"])\
        .set_properties(**{
            "font-family": "DM Mono, monospace",
            "font-size": "12px",
        })

    st.dataframe(
        styled,
        use_container_width=True,
        height=600,
        hide_index=True,
    )

    st.caption(f"Showing {len(df_sorted):,} batches · Material cost = recorded + estimated missing (exact PO match only)")

# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 -- BATCH DRILLDOWN
# ════════════════════════════════════════════════════════════════════════════════
with tab_drilldown:
    col_sel, col_info = st.columns([1, 2])

    with col_sel:
        st.markdown('<div class="section-header">Select Batch</div>', unsafe_allow_html=True)

        batch_options = df_sorted["Batch_Number"].tolist()
        if not batch_options:
            st.warning("No batches match current filters.")
            st.stop()

        sel_batch = st.selectbox(
            "Batch",
            batch_options,
            format_func=lambda b: f"{b} -- {df[df['Batch_Number']==b]['Product_Name'].values[0]}"
                if b in df["Batch_Number"].values else b
        )

    # Batch header info
    batch_row = df[df["Batch_Number"] == sel_batch]
    if batch_row.empty:
        st.warning("Batch not found.")
    else:
        br = batch_row.iloc[0]

        with col_info:
            st.markdown('<div class="section-header">Verification HUD · vs trailing-6 batches</div>', unsafe_allow_html=True)

            # Load silver early so anomalies + yield reconciliation can use it
            with st.spinner("Loading ingredient detail..."):
                silver = load_silver(sel_batch)

            # ── ANOMALY CALLOUT ──────────────────────────────────────────────
            anomalies = compute_anomalies(silver, br)
            if anomalies:
                items_html = "".join([f"<li>{a}</li>" for a in anomalies])
                st.markdown(f"""
                <div style="background:#3d0c0c; border:1px solid #da3633; border-radius:6px; padding:10px 14px; margin-bottom:14px;">
                  <div style="color:#f85149; font-family:'DM Mono',monospace; font-size:12px; font-weight:500; margin-bottom:6px;">
                    ⚠ {len(anomalies)} data quality issue{'s' if len(anomalies)!=1 else ''} on this batch
                  </div>
                  <ul style="margin:0; padding-left:18px; color:#f0a3a3; font-size:12px; line-height:1.6;">
                    {items_html}
                  </ul>
                </div>
                """, unsafe_allow_html=True)

            # ── 4 KPI CARDS WITH DELTAS ──────────────────────────────────────
            mc_text, mc_color = format_delta_pct(
                br.get("total_material_cost_blended"),
                br.get("trailing_6_avg_material_cost"),
                lower_is_better=True
            )
            cpu_text, cpu_color = format_delta_pct(
                br.get("blended_cost_per_unit"),
                br.get("trailing_6_avg_cost_per_unit"),
                lower_is_better=True
            )
            kpi_cards_html = f"""
            <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:14px;">
              <div class="kpi-card" style="padding:12px 14px;">
                <div class="kpi-label">Material Cost</div>
                <div class="kpi-value" style="font-size:18px;">{fmt_currency(br['total_material_cost_blended'])}</div>
                <div class="kpi-sub" style="color:{mc_color};">{mc_text}</div>
              </div>
              <div class="kpi-card" style="padding:12px 14px;">
                <div class="kpi-label">Cost / Unit</div>
                <div class="kpi-value" style="font-size:18px;">{fmt_currency(br['blended_cost_per_unit'], 4)}</div>
                <div class="kpi-sub" style="color:{cpu_color};">{cpu_text}</div>
              </div>
              <div class="kpi-card" style="padding:12px 14px;">
                <div class="kpi-label">Yield</div>
                <div class="kpi-value" style="font-size:18px;">{fmt_num(br['actual_yield']) if pd.notna(br['actual_yield']) else '--'}</div>
                <div class="kpi-sub">{br['yield_units'] or ''}</div>
              </div>
              <div class="kpi-card" style="padding:12px 14px;">
                <div class="kpi-label">Coverage</div>
                <div class="kpi-value" style="font-size:18px;">{br['dollar_coverage_pct']*100:.0f}%</div>
                <div class="kpi-sub">data completeness</div>
              </div>
            </div>
            """
            st.markdown(kpi_cards_html, unsafe_allow_html=True)

            # ── YIELD RECONCILIATION STRIP ───────────────────────────────────
            ry = compute_yield_reconciliation(silver, br)
            if ry["status"] == "computed":
                var_color = "#3fb950" if ry["variance"] >= 0 else "#f85149"
                var_str = f"{'+' if ry['variance']>=0 else ''}{ry['variance']:,}"
                yield_html = f"""
                <div style="background:#161b22; border:1px solid #21262d; border-radius:6px; padding:10px 14px; margin-bottom:14px; font-family:'DM Mono',monospace; font-size:12px;">
                  <span style="color:#8b949e;">Theo Max:</span>
                  <span style="color:#e8e8e8; font-weight:500;"> {ry['theo_max']:,}</span>
                  <span style="color:#8b949e;"> (bound by {ry['binding_ingredient']})</span>
                  <span style="color:#21262d; margin:0 8px;">|</span>
                  <span style="color:#8b949e;">Actual:</span>
                  <span style="color:#e8e8e8; font-weight:500;"> {ry['actual']:,}</span>
                  <span style="color:#21262d; margin:0 8px;">|</span>
                  <span style="color:#8b949e;">Variance:</span>
                  <span style="color:{var_color}; font-weight:500;"> {var_str}</span>
                </div>
                """
                st.markdown(yield_html, unsafe_allow_html=True)
            elif ry["status"] in ("no_reliable_rates", "all_outliers"):
                st.caption("Theoretical yield not computed — recipe rates pending or insufficient batch history for this SKU.")

        # Load ingredient detail
        with st.spinner("Loading ingredient detail..."):
            silver = load_silver(sel_batch)

        if silver.empty:
            st.warning("No ingredient data found for this batch.")
        else:
            st.markdown('<div class="section-header">Raw Material Detail</div>', unsafe_allow_html=True)

            # Ingredient filters
            fc1, fc2 = st.columns(2)
            with fc1:
                cats = ["All"] + sorted(silver["Item_Category"].dropna().unique().tolist())
                sel_cat = st.selectbox("Category", cats, key="cat_filter")
            with fc2:
                exc_types = ["All", "Exceptions only", "Clean only"]
                sel_exc = st.selectbox("Show", exc_types, key="exc_filter")

            s = silver.copy()
            if sel_cat != "All":
                s = s[s["Item_Category"] == sel_cat]
            if sel_exc == "Exceptions only":
                s = s[s["exception_flag"].notna() & (s["exception_flag"] != "")]
            elif sel_exc == "Clean only":
                s = s[s["exception_flag"].isna() | (s["exception_flag"] == "")]

            # Build ingredient display
            ingredient_rows = []
            for _, row in s.iterrows():
                pct_var = row["pct_var_vs_last_po"]
                dollar_var = row["dollar_var_vs_last_po"]
                var_class = var_color_class(pct_var)

                yield_qty = br["actual_yield"] if pd.notna(br["actual_yield"]) and br["actual_yield"] > 0 else None
                ext_cost  = row["batch_extended_cost"] if pd.notna(row["batch_extended_cost"]) else None
                contrib   = (ext_cost / yield_qty) if (ext_cost is not None and yield_qty) else None

                ingredient_rows.append({
                    "Ingredient":     row["rm_item_name"],
                    "Category":       row["Item_Category"] or "--",
                    "Qty":            f"{fmt_num(row['qty_consumed'], 3)} {row['uom'] or ''}".strip(),
                    "$/Fin. Unit":    fmt_currency(contrib, 4) if contrib is not None else "--",
                    "Extended Cost":  fmt_currency(row["batch_extended_cost"], 2) if pd.notna(row["batch_extended_cost"]) else "--",
                    "Supplier":       row["last_po_supplier"] if pd.notna(row["last_po_supplier"]) and row["last_po_supplier"] else ("Roshi (No PO)" if (pd.notna(row["batch_unit_cost"]) and row["batch_unit_cost"] > 0) else "--"),
                    "Cost/Unit":      fmt_currency(row["effective_last_po_cost"], 4) if pd.notna(row["effective_last_po_cost"]) else (fmt_currency(row["batch_unit_cost"], 4) if pd.notna(row["batch_unit_cost"]) and row["batch_unit_cost"] > 0 else "--"),
                    "Source":         "PO" if pd.notna(row["effective_last_po_cost"]) else ("Roshi" if pd.notna(row["batch_unit_cost"]) and row["batch_unit_cost"] > 0 else "Missing"),
                    "PO #":           row["last_po_order_number"] or "--",
                    "PO Date":        str(row["last_po_date"])[:10] if pd.notna(row["last_po_date"]) else "--",
                    "% vs PO":        fmt_pct(pct_var) if pd.notna(pct_var) else "--",
                    "$ vs PO":        fmt_currency(dollar_var, 2) if pd.notna(dollar_var) else "--",
                    "Flag":           row["exception_flag"] or "",
                })

            ing_df = pd.DataFrame(ingredient_rows)

            def color_pct_var(val):
                if val == "--": return ""
                try:
                    num = float(val.replace("+","").replace("%",""))
                    if num > 10: return "color: #f85149; font-weight: 600"
                    if num > 5:  return "color: #f0883e"
                    if num < -5: return "color: #3fb950"
                except: pass
                return ""

            def color_flag(val):
                colors = {
                    "corrupted_unit_cost":     "color: #d2a8ff; background: #3d1a6e",
                    "missing_batch_cost":      "color: #79c0ff; background: #1a2332",
                    "zero_or_negative_cost":   "color: #f0883e; background: #2d1b00",
                    "variance_above_threshold":"color: #f85149; background: #3d0c0c",
                    "no_po_match":             "color: #d29922; background: #2d2006",
                }
                return colors.get(val, "")

            styled_ing = ing_df.style\
                .map(color_pct_var, subset=["% vs PO"])\
                .map(color_flag, subset=["Flag"])\
                .set_properties(**{
                    "font-family": "DM Mono, monospace",
                    "font-size": "11px",
                })

            st.dataframe(
                styled_ing,
                use_container_width=True,
                height=500,
                hide_index=True,
            )

            # Summary below table
            total_ext = s["batch_extended_cost"].sum()
            total_var  = s["dollar_var_vs_last_po"].sum()
            n_flags    = s["exception_flag"].notna().sum()
            n_no_po    = (s["exception_flag"] == "no_po_match").sum()

            st.markdown("---")
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Ingredients Shown", len(s))
            sc2.metric("Extended Cost", fmt_currency(total_ext))
            sc3.metric("Total $ vs PO", fmt_currency(total_var, 2))
            sc4.metric("Exception Lines", int(n_flags))

            if br["prior_batch_number"]:
                st.caption(
                    f"Prior batch: **{br['prior_batch_number']}** "
                    f"({str(br['prior_batch_date'])[:10] if pd.notna(br['prior_batch_date']) else '--'}) · "
                    f"Cost: {fmt_currency(br['prior_batch_blended_cost'])} · "
                    f"CPU: {fmt_currency(br['prior_batch_cost_per_unit'], 3)}"
                )

# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 -- EXCEPTIONS
# ════════════════════════════════════════════════════════════════════════════════
with tab_exceptions:
    st.markdown('<div class="section-header">Batches Requiring Attention</div>', unsafe_allow_html=True)

    ec1, ec2, ec3 = st.columns(3)

    with ec1:
        st.markdown("**🔴 Low / Critical Coverage**")
        low_cov = df[df["coverage_status"].isin(["low","critical"])]\
            [["Batch_Number","Product_Name","batch_date","dollar_coverage_pct","zero_cost_high_value_line_count"]]\
            .copy()
        low_cov["Coverage %"] = (low_cov["dollar_coverage_pct"] * 100).round(1).astype(str) + "%"
        low_cov["THC Zero"] = low_cov["zero_cost_high_value_line_count"].apply(lambda x: "⚠️" if x > 0 else "")
        low_cov["Date"] = low_cov["batch_date"].astype(str).str[:10]
        st.dataframe(
            low_cov[["Batch_Number","Product_Name","Date","Coverage %","THC Zero"]].rename(columns={"Batch_Number":"Batch","Product_Name":"Product"}),
            use_container_width=True,
            hide_index=True,
            height=350,
        )

    with ec2:
        st.markdown("**🟠 High Variance Batches**")
        hi_var = df[df["variance_exception_count"] > 0]\
            [["Batch_Number","Product_Name","batch_date","variance_exception_count","max_abs_dollar_var"]]\
            .sort_values("max_abs_dollar_var", ascending=False).copy()
        hi_var["Date"] = hi_var["batch_date"].astype(str).str[:10]
        hi_var["Max $ Var"] = hi_var["max_abs_dollar_var"].apply(fmt_currency)
        hi_var["# Flags"] = hi_var["variance_exception_count"].astype(int)
        st.dataframe(
            hi_var[["Batch_Number","Product_Name","Date","# Flags","Max $ Var"]].rename(columns={"Batch_Number":"Batch","Product_Name":"Product"}),
            use_container_width=True,
            hide_index=True,
            height=350,
        )

    with ec3:
        st.markdown("**🟡 THC / High-Value Zero Cost**")
        thc_zero = df[df["has_zero_cost_high_value_input"] == True]\
            [["Batch_Number","Product_Name","batch_date","zero_cost_high_value_line_count","total_estimated_missing_cost"]]\
            .copy()
        thc_zero["Date"] = thc_zero["batch_date"].astype(str).str[:10]
        thc_zero["Zero Lines"] = thc_zero["zero_cost_high_value_line_count"].astype(int)
        thc_zero["Est. Exposure"] = thc_zero["total_estimated_missing_cost"].apply(fmt_currency)
        st.dataframe(
            thc_zero[["Batch_Number","Product_Name","Date","Zero Lines","Est. Exposure"]].rename(columns={"Batch_Number":"Batch","Product_Name":"Product"}),
            use_container_width=True,
            hide_index=True,
            height=350,
        )

    st.markdown('<div class="section-header">Cost Trend by Product</div>', unsafe_allow_html=True)

    trend_products = sorted(df["Product_Name"].dropna().unique().tolist())
    sel_trend = st.selectbox("Product", trend_products, key="trend_product")

    trend_df = df[df["Product_Name"] == sel_trend].sort_values("batch_date").copy()
    trend_df = trend_df[trend_df["blended_cost_per_unit"].notna()]

    if len(trend_df) > 1:
        chart_data = trend_df[["batch_date","blended_cost_per_unit","total_material_cost_blended"]].copy()
        chart_data = chart_data.rename(columns={
            "batch_date": "Date",
            "blended_cost_per_unit": "Cost Per Unit ($)",
            "total_material_cost_blended": "Total Batch Cost ($)"
        }).set_index("Date")
        st.line_chart(chart_data[["Cost Per Unit ($)"]], use_container_width=True, height=250)
    else:
        st.info("Need at least 2 batches to show trend.")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 4 -- PRODUCT VIEW
# ════════════════════════════════════════════════════════════════════════════════
with tab_product:
    st.markdown('<div class="section-header">Product Cost Summary</div>', unsafe_allow_html=True)

    # Product selector
    pv_products = sorted(gold["Product_Name"].dropna().unique().tolist())
    sel_pv = st.selectbox("Select Product / Flavor", pv_products, key="pv_product")

    pv = gold[gold["Product_Name"] == sel_pv].copy()
    pv["batch_date"] = pd.to_datetime(pv["batch_date"])
    pv = pv.sort_values("batch_date")

    if pv.empty:
        st.warning("No batch data for this product.")
    else:
        # ── KPIs ─────────────────────────────────────────────────────────────
        n_batches     = len(pv)
        total_cost    = pv["total_material_cost_blended"].sum()
        total_units   = pv["actual_yield"].sum()
        avg_cpu       = pv["blended_cost_per_unit"].mean()
        median_cpu    = pv["blended_cost_per_unit"].median()
        latest_cpu    = pv.iloc[-1]["blended_cost_per_unit"]
        latest_cost   = pv.iloc[-1]["total_material_cost_blended"]
        latest_yield  = pv.iloc[-1]["actual_yield"]
        latest_batch  = pv.iloc[-1]["Batch_Number"]
        first_cpu     = pv.iloc[0]["blended_cost_per_unit"]
        cpu_direction = ((latest_cpu - first_cpu) / first_cpu) if pd.notna(first_cpu) and first_cpu > 0 else None
        best_batch    = pv.loc[pv["blended_cost_per_unit"].idxmin()] if pv["blended_cost_per_unit"].notna().any() else None
        worst_batch   = pv.loc[pv["blended_cost_per_unit"].idxmax()] if pv["blended_cost_per_unit"].notna().any() else None

        dir_arrow = ""
        if cpu_direction is not None:
            dir_arrow = f"{'▲' if cpu_direction > 0 else '▼'} {abs(cpu_direction)*100:.1f}% first→last"

        st.markdown(f"""
        <div class="kpi-row">
            <div class="kpi-card">
                <div class="kpi-label">Production Runs</div>
                <div class="kpi-value">{n_batches}</div>
                <div class="kpi-sub">batches total</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Latest Run Cost</div>
                <div class="kpi-value">{fmt_currency(latest_cost)}</div>
                <div class="kpi-sub">{latest_batch} · {fmt_num(latest_yield)} units</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Latest CPU</div>
                <div class="kpi-value">{fmt_currency(latest_cpu, 3)}</div>
                <div class="kpi-sub">blended cost / unit</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Avg CPU (All Runs)</div>
                <div class="kpi-value">{fmt_currency(avg_cpu, 3)}</div>
                <div class="kpi-sub">median {fmt_currency(median_cpu, 3)}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">CPU Trend</div>
                <div class="kpi-value">{fmt_currency(latest_cpu, 3)}</div>
                <div class="kpi-sub">{dir_arrow}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── BEST / WORST ──────────────────────────────────────────────────────
        if best_batch is not None and worst_batch is not None:
            bw1, bw2 = st.columns(2)
            with bw1:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">🟢 Lowest Cost Run</div>
                    <div class="kpi-value">{fmt_currency(best_batch["blended_cost_per_unit"], 3)}</div>
                    <div class="kpi-sub">{best_batch["Batch_Number"]} · {str(best_batch["batch_date"])[:10]}</div>
                </div>
                """, unsafe_allow_html=True)
            with bw2:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">🔴 Highest Cost Run</div>
                    <div class="kpi-value">{fmt_currency(worst_batch["blended_cost_per_unit"], 3)}</div>
                    <div class="kpi-sub">{worst_batch["Batch_Number"]} · {str(worst_batch["batch_date"])[:10]}</div>
                </div>
                """, unsafe_allow_html=True)

        # ── CPU TREND CHART ───────────────────────────────────────────────────
        st.markdown('<div class="section-header">Cost Per Unit — All Runs</div>', unsafe_allow_html=True)

        chart_pv = pv[pv["blended_cost_per_unit"].notna()][["batch_date","blended_cost_per_unit"]].copy()
        chart_pv = chart_pv.rename(columns={"batch_date": "Date", "blended_cost_per_unit": "Cost Per Unit ($)"}).set_index("Date")

        if len(chart_pv) > 1:
            st.line_chart(chart_pv, use_container_width=True, height=220)
        else:
            st.info("Need at least 2 batches to show trend.")

        # ── BATCH TABLE ───────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Run-by-Run Breakdown</div>', unsafe_allow_html=True)

        pv_rows = []
        for _, r in pv.sort_values("batch_date", ascending=False).iterrows():
            pct_var = r["pct_cpu_vs_prior_batch"]
            pv_rows.append({
                "Batch":          r["Batch_Number"],
                "Date":           str(r["batch_date"])[:10],
                "Yield":          f"{fmt_num(r['actual_yield'])} {r['yield_units'] or ''}".strip(),
                "Material Cost":  fmt_currency(r["total_material_cost_blended"]),
                "CPU (Blended)":  fmt_currency(r["blended_cost_per_unit"], 3),
                "vs Prior Run (CPU)":   fmt_pct(pct_var) if pd.notna(pct_var) else "--",
                "$ vs Prior (CPU)":     fmt_currency(r["dollar_cpu_vs_prior_batch"], 4) if pd.notna(r["dollar_cpu_vs_prior_batch"]) else "--",
                "Coverage":       r["coverage_status"],
                "Var Flags":      int(r["variance_exception_count"]) if pd.notna(r["variance_exception_count"]) else 0,
            })

        pv_df = pd.DataFrame(pv_rows)

        styled_pv = pv_df.style            .map(color_coverage, subset=["Coverage"])            .map(color_variance, subset=["vs Prior Run (CPU)"])            .set_properties(**{"font-family": "DM Mono, monospace", "font-size": "12px"})

        st.dataframe(styled_pv, use_container_width=True, height=400, hide_index=True)

        st.caption(
            f"{sel_pv} · {n_batches} runs · "
            f"Total cost {fmt_currency(total_cost)} · "
            f"Total units {fmt_num(total_units)} · "
            f"Avg CPU {fmt_currency(avg_cpu, 3)}"
        )

        # ── INLINE DRILLDOWN ─────────────────────────────────────────────────
        st.markdown('<div class="section-header">Batch Ingredient Drilldown</div>', unsafe_allow_html=True)

        drill_options = pv.sort_values("batch_date", ascending=False)["Batch_Number"].tolist()
        sel_drill = st.selectbox(
            "Select run to inspect",
            drill_options,
            format_func=lambda b: (
                f"{b}  ·  "
                f"{str(pv[pv['Batch_Number']==b]['batch_date'].values[0])[:10]}  ·  "
                f"CPU {fmt_currency(pv[pv['Batch_Number']==b]['blended_cost_per_unit'].values[0], 3)}  ·  "
                f"{fmt_num(pv[pv['Batch_Number']==b]['actual_yield'].values[0])} units"
            ),
            key="pv_drill"
        )

        with st.spinner("Loading ingredient detail..."):
            drill_silver = load_silver(sel_drill)

        if drill_silver.empty:
            st.warning("No ingredient data for this batch.")
        else:
            drill_br    = pv[pv["Batch_Number"] == sel_drill].iloc[0]
            drill_yield = drill_br["actual_yield"] if pd.notna(drill_br["actual_yield"]) and drill_br["actual_yield"] > 0 else None

            drill_rows = []
            for _, row in drill_silver.iterrows():
                ext_cost   = row["batch_extended_cost"] if pd.notna(row["batch_extended_cost"]) else None
                contrib    = (ext_cost / drill_yield) if (ext_cost is not None and drill_yield) else None
                pct_var    = row["pct_var_vs_last_po"]
                dollar_var = row["dollar_var_vs_last_po"]
                drill_rows.append({
                    "Ingredient":     row["rm_item_name"],
                    "Category":       row["Item_Category"] or "--",
                    "Qty":            f"{fmt_num(row['qty_consumed'], 3)} {row['uom'] or ''}".strip(),
                    "$/Fin. Unit":    fmt_currency(contrib, 4) if contrib is not None else "--",
                    "Extended Cost":  fmt_currency(ext_cost, 2) if ext_cost is not None else "--",
                    "Supplier":       row["last_po_supplier"] if pd.notna(row["last_po_supplier"]) and row["last_po_supplier"] else ("Roshi (No PO)" if (pd.notna(row["batch_unit_cost"]) and row["batch_unit_cost"] > 0) else "--"),
                    "Cost/Unit":      fmt_currency(row["effective_last_po_cost"], 4) if pd.notna(row["effective_last_po_cost"]) else (fmt_currency(row["batch_unit_cost"], 4) if pd.notna(row["batch_unit_cost"]) and row["batch_unit_cost"] > 0 else "--"),
                    "Source":         "PO" if pd.notna(row["effective_last_po_cost"]) else ("Roshi" if pd.notna(row["batch_unit_cost"]) and row["batch_unit_cost"] > 0 else "Missing"),
                    "PO #":           row["last_po_order_number"] or "--",
                    "PO Date":        str(row["last_po_date"])[:10] if pd.notna(row["last_po_date"]) else "--",
                    "% vs PO":        fmt_pct(pct_var) if pd.notna(pct_var) else "--",
                    "$ vs PO":        fmt_currency(dollar_var, 2) if pd.notna(dollar_var) else "--",
                    "Flag":           row["exception_flag"] or "",
                })

            drill_df = pd.DataFrame(drill_rows)

            styled_drill = drill_df.style                 .map(color_pct_var, subset=["% vs PO"])                 .map(color_flag,    subset=["Flag"])                 .set_properties(**{"font-family": "DM Mono, monospace", "font-size": "11px"})

            st.dataframe(styled_drill, use_container_width=True, height=400, hide_index=True)

            dc1, dc2, dc3, dc4 = st.columns(4)
            dc1.metric("Ingredients",        len(drill_silver))
            dc2.metric("Total Extended Cost", fmt_currency(drill_silver["batch_extended_cost"].sum()))
            dc3.metric("Exception Lines",     int(drill_silver["exception_flag"].notna().sum()))
            dc4.metric("No PO Match",         int((drill_silver["match_status"] == "no_po_match").sum()))
