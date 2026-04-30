import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import json
import os
import plotly.graph_objects as go

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="StIdes Brain | Management",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =============================================================================
# STYLING (matches sales.py)
# =============================================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=DM+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; background-color: #0a0e1a; color: #e2e8f0; }
.stApp { background-color: #0a0e1a; }
.pb-logo { font-family: 'DM Mono', monospace; font-size: 1.1rem; font-weight: 500; color: #38bdf8; letter-spacing: 0.1em; }
.pb-subtitle { font-size: 0.75rem; color: #475569; font-family: 'DM Mono', monospace; }
.stButton > button { background: #111827 !important; color: #94a3b8 !important; border: 1px solid #1e2d4a !important; border-radius: 4px !important; font-size: 0.72rem !important; font-family: 'DM Mono', monospace !important; padding: 0.3rem 0.75rem !important; }
.stButton > button:hover { background: #1e2d4a !important; color: #38bdf8 !important; border-color: #38bdf8 !important; }
.kpi-card { background: #111827; border: 1px solid #1e2d4a; border-radius: 6px; padding: 0.75rem 1rem; }
.kpi-label { font-size: 0.65rem; color: #475569; font-family: 'DM Mono', monospace; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.25rem; }
.kpi-value { font-size: 1.3rem; font-weight: 700; color: #f1f5f9; font-family: 'DM Mono', monospace; line-height: 1; }
.kpi-sub { font-size: 0.65rem; color: #64748b; font-family: 'DM Mono', monospace; margin-top: 0.2rem; }
.kpi-positive { color: #34d399 !important; }
.kpi-negative { color: #f87171 !important; }
.kpi-neutral { color: #38bdf8 !important; }
.section-header { font-family: 'DM Mono', monospace; font-size: 0.65rem; color: #475569; text-transform: uppercase; letter-spacing: 0.1em; margin: 1rem 0 0.5rem 0; padding-bottom: 0.25rem; border-bottom: 1px solid #1e2d4a; }
.confidence-badge { display: inline-block; padding: 0.2rem 0.6rem; background: #0c2818; color: #34d399; border-radius: 10px; font-family: 'DM Mono', monospace; font-size: 0.65rem; font-weight: 500; }
.confidence-badge-warn { background: #2a1a08; color: #fbbf24; }
.audit-banner { background: #0c1a2e; border: 1px solid #1e2d4a; border-left: 3px solid #38bdf8; border-radius: 4px; padding: 0.5rem 0.75rem; font-family: 'DM Mono', monospace; font-size: 0.65rem; color: #475569; margin-bottom: 0.75rem; }
.coming-soon-pill { display: inline-block; padding: 0.1rem 0.5rem; background: #1e2d4a; color: #64748b; border-radius: 8px; font-family: 'DM Mono', monospace; font-size: 0.6rem; margin-left: 0.4rem; }
.stTabs [data-baseweb="tab-list"] { background: transparent !important; border-bottom: 1px solid #1e2d4a !important; }
.stTabs [data-baseweb="tab"] { background: transparent !important; color: #475569 !important; font-family: 'DM Mono', monospace !important; font-size: 0.72rem !important; }
.stTabs [aria-selected="true"] { color: #38bdf8 !important; border-bottom: 2px solid #38bdf8 !important; background: transparent !important; }
.stSelectbox > div > div, .stMultiSelect > div > div { background: #111827 !important; border: 1px solid #1e2d4a !important; color: #e2e8f0 !important; font-family: 'DM Mono', monospace !important; font-size: 0.75rem !important; }
.stRadio > div { background: transparent !important; }
.stDataFrame { font-family: 'DM Mono', monospace !important; font-size: 0.72rem !important; }
div[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace !important; }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# BIGQUERY CLIENT (matches sales.py + app.py pattern)
# =============================================================================
@st.cache_resource
def get_bq_client():
    """
    Returns a BigQuery client. Tries Streamlit secrets first (deployed),
    then local key file (Cloud Shell), then default credentials.
    """
    # Try Streamlit secrets (production deployment)
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        return bigquery.Client(credentials=creds, project="amplified-name-490015-e0")
    except Exception:
        pass

    # Try local key file (Cloud Shell)
    key_path = os.path.expanduser("~/pabstbrain/bigquery-key.json")
    if os.path.exists(key_path):
        with open(key_path) as f:
            key_data = json.load(f)
        creds = service_account.Credentials.from_service_account_info(key_data)
        return bigquery.Client(credentials=creds, project="amplified-name-490015-e0")

    # Default credentials
    return bigquery.Client(project="amplified-name-490015-e0")

@st.cache_data(ttl=300)
def run_query(sql):
    client = get_bq_client()
    return client.query(sql).to_dataframe()

# =============================================================================
# CONSTANTS
# =============================================================================
GOLD_VIEW = "`amplified-name-490015-e0.pabst_mis.gold_management_margin_waterfall`"

COST_COMPONENT_LABELS = {
    "cannabis_emulsion_flower":  "Cannabis Emulsion/Flower",
    "other_ingredients_waste":   "Other Ingredients/Waste",
    "container":                 "Container (Can, Bottle, Mylar, etc.)",
    "case_packaging_waste":      "Case/Master Shipper/Packaging Waste",
    "transportation_to_nabis":   "Transportation to Nabis",
    "pallet":                    "Pallet",
    "labor_coa_testing":         "Total Labor Cost and COA Testing",
    "nabis_logistics":           "Nabis Logistics Cost",
}

# =============================================================================
# HEADER
# =============================================================================
col_l, col_r = st.columns([4, 1])
with col_l:
    st.markdown('<div class="pb-logo">⬢ STIDES BRAIN / MANAGEMENT</div>', unsafe_allow_html=True)
    st.markdown('<div class="pb-subtitle">TAB 1 · COGS · PRICING · GROSS MARGIN</div>', unsafe_allow_html=True)
with col_r:
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.markdown("")

# =============================================================================
# LOAD DATA
# =============================================================================
with st.spinner("Loading margin waterfall..."):
    df = run_query(f"SELECT * FROM {GOLD_VIEW} ORDER BY brand, product_type, sku_name")

if df.empty:
    st.error("No data returned from gold_management_margin_waterfall.")
    st.stop()

# =============================================================================
# CONFIDENCE BADGE
# =============================================================================
total_skus    = len(df)
complete_skus = (df["data_quality"] == "complete").sum()
pct           = complete_skus / total_skus if total_skus > 0 else 0
badge_class   = "confidence-badge" if pct >= 0.75 else "confidence-badge confidence-badge-warn"

st.markdown(
    f'<div class="{badge_class}">● '
    f'{"High" if pct >= 0.75 else "Partial"} confidence — '
    f'{complete_skus} of {total_skus} SKUs fully populated</div> '
    f'<span class="pb-subtitle">  ·  refreshed {datetime.now().strftime("%Y-%m-%d %H:%M")}  ·  '
    f'source: gold_management_margin_waterfall</span>',
    unsafe_allow_html=True
)

# =============================================================================
# FILTERS
# =============================================================================
st.markdown('<div class="section-header">FILTERS</div>', unsafe_allow_html=True)

f1, f2, f3, f4 = st.columns([1.2, 1.4, 2, 1.2])

with f1:
    brands = sorted(df["brand"].dropna().unique().tolist())
    sel_brands = st.multiselect("Brand", brands, default=brands)

with f2:
    df_b = df[df["brand"].isin(sel_brands)] if sel_brands else df
    types = sorted(df_b["product_type"].dropna().unique().tolist())
    sel_types = st.multiselect("Product Type", types, default=types)

with f3:
    df_bt = df_b[df_b["product_type"].isin(sel_types)] if sel_types else df_b
    skus = sorted(df_bt["sku_name"].dropna().unique().tolist())
    sel_skus = st.multiselect("SKU", skus, default=skus, help="Default: all in-scope SKUs")

with f4:
    mode = st.radio(
        "Mode",
        ["Std", "Actual (soon)", "Variance (soon)"],
        horizontal=False,
        help="Standard mode is live. Actual and Variance unlock once silver_nabis_orders has the is_promo flag.",
    )
    if mode != "Std":
        st.caption("⏳ Coming soon")
        mode = "Std"

# Apply filters
mask = df["brand"].isin(sel_brands) & df["product_type"].isin(sel_types) & df["sku_name"].isin(sel_skus)
df_f = df[mask].copy()

if df_f.empty:
    st.warning("No SKUs match filters.")
    st.stop()

# Only complete SKUs drive aggregates (avoid NULL contamination)
df_complete = df_f[df_f["data_quality"] == "complete"].copy()

# =============================================================================
# KPIs
# =============================================================================
st.markdown('<div class="section-header">KEY METRICS · STANDARD MODE</div>', unsafe_allow_html=True)

if df_complete.empty:
    st.warning("No complete-data SKUs in current selection. Adjust filters or wait for missing cost data.")
else:
    avg_target = df_complete["target_sales_price"].astype(float).mean()
    avg_cogs   = df_complete["total_cogs_per_unit"].astype(float).mean()
    avg_gm     = df_complete["std_gross_margin_per_unit"].astype(float).mean()
    avg_gm_pct = df_complete["std_gross_margin_pct"].astype(float).mean() * 100

    k1, k2, k3, k4 = st.columns(4)
    for col, label, value, sub, klass in [
        (k1, "AVG TARGET SALES PRICE", f"${avg_target:,.2f}", "wholesale to retailer", "kpi-neutral"),
        (k2, "AVG COGS / UNIT",        f"${avg_cogs:,.2f}",   "most-recent PO basis", ""),
        (k3, "AVG GROSS MARGIN / UNIT", f"${avg_gm:,.2f}",    "standard, pre-promo",  "kpi-positive"),
        (k4, "AVG % GROSS MARGIN",     f"{avg_gm_pct:,.1f}%", "across selected SKUs", "kpi-positive"),
    ]:
        col.markdown(
            f'<div class="kpi-card"><div class="kpi-label">{label}</div>'
            f'<div class="kpi-value {klass}">{value}</div>'
            f'<div class="kpi-sub">{sub}</div></div>',
            unsafe_allow_html=True
        )

# =============================================================================
# WATERFALL CHART
# =============================================================================
st.markdown('<div class="section-header">GROSS MARGIN WATERFALL · AVG ACROSS SELECTION</div>', unsafe_allow_html=True)

if not df_complete.empty:
    avg_target_p   = df_complete["target_sales_price"].astype(float).mean()
    avg_ingred     = df_complete["total_ingredients_cost"].astype(float).mean()
    avg_pkg        = df_complete["total_packaging_cost"].astype(float).mean()
    avg_ship       = df_complete["total_shipping_cost"].astype(float).mean()
    avg_other      = df_complete["total_other_cogs"].astype(float).mean()
    avg_std_margin = df_complete["std_gross_margin_per_unit"].astype(float).mean()

    fig = go.Figure(go.Waterfall(
        name="Margin walk",
        orientation="v",
        measure=["absolute", "relative", "relative", "relative", "relative", "total"],
        x=["Target Sales Price", "Ingredients", "Packaging", "Shipping", "Other COGS", "Std Gross Margin"],
        y=[avg_target_p, -avg_ingred, -avg_pkg, -avg_ship, -avg_other, avg_std_margin],
        text=[f"${avg_target_p:.2f}", f"-${avg_ingred:.2f}", f"-${avg_pkg:.2f}",
              f"-${avg_ship:.2f}", f"-${avg_other:.2f}", f"${avg_std_margin:.2f}"],
        textposition="outside",
        textfont=dict(family="DM Mono", size=12, color="#e2e8f0"),
        connector={"line": {"color": "#1e2d4a", "width": 1, "dash": "dot"}},
        increasing={"marker": {"color": "#38bdf8"}},
        decreasing={"marker": {"color": "#f87171"}},
        totals={"marker": {"color": "#34d399"}},
    ))
    fig.update_layout(
        height=380,
        plot_bgcolor="#0a0e1a",
        paper_bgcolor="#0a0e1a",
        font=dict(family="DM Sans", color="#94a3b8", size=11),
        showlegend=False,
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(showgrid=False, color="#475569"),
        yaxis=dict(
            showgrid=True, gridcolor="#1e2d4a", zerolinecolor="#1e2d4a",
            color="#475569", tickprefix="$"
        ),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown(
        '<div class="audit-banner">⏳ Promo Drag bar will appear here once silver_nabis_orders gains the is_promo flag and Actual mode is enabled.</div>',
        unsafe_allow_html=True
    )

# =============================================================================
# DETAIL TABLE — full 30-row waterfall, SKUs as columns
# =============================================================================
st.markdown('<div class="section-header">DETAIL TABLE · 30-ROW WATERFALL · SKUs AS COLUMNS</div>', unsafe_allow_html=True)

if not df_f.empty:
    df_sorted = df_f.sort_values(["brand", "product_type", "sku_name"]).reset_index(drop=True)

    def fmt_money(v, decimals=4):
        if pd.isna(v) or v is None:
            return "—"
        return f"${float(v):,.{decimals}f}"

    def fmt_int(v):
        if pd.isna(v) or v is None:
            return "—"
        return f"{int(v):,}"

    def fmt_pct(v, decimals=1):
        if pd.isna(v) or v is None:
            return "—"
        return f"{float(v) * 100:,.{decimals}f}%"

    def fmt_str(v):
        if pd.isna(v) or v is None or v == "":
            return "—"
        return str(v)

    def fmt_x(v, decimals=2):
        if pd.isna(v) or v is None:
            return "—"
        return f"{float(v):,.{decimals}f}x"

    rows = []

    rows.append(("__SECTION__", "Dimensions", []))
    rows.append(("Unit Size",         "str", df_sorted["unit_size"].tolist()))
    rows.append(("Units per Case",    "int", df_sorted["units_per_case"].tolist()))
    rows.append(("Cases per Pallet",  "int", df_sorted["cases_per_pallet"].tolist()))
    rows.append(("Units per Pallet",  "int", df_sorted["units_per_pallet"].tolist()))

    rows.append(("__SECTION__", "Economics", []))
    rows.append(("Shelf Price",                       "money2", df_sorted["shelf_price"].tolist()))
    rows.append(("x Retail Markup on Menu Price",     "x",      df_sorted["retail_markup_x"].tolist()))
    rows.append(("Wholesale Menu Price",              "money2", df_sorted["wholesale_menu_price"].tolist()))
    rows.append(("% Discount to Menu Price",          "pct",    df_sorted["pct_discount_to_menu"].tolist()))
    rows.append(("Target Sales Price",                "money2", df_sorted["target_sales_price"].tolist()))

    rows.append(("__SECTION__", "Ingredients", []))
    rows.append(("Cannabis Emulsion/Flower",          "money4", df_sorted["cannabis_emulsion_flower"].tolist()))
    rows.append(("Other Ingredients/Waste",           "money4", df_sorted["other_ingredients_waste"].tolist()))
    rows.append(("__SUBTOTAL__Total Ingredients Cost", "money4", df_sorted["total_ingredients_cost"].tolist()))
    rows.append(("  % of Target Sales Price",         "pct",    (df_sorted["total_ingredients_cost"] / df_sorted["target_sales_price"]).tolist()))

    rows.append(("__SECTION__", "Packaging", []))
    rows.append(("Container (Can, Bottle, Mylar, etc.)", "money4", df_sorted["container"].tolist()))
    rows.append(("Case/Master Shipper/Packaging Waste",  "money4", df_sorted["case_packaging_waste"].tolist()))
    rows.append(("__SUBTOTAL__Total Packaging Cost",     "money4", df_sorted["total_packaging_cost"].tolist()))
    rows.append(("  % of Target Sales Price",            "pct",    (df_sorted["total_packaging_cost"] / df_sorted["target_sales_price"]).tolist()))

    rows.append(("__SECTION__", "Shipping", []))
    rows.append(("Transportation to Nabis",          "money4", df_sorted["transportation_to_nabis"].tolist()))
    rows.append(("Pallet",                           "money4", df_sorted["pallet"].tolist()))
    rows.append(("__SUBTOTAL__Total Shipping Cost",  "money4", df_sorted["total_shipping_cost"].tolist()))
    rows.append(("  % of Target Sales Price",        "pct",    (df_sorted["total_shipping_cost"] / df_sorted["target_sales_price"]).tolist()))

    rows.append(("__SECTION__", "Other COGS", []))
    rows.append(("Total Labor Cost and COA Testing", "money4", df_sorted["labor_coa_testing"].tolist()))
    rows.append(("Nabis Logistics Cost",             "money4", df_sorted["nabis_logistics"].tolist()))
    rows.append(("__SUBTOTAL__Total Other COGS",     "money4", df_sorted["total_other_cogs"].tolist()))
    rows.append(("  % of Target Sales Price",        "pct",    (df_sorted["total_other_cogs"] / df_sorted["target_sales_price"]).tolist()))

    rows.append(("__SECTION__", "Total COGS", []))
    rows.append(("__SUBTOTAL__Total COGS per Unit",  "money4", df_sorted["total_cogs_per_unit"].tolist()))
    rows.append(("  % of Target Sales Price",        "pct",    (df_sorted["total_cogs_per_unit"] / df_sorted["target_sales_price"]).tolist()))

    rows.append(("__SECTION__", "Margin walk", []))
    rows.append(("__GM__Std Gross Margin (per unit)", "money4", df_sorted["std_gross_margin_per_unit"].tolist()))
    rows.append(("__GM__% Std Gross Margin",          "pct",    df_sorted["std_gross_margin_pct"].tolist()))

    formatters = {
        "str": fmt_str,
        "int": fmt_int,
        "money2": lambda v: fmt_money(v, 2),
        "money4": lambda v: fmt_money(v, 4),
        "pct": fmt_pct,
        "x": fmt_x,
    }

    sku_cols = df_sorted["sku_name"].tolist()
    display_data = []
    for label, fmt_type, values in rows:
        if label == "__SECTION__":
            row = {"Metric": f"— {fmt_type} —"}
            for sku in sku_cols:
                row[sku] = ""
            display_data.append(row)
        else:
            display_label = label.replace("__SUBTOTAL__", "").replace("__GM__", "")
            f = formatters.get(fmt_type, str)
            row = {"Metric": display_label}
            for i, sku in enumerate(sku_cols):
                row[sku] = f(values[i]) if i < len(values) else "—"
            display_data.append(row)

    display_df = pd.DataFrame(display_data)
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=900)

    st.caption(f"Showing {len(sku_cols)} SKU(s). Use the SKU filter above to narrow the view. SKUs with no_cost_data show '—' in cost rows.")

# =============================================================================
# DOWNLOADS
# =============================================================================
st.markdown('<div class="section-header">EXPORTS</div>', unsafe_allow_html=True)
d1, d2 = st.columns(2)

with d1:
    st.download_button(
        "⬇️ Download SKU detail (CSV)",
        df_f.to_csv(index=False),
        f"stides_management_tab1_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        "text/csv",
        use_container_width=True,
    )

with d2:
    st.button("📊 Audit workbook (.xlsx) — coming soon", use_container_width=True, disabled=True)

# =============================================================================
# FOOTER
# =============================================================================
st.markdown("")
st.markdown(
    '<div class="audit-banner">⏳ NEXT: Add is_promo flag to silver_nabis_orders → enables Actual mode '
    '(Avg ASP, Promo Drag bar, Volume Breakdown table).  Then Tab 2: per-employee P&L.</div>',
    unsafe_allow_html=True
)
