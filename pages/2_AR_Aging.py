"""
pages/2_AR_Aging.py — AR Aging dashboard for Marissa, Arden, and Roy.

Source: gold_ar_aging (snapshot, manually loaded by Marissa)
Audience: Controllers (full book visibility)
Companion: sales.py AR tab is the rep-facing view (their own book)

Phase 0 scope — limited filters and columns until upstream data lands:
  - Awaiting Marissa CSV expansion: City, Credit settings, Restrictions
  - Awaiting HubSpot sync: Parent retailer, Territory
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# ============================================================================
# CONFIG
# ============================================================================

PROJECT_ID = "amplified-name-490015-e0"
DATASET = "pabst_mis"
TABLE = f"`{PROJECT_ID}.{DATASET}.gold_ar_aging`"
PT = pytz.timezone("America/Los_Angeles")

st.set_page_config(
    page_title="AR Aging — St Ides Brain",
    layout="wide",
)

# ============================================================================
# BIGQUERY
# ============================================================================

@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    return get_bq_client().query(sql).to_dataframe()


@st.cache_data(ttl=300, show_spinner=False)
def load_ar_data() -> pd.DataFrame:
    """All AR rows with computed DPD bucket. DSD bucket comes from gold."""
    sql = f"""
    SELECT
      orderNumber,
      retailer,
      retailerId,
      siteCity,
      soldBy,
      retailerCreditRating,
      paymentStatus,
      paymentTerms,
      deliveryDate,
      subtotalDueDate,
      lastUpdated,
      billableAmount,
      daysSinceDelivery,
      agingBucket AS dsdBucket,
      DATE_DIFF(lastUpdated, subtotalDueDate, DAY) AS dpd,
      CASE
        WHEN DATE_DIFF(lastUpdated, subtotalDueDate, DAY) <= 0 THEN 'Current'
        WHEN DATE_DIFF(lastUpdated, subtotalDueDate, DAY) <= 30 THEN '1-30'
        WHEN DATE_DIFF(lastUpdated, subtotalDueDate, DAY) <= 60 THEN '31-60'
        WHEN DATE_DIFF(lastUpdated, subtotalDueDate, DAY) <= 90 THEN '61-90'
        ELSE '90+'
      END AS dpdBucket
    FROM {TABLE}
    """
    return run_query(sql)


# ============================================================================
# CONSTANTS — bucket definitions and styling
# ============================================================================

DPD_BUCKETS = [
    {"value": "Current", "label": "Current", "tone": "neutral"},
    {"value": "1-30",    "label": "1–30",    "tone": "neutral"},
    {"value": "31-60",   "label": "31–60",   "tone": "neutral"},
    {"value": "61-90",   "label": "61–90",   "tone": "amber"},
    {"value": "90+",     "label": "90+",     "tone": "red"},
]

DSD_BUCKETS = [
    {"value": "Current (0-15)",    "label": "Current 0–15",  "tone": "neutral"},
    {"value": "Early (16-30)",     "label": "Early 16–30",   "tone": "neutral"},
    {"value": "Warning (31-45)",   "label": "Warning 31–45", "tone": "neutral"},
    {"value": "Late (46-60)",      "label": "Late 46–60",    "tone": "amber"},
    {"value": "Serious (61-90)",   "label": "Serious 61–90", "tone": "amber"},
    {"value": "Collections (90+)", "label": "Coll. 90+",     "tone": "red"},
]

TONES = {
    "neutral": {"bg": "#FFFFFF", "text": "#1A1A1A", "label": "#666666", "muted": "#999999"},
    "amber":   {"bg": "#FAEEDA", "text": "#633806", "label": "#854F0B", "muted": "#A06A14"},
    "red":     {"bg": "#FCEBEB", "text": "#791F1F", "label": "#A32D2D", "muted": "#C03939"},
}

# ============================================================================
# HELPERS
# ============================================================================

def fmt_money(x: float) -> str:
    if pd.isna(x) or x is None:
        return "$0"
    return f"${x:,.0f}"


def fmt_date(d) -> str:
    if pd.isna(d) or d is None:
        return "—"
    return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)


def now_pt_str() -> str:
    return datetime.now(PT).strftime("%b %d %H:%M PT")


# ============================================================================
# CSS
# ============================================================================

st.markdown("""
<style>
  .block-container { padding-top: 2rem; }
  .ar-title { font-size: 28px; font-weight: 500; margin: 0; }
  .ar-meta { font-size: 11px; color: #999; margin-bottom: 1.5rem; }
  .live-dot { width: 8px; height: 8px; border-radius: 50%; background: #1D9E75; display: inline-block; margin-right: 6px; vertical-align: middle; }
  .live-pill { font-size: 11px; color: #1D9E75; }

  .bucket-card {
    border-radius: 8px;
    padding: 12px 12px 8px;
    margin-bottom: 4px;
    min-height: 80px;
  }
  .bucket-label { font-size: 11px; }
  .bucket-value { font-size: 16px; font-weight: 500; margin: 4px 0 2px; }
  .bucket-count { font-size: 10px; }

  .section-h { font-size: 13px; font-weight: 500; margin-bottom: 4px; margin-top: 1.5rem; }
  .section-sub { font-size: 11px; color: #999; margin-bottom: 10px; }

  div[data-testid="stMetric"] {
    background: #F7F7F2;
    padding: 14px 16px;
    border-radius: 8px;
  }
  div[data-testid="stMetricLabel"] p { font-size: 11px; color: #666; }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# HEADER
# ============================================================================

hcol1, hcol2 = st.columns([5, 1])
with hcol1:
    st.markdown('<div class="ar-title">AR Aging</div>', unsafe_allow_html=True)
with hcol2:
    if st.button("↻ Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ============================================================================
# LOAD DATA
# ============================================================================

try:
    df = load_ar_data()
except Exception as e:
    st.error(f"Failed to load AR data: {e}")
    st.stop()

snap = df["lastUpdated"].max() if len(df) else None

st.markdown(
    f'<div class="ar-meta">'
    f'<span class="live-pill"><span class="live-dot"></span>Live</span>'
    f' &nbsp;·&nbsp; Snapshot {fmt_date(snap)} (Marissa CSV)'
    f' &nbsp;·&nbsp; View computed {now_pt_str()}'
    f' &nbsp;·&nbsp; {len(df):,} invoices'
    f'</div>',
    unsafe_allow_html=True,
)

# ============================================================================
# FILTERS
# ============================================================================

fcol1, fcol2, fcol3, fcol4, fcol5 = st.columns(5)

with fcol1:
    st.selectbox(
        "Territory",
        options=["All"],
        disabled=True,
        help="Awaiting Marissa CSV expansion — region currently single-value (Cen Cal)",
        key="filter_territory",
    )
with fcol2:
    st.selectbox(
        "City",
        options=["All"],
        disabled=True,
        help="Awaiting Marissa CSV expansion — siteCity currently single-value",
        key="filter_city",
    )
with fcol3:
    st.selectbox(
        "Parent retailer",
        options=["All"],
        disabled=True,
        help="Awaiting HubSpot sync — parent retailer mapping not in source data",
        key="filter_parent",
    )
with fcol4:
    stores = ["All"] + sorted(df["retailer"].dropna().unique().tolist())
    selected_store = st.selectbox("Store", options=stores, key="filter_store")
with fcol5:
    st.selectbox(
        "Credit settings",
        options=["All"],
        disabled=True,
        help="Awaiting Marissa CSV expansion — Brand Hold / Nabis Hold not in source",
        key="filter_credit",
    )

# Apply filters
filtered = df.copy()
if selected_store and selected_store != "All":
    filtered = filtered[filtered["retailer"] == selected_store]

st.caption(f"{len(filtered):,} of {len(df):,} invoices")

# ============================================================================
# KPIS
# ============================================================================

total_ar = filtered["billableAmount"].sum() if len(filtered) else 0
past_due_amt = filtered.loc[filtered["dpd"] > 0, "billableAmount"].sum() if len(filtered) else 0
past_due_pct = (past_due_amt / total_ar * 100) if total_ar else 0
oldest_dpd = int(filtered["dpd"].max()) if len(filtered) else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total AR", fmt_money(total_ar))
k2.metric("Past due", fmt_money(past_due_amt))
k3.metric("Past due %", f"{past_due_pct:.1f}%")
k4.metric("Oldest invoice", f"{oldest_dpd} days" if oldest_dpd > 0 else "—")

# ============================================================================
# DRILLDOWN STATE
# ============================================================================

if "drilldown" not in st.session_state:
    st.session_state.drilldown = None  # tuple: (kind, value, label)


def render_bucket_strip(buckets, df_in, bucket_col, kind):
    cols = st.columns(len(buckets))
    for i, b in enumerate(buckets):
        sub = df_in[df_in[bucket_col] == b["value"]]
        amount = sub["billableAmount"].sum()
        count = len(sub)
        tone = TONES[b["tone"]]
        with cols[i]:
            st.markdown(
                f'<div class="bucket-card" style="background: {tone["bg"]};">'
                f'  <div class="bucket-label" style="color: {tone["label"]};">{b["label"]}</div>'
                f'  <div class="bucket-value" style="color: {tone["text"]};">{fmt_money(amount)}</div>'
                f'  <div class="bucket-count" style="color: {tone["muted"]};">'
                f'{count} invoice{"s" if count != 1 else ""}'
                f'  </div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if count > 0:
                if st.button(
                    f"View {count} ↓",
                    key=f"{kind}_btn_{i}",
                    use_container_width=True,
                ):
                    st.session_state.drilldown = (kind, b["value"], b["label"])
                    st.rerun()


# ============================================================================
# DPD STRIP
# ============================================================================

st.markdown('<div class="section-h">Days past due</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-sub">Computed from invoice due date — Net 30 across all retailers</div>',
    unsafe_allow_html=True,
)
render_bucket_strip(DPD_BUCKETS, filtered, "dpdBucket", "dpd")

# ============================================================================
# DSD STRIP
# ============================================================================

st.markdown('<div class="section-h">Days since delivery</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-sub">Nabis native — matches what retailers see in their portal</div>',
    unsafe_allow_html=True,
)
render_bucket_strip(DSD_BUCKETS, filtered, "dsdBucket", "dsd")

# ============================================================================
# DRILLDOWN PANEL
# ============================================================================

if st.session_state.drilldown:
    kind, value, label = st.session_state.drilldown
    bucket_col = "dpdBucket" if kind == "dpd" else "dsdBucket"
    drill = filtered[filtered[bucket_col] == value].sort_values(
        "billableAmount", ascending=False
    )
    title = f"{'DPD' if kind == 'dpd' else 'DSD'} · {label} · {len(drill)} invoices · {fmt_money(drill['billableAmount'].sum())}"

    with st.expander(title, expanded=True):
        ddcol1, ddcol2 = st.columns([1, 5])
        with ddcol1:
            csv = drill.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download CSV",
                data=csv,
                file_name=f"ar_{kind}_{value.replace(' ', '_').replace('+', 'plus')}_{fmt_date(snap)}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with ddcol2:
            if st.button("Close", use_container_width=True):
                st.session_state.drilldown = None
                st.rerun()

        st.dataframe(
            drill[[
                "retailer", "deliveryDate", "subtotalDueDate", "paymentTerms",
                "dpd", "daysSinceDelivery", "billableAmount",
            ]].rename(columns={
                "retailer": "Location",
                "deliveryDate": "Delivered",
                "subtotalDueDate": "Due date",
                "paymentTerms": "Terms",
                "dpd": "DPD",
                "daysSinceDelivery": "DSD",
                "billableAmount": "Balance",
            }),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Balance": st.column_config.NumberColumn(format="$%.0f"),
            },
        )

# ============================================================================
# DETAIL TABLE
# ============================================================================

st.markdown('<div class="section-h">Detail</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-sub">'
    'Credit settings and Restrictions columns omitted — fields not in current source data'
    '</div>',
    unsafe_allow_html=True,
)

display = filtered.sort_values("billableAmount", ascending=False)[[
    "retailer", "deliveryDate", "paymentTerms", "dpd", "daysSinceDelivery", "billableAmount",
]].rename(columns={
    "retailer": "Location",
    "deliveryDate": "Delivered",
    "paymentTerms": "Terms",
    "dpd": "DPD",
    "daysSinceDelivery": "DSD",
    "billableAmount": "Balance",
})

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Balance": st.column_config.NumberColumn(format="$%.0f"),
        "DPD": st.column_config.NumberColumn(format="%d"),
        "DSD": st.column_config.NumberColumn(format="%d"),
    },
)

# ============================================================================
# FOOTER
# ============================================================================

st.caption(
    "AR Aging · gold_ar_aging · Phase 0 (Store filter only) · "
    "Pending: Marissa CSV expansion (City, Credit settings, Restrictions) · "
    "HubSpot sync (Parent retailer, Territory)"
)