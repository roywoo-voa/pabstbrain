import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="PabstBrain | Sales", page_icon="🧠", layout="wide", initial_sidebar_state="collapsed")

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
.audit-banner { background: #0c1a2e; border: 1px solid #1e2d4a; border-left: 3px solid #38bdf8; border-radius: 4px; padding: 0.5rem 0.75rem; font-family: 'DM Mono', monospace; font-size: 0.65rem; color: #475569; margin-bottom: 0.75rem; }
.stTabs [data-baseweb="tab-list"] { background: transparent !important; border-bottom: 1px solid #1e2d4a !important; }
.stTabs [data-baseweb="tab"] { background: transparent !important; color: #475569 !important; font-family: 'DM Mono', monospace !important; font-size: 0.72rem !important; }
.stTabs [aria-selected="true"] { color: #38bdf8 !important; border-bottom: 2px solid #38bdf8 !important; background: transparent !important; }
.stSelectbox > div > div { background: #111827 !important; border: 1px solid #1e2d4a !important; color: #e2e8f0 !important; font-family: 'DM Mono', monospace !important; font-size: 0.75rem !important; }
div[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace !important; }
.stDataFrame { font-family: 'DM Mono', monospace !important; font-size: 0.75rem !important; }
</style>
""", unsafe_allow_html=True)

# ── AUTH ──────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    from google.oauth2 import service_account
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        return bigquery.Client(credentials=creds, project="amplified-name-490015-e0")
    except:
        return bigquery.Client(project="amplified-name-490015-e0")

@st.cache_data(ttl=300)
def run_query(sql):
    client = get_bq_client()
    return client.query(sql).to_dataframe()

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
SEMANTIC = "`amplified-name-490015-e0.pabst_mis.gold_sales_semantic`"
DETAIL   = "`amplified-name-490015-e0.pabst_mis.gold_sales_detail`"
BRAND_COLORS = {'St Ides': '#38bdf8', 'PBR': '#818cf8', 'NYF': '#a78bfa', 'UNMAPPED': '#475569'}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def get_period_dates(period):
    today = date.today()
    if period == "Curr Month":
        return date(today.year, today.month, 1), today
    elif period == "Prev Month":
        first = date(today.year, today.month, 1)
        end = first - timedelta(days=1)
        return date(end.year, end.month, 1), end
    elif period == "Curr QTR":
        q = ((today.month - 1) // 3) * 3 + 1
        return date(today.year, q, 1), today
    elif period == "Prev QTR":
        q = ((today.month - 1) // 3) * 3 + 1
        end = date(today.year, q, 1) - timedelta(days=1)
        pq = ((end.month - 1) // 3) * 3 + 1
        return date(end.year, pq, 1), end
    elif period == "Curr Year":
        return date(today.year, 1, 1), today
    elif period == "Prev Year":
        return date(today.year - 1, 1, 1), date(today.year - 1, 12, 31)
    elif period == "Last 30D":
        return today - timedelta(days=30), today
    elif period == "Last 90D":
        return today - timedelta(days=90), today
    return date(today.year, today.month, 1), today

def fmt_currency(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "$0"
    if abs(v) >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if abs(v) >= 1_000: return f"${v:,.0f}"
    return f"${v:,.0f}"

def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "0.0%"
    return f"{v:.1f}%"

def fmt_number(v):
    if v is None: return "0"
    return f"{int(v):,}"

def kpi(label, value, sub=None, color=""):
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    return f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value {color}">{value}</div>{sub_html}</div>'

def plotly_defaults():
    return dict(
        paper_bgcolor='#0a0e1a', plot_bgcolor='#111827',
        font=dict(family='DM Mono', color='#94a3b8', size=10),
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis=dict(gridcolor='#1e2d4a', showgrid=False),
        yaxis=dict(gridcolor='#1e2d4a', tickprefix='$', tickformat=',.0f')
    )

# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex;align-items:center;justify-content:space-between;padding:1rem 0 0.5rem;border-bottom:1px solid #1e2d4a;margin-bottom:1rem">
    <div><div class="pb-logo">🧠 PABSTBRAIN</div><div class="pb-subtitle">SALES INTELLIGENCE — DIRECT RETAIL ONLY</div></div>
</div>""", unsafe_allow_html=True)

# ── PERIOD SELECTOR ───────────────────────────────────────────────────────────
periods = ["Curr Month", "Prev Month", "Curr QTR", "Prev QTR", "Curr Year", "Prev Year", "Last 30D", "Last 90D"]
if "period" not in st.session_state:
    st.session_state.period = "Curr Month"

pcols = st.columns(len(periods) + 2)
for i, p in enumerate(periods):
    if pcols[i].button(p, key=f"p_{p}"):
        st.session_state.period = p

start_date, end_date = get_period_dates(st.session_state.period)
with pcols[-2]:
    start_date = st.date_input("From", value=start_date, label_visibility="collapsed")
with pcols[-1]:
    end_date = st.date_input("To", value=end_date, label_visibility="collapsed")

# ── FILTERS ───────────────────────────────────────────────────────────────────
f1, f2, f3 = st.columns(3)
with f1:
    rep_opts = ["All Reps"]
    try:
        rep_opts += run_query(f"SELECT DISTINCT soldBy FROM {SEMANTIC} WHERE orderDate BETWEEN '{start_date}' AND '{end_date}' AND soldBy IS NOT NULL ORDER BY soldBy")['soldBy'].tolist()
    except: pass
    sel_rep = st.selectbox("Rep", rep_opts, label_visibility="collapsed")

with f2:
    brand_opts = ["All Brands", "St Ides", "PBR", "NYF"]
    sel_brand = st.selectbox("Brand", brand_opts, label_visibility="collapsed")

with f3:
    wh_opts = ["All Warehouses"]
    try:
        wh_opts += run_query(f"SELECT DISTINCT sourceWarehouse FROM {SEMANTIC} WHERE sourceWarehouse IS NOT NULL ORDER BY sourceWarehouse")['sourceWarehouse'].tolist()
    except: pass
    sel_wh = st.selectbox("Warehouse", wh_opts, label_visibility="collapsed")

# ── WHERE CLAUSE (canonical) ──────────────────────────────────────────────────
where = [f"orderDate BETWEEN '{start_date}' AND '{end_date}'"]
if sel_rep != "All Reps":    where.append(f"soldBy = '{sel_rep}'")
if sel_brand != "All Brands": where.append(f"brand_clean = '{sel_brand}'")
if sel_wh != "All Warehouses": where.append(f"sourceWarehouse = '{sel_wh}'")
wc = " AND ".join(where)

# ── AUDIT BANNER ──────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="audit-banner">
  SOURCE: gold_sales_semantic (DIRECT_RETAIL only) &nbsp;|&nbsp;
  REVENUE: netRevenue = lineItemSubtotal − allocated_orderDiscount − allocated_creditMemo &nbsp;|&nbsp;
  COGS: materials only (labor excluded) &nbsp;|&nbsp;
  CREDIT MEMO: reliable from Feb 15 2025 &nbsp;|&nbsp;
  PERIOD: {start_date} → {end_date} &nbsp;|&nbsp;
  REFRESH: 5 min
</div>""", unsafe_allow_html=True)

# ── SUMMARY KPIs ──────────────────────────────────────────────────────────────
try:
    s = run_query(f"""
    SELECT
      ROUND(SUM(grossRevenue), 2)                          AS gross,
      ROUND(SUM(netRevenue), 2)                            AS net,
      ROUND(SUM(discount_amount), 2)                       AS disc,
      ROUND(SUM(CASE WHEN isPennyOut THEN pennyOutValue ELSE 0 END), 2) AS promo,
      COUNT(DISTINCT orderNumber)                          AS orders,
      COUNT(DISTINCT retailerId)                           AS accts,
      ROUND(SUM(netRevenue) / NULLIF(COUNT(DISTINCT orderNumber), 0), 2) AS avg_order,
      ROUND(SUM(units), 0)                                 AS units,
      ROUND(SUM(CASE WHEN brand_clean = 'St Ides' THEN netRevenue ELSE 0 END), 2) AS st_ides,
      ROUND(SUM(CASE WHEN brand_clean = 'PBR'     THEN netRevenue ELSE 0 END), 2) AS pbr,
      ROUND(SUM(CASE WHEN brand_clean = 'NYF'     THEN netRevenue ELSE 0 END), 2) AS nyf
    FROM {SEMANTIC}
    WHERE {wc}
    """).iloc[0]

    k = st.columns(8)
    k[0].markdown(kpi("Gross (List)", fmt_currency(s.gross)), unsafe_allow_html=True)
    k[1].markdown(kpi("Net Revenue", fmt_currency(s.net), color="kpi-positive"), unsafe_allow_html=True)
    k[2].markdown(kpi("Discounts", fmt_currency(s.disc), color="kpi-negative"), unsafe_allow_html=True)
    k[3].markdown(kpi("Promos", fmt_currency(s.promo), color="kpi-negative"), unsafe_allow_html=True)
    k[4].markdown(kpi("Orders", fmt_number(s.orders), color="kpi-neutral"), unsafe_allow_html=True)
    k[5].markdown(kpi("Accts", fmt_number(s.accts), f"Avg {fmt_currency(s.avg_order)}"), unsafe_allow_html=True)
    k[6].markdown(kpi("Avg Order", fmt_currency(s.avg_order), color="kpi-neutral"), unsafe_allow_html=True)
    k[7].markdown(kpi("Units", fmt_number(s.units)), unsafe_allow_html=True)

    total_brand = max((s.st_ides or 0) + (s.pbr or 0) + (s.nyf or 0), 1)
    b1, b2, b3 = st.columns(3)
    b1.markdown(kpi("🔵 St Ides", fmt_currency(s.st_ides), f"{(s.st_ides or 0)/total_brand*100:.1f}% of net", "kpi-neutral"), unsafe_allow_html=True)
    b2.markdown(kpi("🟦 PBR",     fmt_currency(s.pbr),     f"{(s.pbr or 0)/total_brand*100:.1f}% of net"), unsafe_allow_html=True)
    b3.markdown(kpi("🟣 NYF",     fmt_currency(s.nyf),     f"{(s.nyf or 0)/total_brand*100:.1f}% of net"), unsafe_allow_html=True)

except Exception as e:
    st.warning(f"KPI error: {str(e)[:120]}")

# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Summary", "Rep Performance", "Account Intelligence", "SKU / Brand", "Audit / Detail"
])

# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — SUMMARY
# ════════════════════════════════════════════════════════════════════════════════
with tab1:
    try:
        cd = run_query(f"""
        SELECT orderDate,
          ROUND(SUM(CASE WHEN brand_clean = 'St Ides' THEN netRevenue ELSE 0 END), 2) AS st_ides,
          ROUND(SUM(CASE WHEN brand_clean = 'PBR'     THEN netRevenue ELSE 0 END), 2) AS pbr,
          ROUND(SUM(CASE WHEN brand_clean = 'NYF'     THEN netRevenue ELSE 0 END), 2) AS nyf,
          COUNT(DISTINCT orderNumber) AS orders
        FROM {SEMANTIC}
        WHERE {wc} GROUP BY orderDate ORDER BY orderDate
        """)
        fig = go.Figure()
        for col, clr, lbl in [('st_ides','#38bdf8','St Ides'),('pbr','#818cf8','PBR'),('nyf','#a78bfa','NYF')]:
            fig.add_trace(go.Bar(name=lbl, x=cd['orderDate'], y=cd[col],
                marker_color=clr, opacity=0.9))
        fig.update_layout(barmode='stack', height=280,
            legend=dict(orientation='h', yanchor='top', y=-0.15, xanchor='center', x=0.5, bgcolor='rgba(0,0,0,0)'),
            **plotly_defaults())
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Chart error: {str(e)[:150]}")

    st.markdown('<div class="section-header">Daily Summary</div>', unsafe_allow_html=True)
    try:
        dd = run_query(f"""
        SELECT
          orderDate                                          AS Date,
          COUNT(DISTINCT orderNumber)                        AS Orders,
          COUNT(DISTINCT retailerId)                         AS Accts,
          ROUND(SUM(grossRevenue), 2)                        AS Gross,
          ROUND(SUM(discount_amount), 2)                     AS Discounts,
          ROUND(SUM(CASE WHEN isPennyOut THEN pennyOutValue ELSE 0 END), 2) AS Promos,
          ROUND(SUM(netRevenue), 2)                          AS Net,
          ROUND(SUM(CASE WHEN brand_clean = 'St Ides' THEN netRevenue ELSE 0 END), 2) AS St_Ides,
          ROUND(SUM(CASE WHEN brand_clean = 'PBR'     THEN netRevenue ELSE 0 END), 2) AS PBR,
          ROUND(SUM(CASE WHEN brand_clean = 'NYF'     THEN netRevenue ELSE 0 END), 2) AS NYF
        FROM {SEMANTIC}
        WHERE {wc} GROUP BY orderDate ORDER BY orderDate DESC
        """)
        for c in ['Gross','Discounts','Promos','Net','St_Ides','PBR','NYF']:
            if c in dd.columns:
                dd[c] = dd[c].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")
        st.dataframe(dd, use_container_width=True, height=300)
    except Exception as e:
        st.error(f"Daily detail error: {str(e)[:150]}")

# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — REP PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════
with tab2:
    try:
        rp = run_query(f"""
        SELECT
          soldBy                                                     AS Rep,
          COUNT(DISTINCT retailerId)                                 AS Accts,
          COUNT(DISTINCT orderNumber)                                AS Orders,
          ROUND(SUM(units), 0)                                       AS Units,
          ROUND(SUM(grossRevenue), 2)                                AS Gross,
          ROUND(SUM(discount_amount), 2)                             AS Discounts,
          ROUND(SUM(CASE WHEN isPennyOut THEN pennyOutValue ELSE 0 END), 2) AS Promos,
          ROUND(SUM(netRevenue), 2)                                  AS Net_Rev,
          ROUND(SUM(netRevenue) / NULLIF(COUNT(DISTINCT retailerId), 0), 2) AS Avg_Acct,
          ROUND(SUM(netRevenue) / NULLIF(COUNT(DISTINCT orderNumber), 0), 2) AS Avg_Order,
          ROUND(SUM(discount_amount) / NULLIF(SUM(grossRevenue), 0) * 100, 1) AS Disc_Pct,
          ROUND(SUM(CASE WHEN isPennyOut THEN pennyOutValue ELSE 0 END)
            / NULLIF(SUM(grossRevenue), 0) * 100, 1)                AS Promo_Pct
        FROM {SEMANTIC}
        WHERE {wc} AND soldBy IS NOT NULL
        GROUP BY soldBy ORDER BY Net_Rev DESC
        """)

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=rp['Rep'], y=rp['Net_Rev'], marker_color='#38bdf8', opacity=0.85,
            text=[fmt_currency(v) for v in rp['Net_Rev']], textposition='outside',
            textfont=dict(family='DM Mono', size=9, color='#94a3b8')
        ))
        fig2.update_layout(height=260, xaxis=dict(tickangle=-30, gridcolor='#1e2d4a'),
            yaxis=dict(gridcolor='#1e2d4a', tickprefix='$', tickformat=',.0f'),
            **{k:v for k,v in plotly_defaults().items() if k not in ['xaxis','yaxis']})
        st.plotly_chart(fig2, use_container_width=True)

        disp = rp.copy()
        for c in ['Gross','Discounts','Promos','Net_Rev','Avg_Acct','Avg_Order']:
            disp[c] = disp[c].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
        disp['Disc_Pct']  = disp['Disc_Pct'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else '0%')
        disp['Promo_Pct'] = disp['Promo_Pct'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else '0%')
        st.dataframe(disp, use_container_width=True, height=350)
    except Exception as e:
        st.error(f"Rep performance error: {str(e)[:150]}")

# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — ACCOUNT INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════════════
with tab3:
    a1, a2 = st.columns(2)
    with a1:
        st.markdown('<div class="section-header">Top Accounts</div>', unsafe_allow_html=True)
        try:
            top = run_query(f"""
            SELECT
              retailerName                                              AS Account,
              siteCity                                                  AS City,
              soldBy                                                    AS Rep,
              retailerCreditRating                                      AS Rating,
              COUNT(DISTINCT orderNumber)                               AS Orders,
              ROUND(SUM(netRevenue), 2)                                 AS Net_Rev,
              ROUND(SUM(netRevenue) / NULLIF(COUNT(DISTINCT orderNumber), 0), 2) AS Avg_Order,
              CAST(MAX(orderDate) AS STRING)                            AS Last_Order
            FROM {SEMANTIC}
            WHERE {wc}
            GROUP BY retailerName, siteCity, soldBy, retailerCreditRating
            ORDER BY Net_Rev DESC
            LIMIT 50
            """)
            disp = top.copy()
            for c in ['Net_Rev','Avg_Order']:
                disp[c] = disp[c].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
            st.dataframe(disp, use_container_width=True, height=400)
        except Exception as e:
            st.error(f"Top accounts error: {str(e)[:150]}")

    with a2:
        st.markdown('<div class="section-header">Inactive / Lapsed (No Order 60+ Days)</div>', unsafe_allow_html=True)
        try:
            lapsed = run_query(f"""
            SELECT
              retailerName  AS Account,
              siteCity      AS City,
              soldBy        AS Rep,
              CAST(MAX(orderDate) AS STRING)            AS Last_Order,
              DATE_DIFF(CURRENT_DATE(), MAX(orderDate), DAY) AS Days_Since,
              ROUND(SUM(netRevenue), 2)                 AS Lifetime_Rev
            FROM {SEMANTIC}
            WHERE soldBy IS NOT NULL
            GROUP BY retailerName, siteCity, soldBy
            HAVING MAX(orderDate) < DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
              AND MAX(orderDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
            ORDER BY Days_Since DESC
            LIMIT 50
            """)
            disp = lapsed.copy()
            disp['Lifetime_Rev'] = disp['Lifetime_Rev'].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
            st.dataframe(disp, use_container_width=True, height=400)
        except Exception as e:
            st.error(f"Lapsed accounts error: {str(e)[:150]}")

    st.markdown('<div class="section-header">Account Drill-Through</div>', unsafe_allow_html=True)
    try:
        acct_list = run_query(f"SELECT DISTINCT retailerName FROM {SEMANTIC} WHERE soldBy IS NOT NULL ORDER BY retailerName")['retailerName'].tolist()
        sel_acct = st.selectbox("Select Account", ["— Select —"] + acct_list, key="acct_drill")
        if sel_acct != "— Select —":
            acct_safe = sel_acct.replace("'", "\\'")
            ah = run_query(f"""
            SELECT
              orderDate     AS Date,
              orderNumber   AS Order_No,
              sku_name_raw  AS SKU,
              brand_clean   AS Brand,
              ROUND(units, 0) AS Units,
              ROUND(grossRevenue, 2)   AS Gross,
              ROUND(discount_amount, 2) AS Discount,
              ROUND(netRevenue, 2)     AS Net,
              data_quality_flag        AS Flag
            FROM {SEMANTIC}
            WHERE retailerName = '{acct_safe}'
            ORDER BY orderDate DESC
            LIMIT 200
            """)
            for c in ['Gross','Discount','Net']:
                ah[c] = ah[c].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")
            st.dataframe(ah, use_container_width=True, height=400)
    except Exception as e:
        st.error(f"Account drill error: {str(e)[:150]}")

# ════════════════════════════════════════════════════════════════════════════════
# TAB 4 — SKU / BRAND PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-header">Brand Performance</div>', unsafe_allow_html=True)
    try:
        bp = run_query(f"""
        SELECT
          brand_clean                                                  AS Brand,
          COUNT(DISTINCT retailerId)                                   AS Accts,
          COUNT(DISTINCT orderNumber)                                  AS Orders,
          ROUND(SUM(units), 0)                                         AS Units,
          ROUND(SUM(grossRevenue), 2)                                  AS Gross,
          ROUND(SUM(discount_amount), 2)                               AS Discounts,
          ROUND(SUM(CASE WHEN isPennyOut THEN pennyOutValue ELSE 0 END), 2) AS Promos,
          ROUND(SUM(netRevenue), 2)                                    AS Net_Rev,
          ROUND(SUM(discount_amount) / NULLIF(SUM(grossRevenue), 0) * 100, 1) AS Disc_Pct
        FROM {SEMANTIC}
        WHERE {wc}
        GROUP BY brand_clean ORDER BY Net_Rev DESC
        """)
        disp = bp.copy()
        for c in ['Gross','Discounts','Promos','Net_Rev']:
            disp[c] = disp[c].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
        disp['Disc_Pct'] = disp['Disc_Pct'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else '0%')
        st.dataframe(disp, use_container_width=True, height=200)
    except Exception as e:
        st.error(f"Brand error: {str(e)[:150]}")

    st.markdown('<div class="section-header">SKU Performance</div>', unsafe_allow_html=True)
    try:
        sku = run_query(f"""
        SELECT
          brand_clean                                                  AS Brand,
          sku_name_raw                                                 AS SKU,
          COUNT(DISTINCT retailerId)                                   AS Accts,
          ROUND(SUM(units), 0)                                         AS Units,
          ROUND(SUM(units) / NULLIF(COUNT(DISTINCT retailerId), 0), 1) AS Velocity,
          ROUND(SUM(grossRevenue), 2)                                  AS Gross,
          ROUND(SUM(discount_amount), 2)                               AS Discounts,
          ROUND(SUM(CASE WHEN isPennyOut THEN pennyOutValue ELSE 0 END), 2) AS Promos,
          ROUND(SUM(netRevenue), 2)                                    AS Net_Rev,
          ROUND(SUM(netRevenue) / NULLIF(SUM(units), 0), 2)           AS Net_Per_Unit
        FROM {SEMANTIC}
        WHERE {wc}
        GROUP BY brand_clean, sku_name_raw ORDER BY Net_Rev DESC
        """)
        disp = sku.copy()
        for c in ['Gross','Discounts','Promos','Net_Rev','Net_Per_Unit']:
            disp[c] = disp[c].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
        disp['Velocity'] = disp['Velocity'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else '0')
        st.dataframe(disp, use_container_width=True, height=500)
    except Exception as e:
        st.error(f"SKU error: {str(e)[:150]}")

# ════════════════════════════════════════════════════════════════════════════════
# TAB 5 — AUDIT / DETAIL
# ════════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown("""
    <div class="audit-banner">
      Row-level data from <strong>gold_sales_detail</strong> (all channels).
      Every number in this dashboard traces back to these rows.
      COGS = materials only. Credit memo reliable from Feb 15 2025.
      Discount = grossRevenue − netRevenue. netRevenue = lineItemSubtotal − allocated_orderDiscount − allocated_creditMemo.
    </div>""", unsafe_allow_html=True)

    aud_col1, aud_col2, aud_col3 = st.columns(3)
    with aud_col1:
        aud_channel = st.selectbox("Channel", ["DIRECT_RETAIL","LEGACY_CONSIGNMENT","INTERCOMPANY","All"], key="aud_ch")
    with aud_col2:
        aud_flag = st.selectbox("Data Quality", ["All","OK","LEGACY_BATCH","PROMO","ORPHAN","MISSING_COGS"], key="aud_fl")
    with aud_col3:
        aud_brand = st.selectbox("Brand", ["All","St Ides","PBR","NYF","UNMAPPED"], key="aud_br")

    aud_where = [f"orderDate BETWEEN '{start_date}' AND '{end_date}'"]
    if aud_channel != "All": aud_where.append(f"sales_channel = '{aud_channel}'")
    if aud_flag != "All":    aud_where.append(f"data_quality_flag = '{aud_flag}'")
    if aud_brand != "All":   aud_where.append(f"brand_clean = '{aud_brand}'")
    aud_wc = " AND ".join(aud_where)

    try:
        aud_summary = run_query(f"""
        SELECT
          sales_channel,
          data_quality_flag,
          COUNT(*) AS lines,
          ROUND(SUM(netRevenue), 2) AS net_revenue
        FROM {DETAIL}
        WHERE {aud_wc}
        GROUP BY 1, 2 ORDER BY 3 DESC
        """)
        st.dataframe(aud_summary, use_container_width=True, height=150)
    except Exception as e:
        st.error(f"Audit summary error: {str(e)[:150]}")

    st.markdown('<div class="section-header">Row-Level Detail (max 500 rows)</div>', unsafe_allow_html=True)
    try:
        detail = run_query(f"""
        SELECT
          orderDate, orderNumber, sales_channel, soldBy,
          retailerName, siteCity, brand_clean, sku_name_raw,
          ROUND(units, 0)                  AS units,
          ROUND(grossRevenue, 2)           AS gross,
          ROUND(allocated_orderDiscount, 2) AS alloc_disc,
          ROUND(allocated_creditMemo, 2)   AS alloc_credit,
          ROUND(netRevenue, 2)             AS net,
          ROUND(discount_amount, 2)        AS discount,
          ROUND(cost_per_unit, 4)          AS cpu,
          ROUND(cogs_this_line, 2)         AS cogs,
          ROUND(gross_profit, 2)           AS gp,
          data_quality_flag                AS flag,
          brand_mapping_confidence         AS brand_conf
        FROM {DETAIL}
        WHERE {aud_wc}
        ORDER BY orderDate DESC, orderNumber
        LIMIT 500
        """)
        for c in ['gross','alloc_disc','alloc_credit','net','discount','cogs','gp']:
            detail[c] = detail[c].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")

        st.dataframe(detail, use_container_width=True, height=500)

        csv = detail.to_csv(index=False)
        st.download_button(
            label="⬇ Export to CSV",
            data=csv,
            file_name=f"pabstbrain_detail_{start_date}_{end_date}.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"Detail error: {str(e)[:150]}")

# ── FOOTER ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:2rem;padding-top:0.75rem;border-top:1px solid #1e2d4a;
font-family:DM Mono,monospace;font-size:0.6rem;color:#334155;
display:flex;justify-content:space-between">
  <span>PABSTBRAIN v2.0</span>
  <span>SOURCE: gold_sales_semantic | DIRECT_RETAIL ONLY</span>
  <span>{start_date} → {end_date} | Refreshes every 5 min</span>
</div>""", unsafe_allow_html=True)
