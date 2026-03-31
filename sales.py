import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import plotly.graph_objects as go

st.set_page_config(page_title="PabstBrain | Sales", page_icon="🧠", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>

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
.stTabs [data-baseweb="tab-list"] { background: transparent !important; border-bottom: 1px solid #1e2d4a !important; }
.stTabs [data-baseweb="tab"] { background: transparent !important; color: #475569 !important; font-family: 'DM Mono', monospace !important; font-size: 0.72rem !important; }
.stTabs [aria-selected="true"] { color: #38bdf8 !important; border-bottom: 2px solid #38bdf8 !important; background: transparent !important; }
.stSelectbox > div > div { background: #111827 !important; border: 1px solid #1e2d4a !important; color: #e2e8f0 !important; font-family: 'DM Mono', monospace !important; font-size: 0.75rem !important; }
div[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace !important; }
</style>
""", unsafe_allow_html=True)

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
    if abs(v) >= 1_000_000: return f"${v:,.0f}"
    if abs(v) >= 1_000: return f"${v:,.0f}"
    return f"${v:,.0f}"

def fmt_number(v):
    if v is None: return "0"
    return f"{int(v):,}"

# Header
st.markdown("""
<div style="display:flex;align-items:center;justify-content:space-between;padding:1rem 0 0.5rem;border-bottom:1px solid #1e2d4a;margin-bottom:1rem">
    <div><div class="pb-logo">🧠 PABSTBRAIN</div><div class="pb-subtitle">SALES INTELLIGENCE PLATFORM</div></div>
</div>""", unsafe_allow_html=True)

# Period buttons
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

# Filters
f1, f2, f3, f4 = st.columns(4)
with f1:
    rep_opts = ["All Reps"]
    try:
        rep_opts += run_query(f"SELECT DISTINCT soldBy FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders` WHERE deliveryDate BETWEEN '{start_date}' AND '{end_date}' AND soldBy IS NOT NULL ORDER BY soldBy")['soldBy'].tolist()
    except: pass
    sel_rep = st.selectbox("Rep", rep_opts, label_visibility="collapsed")

with f2:
    brand_opts = ["All Brands", "St Ides", "PBR", "NYF"]
    sel_brand = st.selectbox("Brand", brand_opts, label_visibility="collapsed")

with f3:
    wh_opts = ["All Warehouses"]
    try:
        wh_opts += run_query("SELECT DISTINCT sourceWarehouse FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders` WHERE sourceWarehouse IS NOT NULL ORDER BY sourceWarehouse")['sourceWarehouse'].tolist()
    except: pass
    sel_wh = st.selectbox("Warehouse", wh_opts, label_visibility="collapsed")

with f4:
    st.markdown(f'<div style="font-family:DM Mono,monospace;font-size:0.65rem;color:#475569;padding-top:0.5rem">{start_date} → {end_date}</div>', unsafe_allow_html=True)

# WHERE clause
where = [f"deliveryDate BETWEEN '{start_date}' AND '{end_date}'"]
if sel_rep != "All Reps": where.append(f"soldBy = '{sel_rep}'")
if sel_brand != "All Brands": where.append(f"skuDisplayName LIKE '%{sel_brand}%'")
if sel_wh != "All Warehouses": where.append(f"sourceWarehouse = '{sel_wh}'")
wc = " AND ".join(where)

# Summary KPIs
try:
    s = run_query(f"""
    SELECT
        ROUND(SUM(lineItemSubtotal),2) as invoiced,
        ROUND(SUM(grossRevenue),2) as gross,
        ROUND(SUM(grossRevenue - netRevenue),2) as disc,
        ROUND(SUM(CASE WHEN isPennyOut THEN grossRevenue ELSE 0 END),2) as penny_out,
        ROUND(SUM(netRevenue),2) as net,
        COUNT(DISTINCT orderNumber) as orders,
        COUNT(DISTINCT retailerId) as accts,
        SUM(units) as units,
        ROUND(SUM(lineItemSubtotalAfterDiscount)/NULLIF(COUNT(DISTINCT retailerId),0),2) as avg_acct,
        COUNTIF(lineItemSubtotalAfterDiscount < 1000) as under1k,
        ROUND(SUM(CASE WHEN skuDisplayName LIKE '%ST IDES%' OR skuName LIKE '%ST IDES%' OR brandName LIKE '%Pabst%' THEN lineItemSubtotalAfterDiscount ELSE 0 END),2) as st_ides,
        ROUND(SUM(CASE WHEN skuDisplayName LIKE '%PBR%' OR skuName LIKE '%PBR%' THEN lineItemSubtotalAfterDiscount ELSE 0 END),2) as pbr,
        ROUND(SUM(CASE WHEN skuDisplayName LIKE '%NYF%' OR skuName LIKE '%NYF%' THEN lineItemSubtotalAfterDiscount ELSE 0 END),2) as nyf
    FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
    WHERE {wc}
    """).iloc[0]

    k1,k2,k3,k4,k5,k6,k7,k8,k9 = st.columns(9)
    k1.markdown(f'<div class="kpi-card"><div class="kpi-label">Gross (List)</div><div class="kpi-value">{fmt_currency(s.gross)}</div></div>', unsafe_allow_html=True)
    k2.markdown(f'<div class="kpi-card"><div class="kpi-label">Invoiced Total</div><div class="kpi-value">{fmt_currency(s.invoiced)}</div></div>', unsafe_allow_html=True)
    k3.markdown(f'<div class="kpi-card"><div class="kpi-label">Total Discount</div><div class="kpi-value kpi-negative">{fmt_currency(s.disc)}</div></div>', unsafe_allow_html=True)
    k4.markdown(f'<div class="kpi-card"><div class="kpi-label">Penny-Out Promos</div><div class="kpi-value kpi-negative">{fmt_currency(s.penny_out)}</div></div>', unsafe_allow_html=True)
    k5.markdown(f'<div class="kpi-card"><div class="kpi-label">Net Revenue</div><div class="kpi-value kpi-positive">{fmt_currency(s.net)}</div></div>', unsafe_allow_html=True)
    k6.markdown(f'<div class="kpi-card"><div class="kpi-label">Orders</div><div class="kpi-value kpi-neutral">{fmt_number(s.orders)}</div></div>', unsafe_allow_html=True)
    k7.markdown(f'<div class="kpi-card"><div class="kpi-label">Active Accts</div><div class="kpi-value">{fmt_number(s.accts)}</div><div class="kpi-sub">Avg {fmt_currency(s.avg_acct)}</div></div>', unsafe_allow_html=True)
    k8.markdown(f'<div class="kpi-card"><div class="kpi-label">Accts &lt;$1K</div><div class="kpi-value kpi-negative">{fmt_number(s.under1k)}</div></div>', unsafe_allow_html=True)
    k9.markdown(f'<div class="kpi-card"><div class="kpi-label">Units</div><div class="kpi-value">{fmt_number(s.units)}</div></div>', unsafe_allow_html=True)

    b1,b2,b3 = st.columns(3)
    total = max((s.st_ides or 0)+(s.pbr or 0)+(s.nyf or 0), 1)
    b1.markdown(f'<div class="kpi-card" style="background:#0c1a2e"><div class="kpi-label">🔵 St Ides</div><div class="kpi-value" style="color:#38bdf8">{fmt_currency(s.st_ides)}</div><div class="kpi-sub">{(s.st_ides or 0)/total*100:.1f}%</div></div>', unsafe_allow_html=True)
    b2.markdown(f'<div class="kpi-card" style="background:#0c1a2e"><div class="kpi-label">🟦 PBR</div><div class="kpi-value" style="color:#818cf8">{fmt_currency(s.pbr)}</div><div class="kpi-sub">{(s.pbr or 0)/total*100:.1f}%</div></div>', unsafe_allow_html=True)
    b3.markdown(f'<div class="kpi-card" style="background:#0c1a2e"><div class="kpi-label">🟣 NYF</div><div class="kpi-value" style="color:#a78bfa">{fmt_currency(s.nyf)}</div><div class="kpi-sub">{(s.nyf or 0)/total*100:.1f}%</div></div>', unsafe_allow_html=True)
except Exception as e:
    st.warning(f"Waiting for data... {str(e)[:80]}")

# Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Summary", "Sold By", "SKU Details", "Accts Rev/Gaps", "AR Aging", "Store Intel"])

with tab1:
    try:
        cd = run_query(f"""
        SELECT deliveryDate,
            ROUND(SUM(CASE WHEN skuDisplayName LIKE '%ST IDES%' OR skuName LIKE '%ST IDES%' OR brandName LIKE '%Pabst%' THEN lineItemSubtotalAfterDiscount ELSE 0 END),2) as st_ides,
            ROUND(SUM(CASE WHEN skuDisplayName LIKE '%PBR%' OR skuName LIKE '%PBR%' THEN lineItemSubtotalAfterDiscount ELSE 0 END),2) as pbr,
            ROUND(SUM(CASE WHEN skuDisplayName LIKE '%NYF%' OR skuName LIKE '%NYF%' THEN lineItemSubtotalAfterDiscount ELSE 0 END),2) as nyf,
            COUNT(DISTINCT orderNumber) as orders,
            ROUND(SUM(grossRevenue - netRevenue),2) as disc
        FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
        WHERE {wc} GROUP BY deliveryDate ORDER BY deliveryDate
        """)
        fig = go.Figure()
        for col, clr, lbl in [('st_ides','#38bdf8','St Ides'),('pbr','#818cf8','PBR'),('nyf','#a78bfa','NYF')]:
            fig.add_trace(go.Bar(name=lbl, x=cd['deliveryDate'], y=cd[col], marker_color=clr, opacity=0.9))
        fig.update_layout(barmode='stack', paper_bgcolor='#0a0e1a', plot_bgcolor='#111827',
            font=dict(family='DM Mono', color='#94a3b8', size=10),
            legend=dict(orientation='h', yanchor='top', y=-0.15, xanchor='center', x=0.5, bgcolor='rgba(0,0,0,0)'),
            margin=dict(l=0,r=0,t=10,b=0), height=280,
            xaxis=dict(gridcolor='#1e2d4a', showgrid=False),
            yaxis=dict(gridcolor='#1e2d4a', tickprefix='$', tickformat=',.0f'))
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e: st.error(f"Chart error: {str(e)[:200]}")

    st.markdown('<div class="section-header">Daily Detail</div>', unsafe_allow_html=True)
    try:
        dd = run_query(f"""
        SELECT deliveryDate as Date,
            ROUND(SUM(grossRevenue),2) as Gross,
            ROUND(SUM(lineItemSubtotal),2) as Invoiced,
            ROUND(SUM(CASE WHEN isPennyOut THEN grossRevenue ELSE 0 END),2) as Penny_Out,
            ROUND(SUM(grossRevenue - netRevenue),2) as Discount,
            ROUND(SUM(netRevenue),2) as Net,
            COUNT(DISTINCT orderNumber) as Orders,
            ROUND(SUM(CASE WHEN skuDisplayName LIKE '%ST IDES%' OR skuName LIKE '%ST IDES%' OR brandName LIKE '%Pabst%' THEN netRevenue ELSE 0 END),2) as St_Ides,
            ROUND(SUM(CASE WHEN skuDisplayName LIKE '%PBR%' OR skuName LIKE '%PBR%' THEN netRevenue ELSE 0 END),2) as PBR,
            ROUND(SUM(CASE WHEN skuDisplayName LIKE '%NYF%' OR skuName LIKE '%NYF%' THEN netRevenue ELSE 0 END),2) as NYF
        FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
        WHERE {wc} GROUP BY deliveryDate ORDER BY deliveryDate DESC
        """)
        for c in ['Gross','Invoiced','Penny_Out','Discount','Net','St_Ides','PBR','NYF']:
            if c in dd.columns:
                dd[c] = dd[c].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "$0.00")
        st.dataframe(dd, use_container_width=True, height=300)
    except Exception as e: st.error(f"Daily detail error: {str(e)[:200]}")

with tab2:
    try:
        sb = run_query(f"""
        SELECT soldBy as Sales_Rep, COUNT(DISTINCT retailerId) as Accts, COUNT(DISTINCT orderNumber) as Orders,
            SUM(units) as Units,
            ROUND(SUM(lineItemSubtotal),2) as Gross_Rev,
            ROUND(SUM(grossRevenue - lineItemSubtotalAfterDiscount),2) as Discounts,
            ROUND(SUM(lineItemSubtotalAfterDiscount),2) as Net_Rev,
            ROUND(SUM(lineItemSubtotalAfterDiscount)/NULLIF(COUNT(DISTINCT retailerId),0),2) as avg_acct,
            ROUND(SUM(grossRevenue - lineItemSubtotalAfterDiscount)/NULLIF(SUM(lineItemSubtotal),0)*100,1) as `Disc%`
        FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
        WHERE {wc} GROUP BY soldBy ORDER BY Net_Rev DESC
        """)
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=sb['Sales_Rep'], y=sb['Net_Rev'], marker_color='#38bdf8', opacity=0.85,
            text=[fmt_currency(v) for v in sb['Net_Rev']], textposition='outside',
            textfont=dict(family='DM Mono', size=9, color='#94a3b8')))
        fig2.update_layout(paper_bgcolor='#0a0e1a', plot_bgcolor='#111827',
            font=dict(family='DM Mono', color='#94a3b8', size=10),
            margin=dict(l=0,r=0,t=30,b=60), height=260,
            xaxis=dict(gridcolor='#1e2d4a', tickangle=-30),
            yaxis=dict(gridcolor='#1e2d4a', tickprefix='$', tickformat=',.0f'))
        st.plotly_chart(fig2, use_container_width=True)
        disp = sb.copy()
        for c in ['Gross_Rev','Discounts','Net_Rev','avg_acct']:
            disp[c] = disp[c].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
        disp['Disc%'] = disp['Disc%'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else '0%')
        st.dataframe(disp, use_container_width=True, height=350)
    except Exception as e:
        st.info(f"Loading... {str(e)[:60]}")

with tab3:
    try:
        sku = run_query(f"""
        SELECT skuName as Product, COUNT(DISTINCT retailerId) as Accts, SUM(units) as Units,
            ROUND(SUM(units)/NULLIF(COUNT(DISTINCT retailerId),0),1) as Velocity,
            ROUND(SUM(lineItemSubtotalAfterDiscount),2) as Revenue,
            ROUND(SUM(lineItemSubtotalAfterDiscount)/NULLIF(SUM(units),0),2) as avg_sale,
            ROUND(SUM(lineItemSubtotal)/NULLIF(SUM(units),0),2) as Target
        FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
        WHERE {wc} GROUP BY skuName ORDER BY Revenue DESC
        """)
        d = sku.copy()
        for c in ['Revenue','avg_sale','Target']:
            d[c] = d[c].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
        d['Velocity'] = d['Velocity'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else '0')
        st.dataframe(d, use_container_width=True, height=500)
    except Exception as e:
        st.info(f"Loading... {str(e)[:60]}")

with tab4:
    try:
        ac = run_query(f"""
        SELECT retailer as Retailer, siteCity as City, soldBy as Sales_Rep,
            retailerCreditRating as `Credit Rating`,
            COUNT(DISTINCT orderNumber) as Orders, SUM(units) as Units,
            ROUND(SUM(lineItemSubtotalAfterDiscount),2) as Revenue,
            ROUND(SUM(grossRevenue - lineItemSubtotalAfterDiscount),2) as Discounts,
            ROUND(SUM(lineItemSubtotalAfterDiscount)/NULLIF(COUNT(DISTINCT orderNumber),0),2) as `Avg Order`,
            CAST(MAX(deliveryDate) AS STRING) as `Last Delivery`
        FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
        WHERE {wc} GROUP BY retailer,siteCity,soldBy,retailerCreditRating ORDER BY Revenue DESC
        """)
        a1,a2,a3,a4 = st.columns(4)
        a1.metric("Total Accounts", fmt_number(len(ac)))
        a2.metric("Avg Rev/Acct", fmt_currency(ac['Revenue'].mean()))
        a3.metric("Top Account", fmt_currency(ac['Revenue'].max()))
        a4.metric("Accts < $500", fmt_number((ac['Revenue'] < 500).sum()))
        d = ac.copy()
        for c in ['Revenue','Discounts','Avg Order']:
            d[c] = d[c].apply(lambda x: fmt_currency(x) if pd.notna(x) else '$0')
        st.dataframe(d, use_container_width=True, height=450)
    except Exception as e:
        st.info(f"Loading... {str(e)[:60]}")


with tab5:
    # AR Aging filters
    col1, col2, col3 = st.columns(3)
    with col1:
        ar_rep = st.selectbox("Rep", ["All Reps"] + run_query("SELECT DISTINCT soldBy FROM `amplified-name-490015-e0.pabst_mis.gold_ar_aging` WHERE soldBy IS NOT NULL ORDER BY soldBy")['soldBy'].tolist(), key="ar_rep")
    with col2:
        ar_city = st.selectbox("City", ["All Cities"] + run_query("SELECT DISTINCT siteCity FROM `amplified-name-490015-e0.pabst_mis.gold_ar_aging` WHERE siteCity IS NOT NULL AND siteCity != '' ORDER BY siteCity")['siteCity'].tolist(), key="ar_city")
    with col3:
        ar_bucket = st.selectbox("Bucket", ["All","Current (0-15)","Early (16-30)","Warning (31-45)","Late (46-60)","Serious (61-90)","Collections (90+)"], key="ar_bucket")

    ar_where = ["paymentStatus NOT IN ('PAID','REMITTED')"]
    if ar_rep != "All Reps": ar_where.append(f"soldBy = '{ar_rep}'")
    if ar_city != "All Cities": ar_where.append(f"siteCity = '{ar_city}'")
    if ar_bucket != "All": ar_where.append(f"agingBucket = '{ar_bucket}'")
    ar_wc = " AND ".join(ar_where)

    try:
        s = run_query(f"""
        SELECT
          SUM(CASE WHEN agingRank=1 THEN billableAmount ELSE 0 END) as a1,
          SUM(CASE WHEN agingRank=2 THEN billableAmount ELSE 0 END) as a2,
          SUM(CASE WHEN agingRank=3 THEN billableAmount ELSE 0 END) as a3,
          SUM(CASE WHEN agingRank=4 THEN billableAmount ELSE 0 END) as a4,
          SUM(CASE WHEN agingRank=5 THEN billableAmount ELSE 0 END) as a5,
          SUM(CASE WHEN agingRank=6 THEN billableAmount ELSE 0 END) as a6,
          SUM(billableAmount) as total
        FROM `amplified-name-490015-e0.pabst_mis.gold_ar_aging` WHERE {ar_wc}
        """).iloc[0]
        k1,k2,k3,k4,k5,k6,k7 = st.columns(7)
        k1.markdown(f'''<div class="kpi-card" style="border-left:3px solid #34d399"><div class="kpi-label">Current (0-15)</div><div class="kpi-value" style="color:#34d399">{fmt_currency(s.a1)}</div></div>''', unsafe_allow_html=True)
        k2.markdown(f'''<div class="kpi-card" style="border-left:3px solid #fbbf24"><div class="kpi-label">Early (16-30)</div><div class="kpi-value" style="color:#fbbf24">{fmt_currency(s.a2)}</div></div>''', unsafe_allow_html=True)
        k3.markdown(f'''<div class="kpi-card" style="border-left:3px solid #f97316"><div class="kpi-label">Warning (31-45)</div><div class="kpi-value" style="color:#f97316">{fmt_currency(s.a3)}</div></div>''', unsafe_allow_html=True)
        k4.markdown(f'''<div class="kpi-card" style="border-left:3px solid #ef4444"><div class="kpi-label">Late (46-60)</div><div class="kpi-value" style="color:#ef4444">{fmt_currency(s.a4)}</div></div>''', unsafe_allow_html=True)
        k5.markdown(f'''<div class="kpi-card" style="border-left:3px solid #dc2626"><div class="kpi-label">Serious (61-90)</div><div class="kpi-value" style="color:#dc2626">{fmt_currency(s.a5)}</div></div>''', unsafe_allow_html=True)
        k6.markdown(f'''<div class="kpi-card" style="border-left:3px solid #7f1d1d"><div class="kpi-label">Collections (90+)</div><div class="kpi-value" style="color:#f87171">{fmt_currency(s.a6)}</div></div>''', unsafe_allow_html=True)
        k7.markdown(f'''<div class="kpi-card" style="border-left:3px solid #38bdf8"><div class="kpi-label">Total Outstanding</div><div class="kpi-value" style="color:#38bdf8">{fmt_currency(s.total)}</div></div>''', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"AR summary error: {str(e)[:200]}")

    try:
        pivot_where = ["1=1"]
        if ar_rep != "All Reps": pivot_where.append(f"Rep = '{ar_rep}'")
        if ar_city != "All Cities": pivot_where.append(f"City = '{ar_city}'")
        pivot_wc = " AND ".join(pivot_where)
        ar = run_query(f"""
        SELECT Retailer, City, Rep, Rating, Open_Orders,
          Not_Yet_Due, Days_1_15, Days_16_30,
          Days_31_60, Days_61_90, Days_91_120, Days_120_Plus,
          Total_Outstanding, Last_Delivery
        FROM `amplified-name-490015-e0.pabst_mis.gold_ar_aging_pivot`
        WHERE {pivot_wc}
        ORDER BY Total_Outstanding DESC
        """)
        for c in ['Not_Yet_Due','Days_1_15','Days_16_30','Days_31_60','Days_61_90','Days_91_120','Days_120_Plus','Total_Outstanding','Write_Off']:
            ar[c] = ar[c].apply(lambda x: f"${x:,.2f}" if pd.notna(x) and x > 0 else "-")
        st.dataframe(ar, use_container_width=True, height=500)
    except Exception as e:
        st.error(f"AR detail error: {str(e)[:200]}")

with tab6:
    st.markdown('<div class="section-header">Store Intelligence</div>', unsafe_allow_html=True)
    st.caption("Select a rep and account to see purchase history and order recommendations.")
    si_col1, si_col2 = st.columns(2)
    with si_col1:
        si_rep = st.selectbox("Sales Rep", ["All Reps"] + run_query("SELECT DISTINCT soldBy FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders` WHERE soldBy IS NOT NULL ORDER BY soldBy")['soldBy'].tolist(), key="si_rep")
    with si_col2:
        if si_rep != "All Reps":
            acct_q = f"SELECT DISTINCT retailer FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders` WHERE soldBy = '{si_rep}' ORDER BY retailer"
        else:
            acct_q = "SELECT DISTINCT retailer FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders` ORDER BY retailer"
        si_acct = st.selectbox("Account", ["Select Account"] + run_query(acct_q)['retailer'].tolist(), key="si_acct")

    if si_acct != "Select Account":
        try:
            acct_sum = run_query(f"""
            SELECT MAX(soldBy) as rep, MAX(siteCity) as city, MAX(siteCity) as city,
              MAX(paymentTerms) as terms, MAX(retailerCreditRating) as rating,
              COUNT(DISTINCT orderNumber) as total_orders,
              ROUND(SUM(netRevenue),2) as lifetime_revenue,
              ROUND(AVG(netRevenue),2) as avg_order,
              MAX(CAST(deliveryDate AS STRING)) as last_order,
              MAX(paymentStatus) as payment_status
            FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
            WHERE retailer = '{si_acct}'
            """).iloc[0]
            a1,a2,a3,a4,a5 = st.columns(5)
            a1.markdown(f'''<div class="kpi-card"><div class="kpi-label">Lifetime Revenue</div><div class="kpi-value">{fmt_currency(acct_sum.lifetime_revenue)}</div></div>''', unsafe_allow_html=True)
            a2.markdown(f'''<div class="kpi-card"><div class="kpi-label">Total Orders</div><div class="kpi-value">{fmt_number(acct_sum.total_orders)}</div></div>''', unsafe_allow_html=True)
            a3.markdown(f'''<div class="kpi-card"><div class="kpi-label">Avg Order</div><div class="kpi-value">{fmt_currency(acct_sum.avg_order)}</div></div>''', unsafe_allow_html=True)
            a4.markdown(f'''<div class="kpi-card"><div class="kpi-label">Last Order</div><div class="kpi-value" style="font-size:0.9rem">{acct_sum.last_order or "N/A"}</div></div>''', unsafe_allow_html=True)
            a5.markdown(f'''<div class="kpi-card"><div class="kpi-label">Terms | Rating</div><div class="kpi-value" style="font-size:0.9rem">{acct_sum.terms or "N/A"} | {acct_sum.rating or "N/A"}</div></div>''', unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Account summary error: {str(e)[:150]}")

        st.markdown('<div class="section-header">SKU Purchase History</div>', unsafe_allow_html=True)
        try:
            sku_hist = run_query(f"""
            WITH ranked AS (
              SELECT skuName, orderNumber, CAST(MAX(deliveryDate) AS STRING) as deliveryDate,
                SUM(units) as units, ROUND(AVG(pricePerUnit),2) as price,
                ROW_NUMBER() OVER (PARTITION BY skuName ORDER BY MAX(deliveryDate) DESC) as rn
              FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
              WHERE retailer = '{si_acct}'
              GROUP BY skuName, orderNumber
            )
            SELECT skuName as SKU,
              MAX(CASE WHEN rn=1 THEN deliveryDate END) as Inv1_Date,
              MAX(CASE WHEN rn=1 THEN units END) as Inv1_Units,
              MAX(CASE WHEN rn=1 THEN price END) as Inv1_Price,
              MAX(CASE WHEN rn=2 THEN deliveryDate END) as Inv2_Date,
              MAX(CASE WHEN rn=2 THEN units END) as Inv2_Units,
              MAX(CASE WHEN rn=2 THEN price END) as Inv2_Price,
              MAX(CASE WHEN rn=3 THEN deliveryDate END) as Inv3_Date,
              MAX(CASE WHEN rn=3 THEN units END) as Inv3_Units,
              MAX(CASE WHEN rn=3 THEN price END) as Inv3_Price,
              MAX(CASE WHEN rn=4 THEN deliveryDate END) as Inv4_Date,
              MAX(CASE WHEN rn=4 THEN units END) as Inv4_Units,
              MAX(CASE WHEN rn=4 THEN price END) as Inv4_Price,
              MAX(CASE WHEN rn=5 THEN deliveryDate END) as Inv5_Date,
              MAX(CASE WHEN rn=5 THEN units END) as Inv5_Units,
              MAX(CASE WHEN rn=5 THEN price END) as Inv5_Price,
              COUNT(*) as Total_Orders,
              ROUND(AVG(units),0) as Avg_Units
            FROM ranked GROUP BY skuName
            ORDER BY Inv1_Date DESC NULLS LAST
            """)
            st.dataframe(sku_hist, use_container_width=True, height=300)
        except Exception as e:
            st.error(f"SKU history error: {str(e)[:150]}")

        st.markdown('<div class="section-header">Order Recommendations</div>', unsafe_allow_html=True)
        try:
            city_code = run_query(f"SELECT MAX(siteCity) as city FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders` WHERE retailer = '{si_acct}'").iloc[0]['city'] or 'Unknown'
            recs = run_query(f"""
            WITH acct_skus AS (
              SELECT skuName, MAX(deliveryDate) as last_ordered,
                COUNT(DISTINCT orderNumber) as times_ordered,
                ROUND(AVG(units),0) as avg_units
              FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
              WHERE retailer = '{si_acct}'
              GROUP BY skuName
            ),
            all_skus AS (
              SELECT DISTINCT skuName FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
            ),
            zip_perf AS (
              SELECT skuName, COUNT(DISTINCT retailer) as zip_accounts,
                ROUND(AVG(units),0) as zip_avg_units
              FROM `amplified-name-490015-e0.pabst_mis.silver_nabis_orders`
              WHERE siteCity = '{city_code}' AND retailer != '{si_acct}'
                AND deliveryDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
              GROUP BY skuName
            )
            SELECT s.skuName as SKU,
              COALESCE(a.times_ordered,0) as Times_Ordered,
              COALESCE(a.avg_units,0) as Avg_Units,
              CAST(a.last_ordered AS STRING) as Last_Ordered,
              COALESCE(z.zip_accounts,0) as Nearby_Accounts,
              COALESCE(z.zip_avg_units,0) as Nearby_Avg_Units,
              CASE
                WHEN a.skuName IS NULL AND z.zip_accounts > 0 THEN '🆕 New Opportunity'
                WHEN a.skuName IS NOT NULL AND DATE_DIFF(CURRENT_DATE(),a.last_ordered,DAY) <= 30 THEN '✅ Reorder'
                WHEN a.skuName IS NOT NULL AND DATE_DIFF(CURRENT_DATE(),a.last_ordered,DAY) <= 60 THEN '🔄 Re-engage'
                WHEN a.skuName IS NOT NULL AND DATE_DIFF(CURRENT_DATE(),a.last_ordered,DAY) > 60 THEN '⚠️ Lapsed'
                ELSE '📋 Review'
              END as Recommendation
            FROM all_skus s
            LEFT JOIN acct_skus a ON s.skuName = a.skuName
            LEFT JOIN zip_perf z ON s.skuName = z.skuName
            WHERE z.zip_accounts > 0 OR a.skuName IS NOT NULL
            ORDER BY CASE WHEN a.skuName IS NULL THEN 1 ELSE 0 END, z.zip_accounts DESC
            LIMIT 50
            """)
            st.dataframe(recs, use_container_width=True, height=400)
        except Exception as e:
            st.error(f"Recommendations error: {str(e)[:150]}")

st.markdown(f'<div style="margin-top:2rem;padding-top:0.75rem;border-top:1px solid #1e2d4a;font-family:DM Mono,monospace;font-size:0.6rem;color:#334155;display:flex;justify-content:space-between"><span>PABSTBRAIN v1.0</span><span>{start_date} → {end_date}</span><span>Refreshes every 5 min</span></div>', unsafe_allow_html=True)
