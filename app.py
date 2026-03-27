import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from datetime import date, timedelta

st.set_page_config(page_title="Pabst Labs Brain", layout="wide", page_icon="🧠")

USERS = {
    "roy":      {"password": "admin2026",   "role": "admin",       "salesperson": None},
    "chad":     {"password": "chad2026",    "role": "salesperson", "salesperson": "Chad Farnsworth"},
    "timo":     {"password": "timo2026",    "role": "salesperson", "salesperson": "Timo Rodriguez"},
    "lorenzo":  {"password": "lorenzo2026", "role": "salesperson", "salesperson": "Lorenzo Hernandez"},
    "denise":   {"password": "denise2026",  "role": "salesperson", "salesperson": "Denise Alvarez"},
    "john":     {"password": "john2026",    "role": "salesperson", "salesperson": "John Peacock"},
    "karen":    {"password": "karen2026",   "role": "salesperson", "salesperson": "Karen Cruz"},
    "amesh":    {"password": "amesh2026",   "role": "salesperson", "salesperson": "Amesh Patel"},
    "erika":    {"password": "erika2026",   "role": "salesperson", "salesperson": "Erika Fletcher"},
    "miles":    {"password": "miles2026",   "role": "salesperson", "salesperson": "Miles Sookoo"},
}

def login():
    st.title("🧠 Pabst Labs Brain")
    st.subheader("Sign in")
    username = st.text_input("Username").strip().lower()
    password = st.text_input("Password", type="password")
    if st.button("Sign in"):
        user = USERS.get(username)
        if user and user["password"] == password:
            st.session_state["user"] = username
            st.session_state["role"] = user["role"]
            st.session_state["salesperson"] = user["salesperson"]
            st.rerun()
        else:
            st.error("Invalid username or password")

if "user" not in st.session_state:
    login()
    st.stop()

@st.cache_resource
def get_bq_client():
    key_path = os.path.expanduser("~/pabstbrain/bigquery-key.json")
    if os.path.exists(key_path):
        with open(key_path) as f:
            key_data = json.load(f)
        creds = service_account.Credentials.from_service_account_info(key_data)
        return bigquery.Client(credentials=creds, project="amplified-name-490015-e0")
    return bigquery.Client(project="amplified-name-490015-e0")

def run_query(sql):
    client = get_bq_client()
    return client.query(sql).to_dataframe()

def get_date_range(period):
    today = date.today()
    if period == "Current Month":
        return today.replace(day=1), today
    elif period == "Previous Month":
        first = today.replace(day=1)
        last = first - timedelta(days=1)
        return last.replace(day=1), last
    elif period == "Current Quarter":
        q = (today.month - 1) // 3
        return date(today.year, q * 3 + 1, 1), today
    elif period == "Last 30 Days":
        return today - timedelta(days=30), today
    elif period == "Last 60 Days":
        return today - timedelta(days=60), today
    elif period == "Last 90 Days":
        return today - timedelta(days=90), today

role = st.session_state["role"]
salesperson = st.session_state["salesperson"]

col1, col2 = st.columns([4, 1])
with col1:
    st.title("🧠 Pabst Labs Brain")
    st.caption(f"Logged in as {st.session_state['user']} · {role.title()}")
with col2:
    if st.button("Sign out"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

st.divider()

col1, col2, col3, col4 = st.columns([2, 2, 2, 1])

with col1:
    period = st.radio("Time period", [
        "Current Month", "Previous Month", "Current Quarter",
        "Last 30 Days", "Last 60 Days", "Last 90 Days"
    ])

with col2:
    date_basis = st.radio("Date basis", ["Delivery Date", "Order Date"])
    delivered_only = st.checkbox("Delivered only", value=True)

with col3:
    if role == "admin":
        reps = ["All", "Chad Farnsworth", "Timo Rodriguez", "Lorenzo Hernandez",
                "Denise Alvarez", "John Peacock", "Karen Cruz",
                "Amesh Patel", "Erika Fletcher", "Miles Sookoo"]
        selected_rep = st.selectbox("Salesperson", reps)
    else:
        st.info(f"Viewing: {salesperson}")
        selected_rep = salesperson

with col4:
    st.write("")
    st.write("")
    refresh = st.button("🔄 Refresh", use_container_width=True)

start_date, end_date = get_date_range(period)
date_col = "Delivery_Date" if date_basis == "Delivery Date" else "Created_Date"

filters = [
    f"{date_col} >= '{start_date}'",
    f"{date_col} <= '{end_date}'",
]
if delivered_only:
    filters.append("Order_Status = 'DELIVERED'")
if selected_rep != "All":
    filters.append(f"Sold_By = '{selected_rep}'")

where = " AND ".join(filters)

sql_summary = f"""
SELECT Sold_By, Region,
  COUNT(DISTINCT Order_Number) as Orders,
  ROUND(SUM(Revenue), 2) as Revenue,
  ROUND(SUM(COGS), 2) as COGS,
  ROUND(SUM(Gross_Profit), 2) as Gross_Profit,
  ROUND(SAFE_DIVIDE(SUM(Gross_Profit), SUM(Revenue)) * 100, 2) as Margin_Pct
FROM `amplified-name-490015-e0.pabst_mis.gold_sales_pnl`
WHERE {where}
GROUP BY Sold_By, Region
ORDER BY Revenue DESC
"""

sql_detail = f"""
SELECT {date_col} as Date, Order_Number, Order_Name, Sold_By, Region,
  Product_Name, Inventory_Category, Units, Revenue, COGS,
  Gross_Profit, Gross_Margin_Pct, Payment_Status
FROM `amplified-name-490015-e0.pabst_mis.gold_sales_pnl`
WHERE {where}
ORDER BY {date_col} DESC
"""

if refresh:
    st.cache_data.clear()

@st.cache_data(ttl=300)
def load_data(s1, s2):
    return run_query(s1), run_query(s2)

with st.spinner("Loading data..."):
    df_sum, df_det = load_data(sql_summary, sql_detail)

st.subheader(f"Summary — {period} ({start_date} to {end_date})")

if df_sum.empty:
    st.warning("No data found for selected filters.")
else:
    rev = df_sum["Revenue"].sum()
    cogs = df_sum["COGS"].sum()
    gp = df_sum["Gross_Profit"].sum()
    margin = round((gp / rev * 100) if rev > 0 else 0, 2)
    orders = int(df_sum["Orders"].sum())

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Revenue", f"${rev:,.2f}")
    k2.metric("Total COGS", f"${cogs:,.2f}")
    k3.metric("Gross Profit", f"${gp:,.2f}")
    k4.metric("Gross Margin", f"{margin}%")
    k5.metric("Orders", orders)

    st.divider()
    st.subheader("P&L by Salesperson")
    st.dataframe(
        df_sum.style.format({
            "Revenue": "${:,.2f}", "COGS": "${:,.2f}",
            "Gross_Profit": "${:,.2f}", "Margin_Pct": "{:.1f}%"
        }),
        use_container_width=True, hide_index=True
    )

    st.subheader("Revenue vs Gross Profit")
    st.bar_chart(df_sum[["Sold_By","Revenue","Gross_Profit"]].set_index("Sold_By"))

    st.divider()
    st.subheader("Order Detail")
    st.dataframe(
        df_det.style.format({
            "Revenue": "${:,.2f}", "COGS": "${:,.2f}",
            "Gross_Profit": "${:,.2f}", "Gross_Margin_Pct": "{:.1f}%"
        }),
        use_container_width=True, hide_index=True
    )

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("⬇️ Download Summary", df_sum.to_csv(index=False),
            f"pabst_summary_{period.replace(' ','_')}_{start_date}.csv", "text/csv",
            use_container_width=True)
    with c2:
        st.download_button("⬇️ Download Detail", df_det.to_csv(index=False),
            f"pabst_detail_{period.replace(' ','_')}_{start_date}.csv", "text/csv",
            use_container_width=True)
