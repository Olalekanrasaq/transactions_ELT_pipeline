import streamlit as st
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

# Retrieve credentials from Streamlit secrets
redshift_config = st.secrets["connections"]["redshift_db"]

# Construct the connection string
# The 'redshift+psycopg2' dialect is crucial for Redshift connections
connection_string = (
    f"redshift+psycopg2://{redshift_config['user']}:{redshift_config['password']}@"
    f"{redshift_config['host']}:{redshift_config['port']}/{redshift_config['database']}"
)

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Page config
st.set_page_config(page_title="Redshift Dashboard", layout="wide",
                   menu_items={
        'Get Help': 'mailto:olalekanrasaq1331@gmail.com',
        'Report a bug': "mailto:olalekanrasaq1331@gmail.com",
        'About': "A transaction dashboard webapp"
    }
)

# create database connection
@st.cache_resource
def get_connection(connection_string):
    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)
    return engine.connect()

# query fact table
@st.cache_data(ttl=3600)
def load_fact_table():
    conn = get_connection(connection_string)
    query = text("SELECT * FROM public.fct_orders ORDER BY order_id DESC;")
    result = conn.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

@st.cache_data(ttl=3600)
def load_customers_table():
    conn = get_connection(connection_string)
    query = text("SELECT * FROM public.customers ORDER BY customer_id;")
    result = conn.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

df = load_fact_table()
cus_df = load_customers_table()


# Sidebar inputs for DB credentials
st.sidebar.header(":chart_with_upwards_trend: Olak Enterprise")

sel = st.sidebar.selectbox("Menu", ["Sales dashboard", "Transaction table", "Customers Information"])

if sel == "Sales dashboard":
    col1, col2, col3 = st.columns(3, gap="medium")
    with col1:
        with st.container(border=True):
            total_rev = float(df["amount"].sum())
            st.metric(":moneybag: **Total Sales (USD)**", value=f"{total_rev:,.2f}")

    with col2:
        with st.container(border=True):
            total_qty = float(df["quantity"].sum())
            st.metric(":baggage_claim: **Total Quantity Ordered**", value=f"{total_qty:,.0f}")

    with col3:
        with st.container(border=True):
            count_orders = len(df)
            st.metric(":writing_hand: **No of Orders**", value=f"{count_orders:,.0f}")

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        with st.container(vertical_alignment="center"):
            st.caption("**Total Quantity sold per Item**")
            item_qty = df.groupby(df.item_name)["quantity"].sum()
            st.bar_chart(item_qty, x_label="Items", color="#8bfb9a", horizontal=True)

    with chart_col2:
        with st.container(vertical_alignment="center"):
            st.caption("**Total revenue per Item**")
            item_rev = df.groupby(df.item_name)["amount"].sum().astype(int)
            st.bar_chart(item_rev, x_label="Items", color="#fb1779", horizontal=True)

    df_col1, df_col2 = st.columns(2)
    with df_col1:
        with st.expander("View Items Category by Order quantities"):
            st.dataframe(item_qty)
    with df_col2:
        with st.expander("View Items Category by Revenue"):
            st.dataframe(item_rev)


if sel == "Transaction table":
    try:
        st.caption("**:chart: Most recent transactions**")
        st.dataframe(df.head(20))
    except Exception as e:
        st.error(f"Error connecting to Redshift or executing query: {e}")

if sel == "Customers Information":
    try:
        cus_col1, cus_col2, cus_col3 = st.columns(3, gap="small")
        with cus_col1:
            with st.container(border=True):
                no_customers = len(cus_df)
                st.metric(":adult: **No of Customers**", value=f"{no_customers:,.0f}")
        with cus_col2:
            with st.container(border=True):
                no_active_customers = len(df["customer_id"].unique())
                st.metric(":adult: **Active Customers**", value=f"{no_active_customers:,.0f}")
        with cus_col3:
            with st.container(border=True):
                inactive_customers = no_customers - no_active_customers
                st.metric(":adult: **Inactive Customers**", value=f"{inactive_customers:,.0f}")
        
        st.caption("**:adult: Customers Information**")
        st.dataframe(cus_df)
    except Exception as e:
        st.error(f"Error connecting to Redshift or executing query: {e}")