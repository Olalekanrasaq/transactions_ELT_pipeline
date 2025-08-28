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

# Sidebar inputs for DB credentials
st.sidebar.header("Olak Consult Enterprise")

sel = st.sidebar.selectbox("Menu", ["Sales dashboard", "Customers Information"])

if sel == "Transaction table":
    try:
        with engine.connect() as connection:
            query = text("SELECT * FROM public.fct_orders LIMIT 10;")
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            st.dataframe(df)
    except Exception as e:
        st.error(f"Error connecting to Redshift or executing query: {e}")