import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Initial page configuration
st.set_page_config(page_title="World Cup Dashboard", layout="wide")
st.title("🏆 Analytics Dashboard: World Cup")
st.write("This dashboard reads data directly from a serverless PostgreSQL database in the cloud (Neon).")

# 2. Load credentials and set up the database connection
load_dotenv()
database_url = os.getenv('DATABASE_URL')


# 3. Function to fetch the data (cached so the app stays fast)
@st.cache_data
def load_data():
    engine = create_engine(database_url)
    # Main analytical query: top scoring teams as home team
    query = """
    SELECT time_casa, SUM(gols_casa) AS total_gols
    FROM partida_copas
    GROUP BY time_casa
    ORDER BY total_gols DESC
    LIMIT 10;
    """
    return pd.read_sql_query(query, con=engine)


# 4. Building the visual interface
try:
    df = load_data()

    st.subheader("Top 10 Highest-Scoring Teams (Home Games)")

    # Interactive bar chart
    st.bar_chart(data=df, x='time_casa', y='total_gols', color="#1E90FF")

    # Raw data table below the chart
    st.write("Raw data table:")
    st.dataframe(df)

except Exception as e:
    st.error(f"Error connecting to the database: {e}")