"""
analytics.py

Example analytical query against the loaded data:
top 5 teams by total goals scored as the home team.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

database_url = os.getenv('DATABASE_URL')
engine = create_engine(database_url)

top_scorers_query = """
    SELECT time_casa, SUM(gols_casa) AS total_gols
    FROM partida_copas
    GROUP BY time_casa
    ORDER BY SUM(gols_casa) DESC
    LIMIT 5;
"""

top_scorers_df = pd.read_sql_query(top_scorers_query, con=engine)
print(top_scorers_df)