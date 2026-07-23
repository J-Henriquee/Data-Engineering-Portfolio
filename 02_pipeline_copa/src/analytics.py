"""
analytics.py

Example analytical query against the loaded data:
top 5 teams by total goals scored as the home team.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

database_url = os.getenv('DATABASE_URL')
engine = create_engine(database_url)

teams_name = """
      SELECT nome_padronizado, SUM(gols_casa) AS total__gols 
      FROM  fato_jogos JOIN dim_selecoes 
      ON fato_jogos.id_time_casa = dim_selecoes.id 
      GROUP BY nome_padronizado;"""

teams_name = pd.read_sql_query(teams_name, con=engine)
print(teams_name)