"""
analytics.py

Example analytical query against the loaded data:
top 5 teams by total goals scored as the home team.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from transform import transform_data
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


tables_df = transform_data()
tables_df = tables_df.drop(columns=['gols_visitante', 'gols_casa', 'fase', 'data_utc'])
tables_df = pd.concat([tables_df['time_visitante'], tables_df['time_casa']], ignore_index=True)
names_df = pd.DataFrame({'nome_padronizado': tables_df})
names_df = names_df.drop_duplicates(subset=['nome_padronizado'], keep='first')

names_df.to_sql(    name='dim_selecoes', 
    con=engine, 
    if_exists='append', 
    index=False          
)

df_dim_selecoes = pd.read_sql_query("SELECT id, nome_padronizado FROM dim_selecoes;", con=engine)
df_fato_jogos = transform_data()
fato_df = df_fato_jogos.merge(df_dim_selecoes, left_on='time_casa', right_on='nome_padronizado', how='left')
fato_df = fato_df.rename(columns={'id': 'id_time_casa'}).drop(columns=['nome_padronizado'])
fato_df = fato_df.merge(df_dim_selecoes, left_on='time_visitante', right_on='nome_padronizado', how='left')
fato_df = fato_df.rename(columns={'id': 'id_time_visitante'}).drop(columns=['nome_padronizado'])
fato_df = fato_df.drop(columns=['time_visitante', 'time_casa'])
print(fato_df)


