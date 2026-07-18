import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

minha_url_secreta = os.getenv('DATABASE_URL')

engine = create_engine(minha_url_secreta)

df_query = pd.read_sql_query("SELECT time_casa, SUM(gols_casa) FROM partida_copas GROUP BY time_casa ORDER BY SUM(gols_casa) DESC LIMIT 5;", con=engine)

print(df_query)
