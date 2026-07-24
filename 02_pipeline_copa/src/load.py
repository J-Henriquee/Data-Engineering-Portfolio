"""
load.py

Runs the transformation step once, then loads the result into two layers:
  1. Staging (raw/flat): a local CSV snapshot + the 'stg_partidas' table
     in PostgreSQL — the full combined dataset, denormalized, useful for
     quick dashboard queries.
  2. Modeled (star schema): 'dim_selecoes' (teams) and 'fato_jogos'
     (matches, referencing teams by ID) — the normalized version.

Both layers are kept intentionally: staging for fast/flat analytics,
modeled for data integrity and deeper SQL practice.
"""

import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from transform import transform_data

load_dotenv()

database_url = os.getenv('DATABASE_URL')
project_root = Path(__file__).parent.parent
engine = create_engine(database_url)

# Run the transformation once; reused below for both the staging and
# modeled layers (avoids calling the API twice).
transformed_data = transform_data()

# --- Staging layer: local CSV snapshot ---
output_path = project_root / 'data' / 'processed' / 'world_cup_final.csv'
output_path.parent.mkdir(parents=True, exist_ok=True)
transformed_data.to_csv(output_path, index=False, encoding='utf-8')

# --- Staging layer: flat table in PostgreSQL (full refresh every run) ---
transformed_data.to_sql(name='stg_partidas', con=engine, if_exists='replace', index=False)

# --- Modeled layer: clear old data before reloading (keeps this script idempotent) ---
with engine.begin() as conn:
    print("Clearing old data from the modeled tables...")
    conn.execute(text("TRUNCATE TABLE fato_jogos, dim_selecoes RESTART IDENTITY CASCADE;"))
    print("Tables ready for a clean load.")

# --- dim_selecoes: one row per unique team name (home + away combined) ---
team_names = transformed_data.drop(columns=['gols_visitante', 'gols_casa', 'fase', 'data_utc'])
team_names = pd.concat([team_names['time_visitante'], team_names['time_casa']], ignore_index=True)

dim_selecoes_df = pd.DataFrame({'nome_padronizado': team_names})
dim_selecoes_df = dim_selecoes_df.drop_duplicates(subset=['nome_padronizado'], keep='first')

dim_selecoes_df.to_sql(name='dim_selecoes', con=engine, if_exists='append', index=False)

# --- fato_jogos: matches, with team names replaced by their dim_selecoes ID ---
dim_selecoes_lookup = pd.read_sql_query("SELECT id, nome_padronizado FROM dim_selecoes;", con=engine)

fato_jogos_df = transformed_data.merge(
    dim_selecoes_lookup, left_on='time_casa', right_on='nome_padronizado', how='left'
)
fato_jogos_df = fato_jogos_df.rename(
    columns={'id': 'id_time_casa', 'data_utc': 'data'}
).drop(columns=['nome_padronizado'])

fato_jogos_df = fato_jogos_df.merge(
    dim_selecoes_lookup, left_on='time_visitante', right_on='nome_padronizado', how='left'
)
fato_jogos_df = fato_jogos_df.rename(
    columns={'id': 'id_time_visitante'}
).drop(columns=['nome_padronizado'])

fato_jogos_df = fato_jogos_df.drop(columns=['time_visitante', 'time_casa'])

fato_jogos_df.to_sql(name='fato_jogos', con=engine, if_exists='append', index=False)

print("Load complete: stg_partidas, dim_selecoes, and fato_jogos are up to date.")