"""
load.py

Runs the transformation step and loads the resulting dataset:
  1. as a CSV file (data/processed/world_cup_final.csv), and
  2. into the PostgreSQL database (table: partida_copas).
"""

import os
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv
from transform import transform_data

load_dotenv()

database_url = os.getenv('DATABASE_URL')
project_root = Path(__file__).parent.parent

transformed_df = transform_data()

# Save a local copy as CSV
output_path = project_root / 'data' / 'processed' / 'world_cup_final.csv'
output_path.parent.mkdir(parents=True, exist_ok=True)
transformed_df.to_csv(output_path, index=False, encoding='utf-8')

# Load into PostgreSQL (replaces the table on every run — a simple full refresh)
engine = create_engine(database_url)
transformed_df.to_sql(name='partida_copas', con=engine, if_exists='replace', index=False)

print(transformed_df)