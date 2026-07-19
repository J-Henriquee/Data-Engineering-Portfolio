"""
transform.py

Combines historical World Cup match data (Kaggle CSV, 1930-2022) with
live match data from the football-data.org API (2026 tournament) into
a single, standardized DataFrame.

Note: column names (time_casa, gols_casa, etc.) are intentionally kept
in Portuguese to match the existing PostgreSQL table schema and the
SQL queries already written against it.
"""

import pandas as pd
from pathlib import Path
from extract import extract_api_data

# Maps inconsistent/legacy country names — including leftover HTML artifacts
# scraped from the original source, e.g. 'rn">Republic of Ireland' — to a
# single standardized name, so the same country isn't split across rows.
country_mapping = {
    'Germany FR': 'Germany',
    'IR Iran': 'Iran',
    'Korea Republic': 'South Korea',
    'Korea DPR': 'North Korea',
    'USA': 'United States',
    "Cte d'Ivoire": "Côte d'Ivoire",
    'rn">Republic of Ireland': 'Republic of Ireland',
    'rn">United Arab Emirates': 'United Arab Emirates',
    'rn">Trinidad and Tobago': 'Trinidad and Tobago',
    'rn">Serbia and Montenegro': 'Serbia and Montenegro',
    'rn">Bosnia and Herzegovina': 'Bosnia-Herzegovina'
}


def transform_data():
    """
    Loads and standardizes the historical (Kaggle) and live (API) match
    datasets, then stacks them into one combined DataFrame.
    """
    project_root = Path(__file__).resolve().parent.parent

    # --- Historical data (Kaggle CSV, 1930-2022) ---
    historical_df = pd.read_csv(project_root / "data" / "raw" / "world_cup_matches.csv")

    selected_columns = ['Date', 'Stage', 'Home Team', 'Away Team', 'Home Goals', 'Away Goals']
    historical_df = historical_df[selected_columns]

    historical_df = historical_df.rename(columns={
        'Date': 'data_utc',
        'Stage': 'fase',
        'Home Team': 'time_casa',
        'Away Team': 'time_visitante',
        'Home Goals': 'gols_casa',
        'Away Goals': 'gols_visitante'
    })

    # --- Live data (football-data.org API, 2026 tournament) ---
    raw_api_data = extract_api_data()
    api_df = pd.json_normalize(raw_api_data)

    api_columns = [
        'utcDate',
        'stage',
        'homeTeam.name',
        'awayTeam.name',
        'score.fullTime.home',
        'score.fullTime.away',
        'status'
    ]
    api_df = api_df.reindex(columns=api_columns)

    api_df = api_df.rename(columns={
        'utcDate': 'data_utc',
        'stage': 'fase',
        'homeTeam.name': 'time_casa',
        'awayTeam.name': 'time_visitante',
        'score.fullTime.home': 'gols_casa',
        'score.fullTime.away': 'gols_visitante'
    })

    # Keep only completed matches (drop scheduled/in-progress games with no score yet)
    api_df = api_df[api_df['status'] == 'FINISHED']
    api_df = api_df.drop(columns=['status'])
    api_df = api_df.dropna()
    api_df['data_utc'] = api_df['data_utc'].str[:10]  # keep only the date part (YYYY-MM-DD)

    # Standardize stage names in the historical dataset (e.g. "Group A" -> "GROUP_A")
    historical_df['fase'] = historical_df['fase'].str.upper().str.strip()
    historical_df['fase'] = historical_df['fase'].str.replace(' ', '_').str.replace('-', '_')

    # Stack historical (1930-2022) and live (2026) data into a single table
    combined_df = pd.concat([historical_df, api_df], ignore_index=True)

    # Apply country name standardization to both team columns
    combined_df['time_casa'] = combined_df['time_casa'].replace(country_mapping)
    combined_df['time_visitante'] = combined_df['time_visitante'].replace(country_mapping)

    return combined_df