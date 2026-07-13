import pandas as pd
import os
import requests
from dotenv import load_dotenv
from pathlib import Path

default_path = Path(__file__).parent.parent

print(default_path)

load_dotenv()

api_key = os.getenv("API_KEY_FOOTBALL")

df_matches = pd.read_csv(default_path / "data" / "raw" / "world_cup_matches.csv")

print(df_matches.shape)

print(df_matches.columns)

print(df_matches.head())

url = 'https://api.football-data.org/v4/competitions/WC/matches'

headers = {'X-Auth-Token': api_key}

response = requests.get(url,  headers=headers)

if response.status_code == 200:
    print(f"Success!")
    dados_json = response.json()
    df_api = pd.DataFrame(dados_json['matches'])

    print(df_api.columns)
    
    print(df_api.head())


else:
    print(f"Failed!' Status Code: {response.status_code}")

