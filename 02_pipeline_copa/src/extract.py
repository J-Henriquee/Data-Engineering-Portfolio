import pandas as pd
import os
import requests
from dotenv import load_dotenv
from pathlib import Path

default_path = Path(__file__).parent.parent

load_dotenv()

api_key = os.getenv("API_KEY_FOOTBALL")

df_matches = pd.read_csv(default_path / "data" / "raw" / "world_cup_matches.csv")

url = 'https://api.football-data.org/v4/competitions/WC/matches'

headers = {'X-Auth-Token': api_key}


def extrair_dados_api():
    
    response = requests.get(url,  headers=headers)
    
    if response.status_code == 200:
        print(f"Success!")
        dados_json = response.json()
        return dados_json['matches']

    else:
        print(f"Failed!' Status Code: {response.status_code}")
