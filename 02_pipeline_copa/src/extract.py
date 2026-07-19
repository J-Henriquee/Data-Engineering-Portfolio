"""
extract.py
 
Handles data extraction from the football-data.org REST API.
Fetches all matches for the FIFA World Cup competition (competition code "WC").
"""
 
import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
 
# Project root directory (two levels up from this file: src/ -> project root)
project_root = Path(__file__).parent.parent
 
load_dotenv()
 
api_key = os.getenv("API_KEY_FOOTBALL")
 
# Historical matches CSV (Kaggle dataset), loaded here for reference/validation.
# Note: currently not used inside extract_api_data() below.
raw_matches_df = pd.read_csv(project_root / "data" / "raw" / "world_cup_matches.csv")
 
API_URL = "https://api.football-data.org/v4/competitions/WC/matches"
headers = {"X-Auth-Token": api_key}
 
 
def extract_api_data():
    """
    Calls the football-data.org API and returns the raw list of matches (JSON).
    Returns None if the request fails.
    """
    response = requests.get(API_URL, headers=headers)
 
    if response.status_code == 200:
        print("Successfully fetched match data from the API.")
        response_json = response.json()
        return response_json["matches"]
    else:
        print(f"Request failed. Status code: {response.status_code}")
 