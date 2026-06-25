import requests
import time
import pandas as pd
from pathlib import Path

"""
Data Extraction Script (Extract)

This script consumes the Jikan API (MyAnimeList) to extract the Top 1000 anime.
It handles pagination automatically, applies rate limiting to prevent IP blocks, 
and persists the raw data into local CSV files for later processing.
"""

def processa_dados(dados_json, n):
    """
    Parses the JSON payload and saves the raw data to a CSV file.

    Args:
        dados_json (dict): The JSON response containing the anime data.
        n (int): The current page number, used for naming the output file.
        
    Returns:
        None
    """
    # Set absolute relative paths for the raw data layer
    default_file = Path(__file__).parent
    raw_data_dir = default_file / 'raw_data'
    
    dados_limpos = []
    
    # Iterate through the API response and extract only the necessary fields
    for anime in dados_json['data']:
        dados_limpos.append({
            'Titulo': anime.get('title'),
            'Score': anime.get('score'),
            'Rank': anime.get('rank'),
            'Status': anime.get('status')
        })
        
    # Convert the parsed data into a pandas DataFrame
    df = pd.DataFrame(dados_limpos)
    
    # Export the raw data partition to the designated directory
    df.to_csv(raw_data_dir / f'top_animes_pg{n}.csv', index=False)

# Main Orchestration Loop (Iterate through 40 pages to get 1000 records)
for n in range(1, 41):
    url = f'https://api.jikan.moe/v4/top/anime?page={n}'
    response = requests.get(url)
    
    # Validate the HTTP response before processing the payload
    if response.status_code == 200:
        print(f"Success! Fetched page {n}")
        dados_json = response.json()
        processa_dados(dados_json, n)
    else:
        print(f"Failed to fetch page {n}. Status Code: {response.status_code}")
        
    # Rate Limiting: Pause execution for 2 seconds to respect the API's constraints
    time.sleep(2)