import pandas as pd
from pathlib import Path

"""
Data Transformation and Quality Script

This script consolidates raw CSV files from the Jikan API (MyAnimeList).
It performs data quality checks, handles pagination shift duplicates,
removes invalid records (like music videos/commercials without a rank),
and exports a clean dataset ready for Exploratory Data Analysis (EDA).
"""

# Set absolute relative paths using pathlib
default_file = Path(__file__).parent
top_animes = sorted(list(Path(default_file / 'raw_data').glob('*.csv')))
top_1k_animes = []

# Iterate through raw data directory and load all CSVs
for file in top_animes:
    top_1k_animes.append(pd.read_csv(file))

# Consolidate all DataFrames into a single one
df = pd.concat(top_1k_animes, ignore_index=True)

# Data Quality: Remove duplicates caused by API pagination shifts
df.drop_duplicates(inplace=True, ignore_index=True)

# Data Quality: Remove records with null ranks (e.g., Music Videos, Commercials)
df.dropna(inplace=True, ignore_index=True, subset=['Rank'])

# Log the final size of the clean dataset
print(f"Total anime after data cleaning: {len(df)}")

# Export the clean dataset to the processed data layer
df.to_csv(default_file / 'processed_data' / 'top_1000_animes.csv', index=False)