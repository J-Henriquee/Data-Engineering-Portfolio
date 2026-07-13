import pandas as pd
from pathlib import Path

"""
Exploratory Data Analysis (EDA) Script

Loads the processed 'top_1000_animes.csv' dataset and performs 
aggregations to extract business insights regarding anime scores and release status.
"""

# Set absolute relative paths to locate the processed dataset
default_file = Path(__file__).parent
targeted_file = Path(default_file / 'processed_data' / 'top_1000_animes.csv')

# Load the clean dataset into memory
df = pd.read_csv(targeted_file)

print("=== Average Score by Release Status ===")
# Calculate the average score grouped by the anime's broadcasting status
print(df.groupby('Status')['Score'].mean())

print("\n=== Total Record Count by Release Status ===")
# Count the exact number of anime in each status category to validate the sample size
print(df['Status'].value_counts())