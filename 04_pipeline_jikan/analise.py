import pandas as pd
from pathlib import Path

default_file = Path(__file__).parent

targeted_file = Path(default_file / 'processed_data' / 'top_1000_animes.csv')

df = pd.read_csv(targeted_file)

print(df.groupby('Status')['Score'].mean())


print(df['Status'][df['Status'] == 'Currently Airing'].size)

print(df['Status'][df['Status'] == 'Finished Airing'].size)
