import pandas as pd
from pathlib import Path

arquivo_padrão = Path(__file__).parent
top_animes = sorted(list(Path(arquivo_padrão /'raw_data').glob('*.csv')))
top_1k_animes = []

for file  in top_animes:
    top_1k_animes.append(pd.read_csv(file))


df = pd.concat(top_1k_animes,  ignore_index=True)

df.drop_duplicates(inplace=True, ignore_index=True)

df.dropna(inplace = True, ignore_index = True, subset=['Rank'])

print(f"Total de animes após a limpeza: {len(df)}")

df.to_csv(arquivo_padrão / 'processed_data' / 'top_1000_animes.csv', index=False)


   
    







