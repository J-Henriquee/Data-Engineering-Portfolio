import pandas as pd
from pathlib import Path

arquivo_padrão = Path(__file__).parent
top_animes = sorted(list(Path(arquivo_padrão /'raw_data').glob('*')))
top_1k_animes = []

for file  in top_animes:
    top_1k_animes.append(pd.read_csv(file))


df = pd.concat(top_1k_animes,  ignore_index=True)

df.to_csv(arquivo_padrão / 'processed_data' / 'top_1000_animes.csv')



   
    




