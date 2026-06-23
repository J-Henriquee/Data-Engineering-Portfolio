import requests
import pandas as pd
from pathlib import Path


arquivo_padrao = Path(__file__).parent

response = requests.get('https://api.jikan.moe/v4/top/anime')

if response.status_code == 200:
    print('Sucesso!')
else:
    raise Exception(f'Erro {response}')


response = response.json()

lista_animes = response['data']
dados_limpos = []

for anime in lista_animes:
    anime_filtrado = {'Titulo': anime['title'], 'Score': anime['score'], 'Rank': anime['rank'], 'Status': anime['status']}
    dados_limpos.append(anime_filtrado)

df = pd.DataFrame(dados_limpos)

print(df)

df.to_csv(arquivo_padrao / 'raw_data' / 'top_animes.csv', index=False)


