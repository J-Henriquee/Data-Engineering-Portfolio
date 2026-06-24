import requests
import pandas as pd
from pathlib import Path
import time

arquivo_padrao = Path(__file__).parent

def processa_dados(response, n):

    lista_animes = response["data"]
    dados_limpos = []

    for anime in lista_animes:
        anime_filtrado = {
            "Titulo": anime["title"],
            "Score": anime["score"],
            "Rank": anime["rank"],
            "Status": anime["status"],
        }
        dados_limpos.append(anime_filtrado)

    df = pd.DataFrame(dados_limpos)


    df.to_csv(arquivo_padrao / "raw_data" / f"top_animes_pg{n}.csv", index=False)



for n in range(1,41):
    response = requests.get(f'https://api.jikan.moe/v4/top/anime?page={n}')

    if response.status_code == 200:
        print(f"Sucesso! Page_number{n}")
    else:
        raise Exception(f"Erro {response}")
    
    dados_json = response.json()

    processa_dados(dados_json, n)

    time.sleep(2) 
