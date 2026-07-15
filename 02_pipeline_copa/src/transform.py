import pandas as pd
from extract import extrair_dados_api
from pathlib import Path

country_mapping = {
    'Germany FR': 'Germany',
    'IR Iran': 'Iran',
    'Korea Republic': 'South Korea',
    'Korea DPR': 'North Korea',
    'USA': 'United States',
    "Cte d'Ivoire": "Côte d'Ivoire", # Tratando aquele erro bizarro de codificação do CSV
    'rn">Republic of Ireland': 'Republic of Ireland', # Outro erro de formatação do CSV antigo
    'rn">United Arab Emirates': 'United Arab Emirates',
    'rn">Trinidad and Tobago': 'Trinidad and Tobago',
    'rn">Serbia and Montenegro': 'Serbia and Montenegro',
    'rn">Bosnia and Herzegovina': 'Bosnia-Herzegovina' # A API usa com hífen
}

def transformar_dados():
    
    caminho_base = Path(__file__).resolve().parent.parent
    df_kaggle = pd.read_csv(caminho_base / "data" / "raw" / "world_cup_matches.csv")

    colunas_originais_desejadas = ['Date', 'Stage', 'Home Team', 'Away Team', 'Home Goals', 'Away Goals']
    df_kaggle = df_kaggle[colunas_originais_desejadas]

    df_kaggle = df_kaggle.rename(columns={
        'Date': 'data_utc',
        'Stage': 'fase',
        'Home Team': 'time_casa',
        'Away Team': 'time_visitante',
        'Home Goals': 'gols_casa',
        'Away Goals': 'gols_visitante'
    })

    dados_brutos_api = extrair_dados_api()
    df_api_plano = pd.json_normalize(dados_brutos_api)

    colunas_desejadas = ['utcDate',
                    'stage',
                    'homeTeam.name',
                    'awayTeam.name',
                    'score.fullTime.home',
                    'score.fullTime.away',
                    'status']

    df_api_plano = df_api_plano.reindex(columns=colunas_desejadas)

    df_api_plano = df_api_plano.rename(columns={
        'utcDate': 'data_utc',
        'stage': 'fase',
        'homeTeam.name': 'time_casa',
        'awayTeam.name': 'time_visitante',
        'score.fullTime.home': 'gols_casa',
        'score.fullTime.away': 'gols_visitante'
    })

    df_api_plano = df_api_plano[df_api_plano['status'] == 'FINISHED']
    df_api_plano = df_api_plano.drop(columns=['status'])
    df_api_plano = df_api_plano.dropna()
    df_api_plano['data_utc'] = df_api_plano['data_utc'].str[:10]
    df_kaggle['fase'] = df_kaggle['fase'].str.upper().str.strip()
    df_kaggle['fase'] = df_kaggle['fase'].str.replace(' ', '_').str.replace('-', '_')
    df_final = pd.concat([df_kaggle, df_api_plano], ignore_index=True)  
    df_final['time_casa'] =  df_final['time_casa'].replace(country_mapping)
    df_final['time_visitante'] =  df_final['time_visitante'].replace(country_mapping)




    
    return df_final

