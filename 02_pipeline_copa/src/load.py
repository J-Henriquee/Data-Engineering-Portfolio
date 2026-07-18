import os
from transform import transformar_dados
from pathlib import  Path
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

minha_url_secreta = os.getenv('DATABASE_URL')

default_path = Path(__file__).parent.parent

df_tranformado = transformar_dados()

file_path = default_path / 'data' /  'processed' / 'world_cup_final.csv'

file_path.parent.mkdir(parents= True, exist_ok= True)

df_tranformado.to_csv(file_path, index=False, encoding='utf-8')

engine = create_engine(minha_url_secreta)

df_tranformado.to_sql(name = 'partida_copas', con=engine, if_exists= 'replace', index= False)

print(df_tranformado)