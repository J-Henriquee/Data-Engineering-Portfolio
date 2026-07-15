from transform import transformar_dados
from pathlib import  Path

default_path = Path(__file__).parent.parent

df_tranformado = transformar_dados()

file_path = default_path / 'data' /  'processed' / 'world_cup_final.csv'

file_path.parent.mkdir(parents= True, exist_ok= True)

df_tranformado.to_csv(file_path, index=False, encoding='utf-8')

