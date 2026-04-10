import pandas as pd
import numpy as np
import os

print("🚀 Iniciando o Pipeline de ETL e Anonimização...")

# Pega o caminho absoluto e lê o arquivo=
PASTA_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_ARQUIVO = os.path.join(PASTA_ATUAL, "clima_bahia_hackathon.csv")
print("📥 Lendo a base de dados bruta...")
df = pd.read_csv(CAMINHO_ARQUIVO) 

# 1. TRANSFORMAÇÃO E SPLIT (Transform)
print("✂️ Separando dados de Treino (<=2020) e Teste (2021)...")
df['Ano'] = pd.to_datetime(df['DATA (YYYY-MM-DD)']).dt.year

df_train = df[df['Ano'] <= 2020].copy()
df_test = df[df['Ano'] == 2021].copy()

# 2. ANONIMIZAÇÃO E SEGURANÇA
print("🔒 Criando IDs e Anonimizando o Gabarito...")

# 1º Passo: Cria o ID fake primeiro para todo mundo!
df_test['ID'] = ['ID_' + str(i).zfill(5) for i in range(len(df_test))]

# 2º Passo: Guarda o Gabarito levando o ID junto!
gabarito_secreto = df_test[['ID', 'PRECIPITACAO TOTAL HORARIO (mm)']].copy()

# 3º Passo: Remove as colunas que dão a resposta da base dos participantes
df_test = df_test.drop(columns=['DATA (YYYY-MM-DD)', 'Ano', 'PRECIPITACAO TOTAL HORARIO (mm)'])
# Cria o ID fake
df_test['ID'] = ['ID_' + str(i).zfill(5) for i in range(len(df_test))]

print("🎲 Injetando Ruído Estatístico (Jitter) para evitar Fingerprinting...")
# Adiciona variação nas colunas de Temp e Umidade =
df_test['TEMPERATURA DO AR - BULBO SECO, HORARIA (C)'] += np.random.normal(0, 0.8, size=len(df_test))
df_test['UMIDADE RELATIVA DO AR, HORARIA (%)'] += np.random.normal(0, 2.0, size=len(df_test))
df_test['PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)'] += np.random.normal(0, 1.5, size=len(df_test))

print("🔀 Embaralhando as linhas do Teste...")
df_test = df_test.sample(frac=1).reset_index(drop=True)

# 3. CARGA (Load)
print("💾 Salvando os arquivos processados na pasta...")
df_train.to_csv(os.path.join(PASTA_ATUAL, "train.csv"), index=False)
df_test.to_csv(os.path.join(PASTA_ATUAL, "test_blind.csv"), index=False)
gabarito_secreto.to_csv(os.path.join(PASTA_ATUAL, "gabarito_secreto_lucca.csv"), index=False)

print("✅ Pipeline finalizado com sucesso! Pode checar sua pasta.")
