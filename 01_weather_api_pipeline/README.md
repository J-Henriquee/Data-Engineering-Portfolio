# 🌦️ ETL Climate Pipeline

> **Pipeline ETL em Python para dados climáticos históricos** — projeto de portfólio que demonstra boas práticas de Engenharia de Dados: modularização, tratamento de erros, logging, tipagem, colunas derivadas e persistência em formato colunar particionado.

---

## 📑 Índice

1. [Visão Geral](#-visão-geral)
2. [Arquitetura](#-arquitetura)
3. [Estrutura do Projeto](#-estrutura-do-projeto)
4. [Pré-requisitos](#-pré-requisitos)
5. [Instalação e Execução (Linux)](#-instalação-e-execução-linux)
6. [Detalhamento das Etapas ETL](#-detalhamento-das-etapas-etl)
7. [Schema do Dado Final](#-schema-do-dado-final)
8. [Lendo os Dados em Parquet](#-lendo-os-dados-em-parquet)
9. [Possíveis Extensões](#-possíveis-extensões)
10. [Tecnologias Utilizadas](#-tecnologias-utilizadas)

---

## 🔍 Visão Geral

Este projeto implementa um pipeline ETL completo que:

| Fase | O que faz |
|------|-----------|
| **Extract** | Consome a API pública [Open-Meteo](https://open-meteo.com/) (gratuita, sem autenticação) para obter dados climáticos horários das últimas 7 dias de 5 cidades brasileiras. |
| **Transform** | Usa **Pandas** para limpar os dados, tratar nulos, converter tipos, decodificar códigos WMO e criar colunas derivadas (heat index, categoria de vento, flag de chuva). |
| **Load** | Persiste o DataFrame final em **Parquet** comprimido (Snappy) e particionado por `year / month / day` via **PyArrow**. |

A API Open-Meteo não exige cadastro nem chave de API — ideal para projetos de portfólio e estudos.

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                         ETL Pipeline                            │
│                                                                 │
│  ┌──────────────┐    ┌──────────────────┐    ┌───────────────┐  │
│  │   EXTRACT    │    │    TRANSFORM     │    │     LOAD      │  │
│  │              │    │                 │    │               │  │
│  │ Open-Meteo   │───▶│ • Renomear cols │───▶│ Parquet       │  │
│  │ REST API     │    │ • Parse datetime │    │ particionado  │  │
│  │              │    │ • Tratar nulos   │    │ year/month/   │  │
│  │ 5 cidades    │    │ • Converter tipos│    │ day (Snappy)  │  │
│  │ 7 dias atrás │    │ • Map WMO codes  │    │               │  │
│  │ Dados horário│    │ • Heat Index     │    │ data/output/  │  │
│  └──────────────┘    │ • Wind Category  │    └───────────────┘  │
│                      │ • Flag chuva     │                       │
│                      └──────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘

Fonte de dados : api.open-meteo.com (HTTPS, sem auth)
Orquestrador   : run_pipeline() em etl_pipeline.py
Logging        : stdout + etl_pipeline.log
```

O código segue o princípio de **funções puras e composáveis**: cada etapa (extract, transform, load) pode ser testada e substituída independentemente.

---

## 📁 Estrutura do Projeto

```
etl-climate-pipeline/
│
├── etl_pipeline.py        # Script principal (Extract → Transform → Load)
├── requirements.txt       # Dependências Python fixadas
├── README.md              # Esta documentação
│
├── data/
│   └── output/            # Gerado automaticamente na primeira execução
│       └── year=YYYY/
│           └── month=MM/
│               └── day=DD/
│                   └── part-0.parquet
│
└── etl_pipeline.log       # Gerado automaticamente (log de execução)
```

---

## ✅ Pré-requisitos

| Requisito | Versão mínima | Observação |
|-----------|---------------|------------|
| Python | 3.11+ | Usa `date` e type hints modernos |
| pip | 23+ | Gerenciador de pacotes |
| Acesso à internet | — | Para consultar a API Open-Meteo |

Dependências Python (veja `requirements.txt`):

```
requests==2.32.3
pandas==2.2.3
numpy==1.26.4
pyarrow==16.1.0
```

---

## 🚀 Instalação e Execução (Linux)

### 1. Clone ou copie o projeto

```bash
git clone https://github.com/seu-usuario/etl-climate-pipeline.git
cd etl-climate-pipeline
```

### 2. Crie e ative o ambiente virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instale as dependências

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Execute o pipeline

```bash
python etl_pipeline.py
```

Você verá uma saída similar a:

```
2024-10-15 09:00:01 | INFO     | ============================================================
2024-10-15 09:00:01 | INFO     | Iniciando pipeline ETL  |  2024-10-08 → 2024-10-15  |  5 cidade(s).
2024-10-15 09:00:01 | INFO     | ============================================================
2024-10-15 09:00:02 | INFO     | Extraindo dados para 'Salvador' (2024-10-08 → 2024-10-15).
2024-10-15 09:00:02 | INFO     | 'Salvador' – 168 registros recebidos.
...
2024-10-15 09:00:06 | INFO     | Consolidação concluída. Total de registros: 840.
2024-10-15 09:00:06 | INFO     | Salvando 840 registros em 'data/output' (particionado por year/month/day).
2024-10-15 09:00:07 | INFO     | Pipeline ETL finalizado com sucesso.

── Amostra dos dados transformados ──────────────────────────────
 city            datetime  temp_celsius  ...  weather_desc  heat_index_celsius  wind_category
Salvador  2024-10-08 00:00         26.5  ...   Céu limpo              27.1         Brisa leve
...
Shape final : (840, 17)
```

### 5. Verificar saída gerada

```bash
# Listar arquivos Parquet criados
find data/output -name "*.parquet"

# Verificar tamanho total
du -sh data/output/
```

### Desativar o ambiente virtual

```bash
deactivate
```

---

## 🔬 Detalhamento das Etapas ETL

### Extract — `extract()`

- Realiza `GET` para `https://api.open-meteo.com/v1/forecast` com os parâmetros de localização e período.
- Variáveis coletadas: `temperature_2m`, `relative_humidity_2m`, `precipitation`, `windspeed_10m`, `weathercode`.
- Tratamento de erros para `HTTPError`, `ConnectionError` e `Timeout`.
- Timeout de 30 segundos por requisição.

### Transform — `transform()`

| Transformação | Detalhe |
|---------------|---------|
| Renomeação | Nomes mais legíveis (ex: `temperature_2m` → `temp_celsius`) |
| Parse de datas | `pd.to_datetime()` com `errors="coerce"` |
| Colunas de tempo | `date`, `hour`, `year`, `month`, `day` extraídas do datetime |
| Tratamento de nulos | Numéricos preenchidos com mediana; categórico com valor padrão |
| Tipagem explícita | `float32`, `int8`, `int16` para economia de memória |
| Decodificação WMO | Código numérico → descrição textual (`"Céu limpo"`, `"Tempestade"`, …) |
| Heat Index | Sensação térmica calculada pela fórmula de Rothfusz (válida para T > 27°C) |
| Wind Category | Escala de Beaufort simplificada via `pd.cut()` em 6 faixas |
| Flag de chuva | `has_precipitation` (bool) quando `precip_mm > 0` |

### Load — `load()`

- Usa `DataFrame.to_parquet()` com engine **PyArrow**.
- Compressão **Snappy** (balanço ideal entre velocidade e tamanho).
- Particionamento por `year / month / day` — padrão Hive, compatível com Spark, DuckDB, Athena, etc.
- `existing_data_behavior="overwrite_or_ignore"` evita duplicação em re-execuções.

---

## 📐 Schema do Dado Final

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `city` | `object` | Nome da cidade |
| `datetime` | `datetime64[ns]` | Data e hora da observação |
| `temp_celsius` | `float32` | Temperatura a 2 m (°C) |
| `humidity_pct` | `float32` | Umidade relativa (%) |
| `precip_mm` | `float32` | Precipitação acumulada na hora (mm) |
| `wind_kmh` | `float32` | Velocidade do vento a 10 m (km/h) |
| `wmo_code` | `int16` | Código WMO de condição meteorológica |
| `weather_desc` | `object` | Descrição textual do código WMO |
| `heat_index_celsius` | `float32` | Sensação térmica (°C) |
| `wind_category` | `object` | Categoria de intensidade do vento |
| `has_precipitation` | `bool` | `True` quando há chuva na hora |
| `date` | `object` | Data da observação (YYYY-MM-DD) |
| `hour` | `int8` | Hora da observação (0–23) |
| `year` | `int32` | Ano (coluna de partição) |
| `month` | `int32` | Mês (coluna de partição) |
| `day` | `int32` | Dia (coluna de partição) |

---

## 📊 Lendo os Dados em Parquet

Após a execução, consuma os dados com Pandas ou DuckDB:

```python
import pandas as pd

# Ler todas as partições
df = pd.read_parquet("data/output/", engine="pyarrow")
print(df.head())

# Ler apenas um dia específico (pushdown de partição)
df_dia = pd.read_parquet(
    "data/output/",
    filters=[("year", "==", 2024), ("month", "==", 10), ("day", "==", 15)]
)
```

```sql
-- Com DuckDB (instale: pip install duckdb)
SELECT city, AVG(temp_celsius) AS media_temp
FROM read_parquet('data/output/**/*.parquet', hive_partitioning=true)
GROUP BY city
ORDER BY media_temp DESC;
```

---

## 🔭 Possíveis Extensões

- **Agendamento** — Usar `cron` (Linux) ou **Apache Airflow** para execução diária automatizada.
- **Testes** — Cobrir funções com `pytest` e dados sintéticos para CI/CD.
- **Ingestão incremental** — Controlar a janela de datas via banco de metadados (ex: SQLite) para evitar re-processamento.
- **Dashboard** — Conectar o Parquet a **Metabase**, **Superset** ou **Streamlit** para visualizações.
- **Cloud** — Substituir o `load()` local por upload para **AWS S3**, **GCS** ou **Azure Blob Storage**.
- **Qualidade de dados** — Integrar **Great Expectations** ou **Pandera** para validação de schema e limites de valores.
- **Containerização** — Empacotar em **Docker** para portabilidade de ambiente.

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Papel no projeto |
|------------|-----------------|
| [Python 3.11+](https://www.python.org/) | Linguagem principal |
| [Open-Meteo API](https://open-meteo.com/) | Fonte de dados climáticos (gratuita, sem auth) |
| [Requests](https://requests.readthedocs.io/) | Cliente HTTP para consumo da API |
| [Pandas](https://pandas.pydata.org/) | Manipulação e transformação de dados |
| [NumPy](https://numpy.org/) | Operações numéricas de baixo nível |
| [PyArrow](https://arrow.apache.org/docs/python/) | Engine de leitura/escrita Parquet |
| [Snappy](https://github.com/google/snappy) | Algoritmo de compressão dos arquivos Parquet |

---

> **Autor:** Portfólio – Engenharia de Dados  
> **Licença:** MIT  
> Contribuições e sugestões são bem-vindas via Issues e Pull Requests.
