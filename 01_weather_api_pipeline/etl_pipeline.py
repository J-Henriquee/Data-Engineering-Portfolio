"""
etl_pipeline.py
===============
Pipeline ETL simples para dados climáticos históricos via Open-Meteo API.

Fluxo:
    Extract  → Consome a API pública Open-Meteo (sem autenticação).
    Transform → Limpa, tipifica e enriquece os dados com Pandas.
    Load      → Persiste em Parquet particionado por data (ano/mês/dia).

Autor  : Portfólio – Engenharia de Dados
Versão : 1.0.0
"""

import logging
import sys
from pathlib import Path
from datetime import date, timedelta

import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Cidades de referência (nome, latitude, longitude)
LOCATIONS: list[dict] = [
    {"city": "Salvador",       "latitude": -12.97,  "longitude": -38.51},
    {"city": "São Paulo",      "latitude": -23.55,  "longitude": -46.63},
    {"city": "Rio de Janeiro", "latitude": -22.91,  "longitude": -43.17},
    {"city": "Manaus",         "latitude":  -3.10,  "longitude": -60.02},
    {"city": "Porto Alegre",   "latitude": -30.03,  "longitude": -51.23},
]

OUTPUT_DIR = Path("data/output")

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "windspeed_10m",
    "weathercode",
]

# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------

def build_params(location: dict, start_date: str, end_date: str) -> dict:
    """
    Constrói o dicionário de parâmetros para a requisição à Open-Meteo API.

    Args:
        location  : Dicionário com 'city', 'latitude' e 'longitude'.
        start_date: Data inicial no formato 'YYYY-MM-DD'.
        end_date  : Data final no formato 'YYYY-MM-DD'.

    Returns:
        Dicionário de query-params pronto para requests.get().
    """
    return {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "America/Sao_Paulo",
    }


def extract(location: dict, start_date: str, end_date: str) -> dict:
    """
    Realiza a requisição HTTP à Open-Meteo API e retorna o JSON bruto.

    Args:
        location  : Dicionário com dados geográficos da cidade.
        start_date: Data inicial no formato 'YYYY-MM-DD'.
        end_date  : Data final no formato 'YYYY-MM-DD'.

    Returns:
        Dicionário com a resposta JSON da API.

    Raises:
        requests.HTTPError     : Quando a API retorna status >= 400.
        requests.ConnectionError: Quando não há conectividade.
        requests.Timeout       : Quando a requisição excede o timeout.
    """
    params = build_params(location, start_date, end_date)
    city = location["city"]

    logger.info("Extraindo dados para '%s' (%s → %s).", city, start_date, end_date)

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        logger.info("'%s' – %d registros recebidos.", city, len(response.json().get("hourly", {}).get("time", [])))
        return response.json()

    except requests.exceptions.HTTPError as exc:
        logger.error("Erro HTTP ao consultar '%s': %s", city, exc)
        raise
    except requests.exceptions.ConnectionError as exc:
        logger.error("Erro de conexão ao consultar '%s': %s", city, exc)
        raise
    except requests.exceptions.Timeout as exc:
        logger.error("Timeout ao consultar '%s': %s", city, exc)
        raise


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------

# Mapeamento do WMO Weather Interpretation Code → descrição legível
WMO_CODE_MAP: dict[int, str] = {
    0: "Céu limpo",
    1: "Principalmente limpo",
    2: "Parcialmente nublado",
    3: "Encoberto",
    45: "Névoa",
    48: "Névoa com geada",
    51: "Garoa leve",
    53: "Garoa moderada",
    55: "Garoa intensa",
    61: "Chuva leve",
    63: "Chuva moderada",
    65: "Chuva forte",
    71: "Neve leve",
    73: "Neve moderada",
    75: "Neve forte",
    80: "Pancadas leves",
    81: "Pancadas moderadas",
    82: "Pancadas violentas",
    95: "Tempestade",
    99: "Tempestade com granizo",
}


def raw_to_dataframe(raw: dict, city: str) -> pd.DataFrame:
    """
    Converte o JSON bruto da API em um DataFrame Pandas estruturado.

    Args:
        raw : JSON retornado pela Open-Meteo API.
        city: Nome da cidade para enriquecer o DataFrame.

    Returns:
        DataFrame com colunas tipadas e coluna 'city' adicionada.

    Raises:
        KeyError: Caso o JSON não contenha a chave 'hourly'.
    """
    try:
        hourly = raw["hourly"]
    except KeyError as exc:
        logger.error("Chave 'hourly' ausente no JSON recebido para '%s'.", city)
        raise KeyError(f"Resposta inesperada da API para '{city}'.") from exc

    df = pd.DataFrame(hourly)
    df.insert(0, "city", city)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas as transformações necessárias ao DataFrame bruto.

    Etapas:
        1. Renomeia colunas para nomes mais legíveis.
        2. Converte a coluna 'datetime' para o tipo datetime64.
        3. Extrai colunas derivadas: date, hour, year, month, day.
        4. Trata valores nulos (mediana para numéricos; 'Desconhecido' para texto).
        5. Converte tipos de dados explicitamente.
        6. Mapeia o código WMO para uma descrição legível.
        7. Cria coluna derivada 'heat_index' (sensação térmica simplificada).
        8. Classifica a velocidade do vento em categorias.
        9. Sinaliza horas com precipitação.

    Args:
        df: DataFrame bruto com colunas da API.

    Returns:
        DataFrame limpo e enriquecido.
    """
    logger.info("Iniciando transformações. Shape inicial: %s.", df.shape)

    # 1. Renomear colunas
    rename_map = {
        "time":                    "datetime",
        "temperature_2m":          "temp_celsius",
        "relative_humidity_2m":    "humidity_pct",
        "precipitation":           "precip_mm",
        "windspeed_10m":           "wind_kmh",
        "weathercode":             "wmo_code",
    }
    df = df.rename(columns=rename_map)

    # 2. Converter datetime
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%dT%H:%M", errors="coerce")

    # 3. Colunas derivadas de tempo
    df["date"]  = df["datetime"].dt.date
    df["hour"]  = df["datetime"].dt.hour.astype("int8")
    df["year"]  = df["datetime"].dt.year.astype("int16")
    df["month"] = df["datetime"].dt.month.astype("int8")
    df["day"]   = df["datetime"].dt.day.astype("int8")

    # 4. Tratar valores nulos
    numeric_cols = ["temp_celsius", "humidity_pct", "precip_mm", "wind_kmh"]
    null_counts_before = df[numeric_cols].isnull().sum()
    if null_counts_before.any():
        logger.warning("Valores nulos detectados antes do tratamento:\n%s", null_counts_before[null_counts_before > 0])

    for col in numeric_cols:
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)
        logger.debug("Coluna '%s': nulos preenchidos com mediana=%.2f.", col, median_val)

    df["wmo_code"] = df["wmo_code"].fillna(-1).astype("int16")

    # 5. Tipos de dados
    df["temp_celsius"] = df["temp_celsius"].astype("float32")
    df["humidity_pct"] = df["humidity_pct"].astype("float32")
    df["precip_mm"]    = df["precip_mm"].astype("float32")
    df["wind_kmh"]     = df["wind_kmh"].astype("float32")

    # 6. Descrição do código WMO
    df["weather_desc"] = df["wmo_code"].map(WMO_CODE_MAP).fillna("Desconhecido")

    # 7. Coluna derivada – Índice de calor aparente (Heat Index simplificado)
    #    Fórmula de Steadman simplificada válida para T > 27°C e UR > 40%
    df["heat_index_celsius"] = df.apply(_calc_heat_index, axis=1).astype("float32")

    # 8. Categorização do vento (escala de Beaufort simplificada)
    df["wind_category"] = pd.cut(
        df["wind_kmh"],
        bins=[-1, 5, 20, 40, 62, 89, float("inf")],
        labels=["Calmaria", "Brisa leve", "Brisa moderada", "Vento forte", "Muito forte", "Tempestade"],
    )

    # 9. Flag de precipitação
    df["has_precipitation"] = (df["precip_mm"] > 0).astype("bool")

    # Remover linhas onde datetime é nulo (conversão falhou)
    rows_before = len(df)
    df = df.dropna(subset=["datetime"])
    if len(df) < rows_before:
        logger.warning("Removidas %d linhas com datetime inválido.", rows_before - len(df))

    logger.info("Transformações concluídas. Shape final: %s.", df.shape)
    return df


def _calc_heat_index(row: pd.Series) -> float:
    """
    Calcula o índice de calor aparente (sensação térmica).

    Para temperaturas ≤ 27°C ou umidade ≤ 40%, retorna a temperatura real.
    Caso contrário, aplica a fórmula de Rothfusz simplificada.

    Args:
        row: Linha do DataFrame com 'temp_celsius' e 'humidity_pct'.

    Returns:
        Float com o índice de calor em °C.
    """
    T = row["temp_celsius"]
    RH = row["humidity_pct"]

    if T <= 27 or RH <= 40:
        return float(T)

    # Fórmula de Rothfusz (adaptada para Celsius)
    hi = (
        -8.784695
        + 1.61139411 * T
        + 2.338549   * RH
        - 0.14611605 * T * RH
        - 0.01230809 * T**2
        - 0.01642482 * RH**2
        + 0.00221173 * T**2 * RH
        + 0.00072546 * T * RH**2
        - 0.00000358 * T**2 * RH**2
    )
    return round(float(hi), 2)


# ---------------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------------

def load(df: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> None:
    """
    Persiste o DataFrame transformado em arquivos Parquet particionados.

    Estrutura de diretórios gerada:
        data/output/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── part-0.parquet

    Args:
        df        : DataFrame transformado e pronto para persistência.
        output_dir: Diretório raiz para os arquivos de saída.

    Raises:
        OSError: Caso não seja possível criar os diretórios de saída.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # pyarrow exige que colunas de partição sejam do tipo correto
    df["year"]  = df["year"].astype("int32")
    df["month"] = df["month"].astype("int32")
    df["day"]   = df["day"].astype("int32")

    # Converter colunas categóricas para string (compatibilidade Parquet)
    df["wind_category"] = df["wind_category"].astype(str)
    # date precisa ser string para particionamento via pyarrow
    df["date"] = df["date"].astype(str)

    logger.info("Salvando %d registros em '%s' (particionado por year/month/day).", len(df), output_dir)

    try:
        df.to_parquet(
            path=str(output_dir),
            engine="pyarrow",
            partition_cols=["year", "month", "day"],
            index=False,
            compression="snappy",
            existing_data_behavior="overwrite_or_ignore",
        )
        logger.info("Dados persistidos com sucesso em '%s'.", output_dir)

    except Exception as exc:
        logger.error("Falha ao salvar Parquet: %s", exc)
        raise


# ---------------------------------------------------------------------------
# ORQUESTRADOR
# ---------------------------------------------------------------------------

def run_pipeline(
    days_back: int = 7,
    locations: list[dict] = None,
    output_dir: Path = OUTPUT_DIR,
) -> pd.DataFrame:
    """
    Orquestra as etapas Extract → Transform → Load para todas as cidades.

    Args:
        days_back  : Número de dias passados a consultar (padrão: 7).
        locations  : Lista de dicionários com city/latitude/longitude.
                     Se None, usa a constante LOCATIONS do módulo.
        output_dir : Diretório de saída para os arquivos Parquet.

    Returns:
        DataFrame consolidado com todos os dados transformados.

    Raises:
        ValueError: Se nenhuma cidade retornar dados válidos.
    """
    if locations is None:
        locations = LOCATIONS

    end_date   = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("Iniciando pipeline ETL  |  %s → %s  |  %d cidade(s).",
                start_date, end_date, len(locations))
    logger.info("=" * 60)

    all_frames: list[pd.DataFrame] = []

    for location in locations:
        city = location["city"]
        try:
            # --- EXTRACT ---
            raw_data = extract(location, start_date, end_date)

            # --- TRANSFORM (parcial) ---
            df_raw = raw_to_dataframe(raw_data, city)
            df_clean = transform(df_raw)

            all_frames.append(df_clean)

        except Exception as exc:  # noqa: BLE001
            logger.error("Cidade '%s' ignorada devido a erro: %s", city, exc)
            continue

    if not all_frames:
        raise ValueError("Nenhum dado foi extraído com sucesso. Verifique os logs.")

    # Consolidar todos os DataFrames
    df_final = pd.concat(all_frames, ignore_index=True)
    logger.info("Consolidação concluída. Total de registros: %d.", len(df_final))

    # --- LOAD ---
    load(df_final, output_dir)

    logger.info("=" * 60)
    logger.info("Pipeline ETL finalizado com sucesso.")
    logger.info("=" * 60)

    return df_final


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        df_result = run_pipeline(days_back=7)

        print("\n── Amostra dos dados transformados ──────────────────────────")
        print(df_result[["city", "datetime", "temp_celsius", "humidity_pct",
                          "precip_mm", "wind_kmh", "weather_desc",
                          "heat_index_celsius", "wind_category"]].head(10).to_string(index=False))
        print(f"\nShape final : {df_result.shape}")
        print(f"Período     : {df_result['datetime'].min()} → {df_result['datetime'].max()}")
        print(f"Cidades     : {sorted(df_result['city'].unique())}")

    except Exception as main_exc:
        logger.critical("Pipeline encerrado com erro crítico: %s", main_exc)
        sys.exit(1)
