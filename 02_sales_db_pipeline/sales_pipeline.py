"""
sales_db_pipeline.py
====================
Microprojeto de Engenharia de Dados: geração de dados sintéticos de vendas,
ingestão eficiente em SQLite e análise via queries SQL analíticas.

Fluxo:
    Generate → Cria 10.000 registros de vendas sintéticos com Pandas + NumPy.
    Load     → Insere os dados em SQLite usando bulk insert via executemany.
    Analyze  → Executa 3 queries SQL analíticas e exibe os resultados.

Autor  : Portfólio – Engenharia de Dados
Versão : 1.0.0
"""

import logging
import sqlite3
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sales_pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
DB_PATH       = Path("sales.db")
N_ROWS        = 10_000
RANDOM_SEED   = 42

PRODUCTS: dict[str, float] = {
    "Notebook Gamer":    5_899.90,
    "Smartphone Pro":    3_299.00,
    "Monitor 4K":        2_199.90,
    "Teclado Mecânico":    449.90,
    "Mouse sem Fio":       189.90,
    "Headset RGB":         329.90,
    "Webcam HD":           249.90,
    "SSD 1TB":             399.90,
    "Cadeira Gamer":     1_599.90,
    "Mesa Gamer":          899.90,
}

REGIONS = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]

SALES_CHANNELS = ["E-commerce", "Loja Física", "Marketplace", "Televendas"]

PAYMENT_METHODS = ["Cartão de Crédito", "Cartão de Débito", "PIX", "Boleto"]

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_SALES = """
CREATE TABLE IF NOT EXISTS sales (
    sale_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date       TEXT    NOT NULL,           -- ISO-8601: YYYY-MM-DD
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,           -- 0=Segunda … 6=Domingo
    product_name    TEXT    NOT NULL,
    unit_price      REAL    NOT NULL,
    quantity        INTEGER NOT NULL,
    discount_pct    REAL    NOT NULL DEFAULT 0, -- 0.0 a 0.30
    total_amount    REAL    NOT NULL,           -- unit_price * qty * (1 - discount)
    region          TEXT    NOT NULL,
    sales_channel   TEXT    NOT NULL,
    payment_method  TEXT    NOT NULL,
    customer_id     INTEGER NOT NULL,
    is_returned     INTEGER NOT NULL DEFAULT 0  -- 0=False, 1=True (SQLite boolean)
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_sales_date    ON sales(sale_date);",
    "CREATE INDEX IF NOT EXISTS idx_sales_product ON sales(product_name);",
    "CREATE INDEX IF NOT EXISTS idx_sales_region  ON sales(region);",
    "CREATE INDEX IF NOT EXISTS idx_sales_channel ON sales(sales_channel);",
]

# ---------------------------------------------------------------------------
# Queries Analíticas
# ---------------------------------------------------------------------------
QUERIES: list[dict] = [
    {
        "title": "📅  Total de Vendas por Mês (últimos 12 meses, excluindo devoluções)",
        "sql": """
            SELECT
                year,
                month,
                PRINTF('%04d-%02d', year, month)        AS year_month,
                COUNT(*)                                 AS num_transactions,
                SUM(quantity)                            AS units_sold,
                ROUND(SUM(total_amount), 2)              AS gross_revenue,
                ROUND(AVG(total_amount), 2)              AS avg_ticket
            FROM sales
            WHERE is_returned = 0
            GROUP BY year, month
            ORDER BY year, month;
        """,
        "columns": ["Ano", "Mês", "Ano-Mês", "Transações", "Unidades", "Receita (R$)", "Ticket Médio (R$)"],
    },
    {
        "title": "🏆  Top 10 Produtos por Receita Líquida e Margem de Desconto Médio",
        "sql": """
            SELECT
                product_name,
                COUNT(*)                                    AS num_sales,
                SUM(quantity)                               AS total_units,
                ROUND(AVG(discount_pct) * 100, 2)           AS avg_discount_pct,
                ROUND(SUM(total_amount), 2)                 AS net_revenue,
                ROUND(SUM(total_amount) / SUM(quantity), 2) AS revenue_per_unit
            FROM sales
            WHERE is_returned = 0
            GROUP BY product_name
            ORDER BY net_revenue DESC
            LIMIT 10;
        """,
        "columns": ["Produto", "Nº Vendas", "Unidades", "Desc. Médio (%)", "Receita Líquida (R$)", "Receita/Unidade (R$)"],
    },
    {
        "title": "📊  Análise por Canal de Vendas e Região (receita, ticket médio e taxa de devolução)",
        "sql": """
            SELECT
                sales_channel,
                region,
                COUNT(*)                                        AS total_orders,
                ROUND(SUM(total_amount), 2)                     AS total_revenue,
                ROUND(AVG(total_amount), 2)                     AS avg_ticket,
                ROUND(
                    100.0 * SUM(is_returned) / COUNT(*), 2
                )                                               AS return_rate_pct,
                ROUND(
                    SUM(CASE WHEN is_returned = 0 THEN total_amount ELSE 0 END), 2
                )                                               AS net_revenue
            FROM sales
            GROUP BY sales_channel, region
            ORDER BY net_revenue DESC;
        """,
        "columns": ["Canal", "Região", "Pedidos", "Receita Total (R$)", "Ticket Médio (R$)", "Taxa Devolução (%)", "Receita Líquida (R$)"],
    },
]


# ---------------------------------------------------------------------------
# Context Manager para conexão SQLite
# ---------------------------------------------------------------------------
@contextmanager
def get_connection(db_path: Path):
    """
    Context manager que abre e fecha a conexão SQLite de forma segura.

    Garante commit em caso de sucesso e rollback automático em erros.

    Args:
        db_path: Caminho para o arquivo .db do SQLite.

    Yields:
        sqlite3.Connection ativa.

    Raises:
        sqlite3.Error: Propagado após rollback automático.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row          # acesso por nome de coluna
    conn.execute("PRAGMA journal_mode=WAL") # melhor performance em writes
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64 MB de cache
    try:
        yield conn
        conn.commit()
    except sqlite3.Error as exc:
        conn.rollback()
        logger.error("Erro SQLite — rollback executado: %s", exc)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GENERATE — Dados Sintéticos
# ---------------------------------------------------------------------------
def generate_sales_data(n_rows: int = N_ROWS, seed: int = RANDOM_SEED) -> pd.DataFrame:
    """
    Gera um DataFrame sintético de vendas com distribuições realistas.

    Estratégia de geração:
    - Datas distribuídas ao longo de 2 anos com sazonalidade manual
      (Nov/Dez têm 2x mais vendas — simulando Black Friday e Natal).
    - Preços com ruído gaussiano (±5%) para simular variações reais.
    - Descontos com distribuição beta (concentrada em valores baixos).
    - Taxa de devolução de ~3%, distribuída aleatoriamente.

    Args:
        n_rows: Número de linhas a gerar.
        seed  : Semente aleatória para reprodutibilidade.

    Returns:
        DataFrame com o schema esperado pela tabela `sales`.
    """
    rng = np.random.default_rng(seed)
    logger.info("Gerando %d registros sintéticos de vendas (seed=%d)…", n_rows, seed)

    # --- Datas com sazonalidade ---
    start = pd.Timestamp("2023-01-01")
    end   = pd.Timestamp("2024-12-31")
    all_days = pd.date_range(start, end, freq="D")

    # Peso sazonal: Nov e Dez têm o dobro de probabilidade
    weights = np.where(all_days.month.isin([11, 12]), 2.0, 1.0)
    weights /= weights.sum()

    dates = pd.to_datetime(
        rng.choice(all_days, size=n_rows, replace=True, p=weights)
    )

    # --- Produtos e preços ---
    product_names  = list(PRODUCTS.keys())
    base_prices    = np.array(list(PRODUCTS.values()))
    product_idx    = rng.integers(0, len(product_names), size=n_rows)
    unit_price_raw = base_prices[product_idx]

    # Ruído de ±5% no preço
    price_noise = rng.normal(loc=1.0, scale=0.05, size=n_rows).clip(0.90, 1.10)
    unit_prices = np.round(unit_price_raw * price_noise, 2)

    # --- Quantidade ---
    quantities = rng.integers(1, 6, size=n_rows)  # 1 a 5 unidades

    # --- Desconto (distribuição beta: maioria abaixo de 15%) ---
    discounts = np.round(rng.beta(a=1.5, b=8, size=n_rows) * 0.30, 4)

    # --- Total ---
    totals = np.round(unit_prices * quantities * (1 - discounts), 2)

    # --- Outros atributos ---
    regions    = rng.choice(REGIONS,          size=n_rows)
    channels   = rng.choice(SALES_CHANNELS,   size=n_rows)
    payments   = rng.choice(PAYMENT_METHODS,  size=n_rows)
    customers  = rng.integers(1_000, 50_001,  size=n_rows)
    is_returned = (rng.random(size=n_rows) < 0.03).astype(int)  # ~3% de devolução

    df = pd.DataFrame({
        "sale_date":      dates.strftime("%Y-%m-%d"),
        "year":           dates.year.astype("int16"),
        "month":          dates.month.astype("int8"),
        "day_of_week":    dates.dayofweek.astype("int8"),
        "product_name":   [product_names[i] for i in product_idx],
        "unit_price":     unit_prices,
        "quantity":       quantities.astype("int8"),
        "discount_pct":   discounts,
        "total_amount":   totals,
        "region":         regions,
        "sales_channel":  channels,
        "payment_method": payments,
        "customer_id":    customers.astype("int32"),
        "is_returned":    is_returned,
    })

    logger.info(
        "Dataset gerado. Shape: %s | Receita bruta total: R$ %s",
        df.shape,
        f"{df['total_amount'].sum():,.2f}",
    )
    return df


# ---------------------------------------------------------------------------
# LOAD — Ingestão no SQLite
# ---------------------------------------------------------------------------
def setup_schema(conn: sqlite3.Connection) -> None:
    """
    Cria a tabela `sales` e os índices analíticos se não existirem.

    Args:
        conn: Conexão SQLite ativa.
    """
    logger.info("Criando schema (tabela + índices)…")
    conn.execute(DDL_SALES)
    for ddl in DDL_INDEXES:
        conn.execute(ddl)
    logger.info("Schema criado/verificado com sucesso.")


def load_to_sqlite(df: pd.DataFrame, conn: sqlite3.Connection, batch_size: int = 1_000) -> None:
    """
    Insere o DataFrame na tabela `sales` usando bulk insert em lotes.

    O método `executemany` com lotes balanceia uso de memória e performance,
    sendo ~10× mais rápido que inserções linha a linha.

    Args:
        df        : DataFrame com os dados de vendas.
        conn      : Conexão SQLite ativa (com transação aberta).
        batch_size: Número de linhas por lote de inserção.
    """
    columns = list(df.columns)
    placeholders = ", ".join("?" * len(columns))
    sql = f"INSERT INTO sales ({', '.join(columns)}) VALUES ({placeholders})"

    records = df.to_records(index=False).tolist()
    total   = len(records)
    batches = [records[i:i + batch_size] for i in range(0, total, batch_size)]

    logger.info("Iniciando ingestão: %d registros em lotes de %d…", total, batch_size)
    t0 = time.perf_counter()

    for batch_num, batch in enumerate(batches, start=1):
        conn.executemany(sql, batch)
        if batch_num % 5 == 0 or batch_num == len(batches):
            logger.info("  Lote %d/%d inserido (%d registros).", batch_num, len(batches), len(batch))

    elapsed = time.perf_counter() - t0
    logger.info(
        "Ingestão concluída em %.2fs — %.0f registros/segundo.",
        elapsed,
        total / elapsed,
    )


def get_row_count(conn: sqlite3.Connection) -> int:
    """Retorna o número de linhas na tabela `sales`."""
    cursor = conn.execute("SELECT COUNT(*) FROM sales;")
    return cursor.fetchone()[0]


# ---------------------------------------------------------------------------
# ANALYZE — Queries SQL Analíticas
# ---------------------------------------------------------------------------
def run_query(conn: sqlite3.Connection, query_def: dict) -> pd.DataFrame:
    """
    Executa uma query SQL e retorna o resultado como DataFrame.

    Args:
        conn     : Conexão SQLite ativa.
        query_def: Dicionário com chaves 'title', 'sql' e 'columns'.

    Returns:
        DataFrame com os resultados da query e colunas renomeadas.
    """
    logger.info("Executando query: '%s'", query_def["title"])
    t0 = time.perf_counter()

    df = pd.read_sql_query(query_def["sql"], conn)
    df.columns = query_def["columns"]

    elapsed = time.perf_counter() - t0
    logger.info("  → %d linhas retornadas em %.4fs.", len(df), elapsed)
    return df


def print_result(title: str, df: pd.DataFrame) -> None:
    """
    Exibe o resultado de uma query de forma formatada no terminal.

    Args:
        title: Título da análise.
        df   : DataFrame com os dados a exibir.
    """
    sep = "─" * 80
    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)
    print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# ORQUESTRADOR
# ---------------------------------------------------------------------------
def run_pipeline(
    db_path: Path = DB_PATH,
    n_rows: int = N_ROWS,
    seed: int = RANDOM_SEED,
) -> None:
    """
    Orquestra as etapas Generate → Load → Analyze.

    Args:
        db_path: Caminho para o arquivo SQLite de saída.
        n_rows : Quantidade de registros sintéticos a gerar.
        seed   : Semente para reprodutibilidade.
    """
    logger.info("=" * 60)
    logger.info("Sales DB Pipeline  |  SQLite: %s  |  Linhas: %d", db_path, n_rows)
    logger.info("=" * 60)

    # 1. GENERATE
    df_sales = generate_sales_data(n_rows=n_rows, seed=seed)

    # 2. LOAD
    with get_connection(db_path) as conn:
        setup_schema(conn)
        # Limpa dados anteriores para idempotência
        conn.execute("DELETE FROM sales;")
        load_to_sqlite(df_sales, conn)
        total_rows = get_row_count(conn)
        logger.info("Total de linhas na tabela após ingestão: %d.", total_rows)

    # 3. ANALYZE  (conexão separada — read-only intent)
    print("\n" + "═" * 80)
    print("  ANÁLISES SQL — Sales Database")
    print("═" * 80)

    with get_connection(db_path) as conn:
        for query_def in QUERIES:
            try:
                df_result = run_query(conn, query_def)
                print_result(query_def["title"], df_result)
            except sqlite3.Error as exc:
                logger.error("Falha ao executar query '%s': %s", query_def["title"], exc)

    logger.info("=" * 60)
    logger.info("Pipeline finalizado com sucesso. DB salvo em: %s", db_path.resolve())
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:
        logger.critical("Pipeline encerrado com erro crítico: %s", exc)
        sys.exit(1)
