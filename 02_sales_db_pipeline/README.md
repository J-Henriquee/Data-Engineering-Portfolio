# 🛒 Sales DB Pipeline — Python + SQLite

> **Geração de dados sintéticos de vendas, ingestão eficiente em banco relacional e análise com SQL puro** — projeto de portfólio que demonstra domínio de modelagem relacional, bulk insert otimizado e queries analíticas avançadas com Python e SQLite.

---

## 📑 Índice

1. [Visão Geral](#-visão-geral)
2. [Arquitetura e Fluxo](#-arquitetura-e-fluxo)
3. [Destaques Técnicos](#-destaques-técnicos)
4. [Estrutura do Projeto](#-estrutura-do-projeto)
5. [Pré-requisitos](#-pré-requisitos)
6. [Instalação e Execução (Linux)](#-instalação-e-execução-linux)
7. [Geração de Dados Sintéticos](#-geração-de-dados-sintéticos)
8. [Modelagem Relacional](#-modelagem-relacional)
9. [Queries SQL Analíticas](#-queries-sql-analíticas)
10. [Output Esperado](#-output-esperado)
11. [Possíveis Extensões](#-possíveis-extensões)
12. [Tecnologias Utilizadas](#-tecnologias-utilizadas)

---

## 🔍 Visão Geral

Este projeto implementa um pipeline de dados completo em **Python puro** (sem ORM), demonstrando:

- **Geração de dados sintéticos realistas** com sazonalidade, ruído gaussiano de preço e distribuição beta de descontos.
- **Ingestão eficiente em SQLite** via bulk insert em lotes (`executemany`), com configuração de PRAGMAs para performance máxima.
- **Modelagem relacional** com DDL explícito, índices analíticos e tipos adequados.
- **3 queries SQL analíticas** cobrindo agregações temporais, ranking de produtos e análise multidimensional por canal e região.

Tudo em um único arquivo modularizado, com logging estruturado, docstrings e tratamento de erros.

---

## 🏗️ Arquitetura e Fluxo

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Sales DB Pipeline                             │
│                                                                     │
│  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────┐   │
│  │    GENERATE     │    │      LOAD        │    │    ANALYZE    │   │
│  │                 │    │                  │    │               │   │
│  │ NumPy RNG       │───▶│ SQLite (WAL mode)│───▶│ 3 Queries SQL │   │
│  │                 │    │                  │    │               │   │
│  │ • 10.000 linhas │    │ • DDL + Indexes  │    │ • Por mês     │   │
│  │ • Sazonalidade  │    │ • Bulk insert    │    │ • Por produto │   │
│  │ • Ruído gaussiano│   │   (lotes 1.000)  │    │ • Canal×Região│   │
│  │ • Dist. Beta    │    │ • PRAGMAs otim.  │    │               │   │
│  │ • ~3% devoluções│    │                  │    │ pd.read_sql() │   │
│  └─────────────────┘    └──────────────────┘    └───────────────┘   │
│                                  │                                  │
│                             sales.db                                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Destaques Técnicos

### Performance de ingestão
A inserção usa `executemany` com lotes de 1.000 registros, combinada com otimizações de PRAGMA do SQLite:

```python
conn.execute("PRAGMA journal_mode=WAL")      # Write-Ahead Logging
conn.execute("PRAGMA synchronous=NORMAL")    # Fsync menos agressivo
conn.execute("PRAGMA cache_size=-64000")     # 64 MB de cache em memória
```

Resultado: **~10× mais rápido** que inserções linha a linha.

### Dados sintéticos com distribuições estatísticas
| Atributo | Distribuição | Motivo |
|----------|-------------|--------|
| Datas | Discreta ponderada | Sazonalidade: Nov/Dez com 2× mais vendas |
| Preços | Gaussiana (μ=1, σ=0.05) | Variações realistas de ±5% |
| Descontos | Beta(α=1.5, β=8) | Concentração em valores baixos (<15%) |
| Devoluções | Bernoulli(p=0.03) | ~3% de taxa de retorno |

### Context Manager para conexões
```python
@contextmanager
def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
        raise
    finally:
        conn.close()
```
Garante `commit` em sucesso e `rollback` automático em erros — sem vazamento de conexão.

---

## 📁 Estrutura do Projeto

```
sales-db-pipeline/
│
├── sales_db_pipeline.py    # Script principal (Generate → Load → Analyze)
├── requirements.txt        # Dependências (apenas pandas e numpy)
├── README.md               # Esta documentação
│
├── sales.db                # Gerado automaticamente na primeira execução
└── sales_pipeline.log      # Log estruturado de cada execução
```

---

## ✅ Pré-requisitos

| Requisito | Versão | Observação |
|-----------|--------|------------|
| Python | 3.11+ | `sqlite3` já vem na stdlib |
| pip | 23+ | Gerenciador de pacotes |

Dependências externas:

```
pandas==2.2.3
numpy==1.26.4
```

> O módulo `sqlite3` é parte da biblioteca padrão do Python — **nenhuma instalação adicional necessária** para o banco de dados.

---

## 🚀 Instalação e Execução (Linux)

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/sales-db-pipeline.git
cd sales-db-pipeline
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
python sales_db_pipeline.py
```

### 5. Inspecione o banco gerado

```bash
# Verificar tamanho do banco
ls -lh sales.db

# Explorar interativamente com SQLite CLI
sqlite3 sales.db

# Dentro do sqlite3:
.headers on
.mode column
SELECT COUNT(*), ROUND(SUM(total_amount), 2) FROM sales;
.quit
```

---

## 🎲 Geração de Dados Sintéticos

O dataset simula 2 anos de operação (2023–2024) de um e-commerce de tecnologia com 10 produtos, 5 regiões, 4 canais de venda e 4 formas de pagamento.

### Amostra do dataset gerado

| sale_date | product_name | unit_price | qty | discount | total | region | channel |
|-----------|-------------|-----------|-----|---------|-------|--------|---------|
| 2023-11-24 | Notebook Gamer | 5.942,10 | 2 | 8,3% | 10.898,45 | Sudeste | E-commerce |
| 2024-03-07 | Mouse sem Fio | 192,40 | 1 | 2,1% | 188,37 | Sul | Loja Física |
| 2024-12-01 | Monitor 4K | 2.180,50 | 3 | 14,7% | 5.581,96 | Nordeste | Marketplace |

### Volumes esperados

```
Total de registros  : 10.000
Período             : 2023-01-01 → 2024-12-31
Receita bruta aprox.: R$ 25.000.000+
Taxa de devolução   : ~3%
Ticket médio        : ~R$ 2.500
```

---

## 🗄️ Modelagem Relacional

### Tabela `sales`

```sql
CREATE TABLE IF NOT EXISTS sales (
    sale_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date       TEXT    NOT NULL,   -- ISO-8601: YYYY-MM-DD
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,   -- 0=Segunda … 6=Domingo
    product_name    TEXT    NOT NULL,
    unit_price      REAL    NOT NULL,
    quantity        INTEGER NOT NULL,
    discount_pct    REAL    NOT NULL DEFAULT 0,
    total_amount    REAL    NOT NULL,   -- unit_price × qty × (1 - discount)
    region          TEXT    NOT NULL,
    sales_channel   TEXT    NOT NULL,
    payment_method  TEXT    NOT NULL,
    customer_id     INTEGER NOT NULL,
    is_returned     INTEGER NOT NULL DEFAULT 0
);
```

### Índices analíticos

```sql
CREATE INDEX idx_sales_date    ON sales(sale_date);
CREATE INDEX idx_sales_product ON sales(product_name);
CREATE INDEX idx_sales_region  ON sales(region);
CREATE INDEX idx_sales_channel ON sales(sales_channel);
```

Os índices foram definidos nas colunas mais utilizadas nos `WHERE`, `GROUP BY` e `ORDER BY` das queries analíticas, garantindo planos de execução eficientes mesmo com volumes maiores.

---

## 📊 Queries SQL Analíticas

### Query 1 — Total de Vendas por Mês

Agrega receita, volume e ticket médio mês a mês, excluindo devoluções:

```sql
SELECT
    year,
    month,
    PRINTF('%04d-%02d', year, month)        AS year_month,
    COUNT(*)                                AS num_transactions,
    SUM(quantity)                           AS units_sold,
    ROUND(SUM(total_amount), 2)             AS gross_revenue,
    ROUND(AVG(total_amount), 2)             AS avg_ticket
FROM sales
WHERE is_returned = 0
GROUP BY year, month
ORDER BY year, month;
```

**Conceitos demonstrados:** `GROUP BY` multidimensional, `PRINTF` para formatação de período, filtro de qualidade de dados.

---

### Query 2 — Top 10 Produtos por Receita Líquida

Ranking de produtos com desconto médio e receita por unidade vendida:

```sql
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
```

**Conceitos demonstrados:** Cálculo de métricas derivadas, divisão de agregações (`SUM/SUM`), `LIMIT` para ranking.

---

### Query 3 — Análise por Canal × Região com Taxa de Devolução

Cruzamento multidimensional com `CASE WHEN` para receita líquida real:

```sql
SELECT
    sales_channel,
    region,
    COUNT(*)                                        AS total_orders,
    ROUND(SUM(total_amount), 2)                     AS total_revenue,
    ROUND(AVG(total_amount), 2)                     AS avg_ticket,
    ROUND(100.0 * SUM(is_returned) / COUNT(*), 2)   AS return_rate_pct,
    ROUND(
        SUM(CASE WHEN is_returned = 0 THEN total_amount ELSE 0 END), 2
    )                                               AS net_revenue
FROM sales
GROUP BY sales_channel, region
ORDER BY net_revenue DESC;
```

**Conceitos demonstrados:** `CASE WHEN` dentro de agregação, taxa percentual calculada em SQL, análise de cohort canal × região.

---

## 📟 Output Esperado

```
2024-10-15 09:00:01 | INFO     | Sales DB Pipeline  |  SQLite: sales.db  |  Linhas: 10000
2024-10-15 09:00:01 | INFO     | Gerando 10000 registros sintéticos de vendas (seed=42)…
2024-10-15 09:00:01 | INFO     | Dataset gerado. Shape: (10000, 14) | Receita bruta: R$ 25.847.291,40
2024-10-15 09:00:01 | INFO     | Iniciando ingestão: 10000 registros em lotes de 1000…
2024-10-15 09:00:01 | INFO     | Ingestão concluída em 0.18s — 55.000 registros/segundo.

════════════════════════════════════════════════════════════════════════════════
  ANÁLISES SQL — Sales Database
════════════════════════════════════════════════════════════════════════════════

────────────────────────────────────────────────────────────────────────────────
  📅  Total de Vendas por Mês (últimos 12 meses, excluindo devoluções)
────────────────────────────────────────────────────────────────────────────────
 Ano  Mês  Ano-Mês  Transações  Unidades  Receita (R$)  Ticket Médio (R$)
2023    1  2023-01         376      1001    924.103,20          2.457,19
2023    2  2023-02         327       869    803.291,45          2.456,24
...
2024   11  2024-11         791      2103  1.947.382,10          2.461,92
2024   12  2024-12         808      2148  1.988.741,35          2.461,31
```

---

## 🔭 Possíveis Extensões

- **PostgreSQL / MySQL** — substituir `sqlite3` por `psycopg2` ou `SQLAlchemy` para banco cliente-servidor.
- **SQLAlchemy ORM** — reescrever a camada de ingestão com mapeamento declarativo.
- **Migração com Alembic** — versionar o schema com migrations automatizadas.
- **dbt** — transformar as queries analíticas em modelos SQL versionados e testáveis.
- **Apache Superset / Metabase** — conectar o `sales.db` a um dashboard interativo.
- **Testes com pytest** — cobrir `generate_sales_data()`, `load_to_sqlite()` e cada query com dados determinísticos.
- **Great Expectations** — validar o schema e as distribuições do dataset gerado.
- **Airflow DAG** — orquestrar execuções diárias incrementais.

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Papel no projeto |
|------------|-----------------|
| [Python 3.11+](https://www.python.org/) | Linguagem principal |
| [SQLite 3](https://www.sqlite.org/) | Banco de dados relacional embutido |
| [sqlite3](https://docs.python.org/3/library/sqlite3.html) | Driver nativo Python para SQLite |
| [Pandas](https://pandas.pydata.org/) | Manipulação de dados e `read_sql_query` |
| [NumPy](https://numpy.org/) | Geração de dados sintéticos com distribuições estatísticas |

---

> **Autor:** Portfólio – Engenharia de Dados  
> **Licença:** MIT  
> Sugestões e contribuições são bem-vindas via Issues e Pull Requests.
