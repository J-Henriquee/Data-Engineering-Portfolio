# 🏆 World Cup 2026 — Data Engineering Pipeline

## Overview

An end-to-end ETL pipeline that combines historical FIFA World Cup match data
(1930–2018) with live match data from the 2026 tournament (via API), loading
everything into a cloud PostgreSQL database — modeled as both a flat staging
table and a normalized star schema — for analysis.

## Architecture & Tech Stack

- **Extract**: Two sources, combined — historical match data from a static
  Kaggle CSV (1930–2018), and live 2026 tournament data pulled from the
  [football-data.org](https://www.football-data.org/) REST API.
- **Transform**: Pandas — standardizes column names and formats between the
  two sources, filters the API data to completed matches only, and resolves
  inconsistent country names across decades of data (see *Data Quality*
  below).
- **Load**: Automated, idempotent load into a serverless cloud PostgreSQL
  database (Neon.tech) via SQLAlchemy, plus a local CSV snapshot. Data is
  loaded into two layers (see *Data Modeling* below).
- **Security**: Credentials (database URL, API key) managed via environment
  variables (`.env`, `python-dotenv`) — never committed to the repository.
- **Analytics/Serving**: SQL queries (including JOINs across the star schema)
  run against the database for analytical insights (e.g. top goal-scoring
  teams), visualized in an interactive Streamlit dashboard.

## Data Modeling

The pipeline loads data into two layers on purpose, each serving a different
use case — a pattern similar to the "staging vs. modeled" layering used in
real-world data platforms (bronze/silver-style separation):

- **`stg_partidas`** — the full combined dataset, denormalized (one row per
  match, team names as plain text). Fast to query, useful for quick,
  one-off analysis.
- **`dim_selecoes`** + **`fato_jogos`** — a normalized star schema.
  `dim_selecoes` holds one row per unique team; `fato_jogos` holds one row
  per match, referencing teams by ID (foreign key) instead of repeating the
  team name on every row. This avoids data redundancy and keeps team names
  consistent in a single place — if a name needs correcting, it's fixed once
  in `dim_selecoes` rather than across thousands of match rows.

Both tables are reloaded idempotently on every run: `fato_jogos` and
`dim_selecoes` are truncated (`TRUNCATE ... RESTART IDENTITY CASCADE`) before
being repopulated, so re-running the pipeline never creates duplicates.

## Data Quality Highlights

Merging decades of football data from two independent sources surfaced
real-world data quality problems, not just textbook ones:

- **Historical name changes**: teams like `"Germany FR"` needed to be mapped
  to their modern equivalent (`"Germany"`) to avoid splitting the same
  country's stats across multiple rows.
- **Scraping artifacts**: the historical dataset contained leftover HTML
  fragments from its original source, e.g. `'rn">Republic of Ireland'`
  instead of a clean country name — a good example of why raw data should
  never be trusted at face value.

Both issues are handled explicitly in `transform.py` via a `country_mapping`
dictionary, rather than being silently ignored.

## Project Structure

```
02_pipeline_copa/
├── data/
│   ├── raw/            # Kaggle historical CSV
│   └── processed/      # Cleaned, combined dataset (CSV snapshot)
├── src/
│   ├── extract.py      # API extraction (football-data.org)
│   ├── transform.py    # Cleaning, standardization, merging
│   ├── load.py         # Loads staging + star schema into Postgres
│   ├── analytics.py    # Analytical queries (e.g. top scorers via JOIN)
│   └── dashboard.py    # Streamlit interactive dashboard
├── .env                 # Local secrets (not committed)
├── requirements.txt
└── README.md
```

> Note: column names inside the database (`time_casa`, `gols_casa`, etc.)
> are kept in Portuguese to match the original table schema; all code
> identifiers, comments, and documentation are in English.

## How to Run Locally

1. Clone the repository and set up a virtual environment:
   ```
   git clone <repo-url>
   cd 02_pipeline_copa
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your credentials:
   ```
   DATABASE_URL=postgresql://user:password@host/dbname
   API_KEY_FOOTBALL=your_api_key_here
   ```

3. Run the pipeline (extracts, transforms, and loads in one go):
   ```
   python src/load.py
   ```

4. Launch the dashboard:
   ```
   streamlit run src/dashboard.py
   ```

## Dashboard

![Dashboard screenshot](docs/screenshot.png)

## Known Limitations

- The historical dataset covers World Cups from 1930 to 2018; the 2022
  tournament is not included (not present in the source dataset used).
  Filling this gap — ideally via the same API already integrated — is a
  natural next step.

## Next Steps

Planned evolutions to take this from a batch pipeline to a more advanced
architecture:

- Add orchestration (Apache Airflow) to schedule and monitor runs.
- Integrate the API to backfill the missing 2022 tournament.
- Replace direct loading with a proper transformation layer using dbt.
- Explore PySpark for distributed processing at larger scale.