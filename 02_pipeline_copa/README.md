# 🏆 World Cup 2026 — Data Engineering Pipeline

## Overview

An end-to-end ETL pipeline that combines historical FIFA World Cup match data
(1930–2022) with live match data from the 2026 tournament, loading everything
into a cloud PostgreSQL database for analysis.

## Architecture & Tech Stack

- **Extract**: Two sources, combined — historical match data from a static
  Kaggle CSV (1930–2022), and live 2026 tournament data pulled from the
  [football-data.org](https://www.football-data.org/) REST API.
- **Transform**: Pandas — standardizes column names and formats between the
  two sources, filters the API data to completed matches only, and resolves
  inconsistent country names across decades of data (see *Data Quality*
  below).
- **Load**: Automated load into a serverless cloud PostgreSQL database
  (Neon.tech) via SQLAlchemy, plus a local CSV snapshot for backup/versioning.
- **Security**: Credentials (database URL, API key) managed via environment
  variables (`.env`, `python-dotenv`) — never committed to the repository.
- **Analytics/Serving**: SQL queries run against the database for analytical
  insights (e.g. top goal-scoring teams), visualized in an interactive
  Streamlit dashboard.

## Data Quality Highlights

Merging 90+ years of football data from two independent sources surfaced
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
│   ├── transform.py     # Cleaning, standardization, merging
│   ├── load.py           # Loads into CSV + PostgreSQL
│   ├── analytics.py     # Example analytical query
│   └── dashboard.py     # Streamlit interactive dashboard
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

## Next Steps

Planned evolutions to take this from a batch pipeline to a more advanced
architecture:

- Replace the full-table `if_exists='replace'` load with an idempotent
  upsert strategy.
- Add orchestration (Apache Airflow or Prefect) to schedule and monitor runs.
- Replace direct loading with a proper transformation layer using dbt.
- Explore PySpark for distributed processing at larger scale.
## Known Limitations
- The historical dataset covers World Cups from 1930 to 2018; the 2022
  tournament is not included (not present in the source dataset used).
  Filling this gap — ideally via the same API already integrated — is a
  natural next step.