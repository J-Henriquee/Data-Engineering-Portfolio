# ⛩️ Jikan API ETL Pipeline: Top 1000 Anime

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Wrangling-150458)
![Linux](https://img.shields.io/badge/Linux-Cron%20Automation-FCC624)
![Status](https://img.shields.io/badge/Status-Completed-success)

## 📖 About the Project
This project is an end-to-end **Data Engineering ETL (Extract, Transform, Load) Pipeline** that interacts with the [Jikan API (MyAnimeList)](https://docs.api.jikan.moe/) to extract, clean, and analyze data from the Top 1000 highest-rated anime. 

The pipeline is fully automated at the operating system level, designed with strict **Data Quality** rules to handle API anomalies, pagination shifts, and business logic inconsistencies.

## ⚙️ Architecture & Data Flow

1. **Extraction (`extracao.py`)**: 
   - Automates pagination through 40 API endpoints.
   - Implements a `time.sleep()` rate-limiting strategy to prevent IP blocks.
   - Saves raw JSON payloads iteratively into a resilient `raw_data` Data Lake layer.

2. **Transformation & Quality (`transformacao.py`)**: 
   - Consolidates 40 partitioned CSV files into a single master DataFrame.
   - **Governance:** Detects and drops duplicate records caused by real-time ranking shifts during extraction (Pagination Shift).
   - **Data Cleaning:** Removes invalid entries (like Music Videos and Commercials) that lack an official ranking.
   - Exports the clean dataset to the `processed_data` layer.

3. **Exploratory Data Analysis (`analise.py`)**: 
   - Uses Pandas `.groupby()` and `.value_counts()` to extract business insights, proving the statistical bias (Hype Effect) between currently airing anime and finished series.

4. **Orchestration (Linux Cron)**: 
   - The entire pipeline is scheduled via `crontab` to run autonomously on the 1st of every month, using absolute paths and virtual environments to ensure stable background execution.

## 📂 Project Structure
```text
01_pipeline_jikan/
│
├── .venv/                      # Python Virtual Environment
├── raw_data/                   # Data Lake (Raw API Partitions)
│   ├── top_animes_pg1.csv
│   └── ...
├── processed_data/             # Cleaned and Consolidated Data
│   └── top_1000_animes.csv
│
├── extracao.py                 # API Consumption and Raw Storage
├── transformacao.py            # Data Cleaning and Deduplication
├── analise.py                  # Business Insights (EDA)
└── README.md                   # Project Documentation
```

## 🚀 How to Run Locally

Clone the repository:
```bash
git clone https://github.com
cd Data-Engineering-Portfolio/01_pipeline_jikan
```

Activate the virtual environment and install dependencies:
```bash
source .venv/bin/activate
pip install pandas requests
```

Run the pipeline manually:
```bash
python extracao.py
python transformacao.py
python analise.py
```

## 🧠 Key Learnings
* **Resilience:** Building API consumers with rate limiting to bypass blocking.
* **Memory Efficiency:** Consolidating data using `pathlib` and list appends before executing `pd.concat`.
* **Data Quality:** Applying `drop_duplicates` and `dropna` with precise subsets for messy data.
* **Automation:** Linux OS orchestration using Cron jobs tied to virtual environments.
