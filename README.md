# Global Trade Pipeline

End-to-end **analytics engineering pipeline** for international trade data — from raw bilateral trade flows to interactive dashboards, on a modern cloud stack.

**Stack:** BigQuery · dbt · Apache Airflow · GitHub Actions · Power BI · Streamlit · Docker

---

## How to run

### Prerequisites

- Python 3.11
- A GCP service account key at `credentials/dbt-service-account-key.json`
- BACI CSV files at `data/` (not committed — download from CEPII)

### Setup

```bash
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 1. Ingest raw data into BigQuery

```bash
python ingestion/load_baci_to_bigquery.py
```

Loads three tables into the `raw` dataset:

| Table | Source |
|---|---|
| `raw.baci_trade_flows` | 15 annual CSV files (2010–2024) |
| `raw.country_codes` | `country_codes_V202601.csv` |
| `raw.product_codes` | `product_codes_HS92_V202601.csv` |

### 2. Run dbt transformations

```bash
cd global_trade_pipeline
dbt run
dbt test
```

---

## Overview

This project ingests global **import/export** data (bilateral trade flows by country and product), models it into an analytics-ready dimensional warehouse, and serves it through interactive dashboards. It answers questions such as:

- What are a country's main trading partners, and how have they evolved?
- Which products drive a country's trade balance?
- How concentrated are global markets for a given product?

## Architecture

```
BACI / CEPII (CSV bulk download)
        │
        ▼
   ingestion/load_baci_to_bigquery.py
        │
        ▼
   BigQuery: raw (baci_trade_flows, country_codes, product_codes)
        │
        ▼
   dbt: staging (views) → marts (tables)
        │
        ├──────────────► Power BI report
        └──────────────► Streamlit app (public)

CI: GitHub Actions runs dbt tests on every push
Reproducibility: Docker
```

## Tech stack

| Layer | Tool | Role |
|---|---|---|
| Warehouse | **BigQuery** | Serverless cloud warehouse; stores and queries trade data at scale |
| Transformation | **dbt** | Staging → marts, with tests and YAML documentation |
| Orchestration | **Apache Airflow** | Schedules and chains the ingestion + dbt run (DAG) |
| CI/CD | **GitHub Actions** | Runs dbt tests automatically on every push |
| BI | **Power BI** | Executive report (published to web) |
| App | **Streamlit** | Public interactive dashboard |
| Reproducibility | **Docker** | Containerized, reproducible environment |

## Data source

**BACI (CEPII)** — cleaned bilateral trade dataset derived from UN Comtrade. Grain: exporter × importer × HS6 product × year. Values in thousand USD, quantities in metric tons. Years 2014–2024, ~200 countries, ~5000 products.

**Scale:** 118 million rows across 11 years of global trade — real production-grade data volume.

## Data model

- **Raw:** `baci_trade_flows`, `country_codes`, `product_codes` — all columns as STRING, no transformations.
- **Staging:** typed columns, NULLs handled, columns renamed.
- **Marts:** `fact_trade_flows`, `dim_country`, `dim_product` — analytics-ready dimensional model.

## Project structure

```
global-trade-pipeline/
├── ingestion/                  # extract + load scripts (CSV → BigQuery raw)
├── global_trade_pipeline/      # dbt project: staging, marts, tests, docs
├── data/                       # raw CSV files (git-ignored)
├── credentials/                # GCP service account key (git-ignored)
├── requirements.txt
└── README.md
```

## Roadmap

- [x] GCP project + BigQuery datasets (`raw`, `staging`, `marts`)
- [x] Service account + IAM permissions
- [x] dbt project initialized and connected to BigQuery
- [x] Ingestion script (CSV → `raw`)
- [ ] dbt staging models (+ not_null / unique tests)
- [ ] dbt marts models — fact + dimensions (+ relationship tests)
- [ ] Streamlit dashboard **← 🎯 v1 publicável**
- [ ] Power BI report (BigQuery connector + publish to web)

--- full pipeline ---

- [ ] Airflow DAG for orchestration
- [ ] GitHub Actions CI (dbt tests on push)
- [ ] Docker

## Author

**Flavio Ribeiro Córdula** — Data Analyst / Analytics Engineer
[LinkedIn](https://www.linkedin.com/in/cordulaflavio) · [GitHub](https://github.com/cordulaflavio)
