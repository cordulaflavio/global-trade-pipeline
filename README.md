# Global Trade Pipeline

End-to-end **analytics engineering pipeline** for international trade data — from raw bilateral trade flows (118M+ rows) to an analytics-ready dimensional model on BigQuery, served in Power BI.

**Stack:** Python · dbt · BigQuery · Power BI

---

## Overview

This project ingests global **import/export** data (bilateral trade flows by country and product, HS6 level), transforms it into a clean dimensional warehouse with dbt, and serves it in Power BI. It answers questions like:

- What are a country's main trading partners, and how have they evolved?
- Which products drive a country's trade balance?
- How concentrated are global markets for a given product?

The emphasis is on **production-minded engineering** on a real data volume — incremental modeling, partitioning/clustering, testing, and working within hard cloud quotas — not a one-off notebook.

## Architecture

The pipeline is split across **two BigQuery projects on purpose** (see *Engineering decisions* for why):

```
BACI / CEPII  (annual CSV bulk download, 2014–2024)
      │   Python ingestion (ingestion/load_baci_to_bigquery.py)
      ▼
┌─────────────────────────────────────────────┐
│ BigQuery project: global-trade-pipeline-raw │   RAW layer (isolated)
│   raw.baci_trade_flows / country / product  │
└─────────────────────────────────────────────┘
      │   cross-project read (dbt sources)
      ▼
┌─────────────────────────────────────────────┐
│ BigQuery project: global-trade-pipeline      │   MODELED layer (dbt)
│   staging (views) → dims (tables) → fact     │
└─────────────────────────────────────────────┘
      │
      ▼
   Power BI report
```

## Tech stack

| Layer | Tool | Role |
|---|---|---|
| Ingestion | **Python** | Loads the BACI CSVs into the BigQuery raw layer |
| Warehouse | **BigQuery** (free tier, billing enabled) | Serverless storage + SQL engine, two projects |
| Transformation | **dbt** | staging → dims → fact, with tests and YAML docs |
| BI | **Power BI** | Report on the dimensional model (BigQuery connector) |

## Data source

**BACI (CEPII)** — cleaned, reconciled bilateral trade dataset derived from UN Comtrade.
Grain: exporter × importer × HS6 product × year. Values in thousand USD, quantities in metric tons.
Scope loaded: **2014–2024**, ~200 countries, ~5,000 products → **118,692,599 rows**.

> **Gotcha handled:** HS6 product codes are read as **STRING**, never numeric, to preserve leading zeros.

## Data model

**Staging (views — zero storage):**
- `stg_countries`, `stg_products`, `stg_trade_flows` — typed columns, renamed, NULLs handled.

**Dimensions (tables):**
- `dim_country` (~238 rows), `dim_product` (~5,022 rows).

**Fact (incremental table):**
- `fact_trade_flows` — **118.7M rows**, grain HS6 × exporter × importer × year.
- Config:
  - `materialized = incremental`
  - `incremental_strategy = insert_overwrite`
  - `partition_by = year` (range 2014–2025)
  - `cluster_by = [exporter_code, importer_code]`
  - `on_schema_change = sync_all_columns`

**Tests:** 24 passing — `not_null`, `unique`, and `relationships` (referential integrity between fact and dims).

## Engineering decisions

### 1. Two BigQuery projects to live within the free-tier quota
The BigQuery Sandbox caps storage at **10 GB per project**. A full `dbt build` failed with `Quota exceeded: free storage for projects`, even though running `fact_trade_flows` in isolation worked.

**Diagnosis** (via `__TABLES__` byte counts): the project already held **raw (4.57 GB) + fact (5.67 GB) = 10.24 GB** at rest — *already over the cap*. Any temporary table the build created tipped it over. It was **not** fact duplication (incremental doesn't duplicate), **not** staging materialized as tables (they were already views), and **not** orphaned junk.

**Solution:** because the 10 GB quota is **per project**, the raw layer was moved to a **second BigQuery project** (`global-trade-pipeline-raw`). The dbt sources now read **cross-project**; the main project keeps only the fact (5.67 GB) with ~4.3 GB of headroom. Billing was also enabled (required for DML/incremental runs) — cost stays within the free tier limits.

### 2. Incremental fact instead of full rebuild
At 118M rows, rebuilding the fact on every run is wasteful and storage-spiky. `incremental` + `insert_overwrite` rewrites **only the affected year partitions**, keeping runs fast and storage flat.

### 3. Partitioning + clustering
`partition_by = year` lets BI queries scan a single year instead of the whole table (cheaper, faster, friendlier to the 1 TB/month query quota). `cluster_by [exporter_code, importer_code]` speeds up the most common filters (by partner country).

### 4. Staging as views
Staging models are **views**, not tables — zero storage cost, and the typing/cleaning logic stays close to the raw without duplicating 100M+ rows.

## How to run

### Prerequisites
- Python 3.11
- Two BigQuery projects in **`us-central1`**: `global-trade-pipeline` (models) and `global-trade-pipeline-raw` (raw)
- A GCP service account key at `credentials/dbt-service-account-key.json` with access to **both** projects
- BACI CSV files in `data/` (not committed — download from CEPII)

### Setup
```bash
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 1. Ingest raw data
```bash
python ingestion/load_baci_to_bigquery.py   # → global-trade-pipeline-raw
```

### 2. Build and test the models
```bash
cd global_trade_pipeline
dbt run
dbt test
```

## Known limitations (free tier)

- **Avoid `dbt run --full-refresh`** on the fact: it re-reads the entire raw cross-project and recreates the table, causing a storage peak that can exceed the 10 GB quota. Use normal incremental runs.
- **Cross-project read depends on the auth identity** (service account vs. OAuth user in `profiles.yml`). Switching identities without granting access to the raw project breaks the build with an access error.
- **Both projects must be in the same region (`us-central1`).** Cross-project reads do not work across regions.

## Roadmap

- [x] Ingestion (Python → BigQuery raw)
- [x] dbt staging → dims → incremental fact (24 tests passing)
- [x] Storage-quota solution (two-project split)
- [ ] Power BI report (publish to web) **← v1 publishable**
- [ ] Streamlit app (public link)
- [ ] Airflow DAG for orchestration
- [ ] GitHub Actions CI (dbt tests on push)
- [ ] Docker

## Author

**Flavio Ribeiro Córdula** — Data Analyst / Analytics Engineer
[LinkedIn](https://www.linkedin.com/in/cordulaflavio) · [GitHub](https://github.com/cordulaflavio)
