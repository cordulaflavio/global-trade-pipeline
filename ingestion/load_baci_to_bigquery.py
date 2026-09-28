"""Load BACI/CEPII bulk CSVs into the BigQuery raw layer.

Run it with `python ingestion/load_baci_to_bigquery.py`.

Configuration comes from environment variables so no path or project id is hardcoded,
and no credential ever lands in the repo:

    BACI_CREDENTIALS_PATH   service account key        (default: ~/.gcp/dbt-service-account-key.json)
    BACI_DATA_DIR           directory holding the CSVs (default: <repo>/data)
    BACI_RAW_PROJECT_ID     BigQuery project for raw   (default: global-trade-pipeline-raw)
    BACI_RAW_DATASET        BigQuery dataset for raw   (default: raw)
"""

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).parent.parent

CREDENTIALS_PATH = Path(
    os.getenv(
        "BACI_CREDENTIALS_PATH",
        str(Path.home() / ".gcp" / "dbt-service-account-key.json"),
    )
)
DATA_DIR = Path(os.getenv("BACI_DATA_DIR", str(REPO_ROOT / "data")))
PROJECT_ID = os.getenv("BACI_RAW_PROJECT_ID", "global-trade-pipeline-raw")
DATASET = os.getenv("BACI_RAW_DATASET", "raw")

BACI_RELEASE = "V202601"
TRADE_FLOWS_TABLE = "baci_trade_flows"

TRADE_FLOWS_SCHEMA = [
    bigquery.SchemaField("t", "STRING"),
    bigquery.SchemaField("i", "STRING"),
    bigquery.SchemaField("j", "STRING"),
    bigquery.SchemaField("k", "STRING"),
    bigquery.SchemaField("v", "STRING"),
    bigquery.SchemaField("q", "STRING"),
]

COUNTRY_SCHEMA = [
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("country_iso2", "STRING"),
    bigquery.SchemaField("country_iso3", "STRING"),
]

PRODUCT_SCHEMA = [
    bigquery.SchemaField("code", "STRING"),
    bigquery.SchemaField("description", "STRING"),
]


def get_client() -> bigquery.Client:
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(
            f"Service account key not found at {CREDENTIALS_PATH}. "
            "Set BACI_CREDENTIALS_PATH to point at your key file."
        )
    return bigquery.Client.from_service_account_json(str(CREDENTIALS_PATH), project=PROJECT_ID)


def trade_flows_file(year: int) -> Path:
    """Path of the BACI CSV for a single year."""
    return DATA_DIR / f"BACI_HS92_Y{year}_{BACI_RELEASE}.csv"


def load_countries(client: bigquery.Client) -> None:
    file = DATA_DIR / f"country_codes_{BACI_RELEASE}.csv"
    logger.info("Loading countries from %s", file.name)

    df = pd.read_csv(file, dtype=str)
    table_id = f"{PROJECT_ID}.{DATASET}.country_codes"
    job_config = bigquery.LoadJobConfig(
        schema=COUNTRY_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows into %s", len(df), table_id)


def load_products(client: bigquery.Client) -> None:
    file = DATA_DIR / f"product_codes_HS92_{BACI_RELEASE}.csv"
    logger.info("Loading products from %s", file.name)

    df = pd.read_csv(file, dtype=str, encoding="latin-1")
    table_id = f"{PROJECT_ID}.{DATASET}.product_codes"
    job_config = bigquery.LoadJobConfig(
        schema=PRODUCT_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows into %s", len(df), table_id)


def _delete_year(client: bigquery.Client, table_id: str, year: int) -> None:
    """Remove rows already loaded for `year` so the task can be safely re-run.

    This is what makes ingestion idempotent: delete-then-append means a retry or a
    backfill leaves the same table state instead of duplicating the year.
    """
    try:
        client.get_table(table_id)
    except NotFound:
        logger.info("%s does not exist yet - nothing to delete", table_id)
        return

    job = client.query(
        f"delete from `{table_id}` where t = @year",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("year", "STRING", str(year))]
        ),
    )
    job.result()
    logger.info("Deleted %s existing rows for year %d", f"{job.num_dml_affected_rows:,}", year)


def load_trade_flows(client: bigquery.Client, year: int) -> None:
    """Load one year of bilateral trade flows, replacing that year if already present."""
    file = trade_flows_file(year)
    if not file.exists():
        raise FileNotFoundError(f"Trade flow file not found: {file}")

    table_id = f"{PROJECT_ID}.{DATASET}.{TRADE_FLOWS_TABLE}"
    _delete_year(client, table_id, year)

    logger.info("Loading %s", file.name)
    job_config = bigquery.LoadJobConfig(
        schema=TRADE_FLOWS_SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    with open(file, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    logger.info("Loaded %s. Total rows in %s: %s", file.name, table_id, f"{table.num_rows:,}")


def load_all_trade_flows(client: bigquery.Client) -> None:
    """Full initial load: every year found in DATA_DIR, truncating the table first."""
    files = sorted(DATA_DIR.glob(f"BACI_HS92_Y*_{BACI_RELEASE}.csv"))
    if not files:
        logger.warning("No trade flow files found in %s", DATA_DIR)
        return

    table_id = f"{PROJECT_ID}.{DATASET}.{TRADE_FLOWS_TABLE}"

    for idx, file in enumerate(files):
        # Truncate on first file, append on subsequent ones
        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if idx == 0
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        logger.info("Loading %s (%d/%d)", file.name, idx + 1, len(files))
        job_config = bigquery.LoadJobConfig(
            schema=TRADE_FLOWS_SCHEMA,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=write_disposition,
        )
        with open(file, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        logger.info("Done: %s", file.name)

    table = client.get_table(table_id)
    logger.info("Total rows in %s: %s", table_id, f"{table.num_rows:,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load BACI CSVs into the BigQuery raw layer.")
    parser.add_argument(
        "--year",
        type=int,
        help="Load only this year (idempotent: replaces the year if already loaded). "
        "Omit to run the full initial load of every year in the data directory.",
    )
    args = parser.parse_args()

    client = get_client()
    load_countries(client)
    load_products(client)

    if args.year:
        load_trade_flows(client, args.year)
    else:
        load_all_trade_flows(client)


if __name__ == "__main__":
    main()
