import logging
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CREDENTIALS_PATH = Path.home() / ".gcp" / "dbt-service-account-key.json"
DATA_DIR = Path(__file__).parent.parent / "data"
PROJECT_ID = "global-trade-pipeline-raw"
DATASET = "raw"

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
    return bigquery.Client.from_service_account_json(
        str(CREDENTIALS_PATH), project=PROJECT_ID
    )


def load_countries(client: bigquery.Client) -> None:
    file = DATA_DIR / "country_codes_V202601.csv"
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
    file = DATA_DIR / "product_codes_HS92_V202601.csv"
    logger.info("Loading products from %s", file.name)

    df = pd.read_csv(file, dtype=str, encoding='latin-1')
    table_id = f"{PROJECT_ID}.{DATASET}.product_codes"
    job_config = bigquery.LoadJobConfig(
        schema=PRODUCT_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows into %s", len(df), table_id)


def load_trade_flows(client: bigquery.Client) -> None:
    files = sorted(DATA_DIR.glob("BACI_HS92_Y*_V202601.csv"))
    if not files:
        logger.warning("No trade flow files found in %s", DATA_DIR)
        return

    table_id = f"{PROJECT_ID}.{DATASET}.baci_trade_flows"

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


if __name__ == "__main__":
    client = get_client()
    load_countries(client)
    load_products(client)
    load_trade_flows(client)
