"""Airflow DAG orchestrating the Global Trade Pipeline.

    BACI CSVs (data/)  ->  BigQuery raw  ->  dbt staging / dims / fact  ->  dbt tests

Design notes (the "why", not the "what"):

* **Annual source, monthly poll.** BACI/CEPII publishes one release per year. Scheduling
  the DAG yearly would mean a single chance to notice a late release, so it polls
  monthly and a `ShortCircuitOperator` skips ingestion when the year's CSVs are not
  there yet. `ignore_downstream_trigger_rules=False` keeps that skip local: the dbt
  branch still runs, so models stay fresh and tests keep guarding the warehouse even
  on a no-new-data month.

* **Idempotent ingestion.** Every task is scoped to one `target_year` and the loader
  deletes that year before appending it. Re-running a task, retrying it, or backfilling
  it produces the same table state instead of duplicating rows.

* **Config, not secrets.** Paths, project ids and the dbt target come from environment
  variables. The service account key is never referenced from this file.

* **Why `BashOperator` for dbt.** dbt is a CLI with its own dependency graph; shelling
  out keeps Airflow responsible for *when* and dbt responsible for *what*. Splitting
  seed/run/test into three tasks makes the failure obvious in the UI: a red `dbt_test`
  means a data-quality problem, a red `dbt_run` means a build problem.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG, Param, TaskGroup
from airflow.task.trigger_rule import TriggerRule

# Airflow only puts the dags folder on sys.path. Add the repo root so the DAG can reuse
# the same ingestion module that runs standalone, instead of duplicating the load logic.
# In the container the layout differs from the checkout, so PIPELINE_ROOT overrides the
# path walked from this file (docker-compose.yml sets it to /opt/airflow).
REPO_ROOT = Path(os.getenv("PIPELINE_ROOT") or Path(__file__).resolve().parents[2])
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion import load_baci_to_bigquery as baci  # noqa: E402

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", str(REPO_ROOT / "global_trade_pipeline"))
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", os.path.expanduser("~/.dbt"))
DBT_TARGET = os.getenv("DBT_TARGET", "dev")
# dbt lives in its own virtualenv (see airflow/README.md) - point DBT_BIN at that
# interpreter's `dbt` shim so Airflow's own dependency pins never constrain dbt's.
DBT_BIN = os.getenv("DBT_BIN", "dbt")

DBT = f"cd {DBT_PROJECT_DIR} && {DBT_BIN}"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"

default_args = {
    "owner": "flavio",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def _target_year(**context) -> int:
    return int(context["params"]["target_year"])


def check_source_files(**context) -> bool:
    """Gate the ingestion branch: is there anything new to load?"""
    return baci.source_files_available(_target_year(**context))


def load_countries(**context) -> None:
    baci.load_countries(baci.get_client())


def load_products(**context) -> None:
    baci.load_products(baci.get_client())


def load_trade_flows(**context) -> None:
    year = _target_year(**context)
    baci.load_trade_flows(baci.get_client(), year)


with DAG(
    dag_id="global_trade_pipeline",
    description="Ingest BACI trade flows into BigQuery and rebuild the dbt warehouse.",
    doc_md=__doc__,
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "bigquery", "trade", "portfolio"],
    params={
        "target_year": Param(
            2024,
            type="integer",
            minimum=1995,
            maximum=2100,
            title="BACI year to load",
            description="Which year of trade flows to (re)load. Override at trigger time to backfill.",
        )
    },
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="ingest") as ingest:
        has_new_data = ShortCircuitOperator(
            task_id="check_source_files",
            python_callable=check_source_files,
            # Skip only the tasks directly downstream, so the dbt branch can still
            # decide for itself whether to run (see trigger_rule on dbt_seed).
            ignore_downstream_trigger_rules=False,
        )

        countries = PythonOperator(
            task_id="load_countries",
            python_callable=load_countries,
        )

        products = PythonOperator(
            task_id="load_products",
            python_callable=load_products,
        )

        trade_flows = PythonOperator(
            task_id="load_trade_flows",
            python_callable=load_trade_flows,
            execution_timeout=timedelta(hours=3),  # ~11M rows for a single year
        )

        has_new_data >> [countries, products] >> trade_flows

    with TaskGroup(group_id="dbt") as dbt:
        seed = BashOperator(
            task_id="dbt_seed",
            bash_command=f"{DBT} seed {DBT_FLAGS}",
            # Run whether or not there was new data to ingest: nothing upstream failed,
            # so the models and their tests should still be refreshed.
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        run = BashOperator(
            task_id="dbt_run",
            bash_command=f"{DBT} run {DBT_FLAGS}",
            execution_timeout=timedelta(hours=2),
        )

        test = BashOperator(
            task_id="dbt_test",
            bash_command=f"{DBT} test {DBT_FLAGS}",
            retries=0,  # a failing test is a data problem, not a flake - don't mask it
        )

        seed >> run >> test

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> ingest >> dbt >> end
