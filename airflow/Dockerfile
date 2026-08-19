# Airflow image for the Global Trade Pipeline.
#
# Build context is the REPO ROOT (see docker-compose.yml), so that requirements.txt
# can be copied in.
#
# Two Python environments live in this image on purpose:
#
#   /usr/local/...          Airflow + the libraries the ingestion tasks import
#   /home/airflow/dbt-venv  dbt and its adapter, isolated
#
# Airflow and dbt each pin a large dependency tree; resolving them together is a
# well-known source of conflicts. The DAG bridges the two with the DBT_BIN
# environment variable, which points at the dbt venv's own entry point.

FROM apache/airflow:3.3.1-python3.11

ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt shells out to git for `dbt deps`; the slim base image does not ship it.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# The ingestion module runs INSIDE the Airflow process (PythonOperator), so its
# libraries are installed in Airflow's environment - resolved under Airflow's own
# constraints so nothing gets silently downgraded.
RUN pip install --no-cache-dir \
      pandas \
      pyarrow \
      google-cloud-bigquery \
      --constraint "${CONSTRAINTS}"

# dbt in its own virtualenv. Invoked by the BashOperator, never imported by Airflow.
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
 && /home/airflow/dbt-venv/bin/dbt --version
