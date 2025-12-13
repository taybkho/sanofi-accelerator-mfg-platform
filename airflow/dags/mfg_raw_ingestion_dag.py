"""
mfg_raw_ingestion_dag.py

Daily "end-to-end" demo DAG:
1) Ingest RAW CSVs -> Snowflake RAW (idempotent: TRUNCATE + reload + audit)
2) dbt run (staging + core + marts)
3) dbt test (all tests)

This DAG is intentionally simple and easy to demo in the Airflow UI.
All commands run inside the Airflow container, with the project mounted at /opt/airflow.
"""

from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_ID = "mfg_raw_ingestion_dag"

# Production-like defaults (sane retries, clear ownership)
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Daily end-to-end: RAW ingestion -> dbt run -> dbt test",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Budapest"),
    schedule="0 5 * * *",  # every day at 05:00
    catchup=False,
    max_active_runs=1,
    tags=["mfg", "ingestion", "dbt", "daily"],
    doc_md="""
### Manufacturing RAW ingestion + full transform (daily)

**Goal:** easy-to-demo daily pipeline.

**Steps**
1. Ingest CSVs into Snowflake `RAW` (idempotent reload + auditing)
2. Run dbt models (staging → core → marts)
3. Run dbt tests (not_null / unique / relationships)

**Paths inside container**
- Project root: `/opt/airflow`
- Ingestion script: `/opt/airflow/ingestion/python/load_raw_to_snowflake.py`
- dbt project: `/opt/airflow/dbt_project`
- dbt profiles: `/opt/airflow/dbt_profiles`
""",
) as dag:
    ingest_raw_to_snowflake = BashOperator(
        task_id="ingest_raw_to_snowflake",
        bash_command="python /opt/airflow/ingestion/python/load_raw_to_snowflake.py",
        doc_md="""
Runs the production-grade ingestion script:
- Reads CSVs from `data/raw/source_system`
- TRUNCATES RAW tables (idempotent)
- Reloads via `write_pandas`
- Writes a run record into `RAW_LOAD_AUDIT`
""",
    )

    dbt_run_all = BashOperator(
        task_id="dbt_run_all",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --profiles-dir /opt/airflow/dbt_profiles"
        ),
        doc_md="Builds all dbt models: staging → core → marts.",
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir /opt/airflow/dbt_profiles"
        ),
        doc_md="Runs all dbt schema/data tests (not_null, unique, relationships).",
    )

    ingest_raw_to_snowflake >> dbt_run_all >> dbt_test_all
