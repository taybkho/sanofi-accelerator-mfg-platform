"""
mfg_raw_ingestion_dag.py

Daily pipeline:
1) Ingest RAW CSVs -> Snowflake RAW (append / run-aware ingestion + audit)
2) dbt run (staging + core + marts)
3) dbt test (all tests)

All commands run inside the Airflow container, with the project mounted at /opt/airflow.
"""

from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_ID = "mfg_raw_ingestion_dag"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
INGEST_SCRIPT = "/opt/airflow/ingestion/python/load_raw_to_snowflake.py"

def _bash(cmd: str) -> str:
    # Production-like bash: fail fast, no silent errors, no unset vars.
    return f"set -euo pipefail; {cmd}"

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Daily end-to-end: RAW ingestion -> dbt run -> dbt test",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Budapest"),
    schedule="0 5 * * *",  # daily 05:00
    catchup=False,
    max_active_runs=1,
    tags=["mfg", "ingestion", "dbt", "daily"],
    doc_md=f"""
### Manufacturing end-to-end DAG (daily)

**Steps**
1. Ingest CSVs into Snowflake `RAW` (append/run-aware + auditing)
2. Run dbt models (staging → core → marts)
3. Run dbt tests

**Paths inside container**
- Project root: `/opt/airflow`
- Ingestion script: `{INGEST_SCRIPT}`
- dbt project: `{DBT_PROJECT_DIR}`
- dbt profiles: `{DBT_PROFILES_DIR}`
""",
) as dag:

    ingest_raw_to_snowflake = BashOperator(
        task_id="ingest_raw_to_snowflake",
        bash_command=_bash(f"python {INGEST_SCRIPT}"),
        # run-aware hint: let your ingestion script optionally read this env var
        env={"MFG_LOAD_RUN_ID": "{{ run_id }}"},
        execution_timeout=timedelta(minutes=20),
        doc_md="""
Runs the ingestion script:
- Reads CSVs from `data/raw/source_system`
- Loads into Snowflake `RAW` (append/run-aware)
- Writes audit records (e.g. RAW_LOAD_AUDIT / run metadata)
""",
    )

    dbt_run_all = BashOperator(
        task_id="dbt_run_all",
        bash_command=_bash(
            f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=30),
        doc_md="Builds all dbt models: staging → core → marts.",
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=_bash(
            f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=30),
        doc_md="Runs all dbt tests (not_null, unique, relationships).",
    )

    ingest_raw_to_snowflake >> dbt_run_all >> dbt_test_all
