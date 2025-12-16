"""
mfg_dbt_transform_dag.py

Transform-only DAG:
- In production, ingestion and transformation often run independently.
- This DAG waits for the ingestion DAG to finish successfully, then runs dbt steps.

Wait strategy:
- ExternalTaskSensor waits for `mfg_raw_ingestion_dag.ingest_raw_to_snowflake`.
"""

from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

DAG_ID = "mfg_dbt_transform_dag"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

def _bash(cmd: str) -> str:
    return f"set -euo pipefail; {cmd}"

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="dbt-only transform flow: staging -> core -> marts -> test",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Budapest"),
    schedule="15 5 * * *",  # 05:15 daily (typically after ingestion starts)
    catchup=False,
    max_active_runs=1,
    tags=["mfg", "dbt", "transform"],
    doc_md=f"""
### Manufacturing dbt transform DAG (daily)

**Goal:** separation between ingestion and transformation.

**Steps**
1. Wait for `mfg_raw_ingestion_dag.ingest_raw_to_snowflake` to succeed
2. dbt run staging
3. dbt run core
4. dbt run marts
5. dbt test all
""",
) as dag:

    wait_for_raw = ExternalTaskSensor(
        task_id="wait_for_raw",
        external_dag_id="mfg_raw_ingestion_dag",
        external_task_id="ingest_raw_to_snowflake",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        mode="reschedule",  # frees worker slot while waiting (prod-friendly)
        poke_interval=60,   # check every 60s
        timeout=60 * 60,    # stop waiting after 60 min
        execution_timeout=timedelta(minutes=70),
        doc_md="""
Waits for RAW ingestion to finish successfully.
Prod note: `mode="reschedule"` avoids burning worker resources while waiting.
""",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_bash(
            f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=20),
        doc_md="Builds STAGING models (stg_*).",
    )

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=_bash(
            f"cd {DBT_PROJECT_DIR} && dbt run --select core --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=25),
        doc_md="Builds CORE models (core_* and fct_*).",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=_bash(
            f"cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=25),
        doc_md="Builds MARTS models (KPIs + ML-ready tables).",
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=_bash(
            f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=30),
        doc_md="Runs all dbt tests after transforms complete.",
    )

    wait_for_raw >> dbt_run_staging >> dbt_run_core >> dbt_run_marts >> dbt_test_all
