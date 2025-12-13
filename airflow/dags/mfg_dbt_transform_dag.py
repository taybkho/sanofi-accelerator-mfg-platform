"""
mfg_dbt_transform_dag.py

Transform-only DAG to demonstrate separation of concerns:
- In production, ingestion and transformation often run independently.
- This DAG assumes RAW data is already loaded and focuses on dbt steps.

Includes a placeholder "wait_for_raw" task:
- In real production, use ExternalTaskSensor to wait for the ingestion DAG to finish.
"""

from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "mfg_dbt_transform_dag"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="dbt-only transform flow: staging -> core -> marts -> test",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Budapest"),
    schedule="15 5 * * *",  # 05:15 daily (typically after ingestion)
    catchup=False,
    max_active_runs=1,
    tags=["mfg", "dbt", "transform"],
    doc_md="""
### Manufacturing dbt transform DAG (daily)

**Goal:** show production-style separation between ingestion and transformation.

**Steps**
1. (placeholder) wait for RAW readiness
2. dbt run staging
3. dbt run core
4. dbt run marts
5. dbt test all

> In real production, `wait_for_raw` would be an **ExternalTaskSensor**
> waiting for `mfg_raw_ingestion_dag.ingest_raw_to_snowflake` to succeed.
""",
) as dag:
    wait_for_raw = EmptyOperator(
        task_id="wait_for_raw",
        doc_md="""
Placeholder step.

**Real prod option:** ExternalTaskSensor waiting for the ingestion DAG run
(or a dataset-based trigger / event-driven ingestion completion marker).
""",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --select staging --profiles-dir /opt/airflow/dbt_profiles"
        ),
        doc_md="Builds STAGING models (stg_*).",
    )

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --select core --profiles-dir /opt/airflow/dbt_profiles"
        ),
        doc_md="Builds CORE models (core_* and fct_*).",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --select marts --profiles-dir /opt/airflow/dbt_profiles"
        ),
        doc_md="Builds MARTS models (KPIs + ML-ready tables).",
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir /opt/airflow/dbt_profiles"
        ),
        doc_md="Runs all dbt tests after transforms complete.",
    )

    wait_for_raw >> dbt_run_staging >> dbt_run_core >> dbt_run_marts >> dbt_test_all
