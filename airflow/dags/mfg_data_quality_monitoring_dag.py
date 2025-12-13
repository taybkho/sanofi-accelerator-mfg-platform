"""
mfg_data_quality_monitoring_dag.py

Goal:
- Run daily data quality checks on key manufacturing models/tables.
- Log results into Snowflake audit table (RAW.DATA_QUALITY_AUDIT).
- Fail fast on true DQ issues, while still writing audit results even on failures.
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule


# -----------------------------
# Snowflake helpers (prod-style)
# -----------------------------

def _sf_connect() -> snowflake.connector.SnowflakeConnection:
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise AirflowFailException(f"Missing Snowflake env vars: {missing}")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    conn.autocommit(False)
    return conn


def _fetch_one(cursor, query: str) -> tuple:
    cursor.execute(query)
    return cursor.fetchone()


def _execute(cursor, query: str, params: tuple | None = None) -> None:
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)


# -----------------------------
# DQ checks
# -----------------------------

def dq_check_rowcounts_raw_vs_core(**context) -> None:
    pairs = [
        ("RAW.MATERIALS", "ANALYTICS_ANALYTICS.CORE_MATERIALS"),
        ("RAW.BATCHES", "ANALYTICS_ANALYTICS.CORE_BATCHES"),
        ("RAW.PRODUCTION_ORDERS", "ANALYTICS_ANALYTICS.FCT_PRODUCTION_ORDERS"),
    ]

    conn = _sf_connect()
    cur = conn.cursor()
    try:
        diffs = []
        for raw_tbl, core_tbl in pairs:
            raw_cnt = _fetch_one(cur, f"SELECT COUNT(*) FROM {raw_tbl}")[0]
            core_cnt = _fetch_one(cur, f"SELECT COUNT(*) FROM {core_tbl}")[0]
            if raw_cnt != core_cnt:
                diffs.append(
                    {
                        "raw_table": raw_tbl,
                        "core_table": core_tbl,
                        "raw_count": int(raw_cnt),
                        "core_count": int(core_cnt),
                        "diff": int(raw_cnt - core_cnt),
                    }
                )

        context["ti"].xcom_push(key="rowcount_diffs", value=diffs)

        if diffs:
            raise AirflowFailException(f"Rowcount mismatch RAW vs CORE: {diffs}")

    finally:
        try:
            cur.close()
        finally:
            conn.close()


def dq_check_duplicates_batches(**context) -> None:
    tbl = "ANALYTICS_ANALYTICS.CORE_BATCHES"
    key = "BATCH_ID"

    conn = _sf_connect()
    cur = conn.cursor()
    try:
        dup_key_count = _fetch_one(
            cur,
            f"""
            SELECT COUNT(*)
            FROM (
              SELECT {key}
              FROM {tbl}
              GROUP BY {key}
              HAVING COUNT(*) > 1
            )
            """,
        )[0]

        null_key_count = _fetch_one(
            cur,
            f"SELECT COUNT(*) FROM {tbl} WHERE {key} IS NULL",
        )[0]

        result = {
            "table": tbl,
            "key": key,
            "duplicate_key_count": int(dup_key_count),
            "null_key_count": int(null_key_count),
        }
        context["ti"].xcom_push(key="dupe_check", value=result)

        if dup_key_count > 0 or null_key_count > 0:
            raise AirflowFailException(f"Duplicate/null key check failed: {result}")

    finally:
        try:
            cur.close()
        finally:
            conn.close()


def dq_check_freshness_kpis(**context) -> None:
    tbl = "ANALYTICS_ANALYTICS.MFG_PLANT_DAILY_KPIS"

    conn = _sf_connect()
    cur = conn.cursor()
    try:
        max_date, days_lag = _fetch_one(
            cur,
            f"""
            SELECT
              MAX(CALENDAR_DATE) AS max_date,
              DATEDIFF('day', MAX(CALENDAR_DATE), CURRENT_DATE()) AS days_lag
            FROM {tbl}
            """,
        )

        result = {
            "table": tbl,
            "max_calendar_date": str(max_date) if max_date else None,
            "days_lag": int(days_lag) if days_lag is not None else None,
        }
        context["ti"].xcom_push(key="freshness_check", value=result)

        if max_date is None:
            raise AirflowFailException(f"Freshness check failed: {tbl} is empty")

        if days_lag is None or days_lag > 1:
            raise AirflowFailException(f"Freshness check failed: {result}")

    finally:
        try:
            cur.close()
        finally:
            conn.close()


def dq_log_results(**context) -> None:
    """
    Log DQ results into RAW.DATA_QUALITY_AUDIT.

    Fixes:
    - Handles existing table created with different schema/casing.
    - Uses quoted identifiers and ALTER TABLE migrations.
    """
    ti = context["ti"]

    rowcount_diffs = ti.xcom_pull(
        key="rowcount_diffs",
        task_ids="dq_check_rowcounts_raw_vs_core",
    )
    dupe_check = ti.xcom_pull(
        key="dupe_check",
        task_ids="dq_check_duplicates_batches",
    )
    freshness_check = ti.xcom_pull(
        key="freshness_check",
        task_ids="dq_check_freshness_kpis",
    )

    dag_run_id = context["dag_run"].run_id if context.get("dag_run") else None
    dag_run_id = dag_run_id or "unknown"

    payload = {
        "run_ts_utc": datetime.utcnow().isoformat(),
        "dag_run_id": dag_run_id,
        "rowcount_diffs": rowcount_diffs or [],
        "dupe_check": dupe_check,
        "freshness_check": freshness_check,
    }
    payload_json = json.dumps(payload)

    conn = _sf_connect()
    cur = conn.cursor()
    try:
        # Create if missing (quoted identifiers = consistent schema)
        _execute(
            cur,
            """
            CREATE TABLE IF NOT EXISTS RAW.DATA_QUALITY_AUDIT (
              "RUN_TS" TIMESTAMP_LTZ,
              "DAG_RUN_ID" STRING,
              "RESULTS" VARIANT
            )
            """
        )

        # Migrate if table existed with different schema (safe, idempotent)
        _execute(cur, 'ALTER TABLE RAW.DATA_QUALITY_AUDIT ADD COLUMN IF NOT EXISTS "RUN_TS" TIMESTAMP_LTZ')
        _execute(cur, 'ALTER TABLE RAW.DATA_QUALITY_AUDIT ADD COLUMN IF NOT EXISTS "DAG_RUN_ID" STRING')
        _execute(cur, 'ALTER TABLE RAW.DATA_QUALITY_AUDIT ADD COLUMN IF NOT EXISTS "RESULTS" VARIANT')

        # Insert using quoted identifiers so casing always matches
        _execute(
            cur,
            """
            INSERT INTO RAW.DATA_QUALITY_AUDIT ("RUN_TS", "DAG_RUN_ID", "RESULTS")
            SELECT CURRENT_TIMESTAMP(), %s, PARSE_JSON(%s)
            """,
            params=(dag_run_id, payload_json),
        )


        conn.commit()

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise AirflowFailException(
            f"Failed to write DQ audit log. dag_run_id={dag_run_id}, "
            f"payload_bytes={len(payload_json)}. Error={e}"
        )

    finally:
        try:
            cur.close()
        finally:
            conn.close()


# -----------------------------
# DAG definition
# -----------------------------

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="mfg_data_quality_monitoring_dag",
    default_args=default_args,
    description="Daily data quality monitoring checks (counts, duplicates, freshness) + audit logging.",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["mfg", "dq", "monitoring"],
) as dag:

    t_rowcounts = PythonOperator(
        task_id="dq_check_rowcounts_raw_vs_core",
        python_callable=dq_check_rowcounts_raw_vs_core,
        execution_timeout=timedelta(minutes=10),
    )

    t_dupes = PythonOperator(
        task_id="dq_check_duplicates_batches",
        python_callable=dq_check_duplicates_batches,
        execution_timeout=timedelta(minutes=10),
    )

    t_fresh = PythonOperator(
        task_id="dq_check_freshness_kpis",
        python_callable=dq_check_freshness_kpis,
        execution_timeout=timedelta(minutes=10),
    )

    t_log = PythonOperator(
        task_id="dq_log_results",
        python_callable=dq_log_results,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=10),
    )

    t_rowcounts >> t_dupes >> t_fresh >> t_log
