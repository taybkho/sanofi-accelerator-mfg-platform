"""
mfg_data_quality_monitoring_dag.py

Step 2 upgrade:
- Introduces severity levels (ERROR vs WARN)
- WARN-level checks do not fail the DAG
- Audit payload separates errors and warnings
- Keeps all core checks: rowcounts, duplicates, freshness, + audit logging

Goal:
- Daily data quality checks + write results to RAW.DATA_QUALITY_AUDIT
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
# Severity model
# -----------------------------
SEVERITY_ERROR = "ERROR"
SEVERITY_WARN = "WARN"


# -----------------------------
# Snowflake helpers
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


def _execute(cur, query: str, params: tuple | None = None) -> None:
    if params:
        cur.execute(query, params)
    else:
        cur.execute(query)


def _fetch_one(cur, query: str) -> tuple:
    cur.execute(query)
    return cur.fetchone()


# -----------------------------
# ERROR-level checks
# -----------------------------
def dq_check_rowcounts_raw_vs_core(**context) -> None:
    """
    ERROR: Table-level RAW vs CORE reconciliation (strict).
    """
    pairs = [
        ("RAW.MATERIALS", "ANALYTICS_ANALYTICS.CORE_MATERIALS"),
        ("RAW.BATCHES", "ANALYTICS_ANALYTICS.CORE_BATCHES"),
        ("RAW.PRODUCTION_ORDERS", "ANALYTICS_ANALYTICS.FCT_PRODUCTION_ORDERS"),
    ]

    conn = _sf_connect()
    cur = conn.cursor()
    try:
        errors = []
        for raw_tbl, core_tbl in pairs:
            raw_cnt = _fetch_one(cur, f"SELECT COUNT(*) FROM {raw_tbl}")[0]
            core_cnt = _fetch_one(cur, f"SELECT COUNT(*) FROM {core_tbl}")[0]

            if raw_cnt != core_cnt:
                errors.append(
                    {
                        "check": "table_rowcount_mismatch",
                        "severity": SEVERITY_ERROR,
                        "raw_table": raw_tbl,
                        "core_table": core_tbl,
                        "raw_count": int(raw_cnt),
                        "core_count": int(core_cnt),
                        "diff": int(raw_cnt - core_cnt),
                    }
                )

        context["ti"].xcom_push(key="rowcount_errors", value=errors)

        if errors:
            raise AirflowFailException(f"Table-level rowcount mismatch: {errors}")

    finally:
        cur.close()
        conn.close()


def dq_check_rowcounts_by_date(**context) -> None:
    """
    ERROR: Partition-aware reconciliation by ACTUAL_START_DATE (NULL excluded).
    """
    conn = _sf_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
              r.ACTUAL_START_DATE AS business_date,
              COUNT(*) AS raw_cnt,
              COALESCE(c.core_cnt, 0) AS core_cnt,
              COUNT(*) - COALESCE(c.core_cnt, 0) AS diff
            FROM RAW.PRODUCTION_ORDERS r
            LEFT JOIN (
              SELECT ACTUAL_START_DATE AS business_date, COUNT(*) AS core_cnt
              FROM ANALYTICS_ANALYTICS.FCT_PRODUCTION_ORDERS
              WHERE ACTUAL_START_DATE IS NOT NULL
              GROUP BY ACTUAL_START_DATE
            ) c
              ON r.ACTUAL_START_DATE = c.business_date
            WHERE r.ACTUAL_START_DATE IS NOT NULL
            GROUP BY r.ACTUAL_START_DATE, c.core_cnt
            HAVING COUNT(*) != COALESCE(c.core_cnt, 0)
            ORDER BY r.ACTUAL_START_DATE
            """
        )

        rows = cur.fetchall()
        errors = [
            {
                "check": "partition_rowcount_mismatch",
                "severity": SEVERITY_ERROR,
                "business_date": str(r[0]),
                "raw_count": int(r[1]),
                "core_count": int(r[2]),
                "diff": int(r[3]),
            }
            for r in rows
        ]

        context["ti"].xcom_push(key="partition_rowcount_errors", value=errors)

        if errors:
            raise AirflowFailException(f"Partition rowcount mismatch: {errors}")

    finally:
        cur.close()
        conn.close()


def dq_check_duplicates_batches(**context) -> None:
    """
    ERROR: Duplicate + NULL key checks on CORE_BATCHES.BATCH_ID
    """
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

        null_key_count = _fetch_one(cur, f"SELECT COUNT(*) FROM {tbl} WHERE {key} IS NULL")[0]

        result = {
            "check": "duplicates_core_batches",
            "severity": SEVERITY_ERROR,
            "table": tbl,
            "key": key,
            "duplicate_key_count": int(dup_key_count),
            "null_key_count": int(null_key_count),
        }

        context["ti"].xcom_push(key="dupe_check", value=result)

        if dup_key_count > 0 or null_key_count > 0:
            raise AirflowFailException(f"Duplicate/NULL key check failed: {result}")

    finally:
        cur.close()
        conn.close()


def dq_check_freshness_kpis(**context) -> None:
    """
    ERROR: KPI table must be fresh (max date within last 1 day).
    """
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
            "check": "freshness_kpis",
            "severity": SEVERITY_ERROR,
            "table": tbl,
            "max_calendar_date": str(max_date) if max_date else None,
            "days_lag": int(days_lag) if days_lag is not None else None,
        }
        context["ti"].xcom_push(key="freshness_check", value=result)

        if max_date is None:
            raise AirflowFailException(f"Freshness failed: {tbl} is empty")

        if days_lag is None or days_lag > 1:
            raise AirflowFailException(f"Freshness failed: {result}")

    finally:
        cur.close()
        conn.close()


# -----------------------------
# WARN-level checks (do not fail)
# -----------------------------
def dq_check_null_actual_start_date(**context) -> None:
    """
    WARN: Missing ACTUAL_START_DATE in RAW.PRODUCTION_ORDERS.
    We log it but do not block the pipeline.
    """
    conn = _sf_connect()
    cur = conn.cursor()
    try:
        null_count = _fetch_one(
            cur,
            """
            SELECT COUNT(*)
            FROM RAW.PRODUCTION_ORDERS
            WHERE ACTUAL_START_DATE IS NULL
            """,
        )[0]

        warn = {
            "check": "null_actual_start_date",
            "severity": SEVERITY_WARN,
            "null_count": int(null_count),
        }

        context["ti"].xcom_push(key="warnings", value=[warn] if null_count > 0 else [])

    finally:
        cur.close()
        conn.close()


# -----------------------------
# Audit logging (always runs)
# -----------------------------
def dq_log_results(**context) -> None:
    ti = context["ti"]

    # Collect ERROR details from upstream tasks (they may be empty if PASS)
    errors = []
    errors += ti.xcom_pull(key="rowcount_errors", task_ids="dq_check_rowcounts_raw_vs_core") or []
    errors += ti.xcom_pull(key="partition_rowcount_errors", task_ids="dq_check_rowcounts_by_date") or []

    # Add structured ERROR results for context (even if they passed)
    dupe_check = ti.xcom_pull(key="dupe_check", task_ids="dq_check_duplicates_batches")
    freshness_check = ti.xcom_pull(key="freshness_check", task_ids="dq_check_freshness_kpis")

    # WARNs
    warnings = ti.xcom_pull(key="warnings", task_ids="dq_check_null_actual_start_date") or []

    dag_run_id = context["dag_run"].run_id if context.get("dag_run") else "unknown"

    payload = {
        "run_ts_utc": datetime.utcnow().isoformat(),
        "dag_run_id": dag_run_id,
        "severity_summary": {"error_count": len(errors), "warn_count": len(warnings)},
        "errors": errors,
        "warnings": warnings,
        "check_outputs": {
            "dupe_check": dupe_check,
            "freshness_check": freshness_check,
        },
    }

    conn = _sf_connect()
    cur = conn.cursor()
    try:
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

        _execute(
            cur,
            """
            INSERT INTO RAW.DATA_QUALITY_AUDIT ("RUN_TS", "DAG_RUN_ID", "RESULTS")
            SELECT CURRENT_TIMESTAMP(), %s, PARSE_JSON(%s)
            """,
            params=(dag_run_id, json.dumps(payload)),
        )

        conn.commit()

    finally:
        cur.close()
        conn.close()


# -----------------------------
# DAG definition
# -----------------------------
default_args = {"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=3)}

with DAG(
    dag_id="mfg_data_quality_monitoring_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["mfg", "dq", "monitoring"],
    description="Production-grade DQ monitoring with severity levels",
) as dag:

    t_rowcounts = PythonOperator(
        task_id="dq_check_rowcounts_raw_vs_core",
        python_callable=dq_check_rowcounts_raw_vs_core,
        execution_timeout=timedelta(minutes=10),
    )

    t_rowcounts_by_date = PythonOperator(
        task_id="dq_check_rowcounts_by_date",
        python_callable=dq_check_rowcounts_by_date,
        execution_timeout=timedelta(minutes=10),
    )

    t_null_dates = PythonOperator(
        task_id="dq_check_null_actual_start_date",
        python_callable=dq_check_null_actual_start_date,
        execution_timeout=timedelta(minutes=5),
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

    # Flow (readable + deterministic)
    t_rowcounts >> t_rowcounts_by_date >> t_null_dates >> t_dupes >> t_fresh >> t_log
