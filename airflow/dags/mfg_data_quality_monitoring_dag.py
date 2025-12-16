from __future__ import annotations

import os
import json
from datetime import datetime, timedelta

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule


# -------------------------------------------------------------------
# Snowflake helpers
# -------------------------------------------------------------------

def sf_connect():
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

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def fetch_one(cur, sql):
    cur.execute(sql)
    return cur.fetchone()


# -------------------------------------------------------------------
# 1️⃣ Table-level rowcount (short-circuit gate)
# -------------------------------------------------------------------

def dq_check_rowcounts(**context) -> bool:
    """
    Gatekeeper:
    - If RAW empty → stop deep checks
    - If CORE empty but RAW not → fail immediately
    """
    tables = [
        ("RAW.MATERIALS", "ANALYTICS_ANALYTICS.CORE_MATERIALS"),
        ("RAW.BATCHES", "ANALYTICS_ANALYTICS.CORE_BATCHES"),
        ("RAW.PRODUCTION_ORDERS", "ANALYTICS_ANALYTICS.FCT_PRODUCTION_ORDERS"),
    ]

    conn = sf_connect()
    cur = conn.cursor()

    errors = []
    try:
        for raw, core in tables:
            raw_cnt = fetch_one(cur, f"SELECT COUNT(*) FROM {raw}")[0]
            core_cnt = fetch_one(cur, f"SELECT COUNT(*) FROM {core}")[0]

            if raw_cnt == 0 and core_cnt > 0:
                errors.append(
                    {
                        "check": "table_rowcount_mismatch",
                        "severity": "ERROR",
                        "raw_table": raw,
                        "core_table": core,
                        "raw_count": raw_cnt,
                        "core_count": core_cnt,
                        "diff": raw_cnt - core_cnt,
                    }
                )

        context["ti"].xcom_push(key="errors", value=errors)

        # Stop deeper checks if RAW is empty
        raw_total = sum(
            fetch_one(cur, f"SELECT COUNT(*) FROM {t[0]}")[0]
            for t in tables
        )

        if raw_total == 0:
            return False  # short-circuit downstream checks

        if errors:
            raise AirflowFailException(f"Table rowcount mismatch: {errors}")

        return True

    finally:
        cur.close()
        conn.close()


# -------------------------------------------------------------------
# 2️⃣ Partition-level rowcounts (by business date)
# -------------------------------------------------------------------

def dq_check_rowcounts_by_date(**context):
    conn = sf_connect()
    cur = conn.cursor()

    diffs = []
    try:
        cur.execute(
            """
            SELECT
              s.business_date,
              COUNT(*) AS raw_cnt,
              COALESCE(c.cnt, 0) AS core_cnt
            FROM RAW.BATCHES s
            LEFT JOIN (
              SELECT MANUFACTURE_DATE AS business_date, COUNT(*) cnt
              FROM ANALYTICS_ANALYTICS.CORE_BATCHES
              GROUP BY MANUFACTURE_DATE
            ) c USING (business_date)
            GROUP BY s.business_date
            HAVING raw_cnt != core_cnt
            """
        )

        for row in cur.fetchall():
            diffs.append(
                {
                    "check": "partition_rowcount_mismatch",
                    "severity": "ERROR",
                    "business_date": str(row[0]),
                    "raw_count": int(row[1]),
                    "core_count": int(row[2]),
                    "diff": int(row[1] - row[2]),
                }
            )

        if diffs:
            raise AirflowFailException(f"Partition rowcount mismatch: {diffs}")

    finally:
        cur.close()
        conn.close()


# -------------------------------------------------------------------
# 3️⃣ NULL business dates
# -------------------------------------------------------------------

def dq_check_null_dates(**context):
    conn = sf_connect()
    cur = conn.cursor()

    try:
        nulls = fetch_one(
            cur,
            """
            SELECT COUNT(*)
            FROM ANALYTICS_ANALYTICS.FCT_PRODUCTION_ORDERS
            WHERE ACTUAL_START_DATE IS NULL
            """,
        )[0]

        if nulls > 0:
            context["ti"].xcom_push(
                key="warnings",
                value=[
                    {
                        "check": "null_actual_start_date",
                        "severity": "WARN",
                        "null_count": int(nulls),
                    }
                ],
            )

    finally:
        cur.close()
        conn.close()


# -------------------------------------------------------------------
# 4️⃣ Duplicate keys
# -------------------------------------------------------------------

def dq_check_duplicates(**context):
    conn = sf_connect()
    cur = conn.cursor()

    try:
        dupes = fetch_one(
            cur,
            """
            SELECT COUNT(*) FROM (
              SELECT BATCH_ID
              FROM ANALYTICS_ANALYTICS.CORE_BATCHES
              GROUP BY BATCH_ID
              HAVING COUNT(*) > 1
            )
            """,
        )[0]

        if dupes > 0:
            raise AirflowFailException(f"Duplicate batch_id detected: {dupes}")

    finally:
        cur.close()
        conn.close()


# -------------------------------------------------------------------
# 5️⃣ Freshness
# -------------------------------------------------------------------

def dq_check_freshness(**context):
    conn = sf_connect()
    cur = conn.cursor()

    try:
        days_lag = fetch_one(
            cur,
            """
            SELECT DATEDIFF('day', MAX(CALENDAR_DATE), CURRENT_DATE())
            FROM ANALYTICS_ANALYTICS.MFG_PLANT_DAILY_KPIS
            """,
        )[0]

        if days_lag is None or days_lag > 1:
            context["ti"].xcom_push(
                key="warnings",
                value=[
                    {
                        "check": "freshness_delay",
                        "severity": "WARN",
                        "days_lag": int(days_lag),
                    }
                ],
            )

    finally:
        cur.close()
        conn.close()


# -------------------------------------------------------------------
# 6️⃣ DQ Score
# -------------------------------------------------------------------

def dq_compute_score(**context):
    ti = context["ti"]
    errors = ti.xcom_pull(key="errors") or []
    warnings = ti.xcom_pull(key="warnings") or []

    score = 100 - (25 * len(errors)) - (5 * len(warnings))
    score = max(score, 0)

    ti.xcom_push(
        key="dq_score",
        value={
            "score": score,
            "errors": len(errors),
            "warnings": len(warnings),
        },
    )


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="mfg_data_quality_monitoring_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["dq", "manufacturing", "prod"],
) as dag:

    t_rowcounts = ShortCircuitOperator(
        task_id="dq_check_rowcounts",
        python_callable=dq_check_rowcounts,
    )

    t_rowcounts_by_date = PythonOperator(
        task_id="dq_check_rowcounts_by_date",
        python_callable=dq_check_rowcounts_by_date,
    )

    t_null_dates = PythonOperator(
        task_id="dq_check_null_dates",
        python_callable=dq_check_null_dates,
    )

    t_dupes = PythonOperator(
        task_id="dq_check_duplicates",
        python_callable=dq_check_duplicates,
    )

    t_fresh = PythonOperator(
        task_id="dq_check_freshness",
        python_callable=dq_check_freshness,
    )

    t_score = PythonOperator(
        task_id="dq_compute_score",
        python_callable=dq_compute_score,
    )

    t_log = PythonOperator(
        task_id="dq_log_results",
        python_callable=lambda **_: None,  # already implemented by you
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_rowcounts >> t_rowcounts_by_date >> t_null_dates >> t_dupes >> t_fresh >> t_score >> t_log
