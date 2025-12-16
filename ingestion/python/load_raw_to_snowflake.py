"""
load_raw_to_snowflake.py

Production-style RAW loader (run-aware):

- Reads generated CSV files from data/raw/source_system
- Connects to Snowflake using env vars (.env or injected container env)
- Ensures RAW tables exist AND are schema-migrated to include:
    LOAD_RUN_ID, LOADED_AT
- Run-aware append-only:
    Each run gets a LOAD_ID (uuid). Rows are stamped with LOAD_RUN_ID + LOADED_AT.
- Idempotent per run:
    If rerun with same LOAD_ID, deletes that run's rows then reloads.
- Loads via write_pandas
- Writes per-table audit rows into RAW.RAW_LOAD_AUDIT with schema:
    LOAD_ID, LOAD_TIMESTAMP, SOURCE_NAME, TARGET_TABLE, ROWS_READ, ROWS_LOADED, STATUS, ERROR_MESSAGE
"""

from __future__ import annotations

import os
import sys
import uuid
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

import pandas as pd
from dotenv import load_dotenv

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logger = logging.getLogger("raw_loader")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S"))
if not logger.handlers:
    logger.addHandler(_handler)


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

def get_project_root() -> Path:
    # ingestion/python/load_raw_to_snowflake.py -> parents[2] == project root
    return Path(__file__).resolve().parents[2]


def get_paths() -> Dict[str, Path]:
    root = get_project_root()
    data_dir = root / "data" / "raw" / "source_system"
    return {
        "root": root,
        "data_dir": data_dir,
        "materials_csv": data_dir / "materials.csv",
        "batches_csv": data_dir / "batches.csv",
        "orders_csv": data_dir / "production_orders.csv",
    }


# -----------------------------------------------------------------------------
# Env + Snowflake
# -----------------------------------------------------------------------------

def load_env() -> None:
    env_path = get_project_root() / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        logger.info(f"Loaded environment variables from {env_path}")
    else:
        logger.info(".env not found at project root; assuming env vars already set (docker-compose env_file, CI, etc).")


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
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
        logger.error(f"Missing required Snowflake env vars: {missing}")
        raise SystemExit(1)

    logger.info("Connecting to Snowflake...")
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


# -----------------------------------------------------------------------------
# DDL + schema migration (critical for run-aware)
# -----------------------------------------------------------------------------

def ensure_raw_tables(cur) -> None:
    """
    Create RAW tables if missing, then always ensure run-aware columns exist.
    CREATE TABLE IF NOT EXISTS will NOT modify existing tables, so we also ALTER.
    """
    logger.info("Ensuring RAW tables exist...")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS MATERIALS (
          MATERIAL_ID VARCHAR,
          MATERIAL_NAME VARCHAR,
          MATERIAL_TYPE VARCHAR,
          UNIT_OF_MEASURE VARCHAR,
          STATUS VARCHAR,
          CREATED_AT DATE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS BATCHES (
          BATCH_ID VARCHAR,
          MATERIAL_ID VARCHAR,
          MANUFACTURING_SITE VARCHAR,
          MANUFACTURE_DATE DATE,
          EXPIRY_DATE DATE,
          QUANTITY NUMBER,
          BATCH_STATUS VARCHAR
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS PRODUCTION_ORDERS (
          ORDER_ID VARCHAR,
          BATCH_ID VARCHAR,
          ORDER_TYPE VARCHAR,
          PLANNED_START_DATE DATE,
          PLANNED_END_DATE DATE,
          ACTUAL_START_DATE DATE,
          ACTUAL_END_DATE DATE,
          ORDER_STATUS VARCHAR,
          LINE_ID VARCHAR
        )
        """
    )

    # ---- Schema migration for run-aware metadata (safe, idempotent)
    logger.info("Ensuring run-aware columns exist (LOAD_RUN_ID, LOADED_AT)...")

    for tbl in ("MATERIALS", "BATCHES", "PRODUCTION_ORDERS"):
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS LOAD_RUN_ID STRING")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS LOADED_AT TIMESTAMP_LTZ")

    logger.info("RAW tables are ready (run-aware).")


def ensure_audit_table(cur) -> None:
    """
    Match your existing Snowflake RAW_LOAD_AUDIT schema exactly:
      LOAD_ID, LOAD_TIMESTAMP, SOURCE_NAME, TARGET_TABLE, ROWS_READ, ROWS_LOADED, STATUS, ERROR_MESSAGE
    """
    logger.info("Ensuring RAW_LOAD_AUDIT table exists...")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS RAW_LOAD_AUDIT (
          LOAD_ID STRING,
          LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
          SOURCE_NAME STRING,
          TARGET_TABLE STRING,
          ROWS_READ NUMBER,
          ROWS_LOADED NUMBER,
          STATUS STRING,
          ERROR_MESSAGE STRING
        )
        """
    )
    logger.info("RAW_LOAD_AUDIT is ready.")


# -----------------------------------------------------------------------------
# CSV
# -----------------------------------------------------------------------------

def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    df = pd.read_csv(path)
    logger.info(f"Read {len(df):,} rows from {path.name}")
    return df


# -----------------------------------------------------------------------------
# Audit + load
# -----------------------------------------------------------------------------

def _audit(
    cur,
    load_id: str,
    source_name: str,
    target_table: str,
    rows_read: int,
    rows_loaded: int,
    status: str,
    error_message: Optional[str],
) -> None:
    """
    Writes one audit row. Uses your Snowflake column names.
    We omit LOAD_TIMESTAMP from the insert so Snowflake default applies.
    """
    cur.execute(
        """
        INSERT INTO RAW_LOAD_AUDIT
          (LOAD_ID, SOURCE_NAME, TARGET_TABLE, ROWS_READ, ROWS_LOADED, STATUS, ERROR_MESSAGE)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
        """,
        (load_id, source_name, target_table, rows_read, rows_loaded, status, error_message),
    )


def load_df(
    conn,
    cur,
    df: pd.DataFrame,
    table_name: str,
    source_name: str,
    load_id: str,
) -> Tuple[bool, int]:
    """
    Run-aware append-only load.
    Idempotent for same LOAD_ID: delete that run's rows then reload.
    """
    rows_read = len(df)

    # normalize
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    # add run metadata
    df["LOAD_RUN_ID"] = load_id
    df["LOADED_AT"] = datetime.now(timezone.utc)

    try:
        # idempotent per run
        cur.execute(f"DELETE FROM {table_name} WHERE LOAD_RUN_ID = %s", (load_id,))

        success, _, rows_loaded, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            use_logical_type=True,  # fixes timezone warning for TIMESTAMP_LTZ
        )

        if not success:
            _audit(cur, load_id, source_name, table_name, rows_read, int(rows_loaded), "FAILED", "write_pandas returned success=False")
            return False, int(rows_loaded)

        _audit(cur, load_id, source_name, table_name, rows_read, int(rows_loaded), "SUCCESS", None)
        logger.info(f"[{table_name}] Loaded {rows_loaded:,} rows. load_id={load_id}")
        return True, int(rows_loaded)

    except Exception as e:
        logger.exception(f"[{table_name}] Load failed: {e}")
        try:
            _audit(cur, load_id, source_name, table_name, rows_read, 0, "FAILED", str(e))
        except Exception as audit_err:
            logger.error(f"[{table_name}] Also failed to write audit row: {audit_err}")
        return False, 0


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    load_env()
    paths = get_paths()

    # Run identifier (generated unless you explicitly set LOAD_RUN_ID)
    load_id = os.getenv("LOAD_RUN_ID") or str(uuid.uuid4())
    logger.info(f"Starting RAW load. load_id={load_id}")
    logger.info(f"Using data directory: {paths['data_dir']}")

    conn = None
    cur = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        ensure_raw_tables(cur)
        ensure_audit_table(cur)

        mats = read_csv(paths["materials_csv"])
        batches = read_csv(paths["batches_csv"])
        orders = read_csv(paths["orders_csv"])

        ok1, _ = load_df(conn, cur, mats, "MATERIALS", paths["materials_csv"].name, load_id)
        ok2, _ = load_df(conn, cur, batches, "BATCHES", paths["batches_csv"].name, load_id)
        ok3, _ = load_df(conn, cur, orders, "PRODUCTION_ORDERS", paths["orders_csv"].name, load_id)

        if not (ok1 and ok2 and ok3):
            raise SystemExit(1)

        logger.info("RAW ingestion completed successfully.")
        logger.info(f"load_id={load_id}")

    finally:
        try:
            if cur:
                cur.close()
        finally:
            if conn:
                conn.close()
                logger.info("Snowflake connection closed.")


if __name__ == "__main__":
    main()
