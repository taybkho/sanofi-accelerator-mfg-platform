# Manufacturing Data Platform (Snowflake + dbt + Airflow) — Portfolio Project

Production-inspired **manufacturing data platform** simulating how a Digital Manufacturing & Supply team builds reliable pipelines for analytics and AI-ready data.

## Architecture (High level)

**Local “prod-like” environment** using Docker + Airflow, loading into **Snowflake**:

1. **Ingestion (Python)**
   - Reads source CSVs from `data/raw/source_system`
   - Loads into Snowflake **RAW** tables using `write_pandas`
   - Uses **idempotent reload** (TRUNCATE + reload)
   - Writes ingestion audit records into `RAW_LOAD_AUDIT`

2. **Transformations (dbt)**
   - **STAGING**: `stg_*` models (type casting, naming standardization)
   - **CORE**: `core_*` and fact-like models (entity integration + derived metrics)
   - **MARTS**: KPI and ML-ready tables (business/AI consumption layer)
   - dbt schema tests: `unique`, `not_null`, `relationships`

3. **Orchestration & Monitoring (Airflow)**
   - Airflow runs in Docker (LocalExecutor + Postgres metadata DB)
   - Project is mounted inside the container at `/opt/airflow`
   - `.env` is injected via Docker `env_file` to supply `SNOWFLAKE_*` credentials

---

## Snowflake Layering

- **RAW**  
  Raw ingestion tables (loaded from CSV): `MATERIALS`, `BATCHES`, `PRODUCTION_ORDERS`  
  Auditing tables: `RAW_LOAD_AUDIT`, `DATA_QUALITY_AUDIT`

- **STAGING**  
  Cleaned views: `STG_MATERIALS`, `STG_BATCHES`, `STG_PRODUCTION_ORDERS`

- **CORE**  
  Integrated entities + facts: `CORE_MATERIALS`, `CORE_BATCHES`, `FCT_PRODUCTION_ORDERS`

- **MARTS**  
  Business KPIs + ML-ready datasets: e.g. `MFG_PLANT_DAILY_KPIS`, `MFG_ML_FEATURES_ORDERS`

---

## Airflow DAGs Overview

All DAGs live in `airflow/dags/`.

### 1) `mfg_raw_ingestion_dag` — Daily end-to-end load
**File:** `mfg_raw_ingestion_dag.py`  
**Schedule:** `0 5 * * *` (05:00 daily)

Tasks:
- `ingest_raw_to_snowflake` → runs ingestion script
- `dbt_run_all` → builds staging/core/marts
- `dbt_test_all` → runs all dbt tests

Dependency:
`ingest_raw_to_snowflake >> dbt_run_all >> dbt_test_all`

---

### 2) `mfg_dbt_transform_dag` — dbt-only transforms
**File:** `mfg_dbt_transform_dag.py`  
**Schedule:** `15 5 * * *` (05:15 daily)

Tasks:
- `wait_for_raw` → placeholder (ExternalTaskSensor in real prod)
- `dbt_run_staging`
- `dbt_run_core`
- `dbt_run_marts`
- `dbt_test_all`

Dependency:
`wait_for_raw >> dbt_run_staging >> dbt_run_core >> dbt_run_marts >> dbt_test_all`

---

### 3) `mfg_data_quality_monitoring_dag` — Data Quality & Monitoring
**File:** `mfg_data_quality_monitoring_dag.py`  
**Schedule:** `30 5 * * *` (05:30 daily)

Checks:
- RAW vs CORE rowcount reconciliation
- Duplicate detection (batch_id in CORE)
- Freshness check (max KPI date close to today)
- Logs results into `RAW.DATA_QUALITY_AUDIT`
- Notification placeholder for Slack/Teams/email in real production

---

## Production-Readiness Highlights

- **Idempotent RAW ingestion**  
  TRUNCATE + reload prevents duplicate accumulation and avoids uniqueness-test failures downstream.

- **Configuration via env vars**  
  `SNOWFLAKE_*` env vars are loaded from `.env` locally and injected into Docker via `env_file`.

- **dbt testing and governance**  
  Schema tests ensure:
  - keys are unique
  - required columns are not null
  - relationships enforce referential integrity

- **Separation of concerns in orchestration**
  - DAG 1: ingestion + full build (easy demo)
  - DAG 2: transformations only (prod-style separation)
  - DAG 3: monitoring & DQ checks (operational reliability)

- **Auditing**
  - `RAW_LOAD_AUDIT`: ingestion run history
  - `DATA_QUALITY_AUDIT`: monitoring results and outcomes

---

## Engineering Challenges & Solutions (What I learned)

- Airflow **LocalExecutor + Postgres** metadata DB (production-aligned vs SQLite)
- Proper **Fernet key** configuration for Airflow security
- Reliable **volume mounting** and DAG discovery in Docker
- Clear separation of env vars:
  - `.env` for local
  - Docker `env_file` for containers
  - dbt `env_var()` usage for credentials in `profiles.yml`
- Robust file path resolution using `Path(__file__).resolve()` (portable execution)
- Solved non-idempotent ingestion causing dbt uniqueness failures:
  - moved to **TRUNCATE + reload**
  - added ingestion auditing

---

## What this project demonstrates

- End-to-end orchestration with **Airflow**
- Realistic ingestion into a cloud DWH (**Snowflake**)
- Transformations + testing with **dbt**
- Data quality & monitoring patterns + audit logging
- Production-like engineering principles:
  idempotence, logging, configuration management, separation of concerns

---

## How to run (local)

1. Ensure `.env` exists at repo root with `SNOWFLAKE_*` variables
2. Start Airflow via Docker Compose (from `airflow/`)
3. Open Airflow UI: `http://localhost:8080`
4. Trigger:
   - `mfg_raw_ingestion_dag` (end-to-end demo)
   - `mfg_dbt_transform_dag` (transform-only)
   - `mfg_data_quality_monitoring_dag` (monitoring)
