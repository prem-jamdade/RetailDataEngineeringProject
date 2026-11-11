# Retail Sales End-to-End Data Engineering Pipeline (Python · Oracle SQL)

An end-to-end, production-style Retail Data Engineering pipeline that extracts daily multi-store CSV drops and Online Sales REST API batches, cleans and validates records using Pandas, stages raw/clean/quarantined data in **Oracle Database 23ai Free (`FREEPDB1`)**, transforms into a **Star Schema Data Warehouse** via **PL/SQL (`PKG_RETAIL_ETL`)**, verifies **Source-to-Target Reconciliation**, and exposes SQL reporting views and analytics queries.

---

## Architecture Overview

```text
+------------------------------+      +-------------------------------+
|  Retail CSV Files (10 Stores)|      |  Online Sales REST API (JSON) |
|  data/raw/*.csv              |      |  http://127.0.0.1:8765/api/.. |
+--------------+---------------+      +---------------+---------------+
               |                                      |
               +------------------+-------------------+
                                  v
+---------------------------------------------------------------------+
| 1. Python Extraction (src/extract.py)                               |
|    - Pandas CSV reader + Requests HTTP API client                   |
|    - SHA-256 Content Watermarking via ETL_FILE_WATERMARK            |
+---------------------------------+-----------------------------------+
                                  v
+---------------------------------------------------------------------+
| 2. Data Cleaning, EDA & Validation (src/clean_validate.py)          |
|    - Standardization, date parsing, missing-value handling          |
|    - Deduplication on TRANSACTION_ID & business-rule validation     |
|    - Generates EDA & DQ report (data/reports/eda_run_<id>.md)       |
+-----------------+-------------------------------+-------------------+
                  | Valid                         | Invalid
                  v                               v
+---------------------------------+   +-------------------------------+
| Oracle Staging: STG_SALES_CLEAN |   | Quarantine: STG_SALES_REJECTS |
| + STG_SALES_RAW (batch insert)  |   | + data/rejected/rejects_*.csv |
+-----------------+---------------+   +-------------------------------+
                  |
                  v
+---------------------------------------------------------------------+
| 3. Oracle PL/SQL Transformation (sql/03_plsql_etl_package.sql)      |
|    - PKG_RETAIL_ETL.SP_PROCESS_RUN                                  |
|    - CTEs + Window Functions + MERGE into Star Schema               |
|    - Hash-based incremental upsert (Insert new / Update changed)    |
+---------------------------------+-----------------------------------+
                                  v
+---------------------------------------------------------------------+
| 4. Oracle Star Schema Data Warehouse (RETAIL_DW)                    |
|    - FACT_SALES                                                     |
|    - DIM_DATE | DIM_STORE | DIM_PRODUCT | DIM_CUSTOMER              |
+---------------------------------+-----------------------------------+
                                  v
+---------------------------------------------------------------------+
| 5. Reconciliation & Reporting Layer                                 |
|    - Python + SQL Reconciliation (ETL_RECONCILIATION_LOG)           |
|    - Oracle Views: VW_SALES_REPORTING, VW_STORE_DAILY_KPI,          |
|      VW_DATA_QUALITY_MONITOR                                        |
|    - SQL Analytics (sql/05_analytics_queries.sql)                   |
+---------------------------------------------------------------------+
```

---

## Implemented Features Matrix

| Feature | Implementation | Key Files |
|---|---|---|
| **Data Extraction** | Multi-store daily CSVs via Pandas + REST API via `requests` (`SalesAPIHandler` simulator included) | [extract.py](src/extract.py), [sales_api_server.py](src/sales_api_server.py) |
| **Data Cleaning & EDA** | Whitespace/case normalization, multi-format date parsing, missing discount imputation, Pandas EDA profiling | [clean_validate.py](src/clean_validate.py), [eda_report.py](src/eda_report.py) |
| **Data Validation** | Mandatory ID checks, positive quantity/price rules, discount bounds (`0-80%`), deduplication, quarantine table & CSV | [clean_validate.py](src/clean_validate.py), [load_staging.py](src/load_staging.py) |
| **Database Connectivity** | Thin-mode `python-oracledb` client against Oracle `FREEPDB1` | [db_oracle.py](src/db_oracle.py) |
| **Batch Data Loading** | Chunked `executemany` batch inserts into `STG_SALES_RAW`, `STG_SALES_CLEAN`, `STG_SALES_REJECTS` | [load_staging.py](src/load_staging.py) |
| **SQL Transformation** | PL/SQL package `PKG_RETAIL_ETL` using CTEs, `ROW_NUMBER()`, `CONNECT BY` date generation, and `MERGE` | [03_plsql_etl_package.sql](sql/03_plsql_etl_package.sql) |
| **Data Warehouse** | Star Schema (`FACT_SALES`, `DIM_DATE`, `DIM_STORE`, `DIM_PRODUCT`, `DIM_CUSTOMER`) with surrogate keys & indexes | [02_dwh_star_schema.sql](sql/02_dwh_star_schema.sql) |
| **Incremental Loading** | SHA-256 file/batch watermarking (`ETL_FILE_WATERMARK`) + row-level `RECORD_HASH` CDC `MERGE` | [extract.py](src/extract.py), [03_plsql_etl_package.sql](sql/03_plsql_etl_package.sql) |
| **Error Handling & Logging** | Structured rotating file + console logging (`logs/retail_etl.log`), `ETL_PIPELINE_RUNS` audit, PL/SQL `EXCEPTION` block | [logger.py](src/logger.py), [pipeline.py](src/pipeline.py) |
| **Automation** | Python `schedule` daemon + Windows Task Scheduler `.bat` and `.ps1` registration scripts | [scheduler.py](src/scheduler.py), [register_windows_task.ps1](scripts/register_windows_task.ps1) |
| **Data Quality & Reconciliation** | 6 automated Python + SQL checks (`ETL_RECONCILIATION_LOG`) verifying raw, clean, reject, DWH counts & net revenue | [reconcile.py](src/reconcile.py), [03_plsql_etl_package.sql](sql/03_plsql_etl_package.sql) |
| **Reporting Layer** | Oracle BI Views and advanced SQL analytics (Pareto ABC, RFM, `LAG`, `RATIO_TO_REPORT`) | [04_reporting_views.sql](sql/04_reporting_views.sql), [05_analytics_queries.sql](sql/05_analytics_queries.sql) |

---

## Verified Dataset Scale & Benchmark

- **Source Volume**: **330 daily files** (`300` multi-store CSVs + `30` REST API JSON batches, **~27.34 MB** on disk) across **30 days**, **11 channels** (10 retail stores + 1 online store), **25 products**, and **121 customers**.
- **Row Accounting (Run #1)**: **105,291** raw rows → **105,000** clean rows loaded into `FACT_SALES` (**29,057,548.69** net revenue) + **250** quarantined rejects (`STG_SALES_REJECTS`) + **41** duplicates removed.
- **Oracle Storage (`RETAIL_DW`)**: **142.31 MB** allocated across staging, Star Schema tables, and indexes.
- **End-to-End Runtime**: **37.27 seconds** for full extraction, cleaning, staging (`ETL_BATCH_SIZE=5000`), PL/SQL Star Schema `MERGE`, and **6/6** source-to-target reconciliation checks (`0.0` variance). Subsequent incremental runs skip unchanged files (`skipped=330`).

---

## Quickstart Guide

### 1. Environment Configuration
1. Copy `.env.example` to `.env` and verify Oracle connection settings (`ORACLE_USER=RETAIL_DW`, `ORACLE_DSN=localhost:1521/freepdb1`).
2. Activate or use the project virtual environment `.venv`:
   ```powershell
   .\.venv\Scripts\python.exe -m pip install -r requirements.txt
   ```

### 2. Initialize Oracle Schema & Objects
```powershell
.\.venv\Scripts\python.exe scripts\setup_oracle_db.py --reset
```
This creates user `RETAIL_DW` in `FREEPDB1` (if missing) and deploys all staging tables, Star Schema tables, `PKG_RETAIL_ETL`, and reporting views.

### 3. Generate Multi-Store Sample Data & Run Pipeline
```powershell
.\.venv\Scripts\python.exe -m src.generate_sample_data
.\.venv\Scripts\python.exe -m src.pipeline --start-api
```

### 4. Run Automated Tests
```powershell
.\.venv\Scripts\python.exe -m pytest -v tests/
```

### 5. Automated Scheduling
- **Python scheduler (daily at 02:00)**:
  ```powershell
  .\.venv\Scripts\python.exe -m src.scheduler --daily-at 02:00 --start-api
  ```
- **Windows Task Scheduler**:
  ```powershell
  powershell -ExecutionPolicy Bypass -File scripts\register_windows_task.ps1 -RunAt "02:00AM"
  ```
