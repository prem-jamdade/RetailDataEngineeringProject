"""Batch load raw, clean, and rejected retail sales records into Oracle Staging."""

from __future__ import annotations

import logging
from pathlib import Path

import oracledb
import pandas as pd

from src.db_oracle import OracleClient

RAW_INSERT_SQL = """
    INSERT INTO STG_SALES_RAW (
        RUN_ID, SOURCE_TYPE, SOURCE_FILE, TRANSACTION_ID, SALE_TIMESTAMP_RAW,
        STORE_ID_RAW, STORE_NAME_RAW, STORE_CITY_RAW, STORE_REGION_RAW,
        CUSTOMER_ID_RAW, CUSTOMER_NAME_RAW, CUSTOMER_EMAIL_RAW, CUSTOMER_SEGMENT_RAW,
        PRODUCT_ID_RAW, PRODUCT_NAME_RAW, CATEGORY_RAW, SUBCATEGORY_RAW,
        UNIT_COST_RAW, UNIT_PRICE_RAW, QUANTITY_RAW, DISCOUNT_PCT_RAW, PAYMENT_METHOD_RAW
    ) VALUES (
        :1, :2, :3, :4, :5,
        :6, :7, :8, :9,
        :10, :11, :12, :13,
        :14, :15, :16, :17,
        :18, :19, :20, :21, :22
    )
"""

CLEAN_INSERT_SQL = """
    INSERT INTO STG_SALES_CLEAN (
        RUN_ID, TRANSACTION_ID, SALE_DATE, SALE_TIMESTAMP,
        STORE_CODE, STORE_NAME, STORE_CITY, STORE_REGION,
        CUSTOMER_CODE, CUSTOMER_NAME, CUSTOMER_EMAIL, CUSTOMER_SEGMENT,
        PRODUCT_CODE, PRODUCT_NAME, CATEGORY, SUBCATEGORY,
        UNIT_COST, UNIT_PRICE, QUANTITY, DISCOUNT_PCT,
        GROSS_AMOUNT, DISCOUNT_AMOUNT, NET_AMOUNT, COST_AMOUNT, PROFIT_AMOUNT,
        PAYMENT_METHOD, RECORD_HASH, SOURCE_TYPE, SOURCE_FILE
    ) VALUES (
        :1, :2, :3, :4,
        :5, :6, :7, :8,
        :9, :10, :11, :12,
        :13, :14, :15, :16,
        :17, :18, :19, :20,
        :21, :22, :23, :24, :25,
        :26, :27, :28, :29
    )
"""

REJECT_INSERT_SQL = """
    INSERT INTO STG_SALES_REJECTS (
        RUN_ID, SOURCE_TYPE, SOURCE_FILE, TRANSACTION_ID, RAW_PAYLOAD, REJECTION_REASON
    ) VALUES (
        :1, :2, :3, :4, :5, :6
    )
"""


def _s(val: object, limit: int) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    text = str(val)
    return text[:limit] if text != "" else None


def load_raw_staging(
    client: OracleClient,
    conn: oracledb.Connection,
    run_id: int,
    raw_df: pd.DataFrame,
    batch_size: int,
    logger: logging.Logger,
) -> int:
    """Batch-insert extracted raw records into STG_SALES_RAW."""
    if raw_df.empty:
        return 0
    rows = [
        (
            run_id,
            _s(r["source_type"], 20) or "CSV",
            _s(r["source_file"], 255) or "UNKNOWN",
            _s(r["transaction_id"], 100),
            _s(r["sale_timestamp"], 100),
            _s(r["store_id"], 50),
            _s(r["store_name"], 150),
            _s(r["store_city"], 100),
            _s(r["store_region"], 100),
            _s(r["customer_id"], 50),
            _s(r["customer_name"], 150),
            _s(r["customer_email"], 150),
            _s(r["customer_segment"], 50),
            _s(r["product_id"], 50),
            _s(r["product_name"], 200),
            _s(r["category"], 100),
            _s(r["subcategory"], 100),
            _s(r["unit_cost"], 50),
            _s(r["unit_price"], 50),
            _s(r["quantity"], 50),
            _s(r["discount_pct"], 50),
            _s(r["payment_method"], 50),
        )
        for r in raw_df.to_dict(orient="records")
    ]
    loaded = client.batch_insert(conn, RAW_INSERT_SQL, rows, batch_size=batch_size)
    logger.info("Loaded %d rows into STG_SALES_RAW (run_id=%d)", loaded, run_id)
    return loaded


def load_clean_staging(
    client: OracleClient,
    conn: oracledb.Connection,
    run_id: int,
    clean_df: pd.DataFrame,
    batch_size: int,
    logger: logging.Logger,
) -> int:
    """Batch-insert validated records into STG_SALES_CLEAN."""
    if clean_df.empty:
        return 0
    rows = [
        (
            run_id,
            r["transaction_id"],
            r["sale_date"],
            r["sale_timestamp"],
            r["store_code"],
            r["store_name"],
            r["store_city"],
            r["store_region"],
            r["customer_code"],
            r["customer_name"],
            r["customer_email"],
            r["customer_segment"],
            r["product_code"],
            r["product_name"],
            r["category"],
            r["subcategory"],
            float(r["unit_cost"]),
            float(r["unit_price"]),
            int(r["quantity"]),
            float(r["discount_pct"]),
            float(r["gross_amount"]),
            float(r["discount_amount"]),
            float(r["net_amount"]),
            float(r["cost_amount"]),
            float(r["profit_amount"]),
            r["payment_method"],
            r["record_hash"],
            r["source_type"],
            r["source_file"],
        )
        for r in clean_df.to_dict(orient="records")
    ]
    loaded = client.batch_insert(conn, CLEAN_INSERT_SQL, rows, batch_size=batch_size)
    logger.info("Loaded %d rows into STG_SALES_CLEAN (run_id=%d)", loaded, run_id)
    return loaded


def load_rejects_staging(
    client: OracleClient,
    conn: oracledb.Connection,
    run_id: int,
    reject_df: pd.DataFrame,
    batch_size: int,
    rejected_dir: Path,
    logger: logging.Logger,
) -> int:
    """Batch-insert quarantined invalid records into STG_SALES_REJECTS and CSV audit."""
    if reject_df.empty:
        return 0

    rows = [
        (
            run_id,
            _s(r["source_type"], 20) or "UNKNOWN",
            _s(r["source_file"], 255) or "UNKNOWN",
            _s(r["transaction_id"], 100),
            str(r["raw_payload"]),
            _s(r["rejection_reason"], 500) or "VALIDATION_ERROR",
        )
        for r in reject_df.to_dict(orient="records")
    ]
    loaded = client.batch_insert(conn, REJECT_INSERT_SQL, rows, batch_size=batch_size)

    rejected_dir.mkdir(parents=True, exist_ok=True)
    export_path = rejected_dir / f"rejects_run_{run_id}.csv"
    reject_df.to_csv(export_path, index=False)
    logger.warning(
        "Quarantined %d invalid rows into STG_SALES_REJECTS and %s",
        loaded,
        export_path.name,
    )
    return loaded
