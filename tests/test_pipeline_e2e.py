"""End-to-end integration test against Oracle FREEPDB1 (RETAIL_DW schema)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.settings import PipelineConfig, get_oracle_config
from src.db_oracle import OracleClient
from src.pipeline import run_pipeline


def _make_cfg(tmp_path: Path) -> PipelineConfig:
    raw_dir = tmp_path / "raw"
    api_dir = tmp_path / "api"
    archive_dir = tmp_path / "archive"
    rejected_dir = tmp_path / "rejected"
    reports_dir = tmp_path / "reports"
    logs_dir = tmp_path / "logs"
    for d in (raw_dir, api_dir, archive_dir, rejected_dir, reports_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)
    return PipelineConfig(
        project_root=tmp_path,
        raw_dir=raw_dir,
        api_feed_dir=api_dir,
        archive_dir=archive_dir,
        rejected_dir=rejected_dir,
        reports_dir=reports_dir,
        logs_dir=logs_dir,
        sql_dir=Path(__file__).resolve().parent.parent / "sql",
        batch_size=100,
        enable_api_extract=False,
        sales_api_url="",
        api_timeout_seconds=5,
        log_level="INFO",
    )


def _cleanup_e2e_rows(client: OracleClient, source_name: str, txn_prefix: str) -> None:
    with client.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM FACT_SALES WHERE TRANSACTION_ID LIKE :1", [f"{txn_prefix}%"])
            cur.execute("DELETE FROM STG_SALES_CLEAN WHERE SOURCE_FILE = :1", [source_name])
            cur.execute("DELETE FROM STG_SALES_RAW WHERE SOURCE_FILE = :1", [source_name])
            cur.execute("DELETE FROM STG_SALES_REJECTS WHERE SOURCE_FILE = :1", [source_name])
            cur.execute("DELETE FROM ETL_FILE_WATERMARK WHERE SOURCE_NAME = :1", [source_name])
        conn.commit()


def test_e2e_oracle_insert_skip_and_incremental_update(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    source_name = "e2e_test_batch.csv"
    txn_prefix = "E2E-TEST-"
    csv_path = cfg.raw_dir / source_name

    o_cfg = get_oracle_config()
    client = OracleClient(o_cfg, logging.getLogger("test_e2e"))
    _cleanup_e2e_rows(client, source_name, txn_prefix)

    try:
        # 1. Initial batch: 1 valid row + 1 invalid row
        pd.DataFrame([
            {
                "transaction_id": f"{txn_prefix}001",
                "sale_timestamp": "2026-11-05 14:00:00",
                "store_id": "ST101",
                "store_name": "Mumbai Flagship Store",
                "store_city": "Mumbai",
                "store_region": "West",
                "customer_id": "CU001",
                "customer_name": "Aarav Sharma",
                "customer_email": "aarav.sharma@example.com",
                "customer_segment": "Loyalty Gold",
                "product_id": "PR001",
                "product_name": "Wireless Noise-Canceling Headphones",
                "category": "Electronics",
                "subcategory": "Audio",
                "unit_cost": 85.00,
                "unit_price": 149.99,
                "quantity": 1,
                "discount_pct": 0,
                "payment_method": "UPI",
            },
            {
                "transaction_id": f"{txn_prefix}BAD",
                "sale_timestamp": "2026-11-05 14:05:00",
                "store_id": "ST101",
                "store_name": "Mumbai Flagship Store",
                "store_city": "Mumbai",
                "store_region": "West",
                "customer_id": "CU001",
                "customer_name": "Aarav Sharma",
                "customer_email": "",
                "customer_segment": "Loyalty Gold",
                "product_id": "PR001",
                "product_name": "Wireless Noise-Canceling Headphones",
                "category": "Electronics",
                "subcategory": "Audio",
                "unit_cost": 85.00,
                "unit_price": 149.99,
                "quantity": -5,
                "discount_pct": 0,
                "payment_method": "UPI",
            },
        ]).to_csv(csv_path, index=False)

        res1 = run_pipeline(start_local_api=False, pipeline_cfg=cfg)
        assert res1.status == "SUCCESS"
        assert res1.raw_rows == 2
        assert res1.clean_rows == 1
        assert res1.reject_rows == 1
        assert res1.dwh_inserted == 1
        assert res1.dwh_updated == 0
        assert res1.reconciliation_passed is True

        # 2. Re-run with unchanged file -> skipped via watermark
        res2 = run_pipeline(start_local_api=False, pipeline_cfg=cfg)
        assert res2.status == "SUCCESS"
        assert res2.raw_rows == 0

        # 3. Modify existing transaction quantity -> triggers incremental MERGE UPDATE
        pd.DataFrame([
            {
                "transaction_id": f"{txn_prefix}001",
                "sale_timestamp": "2026-11-05 14:00:00",
                "store_id": "ST101",
                "store_name": "Mumbai Flagship Store",
                "store_city": "Mumbai",
                "store_region": "West",
                "customer_id": "CU001",
                "customer_name": "Aarav Sharma",
                "customer_email": "aarav.sharma@example.com",
                "customer_segment": "Loyalty Gold",
                "product_id": "PR001",
                "product_name": "Wireless Noise-Canceling Headphones",
                "category": "Electronics",
                "subcategory": "Audio",
                "unit_cost": 85.00,
                "unit_price": 149.99,
                "quantity": 3,
                "discount_pct": 10,
                "payment_method": "UPI",
            }
        ]).to_csv(csv_path, index=False)

        res3 = run_pipeline(start_local_api=False, pipeline_cfg=cfg)
        assert res3.status == "SUCCESS"
        assert res3.dwh_inserted == 0
        assert res3.dwh_updated == 1
        assert res3.reconciliation_passed is True

        with client.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT QUANTITY, NET_AMOUNT FROM FACT_SALES WHERE TRANSACTION_ID = :1",
                    [f"{txn_prefix}001"],
                )
                qty, net_amt = cur.fetchone()
                assert int(qty) == 3
                assert round(float(net_amt), 2) == round(149.99 * 3 * 0.9, 2)
    finally:
        _cleanup_e2e_rows(client, source_name, txn_prefix)
