"""Source-to-target reconciliation between Python source frames and Oracle DWH."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import oracledb
import pandas as pd


@dataclass
class ReconciliationResult:
    """Result of end-to-end source-to-target audit checks."""

    passed: bool
    checks: list[dict]


def run_source_to_target_reconciliation(
    conn: oracledb.Connection,
    run_id: int,
    raw_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    reject_df: pd.DataFrame,
    duplicates_removed: int,
    logger: logging.Logger,
) -> ReconciliationResult:
    """Compare source DataFrames against Oracle staging and FACT_SALES tables."""
    src_raw_cnt = int(len(raw_df))
    src_clean_cnt = int(len(clean_df))
    src_reject_cnt = int(len(reject_df))
    src_net_rev = round(float(clean_df["net_amount"].sum()), 2) if not clean_df.empty else 0.0

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM STG_SALES_RAW WHERE RUN_ID = :1", [run_id])
        stg_raw_cnt = int(cur.fetchone()[0])

        cur.execute(
            "SELECT COUNT(*), NVL(SUM(NET_AMOUNT), 0) FROM STG_SALES_CLEAN WHERE RUN_ID = :1",
            [run_id],
        )
        row_clean = cur.fetchone()
        stg_clean_cnt = int(row_clean[0])
        stg_net_rev = round(float(row_clean[1]), 2)

        cur.execute("SELECT COUNT(*) FROM STG_SALES_REJECTS WHERE RUN_ID = :1", [run_id])
        stg_reject_cnt = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*), NVL(SUM(NET_AMOUNT), 0)
            FROM FACT_SALES
            WHERE TRANSACTION_ID IN (
                SELECT DISTINCT TRANSACTION_ID
                FROM STG_SALES_CLEAN
                WHERE RUN_ID = :1
            )
            """,
            [run_id],
        )
        row_fact = cur.fetchone()
        dwh_fact_cnt = int(row_fact[0])
        dwh_net_rev = round(float(row_fact[1]), 2)

    py_checks = [
        (
            "PY_RAW_LANDING_RECON",
            src_raw_cnt,
            stg_raw_cnt,
            stg_raw_cnt,
            src_raw_cnt - stg_raw_cnt,
            "PASS" if src_raw_cnt == stg_raw_cnt else "FAIL",
            "Source raw rows extracted vs STG_SALES_RAW rows",
        ),
        (
            "PY_ROW_ACCOUNTING_RECON",
            src_raw_cnt,
            stg_clean_cnt + stg_reject_cnt + duplicates_removed,
            dwh_fact_cnt + stg_reject_cnt + duplicates_removed,
            src_raw_cnt - (stg_clean_cnt + stg_reject_cnt + duplicates_removed),
            "PASS" if src_raw_cnt == (stg_clean_cnt + stg_reject_cnt + duplicates_removed) else "FAIL",
            f"Raw = Clean ({stg_clean_cnt}) + Rejects ({stg_reject_cnt}) + Dedup ({duplicates_removed})",
        ),
        (
            "PY_NET_REVENUE_E2E_RECON",
            src_net_rev,
            stg_net_rev,
            dwh_net_rev,
            round(src_net_rev - dwh_net_rev, 2),
            "PASS" if abs(src_net_rev - stg_net_rev) < 0.01 and abs(src_net_rev - dwh_net_rev) < 0.01 else "FAIL",
            "Pandas clean net revenue vs STG_SALES_CLEAN vs FACT_SALES",
        ),
    ]

    insert_sql = """
        INSERT INTO ETL_RECONCILIATION_LOG (
            RUN_ID, CHECK_NAME, SOURCE_VALUE, STAGING_VALUE, DWH_VALUE, VARIANCE, STATUS, DETAILS
        ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
    """
    with conn.cursor() as cur:
        cur.executemany(
            insert_sql,
            [(run_id, *chk) for chk in py_checks],
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT CHECK_NAME, SOURCE_VALUE, STAGING_VALUE, DWH_VALUE, VARIANCE, STATUS, DETAILS
            FROM ETL_RECONCILIATION_LOG
            WHERE RUN_ID = :1
            ORDER BY RECON_ID
            """,
            [run_id],
        )
        all_checks = [
            {
                "check_name": r[0],
                "source_value": float(r[1]) if r[1] is not None else None,
                "staging_value": float(r[2]) if r[2] is not None else None,
                "dwh_value": float(r[3]) if r[3] is not None else None,
                "variance": float(r[4]) if r[4] is not None else None,
                "status": r[5],
                "details": r[6],
            }
            for r in cur.fetchall()
        ]

    all_passed = all(c["status"] == "PASS" for c in all_checks)
    for chk in all_checks:
        log_fn = logger.info if chk["status"] == "PASS" else logger.error
        log_fn(
            "Reconciliation [%s] %s | src=%s stg=%s dwh=%s var=%s",
            chk["status"],
            chk["check_name"],
            chk["source_value"],
            chk["staging_value"],
            chk["dwh_value"],
            chk["variance"],
        )

    return ReconciliationResult(passed=all_passed, checks=all_checks)
