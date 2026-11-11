"""End-to-end Retail Sales ETL Pipeline Orchestrator.

Stages:
1. Extract (CSV multi-store files + REST Sales API via requests, incremental via watermark)
2. Clean, EDA Profile & Validate (Pandas standardization, deduplication, business rules)
3. Load Oracle Staging (batch inserts into STG_SALES_RAW, STG_SALES_CLEAN, STG_SALES_REJECTS)
4. Oracle PL/SQL Transformation (PKG_RETAIL_ETL: CTE + MERGE into Star Schema)
5. Source-to-Target Reconciliation (Python + Oracle SQL audit checks)
"""

from __future__ import annotations

import argparse
import shutil
import uuid
from dataclasses import dataclass

from config.settings import PipelineConfig, get_oracle_config, get_pipeline_config
from src.clean_validate import clean_and_validate
from src.db_oracle import OracleClient
from src.eda_report import write_eda_report
from src.extract import extract_all_sources
from src.load_staging import load_clean_staging, load_raw_staging, load_rejects_staging
from src.logger import get_logger
from src.reconcile import run_source_to_target_reconciliation
from src.sales_api_server import start_api_server_in_thread


@dataclass
class PipelineSummary:
    """Structured outcome returned by `run_pipeline`."""

    run_id: int
    status: str
    raw_rows: int
    clean_rows: int
    reject_rows: int
    duplicates_removed: int
    dwh_inserted: int
    dwh_updated: int
    reconciliation_passed: bool
    eda_report_path: str | None


def run_pipeline(
    start_local_api: bool = False,
    archive_processed: bool = False,
    pipeline_cfg: PipelineConfig | None = None,
) -> PipelineSummary:
    """Execute one full incremental run of the Retail Sales ETL pipeline."""
    p_cfg = pipeline_cfg or get_pipeline_config()
    o_cfg = get_oracle_config()
    logger = get_logger("retail_etl", logs_dir=p_cfg.logs_dir, level=p_cfg.log_level)

    api_server = None
    if start_local_api and p_cfg.enable_api_extract:
        api_server, _ = start_api_server_in_thread(api_feed_dir=p_cfg.api_feed_dir)
        logger.info("Started embedded Sales API server at %s", p_cfg.sales_api_url)

    client = OracleClient(o_cfg, logger)
    run_uuid = str(uuid.uuid4())
    run_id = -1

    try:
        with client.connection() as conn:
            run_id = client.start_pipeline_run(conn, run_uuid)
            logger.info("Pipeline run started | run_id=%d | uuid=%s", run_id, run_uuid)

            watermarks = client.get_watermarks(conn)
            extraction = extract_all_sources(
                raw_dir=p_cfg.raw_dir,
                existing_watermarks=watermarks,
                logger=logger,
                enable_api=p_cfg.enable_api_extract,
                api_url=p_cfg.sales_api_url,
                api_timeout=p_cfg.api_timeout_seconds,
            )

            if extraction.raw_df.empty:
                client.update_run_metrics(
                    conn,
                    run_id=run_id,
                    csv_files=0,
                    api_batches=0,
                    raw_count=0,
                    clean_count=0,
                    reject_count=0,
                )
                client.finish_pipeline_run(conn, run_id, status="SUCCESS")
                logger.info(
                    "No new or modified sources found (skipped=%d). Run #%d finished cleanly.",
                    extraction.skipped_sources,
                    run_id,
                )
                return PipelineSummary(
                    run_id=run_id,
                    status="SUCCESS",
                    raw_rows=0,
                    clean_rows=0,
                    reject_rows=0,
                    duplicates_removed=0,
                    dwh_inserted=0,
                    dwh_updated=0,
                    reconciliation_passed=True,
                    eda_report_path=None,
                )

            cleaning = clean_and_validate(extraction.raw_df, logger)
            eda_path = write_eda_report(
                reports_dir=p_cfg.reports_dir,
                run_id=run_id,
                eda_summary=cleaning.eda_summary,
                clean_df=cleaning.clean_df,
                reject_df=cleaning.reject_df,
                duplicates_removed=cleaning.duplicates_removed,
            )
            logger.info("EDA report written to %s", eda_path)

            raw_cnt = load_raw_staging(
                client, conn, run_id, extraction.raw_df, p_cfg.batch_size, logger
            )
            clean_cnt = load_clean_staging(
                client, conn, run_id, cleaning.clean_df, p_cfg.batch_size, logger
            )
            reject_cnt = load_rejects_staging(
                client,
                conn,
                run_id,
                cleaning.reject_df,
                p_cfg.batch_size,
                p_cfg.rejected_dir,
                logger,
            )

            client.update_run_metrics(
                conn,
                run_id=run_id,
                csv_files=extraction.csv_files_read,
                api_batches=extraction.api_batches_read,
                raw_count=raw_cnt,
                clean_count=clean_cnt,
                reject_count=reject_cnt,
            )

            dwh_inserted, dwh_updated = client.call_pkg_process_run(conn, run_id)

            recon = run_source_to_target_reconciliation(
                conn=conn,
                run_id=run_id,
                raw_df=extraction.raw_df,
                clean_df=cleaning.clean_df,
                reject_df=cleaning.reject_df,
                duplicates_removed=cleaning.duplicates_removed,
                logger=logger,
            )

            wm_payload = [
                (s.source_name, s.source_type, s.content_hash, s.row_count)
                for s in extraction.sources
            ]
            client.upsert_watermarks(conn, run_id, wm_payload)

            if archive_processed:
                for src in extraction.sources:
                    if src.source_type == "CSV":
                        src_path = p_cfg.raw_dir / src.source_name
                        if src_path.exists():
                            shutil.move(str(src_path), str(p_cfg.archive_dir / src_path.name))

            final_status = "SUCCESS" if recon.passed else "PARTIAL"
            client.finish_pipeline_run(
                conn,
                run_id,
                status=final_status,
                error_message=None if recon.passed else "One or more reconciliation checks failed.",
            )
            logger.info(
                "Pipeline run #%d finished with status=%s (inserted=%d, updated=%d, recon_passed=%s)",
                run_id,
                final_status,
                dwh_inserted,
                dwh_updated,
                recon.passed,
            )

            return PipelineSummary(
                run_id=run_id,
                status=final_status,
                raw_rows=raw_cnt,
                clean_rows=clean_cnt,
                reject_rows=reject_cnt,
                duplicates_removed=cleaning.duplicates_removed,
                dwh_inserted=dwh_inserted,
                dwh_updated=dwh_updated,
                reconciliation_passed=recon.passed,
                eda_report_path=str(eda_path),
            )

    except Exception as exc:
        logger.exception("Pipeline run failed: %s", exc)
        if run_id > 0:
            try:
                with client.connection() as err_conn:
                    client.finish_pipeline_run(err_conn, run_id, status="FAILED", error_message=str(exc))
            except Exception:
                logger.exception("Failed to record FAILED status in ETL_PIPELINE_RUNS.")
        raise
    finally:
        if api_server is not None:
            api_server.shutdown()
            api_server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Retail Sales ETL pipeline.")
    parser.add_argument(
        "--start-api",
        action="store_true",
        help="Start embedded local Sales REST API simulator for API extraction.",
    )
    parser.add_argument(
        "--archive-csv",
        action="store_true",
        help="Move processed CSV files from data/raw to data/archive after success.",
    )
    args = parser.parse_args()
    summary = run_pipeline(
        start_local_api=args.start_api,
        archive_processed=args.archive_csv,
    )
    print(
        f"Run #{summary.run_id} | status={summary.status} | raw={summary.raw_rows} "
        f"| clean={summary.clean_rows} | rejects={summary.reject_rows} "
        f"| dedup={summary.duplicates_removed} | inserted={summary.dwh_inserted} "
        f"| updated={summary.dwh_updated} | recon_passed={summary.reconciliation_passed}"
    )


if __name__ == "__main__":
    main()
