"""Automated scheduler for recurring Retail Sales ETL pipeline execution."""

from __future__ import annotations

import argparse
import time

import schedule

from config.settings import get_pipeline_config
from src.logger import get_logger
from src.pipeline import run_pipeline


def _job(start_api: bool) -> None:
    p_cfg = get_pipeline_config()
    logger = get_logger("retail_scheduler", logs_dir=p_cfg.logs_dir, level=p_cfg.log_level)
    logger.info("Scheduled ETL execution triggered.")
    try:
        summary = run_pipeline(start_local_api=start_api)
        logger.info(
            "Scheduled run #%d completed | status=%s | inserted=%d | updated=%d",
            summary.run_id,
            summary.status,
            summary.dwh_inserted,
            summary.dwh_updated,
        )
    except Exception as exc:
        logger.exception("Scheduled ETL run failed: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Schedule automated Retail Sales ETL runs.")
    parser.add_argument("--daily-at", default=None, help="Run daily at HH:MM (24h format, e.g., 02:00)")
    parser.add_argument("--every-minutes", type=int, default=60, help="Interval in minutes if --daily-at not set")
    parser.add_argument("--start-api", action="store_true", help="Use embedded Sales API simulator")
    parser.add_argument("--run-once", action="store_true", help="Run immediately once and exit")
    args = parser.parse_args()

    if args.run_once:
        _job(start_api=args.start_api)
        return

    if args.daily_at:
        schedule.every().day.at(args.daily_at).do(_job, start_api=args.start_api)
        print(f"Scheduler started: running daily at {args.daily_at}. Press Ctrl+C to exit.")
    else:
        schedule.every(args.every_minutes).minutes.do(_job, start_api=args.start_api)
        print(f"Scheduler started: running every {args.every_minutes} minute(s). Press Ctrl+C to exit.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(5)
    except KeyboardInterrupt:
        print("Scheduler stopped.")


if __name__ == "__main__":
    main()
