"""Unit tests for CSV and REST API extraction with incremental watermarking."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from src.extract import extract_all_sources
from src.sales_api_server import start_api_server_in_thread


def test_extract_csv_and_api_with_incremental_watermark(tmp_path: Path) -> None:
    logger = logging.getLogger("test_extract")
    raw_dir = tmp_path / "raw"
    api_dir = tmp_path / "api"
    raw_dir.mkdir()
    api_dir.mkdir()

    csv_file = raw_dir / "st101_2026-11-10.csv"
    pd.DataFrame([{"transaction_id": "TXN-1", "quantity": 2}]).to_csv(csv_file, index=False)

    api_file = api_dir / "online_sales_2026-11-10.json"
    api_file.write_text(
        json.dumps({"date": "2026-11-10", "records": [{"transaction_id": "API-1", "quantity": 1}]}),
        encoding="utf-8",
    )

    server, _ = start_api_server_in_thread(port=8769, api_feed_dir=api_dir)
    try:
        res1 = extract_all_sources(
            raw_dir=raw_dir,
            existing_watermarks={},
            logger=logger,
            enable_api=True,
            api_url="http://127.0.0.1:8769/api/v1/sales",
        )
        assert res1.csv_files_read == 1
        assert res1.api_batches_read == 1
        assert len(res1.raw_df) == 2

        watermarks = {s.source_name: s.content_hash for s in res1.sources}
        res2 = extract_all_sources(
            raw_dir=raw_dir,
            existing_watermarks=watermarks,
            logger=logger,
            enable_api=True,
            api_url="http://127.0.0.1:8769/api/v1/sales",
        )
        assert res2.raw_df.empty
        assert res2.skipped_sources == 2
    finally:
        server.shutdown()
        server.server_close()
