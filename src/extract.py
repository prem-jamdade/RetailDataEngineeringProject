"""Data extraction module for multi-store CSV files and Sales REST API."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import requests

EXPECTED_COLUMNS = [
    "transaction_id",
    "sale_timestamp",
    "store_id",
    "store_name",
    "store_city",
    "store_region",
    "customer_id",
    "customer_name",
    "customer_email",
    "customer_segment",
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "unit_cost",
    "unit_price",
    "quantity",
    "discount_pct",
    "payment_method",
]


@dataclass
class SourceBatchMetadata:
    """Metadata used to update ETL_FILE_WATERMARK after successful ingestion."""

    source_name: str
    source_type: str
    content_hash: str
    row_count: int


@dataclass
class ExtractionResult:
    """Container for extracted raw records and incremental watermark metadata."""

    raw_df: pd.DataFrame
    sources: list[SourceBatchMetadata] = field(default_factory=list)
    csv_files_read: int = 0
    api_batches_read: int = 0
    skipped_sources: int = 0


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[EXPECTED_COLUMNS]


def extract_csv_sources(
    raw_dir: Path,
    existing_watermarks: dict[str, str],
    logger: logging.Logger,
) -> tuple[list[pd.DataFrame], list[SourceBatchMetadata], int, int]:
    """Extract new or modified CSV files from `raw_dir` using Pandas."""
    frames: list[pd.DataFrame] = []
    metadata: list[SourceBatchMetadata] = []
    read_count = 0
    skipped_count = 0

    csv_files = sorted(raw_dir.glob("*.csv"))
    for csv_path in csv_files:
        file_hash = _sha256_bytes(csv_path.read_bytes())
        if existing_watermarks.get(csv_path.name) == file_hash:
            skipped_count += 1
            logger.debug("Skipping already processed CSV: %s", csv_path.name)
            continue

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df = _normalize_columns(df)
        df["source_type"] = "CSV"
        df["source_file"] = csv_path.name
        frames.append(df)
        metadata.append(
            SourceBatchMetadata(
                source_name=csv_path.name,
                source_type="CSV",
                content_hash=file_hash,
                row_count=len(df),
            )
        )
        read_count += 1
        logger.info("Extracted CSV '%s' (%d rows)", csv_path.name, len(df))

    return frames, metadata, read_count, skipped_count


def extract_api_sources(
    api_url: str,
    timeout_seconds: int,
    existing_watermarks: dict[str, str],
    logger: logging.Logger,
) -> tuple[list[pd.DataFrame], list[SourceBatchMetadata], int, int]:
    """Extract new or modified sales batches from the Sales REST API."""
    frames: list[pd.DataFrame] = []
    metadata: list[SourceBatchMetadata] = []
    read_count = 0
    skipped_count = 0

    response = requests.get(api_url, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()

    for batch in payload.get("batches", []):
        batch_name = batch.get("batch_name", "api_batch")
        records = batch.get("records", [])
        canonical_bytes = json.dumps(records, sort_keys=True).encode("utf-8")
        batch_hash = _sha256_bytes(canonical_bytes)

        if existing_watermarks.get(batch_name) == batch_hash:
            skipped_count += 1
            logger.debug("Skipping already processed API batch: %s", batch_name)
            continue

        df = pd.DataFrame(records, dtype=str).fillna("")
        df = _normalize_columns(df)
        df["source_type"] = "API"
        df["source_file"] = batch_name
        frames.append(df)
        metadata.append(
            SourceBatchMetadata(
                source_name=batch_name,
                source_type="API",
                content_hash=batch_hash,
                row_count=len(df),
            )
        )
        read_count += 1
        logger.info("Extracted API batch '%s' (%d rows)", batch_name, len(df))

    return frames, metadata, read_count, skipped_count


def extract_all_sources(
    raw_dir: Path,
    existing_watermarks: dict[str, str],
    logger: logging.Logger,
    enable_api: bool = True,
    api_url: str | None = None,
    api_timeout: int = 10,
) -> ExtractionResult:
    """Extract all incremental CSV and API sales data into a unified raw DataFrame."""
    all_frames: list[pd.DataFrame] = []
    all_meta: list[SourceBatchMetadata] = []

    csv_frames, csv_meta, csv_read, csv_skip = extract_csv_sources(
        raw_dir=raw_dir,
        existing_watermarks=existing_watermarks,
        logger=logger,
    )
    all_frames.extend(csv_frames)
    all_meta.extend(csv_meta)

    api_read = 0
    api_skip = 0
    if enable_api and api_url:
        try:
            api_frames, api_meta, api_read, api_skip = extract_api_sources(
                api_url=api_url,
                timeout_seconds=api_timeout,
                existing_watermarks=existing_watermarks,
                logger=logger,
            )
            all_frames.extend(api_frames)
            all_meta.extend(api_meta)
        except requests.RequestException as exc:
            logger.warning("Sales API unreachable (%s); proceeding with CSV sources only.", exc)

    if all_frames:
        raw_df = pd.concat(all_frames, ignore_index=True)
    else:
        raw_df = pd.DataFrame(columns=[*EXPECTED_COLUMNS, "source_type", "source_file"])

    return ExtractionResult(
        raw_df=raw_df,
        sources=all_meta,
        csv_files_read=csv_read,
        api_batches_read=api_read,
        skipped_sources=csv_skip + api_skip,
    )
