"""Centralized settings loader for Oracle Database and ETL pipeline paths."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class OracleConfig:
    """Oracle Database connection parameters."""

    user: str
    password: str
    dsn: str


@dataclass(frozen=True)
class PipelineConfig:
    """ETL paths and execution settings."""

    project_root: Path
    raw_dir: Path
    api_feed_dir: Path
    archive_dir: Path
    rejected_dir: Path
    reports_dir: Path
    logs_dir: Path
    sql_dir: Path
    batch_size: int
    enable_api_extract: bool
    sales_api_url: str
    api_timeout_seconds: int
    log_level: str


def get_oracle_config() -> OracleConfig:
    """Load Oracle configuration from environment variables."""
    return OracleConfig(
        user=os.getenv("ORACLE_USER", "RETAIL_DW"),
        password=os.getenv("ORACLE_PASSWORD", ""),
        dsn=os.getenv("ORACLE_DSN", "localhost:1521/freepdb1"),
    )


def get_pipeline_config() -> PipelineConfig:
    """Load pipeline directory paths and runtime options."""
    data_dir = PROJECT_ROOT / "data"
    cfg = PipelineConfig(
        project_root=PROJECT_ROOT,
        raw_dir=data_dir / "raw",
        api_feed_dir=data_dir / "api_feed",
        archive_dir=data_dir / "archive",
        rejected_dir=data_dir / "rejected",
        reports_dir=data_dir / "reports",
        logs_dir=PROJECT_ROOT / "logs",
        sql_dir=PROJECT_ROOT / "sql",
        batch_size=int(os.getenv("ETL_BATCH_SIZE", "500")),
        enable_api_extract=os.getenv("ENABLE_API_EXTRACT", "true").lower() == "true",
        sales_api_url=os.getenv("SALES_API_URL", "http://127.0.0.1:8765/api/v1/sales"),
        api_timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "10")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
    for folder in (
        cfg.raw_dir,
        cfg.api_feed_dir,
        cfg.archive_dir,
        cfg.rejected_dir,
        cfg.reports_dir,
        cfg.logs_dir,
    ):
        folder.mkdir(parents=True, exist_ok=True)
    return cfg
