"""Oracle Database connectivity, batch loading, and PL/SQL execution helpers."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator, Sequence

import oracledb

from config.settings import OracleConfig


class OracleClient:
    """Thin-mode python-oracledb client for staging loads and PL/SQL transforms."""

    def __init__(self, config: OracleConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    @contextmanager
    def connection(self) -> Iterator[oracledb.Connection]:
        """Yield an Oracle connection and ensure it is cleanly closed."""
        conn = oracledb.connect(
            user=self.config.user,
            password=self.config.password,
            dsn=self.config.dsn,
        )
        try:
            yield conn
        finally:
            conn.close()

    def test_connection(self) -> str:
        """Verify connectivity and return Oracle database banner/version."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'), BANNER FROM V$VERSION FETCH FIRST 1 ROWS ONLY")
                row = cur.fetchone()
                return f"schema={row[0]} | {row[1]}"

    def get_watermarks(self, conn: oracledb.Connection) -> dict[str, str]:
        """Fetch map of SOURCE_NAME -> CONTENT_HASH from ETL_FILE_WATERMARK."""
        with conn.cursor() as cur:
            cur.execute("SELECT SOURCE_NAME, CONTENT_HASH FROM ETL_FILE_WATERMARK")
            return {row[0]: row[1] for row in cur.fetchall()}

    def start_pipeline_run(self, conn: oracledb.Connection, run_uuid: str) -> int:
        """Insert a RUNNING audit entry in ETL_PIPELINE_RUNS and return RUN_ID."""
        with conn.cursor() as cur:
            out_run_id = cur.var(oracledb.NUMBER)
            cur.execute(
                """
                INSERT INTO ETL_PIPELINE_RUNS (RUN_UUID, STATUS)
                VALUES (:run_uuid, 'RUNNING')
                RETURNING RUN_ID INTO :run_id
                """,
                {"run_uuid": run_uuid, "run_id": out_run_id},
            )
            conn.commit()
            return int(out_run_id.getvalue()[0])

    def update_run_metrics(
        self,
        conn: oracledb.Connection,
        run_id: int,
        csv_files: int,
        api_batches: int,
        raw_count: int,
        clean_count: int,
        reject_count: int,
    ) -> None:
        """Update extraction and staging counts for a pipeline run."""
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ETL_PIPELINE_RUNS
                SET SOURCE_CSV_FILES   = :csv_files,
                    SOURCE_API_BATCHES = :api_batches,
                    RAW_ROW_COUNT      = :raw_count,
                    CLEAN_ROW_COUNT    = :clean_count,
                    REJECT_ROW_COUNT   = :reject_count
                WHERE RUN_ID = :run_id
                """,
                {
                    "csv_files": csv_files,
                    "api_batches": api_batches,
                    "raw_count": raw_count,
                    "clean_count": clean_count,
                    "reject_count": reject_count,
                    "run_id": run_id,
                },
            )
            conn.commit()

    def finish_pipeline_run(
        self,
        conn: oracledb.Connection,
        run_id: int,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Finalize ETL_PIPELINE_RUNS row with final status and timestamp."""
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ETL_PIPELINE_RUNS
                SET STATUS          = :status,
                    ERROR_MESSAGE   = :err_msg,
                    RUN_FINISHED_AT = SYSTIMESTAMP
                WHERE RUN_ID = :run_id
                """,
                {
                    "status": status,
                    "err_msg": error_message[:4000] if error_message else None,
                    "run_id": run_id,
                },
            )
            conn.commit()

    def batch_insert(
        self,
        conn: oracledb.Connection,
        sql: str,
        rows: Sequence[tuple],
        batch_size: int = 500,
    ) -> int:
        """Execute chunked batch inserts via executemany."""
        if not rows:
            return 0
        total = 0
        with conn.cursor() as cur:
            for start in range(0, len(rows), batch_size):
                chunk = rows[start : start + batch_size]
                cur.executemany(sql, chunk)
                total += len(chunk)
        conn.commit()
        return total

    def upsert_watermarks(
        self,
        conn: oracledb.Connection,
        run_id: int,
        sources: Sequence[tuple[str, str, str, int]],
    ) -> None:
        """Persist processed source hashes in ETL_FILE_WATERMARK via MERGE."""
        if not sources:
            return
        sql = """
            MERGE INTO ETL_FILE_WATERMARK tgt
            USING (
                SELECT :1 AS SOURCE_NAME,
                       :2 AS SOURCE_TYPE,
                       :3 AS CONTENT_HASH,
                       :4 AS ROW_COUNT,
                       :5 AS LAST_RUN_ID
                FROM DUAL
            ) src
            ON (tgt.SOURCE_NAME = src.SOURCE_NAME)
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.CONTENT_HASH = src.CONTENT_HASH,
                    tgt.ROW_COUNT    = src.ROW_COUNT,
                    tgt.LAST_RUN_ID  = src.LAST_RUN_ID,
                    tgt.PROCESSED_AT = SYSTIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_NAME, SOURCE_TYPE, CONTENT_HASH, ROW_COUNT, LAST_RUN_ID)
                VALUES (src.SOURCE_NAME, src.SOURCE_TYPE, src.CONTENT_HASH, src.ROW_COUNT, src.LAST_RUN_ID)
        """
        payload = [(name, stype, chash, rcount, run_id) for name, stype, chash, rcount in sources]
        with conn.cursor() as cur:
            cur.executemany(sql, payload)
        conn.commit()

    def call_pkg_process_run(self, conn: oracledb.Connection, run_id: int) -> tuple[int, int]:
        """Invoke PKG_RETAIL_ETL.SP_PROCESS_RUN and return (inserted, updated) counts."""
        with conn.cursor() as cur:
            out_inserted = cur.var(oracledb.NUMBER)
            out_updated = cur.var(oracledb.NUMBER)
            cur.callproc("PKG_RETAIL_ETL.SP_PROCESS_RUN", [run_id, out_inserted, out_updated])
            inserted = int(out_inserted.getvalue() or 0)
            updated = int(out_updated.getvalue() or 0)
            self.logger.info(
                "PL/SQL PKG_RETAIL_ETL.SP_PROCESS_RUN complete | run_id=%d | inserted=%d | updated=%d",
                run_id,
                inserted,
                updated,
            )
            return inserted, updated
