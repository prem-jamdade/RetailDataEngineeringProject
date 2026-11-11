"""Bootstrap Oracle FREEPDB1 schema, staging tables, Star Schema, PL/SQL package, and views."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_oracle_config, get_pipeline_config
from src.db_oracle import OracleClient
from src.logger import get_logger


def _run_sqlplus(connect_str: str, sql_text: str) -> None:
    proc = subprocess.run(
        ["sqlplus", "-S", connect_str],
        input=sql_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.stdout.strip():
        print(proc.stdout.strip())
    if proc.stderr.strip():
        print(proc.stderr.strip(), file=sys.stderr)
    if proc.returncode != 0:
        raise RuntimeError(f"sqlplus failed with exit code {proc.returncode}")


def _reset_schema_objects(connect_str: str) -> None:
    drop_plsql = """
    WHENEVER SQLERROR EXIT SQL.SQLCODE
    SET SERVEROUTPUT ON
    BEGIN
        FOR obj IN (
            SELECT object_name, object_type
            FROM user_objects
            WHERE object_type IN ('VIEW', 'PACKAGE', 'TABLE')
            ORDER BY CASE object_type WHEN 'VIEW' THEN 1 WHEN 'PACKAGE' THEN 2 ELSE 3 END
        ) LOOP
            IF obj.object_type = 'TABLE' THEN
                EXECUTE IMMEDIATE 'DROP TABLE "' || obj.object_name || '" CASCADE CONSTRAINTS PURGE';
            ELSIF obj.object_type = 'PACKAGE' THEN
                EXECUTE IMMEDIATE 'DROP PACKAGE "' || obj.object_name || '"';
            ELSIF obj.object_type = 'VIEW' THEN
                EXECUTE IMMEDIATE 'DROP VIEW "' || obj.object_name || '"';
            END IF;
        END LOOP;
    END;
    /
    EXIT;
    """
    _run_sqlplus(connect_str, drop_plsql)


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize Oracle RETAIL_DW schema and objects.")
    parser.add_argument("--reset", action="store_true", help="Drop existing RETAIL_DW tables/views/packages first.")
    args = parser.parse_args()

    p_cfg = get_pipeline_config()
    o_cfg = get_oracle_config()
    logger = get_logger("setup_oracle", logs_dir=p_cfg.logs_dir, level=p_cfg.log_level)

    # 1. Ensure RETAIL_DW user exists in FREEPDB1
    setup_user_sql = (p_cfg.sql_dir / "00_setup_user.sql").read_text(encoding="utf-8")
    logger.info("Ensuring RETAIL_DW user exists in FREEPDB1...")
    _run_sqlplus("/ as sysdba", setup_user_sql)

    user_connect = f"{o_cfg.user}/{o_cfg.password}@{o_cfg.dsn}"
    if args.reset:
        logger.warning("Resetting all objects in schema %s...", o_cfg.user)
        _reset_schema_objects(user_connect)

    sql_files = [
        "01_staging_tables.sql",
        "02_dwh_star_schema.sql",
        "03_plsql_etl_package.sql",
        "04_reporting_views.sql",
    ]
    for fname in sql_files:
        fpath = p_cfg.sql_dir / fname
        logger.info("Executing %s...", fname)
        script = "WHENEVER SQLERROR EXIT SQL.SQLCODE\nSET DEFINE OFF\n" + fpath.read_text(encoding="utf-8") + "\nEXIT;\n"
        _run_sqlplus(user_connect, script)

    # Verify via python-oracledb that all objects are VALID
    client = OracleClient(o_cfg, logger)
    with client.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_type, object_name, status
                FROM user_objects
                WHERE object_type IN ('TABLE', 'PACKAGE', 'PACKAGE BODY', 'VIEW')
                ORDER BY object_type, object_name
                """
            )
            objects = cur.fetchall()
            invalid = [o for o in objects if o[2] != "VALID"]
            logger.info("Verified %d schema objects in %s (invalid=%d)", len(objects), o_cfg.user, len(invalid))
            for obj_type, obj_name, status in objects:
                print(f"  {obj_type:<14} {obj_name:<28} {status}")
            if invalid:
                raise RuntimeError(f"Found invalid Oracle objects: {invalid}")


if __name__ == "__main__":
    main()
