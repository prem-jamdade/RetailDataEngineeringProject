-- =============================================================================
-- File: 00_setup_user.sql
-- Purpose: Create dedicated RETAIL_DW schema in Oracle FREEPDB1 for the
--          Retail Sales Data Engineering & Data Warehouse project.
-- Execution: sqlplus sys/<password>@localhost:1521/freepdb1 as sysdba @00_setup_user.sql
-- =============================================================================

ALTER SESSION SET CONTAINER = FREEPDB1;

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM dba_users
    WHERE username = 'RETAIL_DW';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE USER RETAIL_DW IDENTIFIED BY "RetailDW_Local2026!"
            DEFAULT TABLESPACE USERS
            TEMPORARY TABLESPACE TEMP
            QUOTA UNLIMITED ON USERS
        ';
        DBMS_OUTPUT.PUT_LINE('User RETAIL_DW created.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('User RETAIL_DW already exists.');
    END IF;
END;
/

GRANT CONNECT, RESOURCE TO RETAIL_DW;
GRANT CREATE VIEW, CREATE PROCEDURE, CREATE SEQUENCE, CREATE TABLE TO RETAIL_DW;
GRANT UNLIMITED TABLESPACE TO RETAIL_DW;

EXIT;
