-- =============================================================================
-- File: 03_plsql_etl_package.sql
-- Schema: RETAIL_DW
-- Purpose: PL/SQL Package PKG_RETAIL_ETL for Star Schema transformations:
--          - Dynamic DIM_DATE population
--          - CTE + Window Function + MERGE upserts for Dimensions
--          - Incremental MERGE (hash-based CDC) into FACT_SALES
--          - In-database Source-to-Target reconciliation
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_RETAIL_ETL AS
    PROCEDURE SP_POPULATE_DIM_DATE(
        p_start_date IN DATE,
        p_end_date   IN DATE
    );

    PROCEDURE SP_LOAD_DIMENSIONS(
        p_run_id IN NUMBER
    );

    PROCEDURE SP_LOAD_FACT_SALES(
        p_run_id   IN  NUMBER,
        p_inserted OUT NUMBER,
        p_updated  OUT NUMBER
    );

    PROCEDURE SP_RUN_RECONCILIATION(
        p_run_id IN NUMBER
    );

    PROCEDURE SP_PROCESS_RUN(
        p_run_id   IN  NUMBER,
        p_inserted OUT NUMBER,
        p_updated  OUT NUMBER
    );
END PKG_RETAIL_ETL;
/

CREATE OR REPLACE PACKAGE BODY PKG_RETAIL_ETL AS

    -- -------------------------------------------------------------------------
    -- Populate DIM_DATE idempotently for a given date range
    -- -------------------------------------------------------------------------
    PROCEDURE SP_POPULATE_DIM_DATE(
        p_start_date IN DATE,
        p_end_date   IN DATE
    ) IS
        v_days NUMBER;
    BEGIN
        v_days := TRUNC(p_end_date) - TRUNC(p_start_date) + 1;
        IF v_days <= 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'p_end_date must be >= p_start_date');
        END IF;

        MERGE INTO DIM_DATE tgt
        USING (
            SELECT
                TO_NUMBER(TO_CHAR(cal_date, 'YYYYMMDD')) AS DATE_KEY,
                cal_date AS FULL_DATE,
                TO_NUMBER(TO_CHAR(cal_date, 'YYYY')) AS YEAR_NUM,
                TO_NUMBER(TO_CHAR(cal_date, 'Q')) AS QUARTER_NUM,
                'Q' || TO_CHAR(cal_date, 'Q') || '-' || TO_CHAR(cal_date, 'YYYY') AS QUARTER_NAME,
                TO_NUMBER(TO_CHAR(cal_date, 'MM')) AS MONTH_NUM,
                TRIM(TO_CHAR(cal_date, 'Month', 'NLS_DATE_LANGUAGE=ENGLISH')) AS MONTH_NAME,
                UPPER(TRIM(TO_CHAR(cal_date, 'Mon', 'NLS_DATE_LANGUAGE=ENGLISH'))) AS MONTH_SHORT,
                TO_CHAR(cal_date, 'YYYY-MM') AS YEAR_MONTH,
                TO_NUMBER(TO_CHAR(cal_date, 'IW')) AS WEEK_OF_YEAR,
                TO_NUMBER(TO_CHAR(cal_date, 'DD')) AS DAY_OF_MONTH,
                MOD(TRUNC(cal_date) - TRUNC(cal_date, 'IW'), 7) + 1 AS DAY_OF_WEEK,
                TRIM(TO_CHAR(cal_date, 'Day', 'NLS_DATE_LANGUAGE=ENGLISH')) AS DAY_NAME,
                CASE
                    WHEN MOD(TRUNC(cal_date) - TRUNC(cal_date, 'IW'), 7) + 1 IN (6, 7) THEN 'Y'
                    ELSE 'N'
                END AS IS_WEEKEND
            FROM (
                SELECT TRUNC(p_start_date) + LEVEL - 1 AS cal_date
                FROM DUAL
                CONNECT BY LEVEL <= v_days
            )
        ) src
        ON (tgt.DATE_KEY = src.DATE_KEY)
        WHEN NOT MATCHED THEN
            INSERT (
                DATE_KEY, FULL_DATE, YEAR_NUM, QUARTER_NUM, QUARTER_NAME,
                MONTH_NUM, MONTH_NAME, MONTH_SHORT, YEAR_MONTH,
                WEEK_OF_YEAR, DAY_OF_MONTH, DAY_OF_WEEK, DAY_NAME, IS_WEEKEND
            )
            VALUES (
                src.DATE_KEY, src.FULL_DATE, src.YEAR_NUM, src.QUARTER_NUM, src.QUARTER_NAME,
                src.MONTH_NUM, src.MONTH_NAME, src.MONTH_SHORT, src.YEAR_MONTH,
                src.WEEK_OF_YEAR, src.DAY_OF_MONTH, src.DAY_OF_WEEK, src.DAY_NAME, src.IS_WEEKEND
            );
    END SP_POPULATE_DIM_DATE;

    -- -------------------------------------------------------------------------
    -- Load/Upsert Dimension tables from STG_SALES_CLEAN using CTE + MERGE
    -- -------------------------------------------------------------------------
    PROCEDURE SP_LOAD_DIMENSIONS(
        p_run_id IN NUMBER
    ) IS
        v_min_date DATE;
        v_max_date DATE;
    BEGIN
        -- Ensure DIM_DATE covers all sale dates present in the current staging batch
        SELECT MIN(TRUNC(SALE_DATE)), MAX(TRUNC(SALE_DATE))
        INTO v_min_date, v_max_date
        FROM STG_SALES_CLEAN
        WHERE RUN_ID = p_run_id
          AND PROCESSED_FLAG = 'N';

        IF v_min_date IS NOT NULL THEN
            SP_POPULATE_DIM_DATE(v_min_date, v_max_date);
        END IF;

        -- 1. MERGE DIM_STORE (latest attribute snapshot per STORE_CODE)
        MERGE INTO DIM_STORE tgt
        USING (
            WITH ranked_stores AS (
                SELECT
                    STORE_CODE,
                    STORE_NAME,
                    STORE_CITY,
                    STORE_REGION,
                    CASE
                        WHEN UPPER(STORE_CODE) LIKE 'ONL%' OR UPPER(SOURCE_TYPE) = 'API' THEN 'ONLINE'
                        ELSE 'RETAIL'
                    END AS STORE_CHANNEL,
                    ROW_NUMBER() OVER (
                        PARTITION BY STORE_CODE
                        ORDER BY SALE_TIMESTAMP DESC, CLEAN_ID DESC
                    ) AS rn
                FROM STG_SALES_CLEAN
                WHERE RUN_ID = p_run_id
                  AND PROCESSED_FLAG = 'N'
            )
            SELECT STORE_CODE, STORE_NAME, STORE_CITY, STORE_REGION, STORE_CHANNEL
            FROM ranked_stores
            WHERE rn = 1
        ) src
        ON (tgt.STORE_CODE = src.STORE_CODE)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.STORE_NAME    = src.STORE_NAME,
                tgt.CITY          = src.STORE_CITY,
                tgt.REGION        = src.STORE_REGION,
                tgt.STORE_CHANNEL = src.STORE_CHANNEL,
                tgt.UPDATED_AT    = SYSTIMESTAMP
            WHERE tgt.STORE_NAME    <> src.STORE_NAME
               OR tgt.CITY          <> src.STORE_CITY
               OR tgt.REGION        <> src.STORE_REGION
               OR tgt.STORE_CHANNEL <> src.STORE_CHANNEL
        WHEN NOT MATCHED THEN
            INSERT (STORE_CODE, STORE_NAME, CITY, REGION, STORE_CHANNEL)
            VALUES (src.STORE_CODE, src.STORE_NAME, src.STORE_CITY, src.STORE_REGION, src.STORE_CHANNEL);

        -- 2. MERGE DIM_PRODUCT
        MERGE INTO DIM_PRODUCT tgt
        USING (
            WITH ranked_products AS (
                SELECT
                    PRODUCT_CODE,
                    PRODUCT_NAME,
                    CATEGORY,
                    SUBCATEGORY,
                    UNIT_COST,
                    UNIT_PRICE,
                    ROW_NUMBER() OVER (
                        PARTITION BY PRODUCT_CODE
                        ORDER BY SALE_TIMESTAMP DESC, CLEAN_ID DESC
                    ) AS rn
                FROM STG_SALES_CLEAN
                WHERE RUN_ID = p_run_id
                  AND PROCESSED_FLAG = 'N'
            )
            SELECT PRODUCT_CODE, PRODUCT_NAME, CATEGORY, SUBCATEGORY, UNIT_COST, UNIT_PRICE
            FROM ranked_products
            WHERE rn = 1
        ) src
        ON (tgt.PRODUCT_CODE = src.PRODUCT_CODE)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.PRODUCT_NAME       = src.PRODUCT_NAME,
                tgt.CATEGORY           = src.CATEGORY,
                tgt.SUBCATEGORY        = src.SUBCATEGORY,
                tgt.CURRENT_UNIT_COST  = src.UNIT_COST,
                tgt.CURRENT_UNIT_PRICE = src.UNIT_PRICE,
                tgt.UPDATED_AT         = SYSTIMESTAMP
            WHERE tgt.PRODUCT_NAME       <> src.PRODUCT_NAME
               OR tgt.CATEGORY           <> src.CATEGORY
               OR tgt.SUBCATEGORY        <> src.SUBCATEGORY
               OR tgt.CURRENT_UNIT_COST  <> src.UNIT_COST
               OR tgt.CURRENT_UNIT_PRICE <> src.UNIT_PRICE
        WHEN NOT MATCHED THEN
            INSERT (PRODUCT_CODE, PRODUCT_NAME, CATEGORY, SUBCATEGORY, CURRENT_UNIT_COST, CURRENT_UNIT_PRICE)
            VALUES (src.PRODUCT_CODE, src.PRODUCT_NAME, src.CATEGORY, src.SUBCATEGORY, src.UNIT_COST, src.UNIT_PRICE);

        -- 3. MERGE DIM_CUSTOMER
        MERGE INTO DIM_CUSTOMER tgt
        USING (
            WITH ranked_customers AS (
                SELECT
                    CUSTOMER_CODE,
                    CUSTOMER_NAME,
                    CUSTOMER_EMAIL,
                    CUSTOMER_SEGMENT,
                    ROW_NUMBER() OVER (
                        PARTITION BY CUSTOMER_CODE
                        ORDER BY SALE_TIMESTAMP DESC, CLEAN_ID DESC
                    ) AS rn
                FROM STG_SALES_CLEAN
                WHERE RUN_ID = p_run_id
                  AND PROCESSED_FLAG = 'N'
            )
            SELECT CUSTOMER_CODE, CUSTOMER_NAME, CUSTOMER_EMAIL, CUSTOMER_SEGMENT
            FROM ranked_customers
            WHERE rn = 1
        ) src
        ON (tgt.CUSTOMER_CODE = src.CUSTOMER_CODE)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.CUSTOMER_NAME    = src.CUSTOMER_NAME,
                tgt.EMAIL            = src.CUSTOMER_EMAIL,
                tgt.CUSTOMER_SEGMENT = src.CUSTOMER_SEGMENT,
                tgt.UPDATED_AT       = SYSTIMESTAMP
            WHERE tgt.CUSTOMER_NAME            <> src.CUSTOMER_NAME
               OR NVL(tgt.EMAIL, '~')          <> NVL(src.CUSTOMER_EMAIL, '~')
               OR tgt.CUSTOMER_SEGMENT         <> src.CUSTOMER_SEGMENT
        WHEN NOT MATCHED THEN
            INSERT (CUSTOMER_CODE, CUSTOMER_NAME, EMAIL, CUSTOMER_SEGMENT)
            VALUES (src.CUSTOMER_CODE, src.CUSTOMER_NAME, src.CUSTOMER_EMAIL, src.CUSTOMER_SEGMENT);
    END SP_LOAD_DIMENSIONS;

    -- -------------------------------------------------------------------------
    -- Incremental MERGE into FACT_SALES using hash comparison
    -- -------------------------------------------------------------------------
    PROCEDURE SP_LOAD_FACT_SALES(
        p_run_id   IN  NUMBER,
        p_inserted OUT NUMBER,
        p_updated  OUT NUMBER
    ) IS
    BEGIN
        -- Count expected inserts & hash-changed updates prior to MERGE
        WITH batch_dedup AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY s.TRANSACTION_ID
                    ORDER BY s.SALE_TIMESTAMP DESC, s.CLEAN_ID DESC
                ) AS rn
            FROM STG_SALES_CLEAN s
            WHERE s.RUN_ID = p_run_id
              AND s.PROCESSED_FLAG = 'N'
        )
        SELECT
            NVL(SUM(CASE WHEN f.TRANSACTION_ID IS NULL THEN 1 ELSE 0 END), 0),
            NVL(SUM(CASE WHEN f.TRANSACTION_ID IS NOT NULL AND f.RECORD_HASH <> b.RECORD_HASH THEN 1 ELSE 0 END), 0)
        INTO p_inserted, p_updated
        FROM batch_dedup b
        LEFT JOIN FACT_SALES f ON f.TRANSACTION_ID = b.TRANSACTION_ID
        WHERE b.rn = 1;

        MERGE INTO FACT_SALES tgt
        USING (
            WITH batch_dedup AS (
                SELECT
                    s.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.TRANSACTION_ID
                        ORDER BY s.SALE_TIMESTAMP DESC, s.CLEAN_ID DESC
                    ) AS rn
                FROM STG_SALES_CLEAN s
                WHERE s.RUN_ID = p_run_id
                  AND s.PROCESSED_FLAG = 'N'
            )
            SELECT
                b.TRANSACTION_ID,
                d.DATE_KEY,
                st.STORE_KEY,
                pr.PRODUCT_KEY,
                cu.CUSTOMER_KEY,
                b.SALE_TIMESTAMP,
                b.QUANTITY,
                b.UNIT_COST,
                b.UNIT_PRICE,
                b.DISCOUNT_PCT,
                b.GROSS_AMOUNT,
                b.DISCOUNT_AMOUNT,
                b.NET_AMOUNT,
                b.COST_AMOUNT,
                b.PROFIT_AMOUNT,
                b.PAYMENT_METHOD,
                b.SOURCE_TYPE,
                b.RECORD_HASH,
                b.RUN_ID
            FROM batch_dedup b
            INNER JOIN DIM_DATE d      ON d.FULL_DATE      = TRUNC(b.SALE_DATE)
            INNER JOIN DIM_STORE st    ON st.STORE_CODE    = b.STORE_CODE
            INNER JOIN DIM_PRODUCT pr  ON pr.PRODUCT_CODE  = b.PRODUCT_CODE
            INNER JOIN DIM_CUSTOMER cu ON cu.CUSTOMER_CODE = b.CUSTOMER_CODE
            WHERE b.rn = 1
        ) src
        ON (tgt.TRANSACTION_ID = src.TRANSACTION_ID)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.DATE_KEY        = src.DATE_KEY,
                tgt.STORE_KEY       = src.STORE_KEY,
                tgt.PRODUCT_KEY     = src.PRODUCT_KEY,
                tgt.CUSTOMER_KEY    = src.CUSTOMER_KEY,
                tgt.SALE_TIMESTAMP  = src.SALE_TIMESTAMP,
                tgt.QUANTITY        = src.QUANTITY,
                tgt.UNIT_COST       = src.UNIT_COST,
                tgt.UNIT_PRICE      = src.UNIT_PRICE,
                tgt.DISCOUNT_PCT    = src.DISCOUNT_PCT,
                tgt.GROSS_AMOUNT    = src.GROSS_AMOUNT,
                tgt.DISCOUNT_AMOUNT = src.DISCOUNT_AMOUNT,
                tgt.NET_AMOUNT      = src.NET_AMOUNT,
                tgt.COST_AMOUNT     = src.COST_AMOUNT,
                tgt.PROFIT_AMOUNT   = src.PROFIT_AMOUNT,
                tgt.PAYMENT_METHOD  = src.PAYMENT_METHOD,
                tgt.SOURCE_TYPE     = src.SOURCE_TYPE,
                tgt.RECORD_HASH     = src.RECORD_HASH,
                tgt.ETL_RUN_ID      = src.RUN_ID,
                tgt.UPDATED_AT      = SYSTIMESTAMP
            WHERE tgt.RECORD_HASH <> src.RECORD_HASH
        WHEN NOT MATCHED THEN
            INSERT (
                TRANSACTION_ID, DATE_KEY, STORE_KEY, PRODUCT_KEY, CUSTOMER_KEY,
                SALE_TIMESTAMP, QUANTITY, UNIT_COST, UNIT_PRICE, DISCOUNT_PCT,
                GROSS_AMOUNT, DISCOUNT_AMOUNT, NET_AMOUNT, COST_AMOUNT, PROFIT_AMOUNT,
                PAYMENT_METHOD, SOURCE_TYPE, RECORD_HASH, ETL_RUN_ID
            )
            VALUES (
                src.TRANSACTION_ID, src.DATE_KEY, src.STORE_KEY, src.PRODUCT_KEY, src.CUSTOMER_KEY,
                src.SALE_TIMESTAMP, src.QUANTITY, src.UNIT_COST, src.UNIT_PRICE, src.DISCOUNT_PCT,
                src.GROSS_AMOUNT, src.DISCOUNT_AMOUNT, src.NET_AMOUNT, src.COST_AMOUNT, src.PROFIT_AMOUNT,
                src.PAYMENT_METHOD, src.SOURCE_TYPE, src.RECORD_HASH, src.RUN_ID
            );

        -- Mark processed staging rows
        UPDATE STG_SALES_CLEAN
        SET PROCESSED_FLAG = 'Y'
        WHERE RUN_ID = p_run_id
          AND PROCESSED_FLAG = 'N';
    END SP_LOAD_FACT_SALES;

    -- -------------------------------------------------------------------------
    -- Source-to-Target Reconciliation inside Oracle
    -- -------------------------------------------------------------------------
    PROCEDURE SP_RUN_RECONCILIATION(
        p_run_id IN NUMBER
    ) IS
        v_clean_cnt      NUMBER;
        v_fact_run_cnt   NUMBER;
        v_clean_net_amt  NUMBER(18, 2);
        v_fact_net_amt   NUMBER(18, 2);
        v_orphan_cnt     NUMBER;
    BEGIN
        SELECT COUNT(DISTINCT TRANSACTION_ID), NVL(SUM(NET_AMOUNT), 0)
        INTO v_clean_cnt, v_clean_net_amt
        FROM (
            SELECT
                TRANSACTION_ID,
                NET_AMOUNT,
                ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID ORDER BY SALE_TIMESTAMP DESC, CLEAN_ID DESC) AS rn
            FROM STG_SALES_CLEAN
            WHERE RUN_ID = p_run_id
        )
        WHERE rn = 1;

        SELECT COUNT(*), NVL(SUM(NET_AMOUNT), 0)
        INTO v_fact_run_cnt, v_fact_net_amt
        FROM FACT_SALES
        WHERE TRANSACTION_ID IN (
            SELECT DISTINCT TRANSACTION_ID
            FROM STG_SALES_CLEAN
            WHERE RUN_ID = p_run_id
        );

        -- Check 1: Distinct clean transaction count vs FACT_SALES presence
        INSERT INTO ETL_RECONCILIATION_LOG (
            RUN_ID, CHECK_NAME, SOURCE_VALUE, STAGING_VALUE, DWH_VALUE, VARIANCE, STATUS, DETAILS
        ) VALUES (
            p_run_id,
            'SQL_TXN_COUNT_RECON',
            v_clean_cnt,
            v_clean_cnt,
            v_fact_run_cnt,
            v_clean_cnt - v_fact_run_cnt,
            CASE WHEN v_clean_cnt = v_fact_run_cnt THEN 'PASS' ELSE 'FAIL' END,
            'Distinct clean staging transactions vs FACT_SALES rows for batch'
        );

        -- Check 2: Net Revenue sum reconciliation
        INSERT INTO ETL_RECONCILIATION_LOG (
            RUN_ID, CHECK_NAME, SOURCE_VALUE, STAGING_VALUE, DWH_VALUE, VARIANCE, STATUS, DETAILS
        ) VALUES (
            p_run_id,
            'SQL_NET_REVENUE_RECON',
            v_clean_net_amt,
            v_clean_net_amt,
            v_fact_net_amt,
            v_clean_net_amt - v_fact_net_amt,
            CASE WHEN ABS(v_clean_net_amt - v_fact_net_amt) < 0.01 THEN 'PASS' ELSE 'FAIL' END,
            'Batch net revenue sum in STG_SALES_CLEAN vs FACT_SALES'
        );

        -- Check 3: Referential Integrity / Zero Orphan Keys check
        SELECT COUNT(*)
        INTO v_orphan_cnt
        FROM FACT_SALES f
        LEFT JOIN DIM_DATE d     ON f.DATE_KEY     = d.DATE_KEY
        LEFT JOIN DIM_STORE s    ON f.STORE_KEY    = s.STORE_KEY
        LEFT JOIN DIM_PRODUCT p  ON f.PRODUCT_KEY  = p.PRODUCT_KEY
        LEFT JOIN DIM_CUSTOMER c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY
        WHERE d.DATE_KEY IS NULL
           OR s.STORE_KEY IS NULL
           OR p.PRODUCT_KEY IS NULL
           OR c.CUSTOMER_KEY IS NULL;

        INSERT INTO ETL_RECONCILIATION_LOG (
            RUN_ID, CHECK_NAME, SOURCE_VALUE, STAGING_VALUE, DWH_VALUE, VARIANCE, STATUS, DETAILS
        ) VALUES (
            p_run_id,
            'SQL_STAR_INTEGRITY_CHECK',
            0,
            0,
            v_orphan_cnt,
            v_orphan_cnt,
            CASE WHEN v_orphan_cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
            'Checks that every FACT_SALES row joins cleanly to all 4 Dimensions'
        );
    END SP_RUN_RECONCILIATION;

    -- -------------------------------------------------------------------------
    -- Master ETL Run Orchestrator inside Oracle
    -- -------------------------------------------------------------------------
    PROCEDURE SP_PROCESS_RUN(
        p_run_id   IN  NUMBER,
        p_inserted OUT NUMBER,
        p_updated  OUT NUMBER
    ) IS
        v_err_msg VARCHAR2(4000);
    BEGIN
        p_inserted := 0;
        p_updated  := 0;

        SP_LOAD_DIMENSIONS(p_run_id);
        SP_LOAD_FACT_SALES(p_run_id, p_inserted, p_updated);
        SP_RUN_RECONCILIATION(p_run_id);

        UPDATE ETL_PIPELINE_RUNS
        SET DWH_INSERTED_COUNT = p_inserted,
            DWH_UPDATED_COUNT  = p_updated
        WHERE RUN_ID = p_run_id;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            v_err_msg := SUBSTR(SQLERRM, 1, 4000);
            ROLLBACK;
            UPDATE ETL_PIPELINE_RUNS
            SET STATUS        = 'FAILED',
                ERROR_MESSAGE = v_err_msg,
                RUN_FINISHED_AT = SYSTIMESTAMP
            WHERE RUN_ID = p_run_id;
            COMMIT;
            RAISE;
    END SP_PROCESS_RUN;

END PKG_RETAIL_ETL;
/
