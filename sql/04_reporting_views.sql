-- =============================================================================
-- File: 04_reporting_views.sql
-- Schema: RETAIL_DW
-- Purpose: Denormalized & aggregated Reporting Layer views for SQL Analytics.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. VW_SALES_REPORTING: Wide semantic view joining FACT_SALES with all 4 Dims
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_SALES_REPORTING AS
SELECT
    f.SALES_KEY,
    f.TRANSACTION_ID,
    f.SALE_TIMESTAMP,
    d.DATE_KEY,
    d.FULL_DATE          AS SALE_DATE,
    d.YEAR_NUM,
    d.QUARTER_NAME,
    d.YEAR_MONTH,
    d.MONTH_NUM,
    d.MONTH_NAME,
    d.DAY_NAME,
    d.IS_WEEKEND,
    s.STORE_KEY,
    s.STORE_CODE,
    s.STORE_NAME,
    s.CITY               AS STORE_CITY,
    s.REGION             AS STORE_REGION,
    s.STORE_CHANNEL,
    p.PRODUCT_KEY,
    p.PRODUCT_CODE,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUBCATEGORY,
    c.CUSTOMER_KEY,
    c.CUSTOMER_CODE,
    c.CUSTOMER_NAME,
    c.CUSTOMER_SEGMENT,
    f.PAYMENT_METHOD,
    f.SOURCE_TYPE,
    f.QUANTITY,
    f.UNIT_COST,
    f.UNIT_PRICE,
    f.DISCOUNT_PCT,
    f.GROSS_AMOUNT,
    f.DISCOUNT_AMOUNT,
    f.NET_AMOUNT,
    f.COST_AMOUNT,
    f.PROFIT_AMOUNT,
    CASE
        WHEN f.NET_AMOUNT > 0 THEN ROUND((f.PROFIT_AMOUNT / f.NET_AMOUNT) * 100, 2)
        ELSE 0
    END AS PROFIT_MARGIN_PCT
FROM FACT_SALES f
INNER JOIN DIM_DATE d     ON f.DATE_KEY     = d.DATE_KEY
INNER JOIN DIM_STORE s    ON f.STORE_KEY    = s.STORE_KEY
INNER JOIN DIM_PRODUCT p  ON f.PRODUCT_KEY  = p.PRODUCT_KEY
INNER JOIN DIM_CUSTOMER c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY;

-- -----------------------------------------------------------------------------
-- 2. VW_STORE_DAILY_KPI: Aggregated Store-Date KPI summary for executive cards
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_STORE_DAILY_KPI AS
SELECT
    d.FULL_DATE                  AS SALE_DATE,
    d.YEAR_MONTH,
    s.STORE_CODE,
    s.STORE_NAME,
    s.CITY,
    s.REGION,
    s.STORE_CHANNEL,
    COUNT(f.SALES_KEY)           AS TRANSACTION_COUNT,
    COUNT(DISTINCT f.CUSTOMER_KEY) AS UNIQUE_CUSTOMERS,
    SUM(f.QUANTITY)              AS TOTAL_UNITS_SOLD,
    SUM(f.GROSS_AMOUNT)          AS TOTAL_GROSS_REVENUE,
    SUM(f.DISCOUNT_AMOUNT)       AS TOTAL_DISCOUNT,
    SUM(f.NET_AMOUNT)            AS TOTAL_NET_REVENUE,
    SUM(f.PROFIT_AMOUNT)         AS TOTAL_PROFIT,
    ROUND(AVG(f.NET_AMOUNT), 2)  AS AVG_BASKET_VALUE,
    CASE
        WHEN SUM(f.NET_AMOUNT) > 0 THEN ROUND((SUM(f.PROFIT_AMOUNT) / SUM(f.NET_AMOUNT)) * 100, 2)
        ELSE 0
    END AS PROFIT_MARGIN_PCT
FROM FACT_SALES f
INNER JOIN DIM_DATE d  ON f.DATE_KEY  = d.DATE_KEY
INNER JOIN DIM_STORE s ON f.STORE_KEY = s.STORE_KEY
GROUP BY
    d.FULL_DATE, d.YEAR_MONTH,
    s.STORE_CODE, s.STORE_NAME, s.CITY, s.REGION, s.STORE_CHANNEL;

-- -----------------------------------------------------------------------------
-- 3. VW_DATA_QUALITY_MONITOR: Pipeline health & Data Quality dashboard view
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_DATA_QUALITY_MONITOR AS
SELECT
    r.RUN_ID,
    r.RUN_UUID,
    r.RUN_STARTED_AT,
    r.RUN_FINISHED_AT,
    r.STATUS,
    r.SOURCE_CSV_FILES,
    r.SOURCE_API_BATCHES,
    r.RAW_ROW_COUNT,
    r.CLEAN_ROW_COUNT,
    r.REJECT_ROW_COUNT,
    r.DWH_INSERTED_COUNT,
    r.DWH_UPDATED_COUNT,
    CASE
        WHEN r.RAW_ROW_COUNT > 0
        THEN ROUND((r.CLEAN_ROW_COUNT / r.RAW_ROW_COUNT) * 100, 2)
        ELSE 100
    END AS DATA_PASS_RATE_PCT,
    (
        SELECT COUNT(*)
        FROM ETL_RECONCILIATION_LOG rec
        WHERE rec.RUN_ID = r.RUN_ID AND rec.STATUS = 'FAIL'
    ) AS RECON_FAIL_COUNT
FROM ETL_PIPELINE_RUNS r;
