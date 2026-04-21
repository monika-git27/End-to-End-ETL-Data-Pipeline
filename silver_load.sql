USE DATABASE salesdb;
USE SCHEMA salessch;


CREATE OR REPLACE TABLE silver_sales_clean AS
SELECT
    transaction_id,

    -- Clean + parse 
    TRY_TO_DATE(TRIM(transaction_date), 'DD-MM-YYYY') AS transaction_date,

    TRY_TO_TIME(TRIM(transaction_time), 'HH24:MI:SS') AS transaction_time,

    --  timestamp from parsed values
    TO_TIMESTAMP(
        TRY_TO_DATE(TRIM(transaction_date), 'DD-MM-YYYY') || ' ' ||
        TRY_TO_TIME(TRIM(transaction_time), 'HH24:MI:SS')
    ) AS transaction_timestamp,

    transaction_qty,
    store_id,
    TRIM(store_location) AS store_location,
    product_id,
    unit_price,
    TRIM(product_category) AS product_category,
    TRIM(product_type) AS product_type,
    TRIM(product_detail) AS product_detail,

    transaction_qty * unit_price AS total_amount

FROM bronze_sales_raw

-- Filtering only valid rows
WHERE TRY_TO_DATE(TRIM(transaction_date), 'DD-MM-YYYY') IS NOT NULL
  AND TRY_TO_TIME(TRIM(transaction_time), 'HH24:MI:SS') IS NOT NULL;


-- check
SELECT COUNT(*) FROM silver_sales_clean;
SELECT * FROM silver_sales_clean LIMIT 10;