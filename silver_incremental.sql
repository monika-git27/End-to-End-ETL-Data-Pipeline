USE DATABASE salesdb;
USE SCHEMA salessch;

 -- last loaded record
SELECT MAX(transaction_id) FROM silver_sales_clean;

-- Incremental load from Bronze to Silver
INSERT INTO silver_sales_clean
SELECT
    transaction_id,

    TRY_TO_DATE(TRIM(transaction_date), 'DD-MM-YYYY') AS transaction_date,
    TRY_TO_TIME(TRIM(transaction_time), 'HH24:MI:SS') AS transaction_time,

    TO_TIMESTAMP(
        TRY_TO_DATE(TRIM(transaction_date), 'DD-MM-YYYY') || ' ' ||
        TRY_TO_TIME(TRIM(transaction_time), 'HH24:MI:SS')
    ) AS transaction_timestamp,

    transaction_qty,
    store_id,
    TRIM(store_location),
    product_id,
    unit_price,
    TRIM(product_category),
    TRIM(product_type),
    TRIM(product_detail),

    transaction_qty * unit_price AS total_amount

FROM bronze_sales_raw

--only new records
WHERE transaction_id > (
    SELECT COALESCE(MAX(transaction_id), 0)
    FROM silver_sales_clean
);
