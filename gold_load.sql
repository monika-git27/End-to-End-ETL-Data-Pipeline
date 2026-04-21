USE DATABASE salesdb;
USE SCHEMA salessch;

-- dim_product
CREATE OR REPLACE TABLE dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id,
    product_category,
    product_type,
    product_detail
FROM (
    SELECT DISTINCT
        product_id,
        product_category,
        product_type,
        product_detail
    FROM silver_sales_clean
);

-- dim_date
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY transaction_date) AS date_key,
    transaction_date,
    EXTRACT(YEAR FROM transaction_date) AS year,
    EXTRACT(MONTH FROM transaction_date) AS month,
    EXTRACT(DAY FROM transaction_date) AS day,
    DAYNAME(transaction_date) AS weekday
FROM (
    SELECT DISTINCT transaction_date
    FROM silver_sales_clean
);

SELECT * FROM dim_date LIMIT 5;

-- FACT TABLE
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    s.transaction_id AS sales_id,

    p.product_key,
    st.store_key,
    d.date_key,

    s.transaction_qty AS quantity,
    s.unit_price,
    s.total_amount

FROM silver_sales_clean s

JOIN dim_product p 
    ON s.product_id = p.product_id

JOIN dim_store st 
    ON s.store_id = st.store_id

JOIN dim_date d 
    ON s.transaction_date = d.transaction_date;


    -- Data Validation
    -- check duplicates
SELECT transaction_id, COUNT(*)
FROM silver_sales_clean
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- check nulls
SELECT COUNT(*) 
FROM silver_sales_clean 
WHERE transaction_date IS NULL;