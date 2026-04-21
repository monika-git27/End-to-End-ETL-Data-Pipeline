USE DATABASE salesdb;
USE SCHEMA salessch;

-- Incremental load into FACT table
INSERT INTO fact_sales
SELECT
    s.transaction_id,
    p.product_key,
    st.store_key,
    d.date_key,
    s.transaction_qty,
    s.unit_price,
    s.total_amount

FROM silver_sales_clean s

JOIN dim_product p ON s.product_id = p.product_id
JOIN dim_store st ON s.store_id = st.store_id
JOIN dim_date d ON s.transaction_date = d.transaction_date

--only new records
WHERE s.transaction_id > (
    SELECT COALESCE(MAX(sales_id), 0)
    FROM fact_sales
);