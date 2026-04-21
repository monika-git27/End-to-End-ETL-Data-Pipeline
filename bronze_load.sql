CREATE DATABASE salesdb;
CREATE SCHEMA salessch;

CREATE OR REPLACE STORAGE INTEGRATION aws_s3_int 
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '<role-arn>'
STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/<path>/');


DESC INTEGRATION aws_s3_int;

GRANT USAGE ON INTEGRATION aws_s3_int TO ROLE accountadmin;

CREATE OR REPLACE FILE FORMAT my_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE stg_sales
URL = 's3://<bucket>/<path>/'
STORAGE_INTEGRATION = aws_s3_int
FILE_FORMAT = my_file_format;

LIST @stg_sales;

CREATE OR REPLACE TABLE bronze_sales_raw (
    transaction_id INT,
    transaction_date STRING,
    transaction_time STRING,
    transaction_qty INT,
    store_id INT,
    store_location STRING,
    product_id INT,
    unit_price DECIMAL(10,2),
    product_category STRING,
    product_type STRING,
    product_detail STRING
);

COPY INTO bronze_sales_raw
FROM @stg_sales
ON_ERROR = 'CONTINUE';

-- check
SELECT COUNT(*) FROM bronze_sales_raw;
SELECT * FROM bronze_sales_raw LIMIT 10;