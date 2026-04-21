# End-to-End-ETL-Data-Pipeline

## Overview
This project implements an end-to-end ETL (Extract, Transform, Load) data pipeline using Snowflake and AWS S3. It ingests raw transactional data from S3, processes it through layered transformations (Bronze → Silver → Gold), and models it into a star schema for analytics.

The pipeline includes robust error handling, data validation, and incremental loading to ensure reliability, scalability, and production-style data processing.

## Tech Stack
- Snowflake – Data warehouse & compute engine  
- AWS S3 – Source data storage  
- SQL – Transformation, modeling, and validation  

## Architecture (Medallion Model)
**Flow:**  
S3 → Bronze (Raw) → Silver (Cleaned) → Gold (Star Schema - Fact & Dimensions)

## Key Features
- End-to-end ETL pipeline from AWS S3 to Snowflake  
- Medallion architecture (Bronze → Silver → Gold) for structured data flow  
- Robust error handling using `ON_ERROR = CONTINUE`  
- Data cleansing using `TRY_TO_DATE`, `TRY_TO_TIME`, and `TRIM`  
- Derived metrics (e.g., `total_amount`)  
- Star schema modeling with fact and dimension tables  
- Data validation checks (duplicates, NULLs, record counts)  
- Incremental loading for efficient data processing  

## Pipeline Workflow

### 1. Extract (S3 → Snowflake Bronze Layer)
- External stage created using storage integration  
- Raw CSV data loaded into `bronze_sales_raw`  
- Fault-tolerant ingestion using `ON_ERROR = CONTINUE`  

### 2. Transform (Silver Layer)
- Cleaned and standardized raw data  
- Parsed date/time using `TRY_TO_DATE` and `TRY_TO_TIME`  
- Generated `transaction_timestamp`  
- Removed invalid records  
- Created derived column: `total_amount`  

### 3. Load (Gold Layer - Star Schema)

#### Dimension Tables
- `dim_store`  
- `dim_product`  
- `dim_date`  

#### Fact Table
- `fact_sales`  

- Surrogate keys generated using `ROW_NUMBER()`  
- Fact table linked via foreign keys for analytics  

## Data Modeling
- Implemented Star Schema  
- Fact table stores transactional metrics  
- Dimension tables provide descriptive attributes  
- Enables efficient analytical queries and reporting  

## Incremental Loading

### Bronze → Silver
- Loads only new records using `transaction_id`  

### Silver → Fact
- Inserts only unseen records into fact table  

**This ensures:**
- Faster processing  
- Avoidance of duplicate loads  
- Scalable pipeline behavior  

## Data Validation
- Duplicate check using `GROUP BY`  
- NULL validation on critical fields  
- Record count verification  
- Latest load tracking using `MAX(transaction_id)`

## Outcome
- Built a scalable ETL pipeline using Snowflake and AWS S3  
- Transformed raw data into structured, analytics-ready datasets  
- Designed a star schema for efficient querying  
- Implemented incremental loading and validation for production-like behavior  
