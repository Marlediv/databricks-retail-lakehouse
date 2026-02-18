-- 02_bronze.sql
-- Bronze-Layer aus CSV-Dateien im Unity Catalog Volume laden.
CREATE DATABASE IF NOT EXISTS retail_lakehouse;
USE retail_lakehouse;

CREATE OR REPLACE TABLE retail_lakehouse.bronze_customers
USING DELTA AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  signup_date,
  updated_at,
  current_timestamp() AS ingestion_ts,
  _metadata.file_path AS source_file
FROM read_files(
  '/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/customers.csv',
  format => 'csv',
  header => true
);

CREATE OR REPLACE TABLE retail_lakehouse.bronze_products
USING DELTA AS
SELECT
  product_id,
  product_name,
  category,
  price,
  updated_at,
  current_timestamp() AS ingestion_ts,
  _metadata.file_path AS source_file
FROM read_files(
  '/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/products.csv',
  format => 'csv',
  header => true
);

CREATE OR REPLACE TABLE retail_lakehouse.bronze_orders
USING DELTA AS
SELECT
  order_line_id,
  order_id,
  order_date,
  customer_id,
  product_id,
  quantity,
  updated_at,
  current_timestamp() AS ingestion_ts,
  _metadata.file_path AS source_file
FROM read_files(
  '/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/orders.csv',
  format => 'csv',
  header => true
);
