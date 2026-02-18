-- 03_silver.sql
-- Silver-Layer: Typisierung, Dedupe und fachliche Anreicherung.
CREATE DATABASE IF NOT EXISTS retail_lakehouse;
USE retail_lakehouse;

CREATE OR REPLACE TABLE retail_lakehouse.silver_products
USING DELTA AS
SELECT
  CAST(product_id AS INT) AS product_id,
  CAST(product_name AS STRING) AS product_name,
  CAST(category AS STRING) AS category,
  CAST(price AS DECIMAL(10,2)) AS price,
  TO_TIMESTAMP(updated_at) AS updated_at,
  CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
  CAST(source_file AS STRING) AS source_file
FROM retail_lakehouse.bronze_products;

CREATE OR REPLACE TABLE retail_lakehouse.silver_customers_current
USING DELTA AS
WITH typed AS (
  SELECT
    CAST(customer_id AS INT) AS customer_id,
    COALESCE(CAST(first_name AS STRING), 'Unknown') AS first_name,
    COALESCE(CAST(last_name AS STRING), 'Unknown') AS last_name,
    COALESCE(CAST(email AS STRING), CONCAT('unknown_', customer_id, '@example.com')) AS email,
    COALESCE(CAST(city AS STRING), 'Unknown') AS city,
    TO_DATE(signup_date) AS signup_date,
    TO_TIMESTAMP(updated_at) AS updated_at,
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
    CAST(source_file AS STRING) AS source_file
  FROM retail_lakehouse.bronze_customers
  WHERE customer_id IS NOT NULL
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY updated_at DESC, ingestion_ts DESC
    ) AS rn
  FROM typed
)
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  signup_date,
  updated_at,
  ingestion_ts,
  source_file
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE TABLE retail_lakehouse.silver_order_lines
USING DELTA AS
WITH typed_orders AS (
  SELECT
    CAST(order_line_id AS INT) AS order_line_id,
    CAST(order_id AS INT) AS order_id,
    TO_DATE(order_date) AS order_date,
    CAST(customer_id AS INT) AS customer_id,
    CAST(product_id AS INT) AS product_id,
    COALESCE(CAST(quantity AS INT), 1) AS quantity,
    TO_TIMESTAMP(updated_at) AS updated_at,
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
    CAST(source_file AS STRING) AS source_file
  FROM retail_lakehouse.bronze_orders
  WHERE order_line_id IS NOT NULL
),
dedup_orders AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_line_id
      ORDER BY updated_at DESC, ingestion_ts DESC
    ) AS rn
  FROM typed_orders
)
SELECT
  o.order_line_id,
  o.order_id,
  o.order_date,
  o.customer_id,
  o.product_id,
  p.product_name,
  p.category,
  p.price,
  o.quantity,
  CAST(o.quantity * p.price AS DECIMAL(12,2)) AS line_revenue,
  o.updated_at,
  o.ingestion_ts,
  o.source_file
FROM dedup_orders o
LEFT JOIN retail_lakehouse.silver_products p
  ON o.product_id = p.product_id
WHERE o.rn = 1;
