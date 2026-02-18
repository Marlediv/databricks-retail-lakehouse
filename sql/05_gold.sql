-- 05_gold.sql
-- Gold-Layer mit KPI-Tabellen für Reporting.
CREATE DATABASE IF NOT EXISTS retail_lakehouse;
USE retail_lakehouse;

CREATE OR REPLACE TABLE retail_lakehouse.gold_daily_revenue
USING DELTA AS
SELECT
  order_date,
  CAST(SUM(line_revenue) AS DECIMAL(14,2)) AS daily_revenue,
  COUNT(DISTINCT order_id) AS orders
FROM retail_lakehouse.silver_order_lines
GROUP BY order_date;

CREATE OR REPLACE TABLE retail_lakehouse.gold_product_revenue
USING DELTA AS
SELECT
  product_id,
  CAST(SUM(line_revenue) AS DECIMAL(14,2)) AS total_revenue,
  SUM(quantity) AS total_quantity
FROM retail_lakehouse.silver_order_lines
GROUP BY product_id;

CREATE OR REPLACE TABLE retail_lakehouse.gold_top_customers
USING DELTA AS
SELECT
  customer_id,
  CAST(SUM(line_revenue) AS DECIMAL(14,2)) AS total_revenue
FROM retail_lakehouse.silver_order_lines
GROUP BY customer_id;

CREATE OR REPLACE TABLE retail_lakehouse.gold_aov
USING DELTA AS
WITH order_totals AS (
  SELECT
    order_id,
    SUM(line_revenue) AS order_revenue
  FROM retail_lakehouse.silver_order_lines
  GROUP BY order_id
)
SELECT
  CAST(AVG(order_revenue) AS DECIMAL(14,2)) AS aov
FROM order_totals;
