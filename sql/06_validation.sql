-- 06_validation.sql
-- Validierungsabfragen für Vollständigkeit und Dedupe.
CREATE DATABASE IF NOT EXISTS retail_lakehouse;
USE retail_lakehouse;

SHOW TABLES IN retail_lakehouse;

-- Zeilenzahlen zentraler Tabellen.
SELECT 'bronze_customers' AS table_name, COUNT(*) AS row_count
FROM retail_lakehouse.bronze_customers
UNION ALL
SELECT 'silver_customers_current' AS table_name, COUNT(*) AS row_count
FROM retail_lakehouse.silver_customers_current
UNION ALL
SELECT 'silver_customers_scd2_current' AS table_name, COUNT(*) AS row_count
FROM retail_lakehouse.silver_customers_scd2
WHERE is_current = true
UNION ALL
SELECT 'gold_daily_revenue' AS table_name, COUNT(*) AS row_count
FROM retail_lakehouse.gold_daily_revenue;

-- Dedupe-Check: muss 0 Zeilen zurückgeben.
SELECT customer_id, COUNT(*) AS cnt
FROM retail_lakehouse.silver_customers_current
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Dedupe-Check SCD2 Current: muss 0 Zeilen zurückgeben.
SELECT customer_id, COUNT(*) AS cnt
FROM retail_lakehouse.silver_customers_scd2
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;
