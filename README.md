# databricks-retail-lakehouse

Retail Lakehouse Mini-Projekt auf Databricks mit einem klaren Medallion-Aufbau (Bronze, Silver, Gold) und Delta-Tabellen. Die Pipeline verarbeitet CSV-Daten aus Unity Catalog Volumes, bereitet sie schrittweise auf und liefert KPI-Tabellen für Reporting. Zusätzlich wird eine SCD2-Kundendimension aufgebaut und mit einfachen Qualitätsprüfungen validiert.

## Warum dieses Projekt?

- Zeigt, wie operative Retail-Daten in eine stabile Analysebasis überführt werden.
- Trennt Rohdatenverarbeitung und KPI-Berechnung, damit Änderungen kontrollierbar bleiben.
- Liefert nachvollziehbare Kennzahlen als Grundlage für Produkt-, Umsatz- und Kundenanalysen.

## Was demonstriert es technisch?

- Datenzugriff über Unity Catalog Volume Paths.
- Delta-Tabellen als Speicherformat (`USING DELTA`).
- Medallion-Architektur (Bronze/Silver/Gold).
- Deduplizierung mit `ROW_NUMBER()` auf Basis von `updated_at`.
- SCD Type 2 für Kunden mit Hash-basierter Änderungslogik.
- Idempotente Ausführung durch `CREATE OR REPLACE` / kontrollierte Reloads.
- Data Quality Checks über dedizierte Validierungsqueries.
- KPI-Layer für tägliche Umsätze, Produktumsatz und Top-Kunden.

## Projektstruktur

```text
databricks-retail-lakehouse/
  data/
    customers.csv
    products.csv
    orders.csv
  notebooks/
    01_bronze_ingestion.py
    02_silver_transform.py
    03_gold_kpis.py
    04_scd2_customers.py
  sql/
    01_setup.sql
    02_bronze.sql
    03_silver.sql
    04_scd2.sql
    05_gold.sql
    06_validation.sql
  docs/
    architecture.mmd
    architecture.png
    data_dictionary.md
    screenshots/
      .gitkeep
      README.md
  README.md
```

## Architektur

![Architecture](docs/architecture.png)

Editierbare Quelle: `docs/architecture.mmd`

## Databricks SQL Runbook

### Setup

1. SQL Warehouse starten.
2. CSV-Dateien ins Volume legen:
- `/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/customers.csv`
- `/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/products.csv`
- `/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/orders.csv`

### Run order

1. Setup: `sql/01_setup.sql`
2. Daten laden (Bronze): `sql/02_bronze.sql`
3. Silver-Transformation: `sql/03_silver.sql`
4. SCD2 aufbauen: `sql/04_scd2.sql`
5. Gold-KPIs berechnen: `sql/05_gold.sql`
6. Validierung ausführen: `sql/06_validation.sql`

## Expected Outputs

Bronze:
- `retail_lakehouse.bronze_customers`
- `retail_lakehouse.bronze_products`
- `retail_lakehouse.bronze_orders`

Silver:
- `retail_lakehouse.silver_customers_current`
- `retail_lakehouse.silver_products`
- `retail_lakehouse.silver_order_lines`
- `retail_lakehouse.silver_customers_scd2`

Gold:
- `retail_lakehouse.gold_daily_revenue`
- `retail_lakehouse.gold_product_revenue`
- `retail_lakehouse.gold_top_customers`

## Validierung

Wichtige Prüfungen aus `sql/06_validation.sql`:
- `SHOW TABLES IN retail_lakehouse;`
- Row counts für `bronze_customers`, `silver_customers_current`, `silver_customers_scd2` (nur `is_current = true`) und `gold_daily_revenue`
- Dedupe-Checks auf `customer_id` in `silver_customers_current`
- Dedupe-Checks auf aktuelle Datensätze (`is_current = true`) in `silver_customers_scd2`

## Screenshots (Evidence)

Belege aus dem Databricks SQL Editor für die wichtigsten Pipeline-Schritte.

### Bronze

![Bronze Preview](docs/screenshots/01_bronze_preview.png)

### Silver

![Silver Preview](docs/screenshots/02_silver_preview.png)
![SCD2 Validation](docs/screenshots/03_scd2_validation.png)

### Gold

![Gold Daily Revenue](docs/screenshots/04_gold_daily_revenue.png)
![Gold Product Revenue](docs/screenshots/05_gold_product_revenue.png)
![Gold Top Customers](docs/screenshots/06_gold_top_customers.png)

### Übersicht

![Table Overview](docs/screenshots/07_table_overview.png)

## Troubleshooting

- Falscher DB-Kontext: sicherstellen, dass `USE retail_lakehouse;` aktiv ist.
- Fehlerhafte Volume-Pfade: Pfade exakt wie oben verwenden und Dateinamen prüfen.
- Warehouse nicht aktiv: SQL-Queries laufen nur mit gestartetem SQL Warehouse.
- Leere Gold-Ergebnisse: zuerst prüfen, ob Bronze- und Silver-Tabellen erfolgreich aufgebaut wurden.

## Notebooks (optional zur SQL-Variante)

Das SQL-Runbook unter `sql/` ist die primäre Referenz (Source of Truth).  
Die Notebook-Implementierung unter `notebooks/` ist eine getestete alternative PySpark-Ausführung und erzeugt dieselben Kern-Tabellen für Bronze, Silver, SCD2 und Gold.

Run Checklist (Notebook-Reihenfolge):
1. `notebooks/01_bronze_ingestion.py`
2. `notebooks/02_silver_transform.py`
3. `notebooks/04_scd2_customers.py`
4. `notebooks/03_gold_kpis.py`
