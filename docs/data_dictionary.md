# Data Dictionary

## Source CSV files (`data/`)

### `customers.csv`
- `customer_id` (int): eindeutige Kunden-ID
- `first_name` (string): Vorname
- `last_name` (string): Nachname
- `email` (string): E-Mail
- `city` (string): Stadt
- `signup_date` (date): Registrierungsdatum
- `updated_at` (timestamp): Zeitstempel der letzten Änderung im Quellsystem

### `products.csv`
- `product_id` (int): eindeutige Produkt-ID
- `product_name` (string): Produktname
- `category` (string): Kategorie
- `price` (double): Einzelpreis
- `updated_at` (timestamp): Zeitstempel der letzten Änderung im Quellsystem

### `orders.csv`
- `order_line_id` (int): eindeutige Zeilen-ID
- `order_id` (int): Bestell-ID
- `order_date` (date): Bestelldatum
- `customer_id` (int): Kunden-ID
- `product_id` (int): Produkt-ID
- `quantity` (int): Menge
- `updated_at` (timestamp): Zeitstempel der letzten Änderung im Quellsystem

## Bronze tables

### `bronze_customers`, `bronze_products`, `bronze_orders`
- Entspricht der jeweiligen CSV-Struktur
- Zusätzliche Metadaten:
  - `ingestion_ts` (timestamp): Ingestion-Zeit
  - `source_file` (string): Dateipfad der Quelldatei

## Silver tables

### `silver_customers_current`
- Aktueller deduplizierter Kundenzustand (ohne Historie)
- PK: `customer_id`

### `silver_products`
- Deduplizierte Produkte
- PK: `product_id`

### `silver_order_lines`
- Deduplizierte Bestellzeilen mit Produktattributen
- PK: `order_line_id`
- Enthält berechnetes Feld `line_revenue = quantity * price`

### `silver_customers_scd2`
- Historisierte Kundendimension (SCD Type 2)
- PK technisch: (`customer_id`, `valid_from`)
- `is_current = true` markiert aktuelle Version

## Gold tables

### `gold_daily_revenue`
- Tagesumsatz, Bestellanzahl und Stückzahl

### `gold_product_revenue`
- Umsatz und Menge je Produkt

### `gold_top_customers`
- Umsatzstärkste Kunden inkl. Stammdaten

### `gold_aov`
- Average Order Value (Gesamtumsatz / Anzahl Bestellungen)
