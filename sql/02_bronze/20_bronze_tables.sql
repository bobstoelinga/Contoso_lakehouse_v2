-- =============================================================================
-- 20_bronze_tables.sql
-- Bronze Delta tabellen. Auto Loader schrijft hier incrementeel naartoe met
-- schema evolution (addNewColumns): nieuwe bronkolommen worden automatisch
-- toegevoegd. Onderstaande DDL legt alleen het technische raamwerk vast.
-- =============================================================================
USE CATALOG contoso_bronze_${env};
USE SCHEMA sales;

-- Technische kolommen (identiek voor elke Bronze tabel, gezet door het framework):
--   _delivery_id        : SALES|yyyy-MM-dd
--   _delivery_date      : datum van de leveringsfolder (partitiekolom)
--   _source_file_path   : _metadata.file_path
--   _source_file_name   : _metadata.file_name
--   _source_file_size   : _metadata.file_size
--   _source_file_mtime  : _metadata.file_modification_time
--   _ingest_timestamp   : moment van verwerken
--   _batch_id           : end-to-end run identifier
--   _record_source      : source_object_id
--   _rescued_data       : Auto Loader rescued data column

CREATE TABLE IF NOT EXISTS br_orders (
  order_key           STRING,
  order_line_number   INT,
  customer_key        STRING,
  product_key         STRING,
  employee_key        STRING,
  order_date          DATE,
  ship_date           DATE,
  delivery_date       DATE,
  order_status        STRING,
  quantity            INT,
  unit_price          DECIMAL(18,4),
  discount_amount     DECIMAL(18,4),
  net_amount          DECIMAL(18,4),
  currency_code       STRING,

  _delivery_id        STRING    NOT NULL,
  _delivery_date      DATE      NOT NULL,
  _source_file_path   STRING    NOT NULL,
  _source_file_name   STRING    NOT NULL,
  _source_file_size   BIGINT,
  _source_file_mtime  TIMESTAMP,
  _ingest_timestamp   TIMESTAMP NOT NULL,
  _batch_id           STRING    NOT NULL,
  _record_source      STRING    NOT NULL,
  _rescued_data       STRING
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: onbewerkte orderregels per levering.'
TBLPROPERTIES (
  delta.enableChangeDataFeed          = true,
  delta.autoOptimize.optimizeWrite    = true,
  delta.autoOptimize.autoCompact      = true,
  delta.columnMapping.mode            = 'name',
  delta.minReaderVersion              = '2',
  delta.minWriterVersion              = '5'
);

CREATE TABLE IF NOT EXISTS br_employees (
  employee_key        STRING,
  first_name          STRING,
  last_name           STRING,
  job_title           STRING,
  office_city         STRING,
  hire_date           DATE,
  is_deleted          BOOLEAN,

  _delivery_id        STRING    NOT NULL,
  _delivery_date      DATE      NOT NULL,
  _source_file_path   STRING    NOT NULL,
  _source_file_name   STRING    NOT NULL,
  _source_file_size   BIGINT,
  _source_file_mtime  TIMESTAMP,
  _ingest_timestamp   TIMESTAMP NOT NULL,
  _batch_id           STRING    NOT NULL,
  _record_source      STRING    NOT NULL,
  _rescued_data       STRING
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: medewerkerssnapshot per levering.'
TBLPROPERTIES (
  delta.enableChangeDataFeed       = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true,
  delta.columnMapping.mode         = 'name',
  delta.minReaderVersion           = '2',
  delta.minWriterVersion           = '5'
);

CREATE TABLE IF NOT EXISTS br_returns (
  return_key          STRING,
  order_key           STRING,
  order_line_number   INT,
  product_key         STRING,
  employee_key        STRING,
  return_date         DATE,
  return_status       STRING,
  return_reason_code  STRING,
  return_quantity     INT,
  refund_amount       DECIMAL(18,4),
  currency_code       STRING,

  _delivery_id        STRING    NOT NULL,
  _delivery_date      DATE      NOT NULL,
  _source_file_path   STRING    NOT NULL,
  _source_file_name   STRING    NOT NULL,
  _source_file_size   BIGINT,
  _source_file_mtime  TIMESTAMP,
  _ingest_timestamp   TIMESTAMP NOT NULL,
  _batch_id           STRING    NOT NULL,
  _record_source      STRING    NOT NULL,
  _rescued_data       STRING
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: onbewerkte retourregels per levering.'
TBLPROPERTIES (
  delta.enableChangeDataFeed       = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true,
  delta.columnMapping.mode         = 'name',
  delta.minReaderVersion           = '2',
  delta.minWriterVersion           = '5'
);

CREATE TABLE IF NOT EXISTS br_customers (
  customer_key        STRING,
  customer_name       STRING,
  email               STRING,
  phone               STRING,
  address_line1       STRING,
  city                STRING,
  state_province      STRING,
  postal_code         STRING,
  country             STRING,
  customer_segment    STRING,
  customer_since      DATE,
  is_deleted          BOOLEAN,

  _delivery_id        STRING    NOT NULL,
  _delivery_date      DATE      NOT NULL,
  _source_file_path   STRING    NOT NULL,
  _source_file_name   STRING    NOT NULL,
  _source_file_size   BIGINT,
  _source_file_mtime  TIMESTAMP,
  _ingest_timestamp   TIMESTAMP NOT NULL,
  _batch_id           STRING    NOT NULL,
  _record_source      STRING    NOT NULL,
  _rescued_data       STRING
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: klantsnapshot per levering.'
TBLPROPERTIES (
  delta.enableChangeDataFeed       = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true,
  delta.columnMapping.mode         = 'name',
  delta.minReaderVersion           = '2',
  delta.minWriterVersion           = '5'
);

CREATE TABLE IF NOT EXISTS br_products (
  product_key         STRING,
  product_name        STRING,
  product_category    STRING,
  product_subcategory STRING,
  brand               STRING,
  unit_cost           DECIMAL(18,4),
  unit_price          DECIMAL(18,4),
  is_discontinued     BOOLEAN,
  is_deleted          BOOLEAN,

  _delivery_id        STRING    NOT NULL,
  _delivery_date      DATE      NOT NULL,
  _source_file_path   STRING    NOT NULL,
  _source_file_name   STRING    NOT NULL,
  _source_file_size   BIGINT,
  _source_file_mtime  TIMESTAMP,
  _ingest_timestamp   TIMESTAMP NOT NULL,
  _batch_id           STRING    NOT NULL,
  _record_source      STRING    NOT NULL,
  _rescued_data       STRING
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: productsnapshot per levering.'
TBLPROPERTIES (
  delta.enableChangeDataFeed       = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true,
  delta.columnMapping.mode         = 'name',
  delta.minReaderVersion           = '2',
  delta.minWriterVersion           = '5'
);

USE SCHEMA cbs;

CREATE TABLE IF NOT EXISTS br_jeugdzorg_wijk_2025 (
  Vormen                                  STRING,
  Wijken                                  STRING,
  Perioden                                STRING,
  TotaalJongerenMetJeugdzorgInNatur_1     BIGINT,
  TotaalTrajectenInNatur_9                BIGINT,
  Gemeentenaam_18                         STRING,
  SoortRegio_19                           STRING,
  Codering_20                             STRING,
  _delivery_id                            STRING NOT NULL,
  _delivery_date                          DATE NOT NULL,
  _source_file_path                       STRING NOT NULL,
  _source_file_name                       STRING NOT NULL,
  _source_file_size                       BIGINT,
  _source_file_mtime                      TIMESTAMP,
  _ingest_timestamp                       TIMESTAMP NOT NULL,
  _batch_id                               STRING NOT NULL,
  _record_source                          STRING NOT NULL,
  _rescued_data                           STRING
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: onbewerkte CBS StatLine jeugdzorgcijfers per wijk en periode.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA fabric_sales;

CREATE TABLE IF NOT EXISTS br_order_lines (
  sales_order_detail_id BIGINT, order_quantity INT, unit_price DECIMAL(18,4),
  unit_price_discount DECIMAL(9,6), total_due_amount DECIMAL(18,4), order_status_code STRING,
  source_last_modified_at TIMESTAMP, is_deleted BOOLEAN,
  _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL, _source_file_path STRING NOT NULL,
  _source_file_name STRING NOT NULL, _source_file_size BIGINT, _source_file_mtime TIMESTAMP,
  _ingest_timestamp TIMESTAMP NOT NULL, _batch_id STRING NOT NULL, _record_source STRING NOT NULL,
  _rescued_data STRING
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: ongewijzigde Fabric SQL orderregel-feed.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA ecb;

CREATE TABLE IF NOT EXISTS br_exchange_rate (
  CURRENCY              STRING,
  TIME_PERIOD           STRING,
  OBS_VALUE             STRING,
  FREQ                  STRING,
  CURRENCY_DENOM        STRING,
  EXR_TYPE              STRING,
  EXR_SUFFIX            STRING,
  _delivery_id          STRING NOT NULL,
  _delivery_date        DATE NOT NULL,
  _source_file_path     STRING NOT NULL,
  _source_file_name     STRING NOT NULL,
  _source_file_size     BIGINT,
  _source_file_mtime    TIMESTAMP,
  _ingest_timestamp     TIMESTAMP NOT NULL,
  _batch_id             STRING NOT NULL,
  _record_source        STRING NOT NULL,
  _rescued_data         STRING
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: onbewerkte ECB dagelijkse referentiekoersen tegenover EUR.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA sharepoint;

CREATE TABLE IF NOT EXISTS br_price_agreements (
  agreement_id STRING, customer_segment STRING, product_key STRING, currency_code STRING,
  list_price DECIMAL(18,4), discount_pct DECIMAL(9,4), valid_from DATE, valid_to DATE,
  agreement_status STRING, updated_at TIMESTAMP, is_deleted BOOLEAN,
  _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL, _source_file_path STRING NOT NULL,
  _source_file_name STRING NOT NULL, _source_file_size BIGINT, _source_file_mtime TIMESTAMP,
  _ingest_timestamp TIMESTAMP NOT NULL, _batch_id STRING NOT NULL, _record_source STRING NOT NULL,
  _rescued_data STRING
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: ongewijzigde prijsafspraken uit SharePoint.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS br_sales_budgets (
  budget_id STRING, budget_month DATE, country STRING, product_category STRING, currency_code STRING,
  budget_revenue DECIMAL(18,4), budget_units INT, forecast_revenue DECIMAL(18,4), forecast_units INT,
  approved_at TIMESTAMP, updated_at TIMESTAMP, is_deleted BOOLEAN,
  _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL, _source_file_path STRING NOT NULL,
  _source_file_name STRING NOT NULL, _source_file_size BIGINT, _source_file_mtime TIMESTAMP,
  _ingest_timestamp TIMESTAMP NOT NULL, _batch_id STRING NOT NULL, _record_source STRING NOT NULL,
  _rescued_data STRING
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: ongewijzigde verkoopbudgetten uit SharePoint.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA nager;

CREATE TABLE IF NOT EXISTS br_holidays_nl (
  date DATE, localName STRING, name STRING, countryCode STRING, fixed BOOLEAN, `global` BOOLEAN,
  counties ARRAY<STRING>, launchYear INT, types ARRAY<STRING>,
  _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL, _source_file_path STRING NOT NULL,
  _source_file_name STRING NOT NULL, _source_file_size BIGINT, _source_file_mtime TIMESTAMP,
  _ingest_timestamp TIMESTAMP NOT NULL, _batch_id STRING NOT NULL, _record_source STRING NOT NULL,
  _rescued_data STRING
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Bronze: ongewijzigde Nederlandse feestdagen uit Nager.Date.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);
