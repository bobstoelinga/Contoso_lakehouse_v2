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
