-- =============================================================================
-- 30_quality_tables.sql
-- Quality laag: getypeerde, gevalideerde records (alleen records die alle
-- ERROR-regels doorstaan). Kolommen volgen uit meta_mapping (target_layer=QUALITY).
-- =============================================================================
USE CATALOG contoso_quality_${env};
USE SCHEMA sales;

CREATE TABLE IF NOT EXISTS qa_customers (
  customer_key      STRING  NOT NULL,
  customer_name     STRING,
  email             STRING,
  phone             STRING,
  address_line1     STRING,
  city              STRING,
  state_province    STRING,
  postal_code       STRING,
  country_code      STRING,
  customer_segment  STRING,
  customer_since    DATE,
  is_deleted        BOOLEAN NOT NULL,

  _delivery_id      STRING    NOT NULL,
  _delivery_date    DATE      NOT NULL,
  _batch_id         STRING    NOT NULL,
  _record_source    STRING    NOT NULL,
  _quality_status   STRING    NOT NULL COMMENT 'PASSED | PASSED_WITH_WARNINGS',
  _warning_codes    ARRAY<STRING>,
  _processed_at     TIMESTAMP NOT NULL,
  CONSTRAINT pk_qa_customers PRIMARY KEY (customer_key, _delivery_id) RELY
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Quality: goedgekeurde klantrecords.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS qa_products (
  product_key         STRING  NOT NULL,
  product_name        STRING,
  product_category    STRING,
  product_subcategory STRING,
  brand               STRING,
  unit_cost           DECIMAL(18,4),
  unit_price          DECIMAL(18,4),
  is_discontinued     BOOLEAN NOT NULL,
  unit_margin         DECIMAL(18,4),
  is_deleted          BOOLEAN NOT NULL,

  _delivery_id      STRING    NOT NULL,
  _delivery_date    DATE      NOT NULL,
  _batch_id         STRING    NOT NULL,
  _record_source    STRING    NOT NULL,
  _quality_status   STRING    NOT NULL,
  _warning_codes    ARRAY<STRING>,
  _processed_at     TIMESTAMP NOT NULL,
  CONSTRAINT pk_qa_products PRIMARY KEY (product_key, _delivery_id) RELY
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Quality: goedgekeurde productrecords.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS qa_orders (
  order_key         STRING  NOT NULL,
  order_line_number INT     NOT NULL,
  customer_key      STRING  NOT NULL,
  product_key       STRING  NOT NULL,
  order_date        DATE    NOT NULL,
  ship_date         DATE,
  delivery_date     DATE,
  order_status      STRING,
  quantity          INT     NOT NULL,
  unit_price        DECIMAL(18,4),
  discount_amount   DECIMAL(18,4) NOT NULL,
  net_amount        DECIMAL(18,4),
  currency_code     STRING  NOT NULL,

  _delivery_id      STRING    NOT NULL,
  _delivery_date    DATE      NOT NULL,
  _batch_id         STRING    NOT NULL,
  _record_source    STRING    NOT NULL,
  _quality_status   STRING    NOT NULL,
  _warning_codes    ARRAY<STRING>,
  _processed_at     TIMESTAMP NOT NULL,
  CONSTRAINT pk_qa_orders PRIMARY KEY (order_key, order_line_number, _delivery_id) RELY
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Quality: goedgekeurde orderregels.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);
