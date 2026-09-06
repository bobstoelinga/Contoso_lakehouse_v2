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
  employee_key      STRING  NOT NULL,
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

CREATE TABLE IF NOT EXISTS qa_employees (
  employee_key      STRING  NOT NULL,
  first_name        STRING,
  last_name         STRING,
  job_title         STRING,
  office_city       STRING,
  hire_date         DATE,
  is_deleted        BOOLEAN NOT NULL,

  _delivery_id      STRING    NOT NULL,
  _delivery_date    DATE      NOT NULL,
  _batch_id         STRING    NOT NULL,
  _record_source    STRING    NOT NULL,
  _quality_status   STRING    NOT NULL COMMENT 'PASSED | PASSED_WITH_WARNINGS',
  _warning_codes    ARRAY<STRING>,
  _processed_at     TIMESTAMP NOT NULL,
  CONSTRAINT pk_qa_employees PRIMARY KEY (employee_key, _delivery_id) RELY
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Quality: goedgekeurde medewerkerrecords.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS qa_returns (
  return_key         STRING  NOT NULL,
  order_key          STRING  NOT NULL,
  order_line_number  INT     NOT NULL,
  product_key        STRING  NOT NULL,
  employee_key       STRING  NOT NULL,
  return_date        DATE    NOT NULL,
  return_status      STRING  NOT NULL,
  return_reason_code STRING,
  return_quantity    INT     NOT NULL,
  refund_amount      DECIMAL(18,4),
  currency_code      STRING  NOT NULL,

  _delivery_id      STRING    NOT NULL,
  _delivery_date    DATE      NOT NULL,
  _batch_id         STRING    NOT NULL,
  _record_source    STRING    NOT NULL,
  _quality_status   STRING    NOT NULL,
  _warning_codes    ARRAY<STRING>,
  _processed_at     TIMESTAMP NOT NULL,
  CONSTRAINT pk_qa_returns PRIMARY KEY (return_key, _delivery_id) RELY
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Quality: goedgekeurde retourregels.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA cbs;

CREATE TABLE IF NOT EXISTS qa_jeugdzorg_wijk_2025 (
  vorm_code        STRING NOT NULL,
  wijk_code        STRING NOT NULL,
  periode_code     STRING NOT NULL,
  jongeren_totaal  BIGINT,
  trajecten_totaal BIGINT,
  gemeente_naam    STRING,
  regio_type       STRING,
  _delivery_id     STRING NOT NULL,
  _delivery_date   DATE NOT NULL,
  _batch_id        STRING NOT NULL,
  _record_source   STRING NOT NULL,
  _quality_status  STRING NOT NULL,
  _warning_codes   ARRAY<STRING>,
  _processed_at    TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Quality: gevalideerde CBS-jeugdzorgcijfers.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA ecb;

CREATE TABLE IF NOT EXISTS qa_exchange_rate (
  currency_code    STRING NOT NULL,
  rate_date        DATE NOT NULL,
  rate_to_eur      DECIMAL(18,8) NOT NULL,
  source_series    STRING NOT NULL,
  _delivery_id     STRING NOT NULL,
  _delivery_date   DATE NOT NULL,
  _batch_id        STRING NOT NULL,
  _record_source   STRING NOT NULL,
  _quality_status  STRING NOT NULL,
  _warning_codes   ARRAY<STRING>,
  _processed_at    TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Quality: gevalideerde ECB dagelijkse wisselkoersen tegenover EUR.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA sharepoint;

CREATE TABLE IF NOT EXISTS qa_price_agreements (
  agreement_id STRING NOT NULL, customer_segment STRING NOT NULL, product_key STRING NOT NULL,
  currency_code STRING NOT NULL, list_price DECIMAL(18,4) NOT NULL, discount_pct DECIMAL(9,4) NOT NULL,
  valid_from DATE NOT NULL, valid_to DATE, agreement_status STRING NOT NULL, updated_at TIMESTAMP NOT NULL,
  is_deleted BOOLEAN NOT NULL, _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL,
  _batch_id STRING NOT NULL, _record_source STRING NOT NULL, _quality_status STRING NOT NULL,
  _warning_codes ARRAY<STRING>, _processed_at TIMESTAMP NOT NULL
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Quality: getypeerde en gevalideerde prijsafspraken uit SharePoint.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS qa_sales_budgets (
  budget_id STRING NOT NULL, budget_month DATE NOT NULL, country_code STRING NOT NULL,
  product_category STRING NOT NULL, currency_code STRING NOT NULL, budget_revenue DECIMAL(18,4) NOT NULL,
  budget_units INT NOT NULL, forecast_revenue DECIMAL(18,4) NOT NULL, forecast_units INT NOT NULL,
  approved_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, is_deleted BOOLEAN NOT NULL,
  _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL, _batch_id STRING NOT NULL,
  _record_source STRING NOT NULL, _quality_status STRING NOT NULL, _warning_codes ARRAY<STRING>,
  _processed_at TIMESTAMP NOT NULL
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Quality: getypeerde en gevalideerde verkoopbudgetten uit SharePoint.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA nager;

CREATE TABLE IF NOT EXISTS qa_holidays_nl (
  holiday_date DATE NOT NULL, holiday_name STRING NOT NULL, local_name STRING,
  country_code STRING NOT NULL, global_holiday BOOLEAN NOT NULL, holiday_types ARRAY<STRING>,
  _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL, _batch_id STRING NOT NULL,
  _record_source STRING NOT NULL, _quality_status STRING NOT NULL, _warning_codes ARRAY<STRING>,
  _processed_at TIMESTAMP NOT NULL
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Quality: gevalideerde Nederlandse feestdagen uit Nager.Date.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

USE SCHEMA fabric_sales;

CREATE TABLE IF NOT EXISTS qa_order_lines (
  sales_order_detail_id BIGINT NOT NULL, order_quantity INT NOT NULL,
  unit_price DECIMAL(18,4) NOT NULL, unit_price_discount DECIMAL(9,6) NOT NULL,
  total_due_amount DECIMAL(18,4) NOT NULL, order_status_code STRING NOT NULL,
  source_last_modified_at TIMESTAMP NOT NULL, is_deleted BOOLEAN NOT NULL,
  _delivery_id STRING NOT NULL, _delivery_date DATE NOT NULL, _batch_id STRING NOT NULL,
  _record_source STRING NOT NULL, _quality_status STRING NOT NULL,
  _warning_codes ARRAY<STRING>, _processed_at TIMESTAMP NOT NULL
)
USING DELTA PARTITIONED BY (_delivery_date)
COMMENT 'Quality: gevalideerde Fabric SQL orderregels.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);
