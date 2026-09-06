-- =============================================================================
-- 50_gold_historical.sql
-- Gold Historisch: volledige SCD2 historie. Wordt met MERGE bijgewerkt op basis
-- van meta_gold_entity.select_sql (gold_layer = 'HISTORICAL').
-- =============================================================================
USE CATALOG contoso_gold_${env};
USE SCHEMA historical;

CREATE TABLE IF NOT EXISTS dim_customer_hist (
  customer_hk      STRING    NOT NULL,
  customer_key     STRING    NOT NULL,
  customer_name    STRING,
  email            STRING,
  phone            STRING,
  full_address     STRING,
  city             STRING,
  state_province   STRING,
  postal_code      STRING,
  country_code     STRING,
  customer_segment STRING,
  customer_type    STRING,
  tenure_band      STRING,
  is_contactable   BOOLEAN,
  customer_since   DATE,
  is_deleted       BOOLEAN,
  valid_from       TIMESTAMP NOT NULL,
  valid_to         TIMESTAMP NOT NULL,
  is_current       BOOLEAN   NOT NULL,
  record_source    STRING    NOT NULL,
  _batch_id        STRING    NOT NULL,
  _loaded_at       TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_customer_hist PRIMARY KEY (customer_hk, valid_from) RELY
)
USING DELTA
CLUSTER BY (customer_hk)
COMMENT 'Gold Historisch: klantdimensie met volledige SCD2 historie.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS dim_product_hist (
  product_hk          STRING    NOT NULL,
  product_key         STRING    NOT NULL,
  product_name        STRING,
  product_category    STRING,
  product_subcategory STRING,
  brand               STRING,
  unit_cost           DECIMAL(18,4),
  unit_price          DECIMAL(18,4),
  unit_margin         DECIMAL(18,4),
  is_discontinued     BOOLEAN,
  is_deleted          BOOLEAN,
  valid_from          TIMESTAMP NOT NULL,
  valid_to            TIMESTAMP NOT NULL,
  is_current          BOOLEAN   NOT NULL,
  record_source       STRING    NOT NULL,
  _batch_id           STRING    NOT NULL,
  _loaded_at          TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_product_hist PRIMARY KEY (product_hk, valid_from) RELY
)
USING DELTA
CLUSTER BY (product_hk)
COMMENT 'Gold Historisch: productdimensie met volledige SCD2 historie.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS dim_employee_hist (
  employee_hk  STRING NOT NULL,
  employee_key STRING NOT NULL,
  first_name   STRING,
  last_name    STRING,
  job_title    STRING,
  office_city  STRING,
  hire_date    DATE,
  is_deleted   BOOLEAN,
  valid_from   TIMESTAMP NOT NULL,
  valid_to     TIMESTAMP NOT NULL,
  is_current   BOOLEAN NOT NULL,
  record_source STRING NOT NULL,
  _batch_id    STRING NOT NULL,
  _loaded_at   TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_employee_hist PRIMARY KEY (employee_hk, valid_from) RELY
)
USING DELTA
CLUSTER BY (employee_hk)
COMMENT 'Gold Historisch: medewerkerdimensie met volledige SCD2 historie.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS fct_sales_hist (
  sales_line_hk     STRING    NOT NULL,
  order_hk          STRING    NOT NULL,
  product_hk        STRING    NOT NULL,
  customer_hk       STRING,
  employee_hk       STRING,
  order_key         STRING    NOT NULL,
  order_line_number INT       NOT NULL,
  order_date        DATE,
  order_date_key    INT,
  order_status      STRING,
  currency_code     STRING,
  quantity          INT,
  unit_price        DECIMAL(18,4),
  discount_amount   DECIMAL(18,4),
  gross_amount      DECIMAL(18,4),
  net_amount        DECIMAL(18,4),
  discount_rate     DECIMAL(9,6),
  lead_time_days    INT,
  is_cancelled      BOOLEAN,
  ship_date         DATE,
  delivery_date     DATE,
  ship_date_key     INT,
  delivery_date_key INT,
  valid_from        TIMESTAMP NOT NULL,
  valid_to          TIMESTAMP NOT NULL,
  is_current        BOOLEAN   NOT NULL,
  record_source     STRING    NOT NULL,
  _batch_id         STRING    NOT NULL,
  _loaded_at        TIMESTAMP NOT NULL,
  CONSTRAINT pk_fct_sales_hist PRIMARY KEY (sales_line_hk, valid_from) RELY
)
USING DELTA
CLUSTER BY (order_date, customer_hk, product_hk)
COMMENT 'Gold Historisch: verkoopfeiten met volledige SCD2 historie.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS fct_returns_hist (
  return_line_hk     STRING NOT NULL,
  return_hk          STRING NOT NULL,
  order_hk           STRING NOT NULL,
  product_hk         STRING NOT NULL,
  customer_hk        STRING,
  employee_hk        STRING,
  return_key         STRING NOT NULL,
  order_key          STRING NOT NULL,
  order_line_number  INT NOT NULL,
  return_date        DATE,
  return_date_key    INT,
  return_status      STRING,
  return_reason_code STRING,
  return_quantity    INT,
  refund_amount      DECIMAL(18,4),
  currency_code      STRING,
  valid_from         TIMESTAMP NOT NULL,
  valid_to           TIMESTAMP NOT NULL,
  is_current         BOOLEAN NOT NULL,
  record_source      STRING NOT NULL,
  _batch_id          STRING NOT NULL,
  _loaded_at         TIMESTAMP NOT NULL,
  CONSTRAINT pk_fct_returns_hist PRIMARY KEY (return_line_hk, valid_from) RELY
)
USING DELTA
CLUSTER BY (return_date, employee_hk, product_hk)
COMMENT 'Gold Historisch: retourfeiten met medewerker- en datumrelaties.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS dim_price_agreement_hist (
  price_agreement_hk STRING NOT NULL, agreement_id STRING NOT NULL, customer_segment STRING,
  product_key STRING, currency_code STRING, list_price DECIMAL(18,4), discount_pct DECIMAL(9,4),
  valid_from DATE, valid_to DATE, agreement_status STRING, updated_at TIMESTAMP, is_deleted BOOLEAN,
  version_valid_from TIMESTAMP NOT NULL, version_valid_to TIMESTAMP NOT NULL, is_current BOOLEAN NOT NULL,
  record_source STRING NOT NULL, _batch_id STRING NOT NULL, _loaded_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_price_agreement_hist PRIMARY KEY (price_agreement_hk, version_valid_from) RELY
)
USING DELTA CLUSTER BY (price_agreement_hk)
COMMENT 'Gold Historisch: prijsafspraken met technische en zakelijke geldigheid.';

CREATE TABLE IF NOT EXISTS fct_sales_budget_hist (
  sales_budget_hk STRING NOT NULL, budget_month DATE NOT NULL, country_code STRING NOT NULL,
  product_category STRING NOT NULL, budget_id STRING, currency_code STRING, budget_revenue DECIMAL(18,4),
  budget_units INT, forecast_revenue DECIMAL(18,4), forecast_units INT, approved_at TIMESTAMP,
  updated_at TIMESTAMP, is_deleted BOOLEAN, valid_from TIMESTAMP NOT NULL, valid_to TIMESTAMP NOT NULL,
  is_current BOOLEAN NOT NULL, record_source STRING NOT NULL, _batch_id STRING NOT NULL, _loaded_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_fct_sales_budget_hist PRIMARY KEY (sales_budget_hk, valid_from) RELY
)
USING DELTA CLUSTER BY (budget_month, country_code, product_category)
COMMENT 'Gold Historisch: maandbudgetten en forecasts.';

CREATE TABLE IF NOT EXISTS fct_cbs_jeugdzorg_hist (
  cbs_jeugdzorg_hk STRING NOT NULL, vorm_code STRING NOT NULL, wijk_code STRING NOT NULL,
  periode_code STRING NOT NULL, jongeren_totaal BIGINT, trajecten_totaal BIGINT, gemeente_naam STRING,
  regio_type STRING, valid_from TIMESTAMP NOT NULL, valid_to TIMESTAMP NOT NULL, is_current BOOLEAN NOT NULL,
  record_source STRING NOT NULL, _batch_id STRING NOT NULL, _loaded_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_fct_cbs_jeugdzorg_hist PRIMARY KEY (cbs_jeugdzorg_hk, valid_from) RELY
)
USING DELTA CLUSTER BY (periode_code, wijk_code)
COMMENT 'Gold Historisch: CBS-jeugdzorgcijfers per vorm, wijk en periode.';

CREATE TABLE IF NOT EXISTS dim_ecb_exchange_rate_hist (
  currency_code STRING NOT NULL, rate_date DATE NOT NULL, rate_to_eur DECIMAL(18,8) NOT NULL,
  source_series STRING NOT NULL, version_valid_from TIMESTAMP NOT NULL, version_valid_to TIMESTAMP NOT NULL,
  is_current BOOLEAN NOT NULL, record_source STRING NOT NULL, _batch_id STRING NOT NULL, _loaded_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_ecb_exchange_rate_hist PRIMARY KEY (currency_code, rate_date, version_valid_from) RELY
)
USING DELTA CLUSTER BY (currency_code, rate_date)
COMMENT 'Gold Historisch: ECB-wisselkoersen met volledige referentiehistorie.';

CREATE TABLE IF NOT EXISTS dim_nager_holiday_hist (
  holiday_date DATE NOT NULL, holiday_name STRING NOT NULL, local_name STRING, country_code STRING NOT NULL,
  global_holiday BOOLEAN NOT NULL, holiday_types ARRAY<STRING>, version_valid_from TIMESTAMP NOT NULL,
  version_valid_to TIMESTAMP NOT NULL, is_current BOOLEAN NOT NULL, record_source STRING NOT NULL,
  _batch_id STRING NOT NULL, _loaded_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_nager_holiday_hist PRIMARY KEY (holiday_date, country_code, holiday_name, version_valid_from) RELY
)
USING DELTA CLUSTER BY (holiday_date, country_code)
COMMENT 'Gold Historisch: Nederlandse feestdagen met volledige referentiehistorie.';

CREATE TABLE IF NOT EXISTS fct_fabric_sales_order_line_hist (
  sales_order_detail_id BIGINT NOT NULL, order_quantity INT NOT NULL,
  unit_price DECIMAL(18,4) NOT NULL, unit_price_discount DECIMAL(9,6) NOT NULL,
  total_due_amount DECIMAL(18,4) NOT NULL, order_status_code STRING NOT NULL,
  source_last_modified_at TIMESTAMP NOT NULL, is_deleted BOOLEAN NOT NULL,
  version_valid_from TIMESTAMP NOT NULL, version_valid_to TIMESTAMP NOT NULL,
  is_current BOOLEAN NOT NULL, record_source STRING NOT NULL, _batch_id STRING NOT NULL,
  _loaded_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_fct_fabric_sales_order_line_hist PRIMARY KEY (sales_order_detail_id, version_valid_from) RELY
)
USING DELTA CLUSTER BY (sales_order_detail_id, source_last_modified_at)
COMMENT 'Gold Historisch: Fabric SQL orderregelcontract met referentiehistorie.';
