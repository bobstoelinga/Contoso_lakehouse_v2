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
