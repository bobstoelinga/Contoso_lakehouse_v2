-- =============================================================================
-- 51_gold_current.sql
-- Gold Actueel — publish-by-pointer.
--
-- Werking:
--  1. De engine bouwt een nieuwe versie in het inactieve slot
--     contoso_gold_${env}.current_internal.<tabel>_v1 of _v2.
--  2. Pas als ALLE entiteiten van dezelfde publication_group succesvol zijn
--     gebouwd, worden de views in current in één stap omgezet.
--  3. Faalt een stap, dan blijven de views naar het vorige slot wijzen: de
--     vorige versie blijft dus actief en consistent.
--
-- Onderstaande DDL zet de startsituatie neer (v1 als actief slot).
-- =============================================================================
USE CATALOG contoso_gold_${env};

-- -----------------------------------------------------------------------------
-- Fysieke slots
-- -----------------------------------------------------------------------------
USE SCHEMA current_internal;

CREATE TABLE IF NOT EXISTS dim_customer_v1 (
  customer_hk      STRING NOT NULL,
  customer_key     STRING NOT NULL,
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
  last_changed_at  TIMESTAMP,
  _as_of_delivery_id STRING    NOT NULL,
  _as_of_timestamp   TIMESTAMP NOT NULL,
  _batch_id          STRING    NOT NULL
)
USING DELTA CLUSTER BY (customer_hk)
COMMENT 'Gold Actueel slot 1 voor dim_customer.';

CREATE TABLE IF NOT EXISTS dim_customer_v2 LIKE dim_customer_v1;
CREATE TABLE IF NOT EXISTS dim_product_v1 (
  product_hk          STRING NOT NULL,
  product_key         STRING NOT NULL,
  product_name        STRING,
  product_category    STRING,
  product_subcategory STRING,
  brand               STRING,
  unit_cost           DECIMAL(18,4),
  unit_price          DECIMAL(18,4),
  unit_margin         DECIMAL(18,4),
  is_discontinued     BOOLEAN,
  last_changed_at     TIMESTAMP,
  _as_of_delivery_id  STRING    NOT NULL,
  _as_of_timestamp    TIMESTAMP NOT NULL,
  _batch_id           STRING    NOT NULL
)
USING DELTA CLUSTER BY (product_hk)
COMMENT 'Gold Actueel slot 1 voor dim_product.';

CREATE TABLE IF NOT EXISTS dim_product_v2 LIKE dim_product_v1;

CREATE TABLE IF NOT EXISTS dim_employee_v1 (
  employee_hk STRING NOT NULL,
  employee_key STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  job_title STRING,
  office_city STRING,
  hire_date DATE,
  last_changed_at TIMESTAMP,
  _as_of_delivery_id STRING NOT NULL,
  _as_of_timestamp TIMESTAMP NOT NULL,
  _batch_id STRING NOT NULL
)
USING DELTA CLUSTER BY (employee_hk)
COMMENT 'Gold Actueel slot 1 voor dim_employee.';

CREATE TABLE IF NOT EXISTS dim_employee_v2 LIKE dim_employee_v1;

CREATE TABLE IF NOT EXISTS dim_date_v1 (
  date_key INT NOT NULL,
  calendar_date DATE NOT NULL,
  calendar_year INT,
  calendar_quarter INT,
  month_number INT,
  month_name STRING,
  week_number INT,
  day_of_month INT,
  day_of_week INT,
  day_name STRING,
  is_weekend BOOLEAN,
  _as_of_delivery_id STRING NOT NULL,
  _as_of_timestamp TIMESTAMP NOT NULL,
  _batch_id STRING NOT NULL
)
USING DELTA CLUSTER BY (date_key)
COMMENT 'Gold Actueel slot 1 voor dim_date.';

CREATE TABLE IF NOT EXISTS dim_date_v2 LIKE dim_date_v1;

CREATE TABLE IF NOT EXISTS fct_sales_v1 (
  sales_line_hk     STRING NOT NULL,
  order_hk          STRING NOT NULL,
  product_hk        STRING NOT NULL,
  customer_hk       STRING,
  employee_hk       STRING,
  order_key         STRING NOT NULL,
  order_line_number INT    NOT NULL,
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
  _as_of_delivery_id STRING    NOT NULL,
  _as_of_timestamp   TIMESTAMP NOT NULL,
  _batch_id          STRING    NOT NULL
)
USING DELTA CLUSTER BY (order_date, customer_hk, product_hk)
COMMENT 'Gold Actueel slot 1 voor fct_sales.';

CREATE TABLE IF NOT EXISTS fct_sales_v2 LIKE fct_sales_v1;

CREATE TABLE IF NOT EXISTS fct_returns_v1 (
  return_line_hk STRING NOT NULL,
  return_hk STRING NOT NULL,
  order_hk STRING NOT NULL,
  product_hk STRING NOT NULL,
  customer_hk STRING,
  employee_hk STRING,
  return_key STRING NOT NULL,
  order_key STRING NOT NULL,
  order_line_number INT NOT NULL,
  return_date DATE,
  return_date_key INT,
  return_status STRING,
  return_reason_code STRING,
  return_quantity INT,
  refund_amount DECIMAL(18,4),
  currency_code STRING,
  _as_of_delivery_id STRING NOT NULL,
  _as_of_timestamp TIMESTAMP NOT NULL,
  _batch_id STRING NOT NULL
)
USING DELTA CLUSTER BY (return_date, employee_hk, product_hk)
COMMENT 'Gold Actueel slot 1 voor fct_returns.';

CREATE TABLE IF NOT EXISTS fct_returns_v2 LIKE fct_returns_v1;

-- -----------------------------------------------------------------------------
-- Publieke views. Alleen deze objecten zijn zichtbaar voor BI.
-- De groepsreleasepointer wordt in één Delta MERGE bijgewerkt; zo zien alle
-- views binnen SALES_MART steeds dezelfde batch, zonder per-view DDL-switch.
-- -----------------------------------------------------------------------------
USE SCHEMA current;

CREATE OR REPLACE VIEW dim_customer
COMMENT 'Gold Actueel: klantdimensie van de laatste succesvolle business load.'
AS
SELECT * FROM contoso_gold_${env}.current_internal.dim_customer_v1
WHERE EXISTS (
  SELECT 1
  FROM contoso_meta_${env}.audit.audit_gold_publication_group g
  JOIN contoso_meta_${env}.audit.audit_gold_publication p
    ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_CUSTOMER'
  WHERE g.publication_group_id = 'SALES_MART'
    AND g.release_status = 'ACTIVE'
    AND p.physical_slot = 'dim_customer_v1'
)
UNION ALL
SELECT * FROM contoso_gold_${env}.current_internal.dim_customer_v2
WHERE EXISTS (
  SELECT 1
  FROM contoso_meta_${env}.audit.audit_gold_publication_group g
  JOIN contoso_meta_${env}.audit.audit_gold_publication p
    ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_CUSTOMER'
  WHERE g.publication_group_id = 'SALES_MART'
    AND g.release_status = 'ACTIVE'
    AND p.physical_slot = 'dim_customer_v2'
);

CREATE OR REPLACE VIEW dim_product
COMMENT 'Gold Actueel: productdimensie van de laatste succesvolle business load.'
AS
SELECT * FROM contoso_gold_${env}.current_internal.dim_product_v1
WHERE EXISTS (
  SELECT 1
  FROM contoso_meta_${env}.audit.audit_gold_publication_group g
  JOIN contoso_meta_${env}.audit.audit_gold_publication p
    ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_PRODUCT'
  WHERE g.publication_group_id = 'SALES_MART'
    AND g.release_status = 'ACTIVE'
    AND p.physical_slot = 'dim_product_v1'
)
UNION ALL
SELECT * FROM contoso_gold_${env}.current_internal.dim_product_v2
WHERE EXISTS (
  SELECT 1
  FROM contoso_meta_${env}.audit.audit_gold_publication_group g
  JOIN contoso_meta_${env}.audit.audit_gold_publication p
    ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_PRODUCT'
  WHERE g.publication_group_id = 'SALES_MART'
    AND g.release_status = 'ACTIVE'
    AND p.physical_slot = 'dim_product_v2'
);

  CREATE OR REPLACE VIEW dim_employee
  COMMENT 'Gold Actueel: medewerkerdimensie van de laatste succesvolle business load.'
  AS
  SELECT * FROM contoso_gold_${env}.current_internal.dim_employee_v1
  WHERE EXISTS (SELECT 1 FROM contoso_meta_${env}.audit.audit_gold_publication_group g JOIN contoso_meta_${env}.audit.audit_gold_publication p ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_EMPLOYEE' WHERE g.publication_group_id = 'SALES_MART' AND g.release_status = 'ACTIVE' AND p.physical_slot = 'dim_employee_v1')
  UNION ALL
  SELECT * FROM contoso_gold_${env}.current_internal.dim_employee_v2
  WHERE EXISTS (SELECT 1 FROM contoso_meta_${env}.audit.audit_gold_publication_group g JOIN contoso_meta_${env}.audit.audit_gold_publication p ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_EMPLOYEE' WHERE g.publication_group_id = 'SALES_MART' AND g.release_status = 'ACTIVE' AND p.physical_slot = 'dim_employee_v2');

  CREATE OR REPLACE VIEW dim_date
  COMMENT 'Gold Actueel: kalenderdimensie voor verkoop- en retourdatums.'
  AS
  SELECT * FROM contoso_gold_${env}.current_internal.dim_date_v1
  WHERE EXISTS (SELECT 1 FROM contoso_meta_${env}.audit.audit_gold_publication_group g JOIN contoso_meta_${env}.audit.audit_gold_publication p ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_DATE' WHERE g.publication_group_id = 'SALES_MART' AND g.release_status = 'ACTIVE' AND p.physical_slot = 'dim_date_v1')
  UNION ALL
  SELECT * FROM contoso_gold_${env}.current_internal.dim_date_v2
  WHERE EXISTS (SELECT 1 FROM contoso_meta_${env}.audit.audit_gold_publication_group g JOIN contoso_meta_${env}.audit.audit_gold_publication p ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_DIM_DATE' WHERE g.publication_group_id = 'SALES_MART' AND g.release_status = 'ACTIVE' AND p.physical_slot = 'dim_date_v2');

CREATE OR REPLACE VIEW fct_sales
COMMENT 'Gold Actueel: verkoopfeiten van de laatste succesvolle business load.'
AS
SELECT * FROM contoso_gold_${env}.current_internal.fct_sales_v1
WHERE EXISTS (
  SELECT 1
  FROM contoso_meta_${env}.audit.audit_gold_publication_group g
  JOIN contoso_meta_${env}.audit.audit_gold_publication p
    ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_FCT_SALES'
  WHERE g.publication_group_id = 'SALES_MART'
    AND g.release_status = 'ACTIVE'
    AND p.physical_slot = 'fct_sales_v1'
)
UNION ALL
SELECT * FROM contoso_gold_${env}.current_internal.fct_sales_v2
WHERE EXISTS (
  SELECT 1
  FROM contoso_meta_${env}.audit.audit_gold_publication_group g
  JOIN contoso_meta_${env}.audit.audit_gold_publication p
    ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_FCT_SALES'
  WHERE g.publication_group_id = 'SALES_MART'
    AND g.release_status = 'ACTIVE'
    AND p.physical_slot = 'fct_sales_v2'
);

  CREATE OR REPLACE VIEW fct_returns
  COMMENT 'Gold Actueel: retourfeiten van de laatste succesvolle business load.'
  AS
  SELECT * FROM contoso_gold_${env}.current_internal.fct_returns_v1
  WHERE EXISTS (SELECT 1 FROM contoso_meta_${env}.audit.audit_gold_publication_group g JOIN contoso_meta_${env}.audit.audit_gold_publication p ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_FCT_RETURNS' WHERE g.publication_group_id = 'SALES_MART' AND g.release_status = 'ACTIVE' AND p.physical_slot = 'fct_returns_v1')
  UNION ALL
  SELECT * FROM contoso_gold_${env}.current_internal.fct_returns_v2
  WHERE EXISTS (SELECT 1 FROM contoso_meta_${env}.audit.audit_gold_publication_group g JOIN contoso_meta_${env}.audit.audit_gold_publication p ON p.batch_id = g.batch_id AND p.gold_entity_id = 'GC_FCT_RETURNS' WHERE g.publication_group_id = 'SALES_MART' AND g.release_status = 'ACTIVE' AND p.physical_slot = 'fct_returns_v2');

-- -----------------------------------------------------------------------------
-- Freshness monitoring: maakt zichtbaar hoe oud de actieve versie is.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_gold_freshness
COMMENT 'Actualiteit van elke Gold Actueel dataset; alarmeert bij bevroren marts.'
AS
WITH customer_freshness AS (
  SELECT 'dim_customer' AS entity, _as_of_delivery_id, _as_of_timestamp, _batch_id,
         round((unix_timestamp(current_timestamp()) - unix_timestamp(_as_of_timestamp)) / 3600.0, 2) AS freshness_hours
  FROM dim_customer
  LIMIT 1
), product_freshness AS (
  SELECT 'dim_product' AS entity, _as_of_delivery_id, _as_of_timestamp, _batch_id,
         round((unix_timestamp(current_timestamp()) - unix_timestamp(_as_of_timestamp)) / 3600.0, 2) AS freshness_hours
  FROM dim_product
  LIMIT 1
), sales_freshness AS (
  SELECT 'fct_sales' AS entity, _as_of_delivery_id, _as_of_timestamp, _batch_id,
         round((unix_timestamp(current_timestamp()) - unix_timestamp(_as_of_timestamp)) / 3600.0, 2) AS freshness_hours
  FROM fct_sales
  LIMIT 1
)
SELECT * FROM customer_freshness
UNION ALL
SELECT * FROM product_freshness
UNION ALL
SELECT * FROM sales_freshness;
