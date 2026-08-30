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

CREATE TABLE IF NOT EXISTS fct_sales_v1 (
  sales_line_hk     STRING NOT NULL,
  order_hk          STRING NOT NULL,
  product_hk        STRING NOT NULL,
  customer_hk       STRING,
  order_key         STRING NOT NULL,
  order_line_number INT    NOT NULL,
  order_date        DATE,
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
  _as_of_delivery_id STRING    NOT NULL,
  _as_of_timestamp   TIMESTAMP NOT NULL,
  _batch_id          STRING    NOT NULL
)
USING DELTA CLUSTER BY (order_date, customer_hk, product_hk)
COMMENT 'Gold Actueel slot 1 voor fct_sales.';

CREATE TABLE IF NOT EXISTS fct_sales_v2 LIKE fct_sales_v1;

-- -----------------------------------------------------------------------------
-- Publieke views (de pointer). Alleen deze objecten zijn zichtbaar voor BI.
-- -----------------------------------------------------------------------------
USE SCHEMA current;

CREATE OR REPLACE VIEW dim_customer
COMMENT 'Gold Actueel: klantdimensie van de laatste succesvolle business load.'
AS SELECT * FROM contoso_gold_${env}.current_internal.dim_customer_v1;

CREATE OR REPLACE VIEW dim_product
COMMENT 'Gold Actueel: productdimensie van de laatste succesvolle business load.'
AS SELECT * FROM contoso_gold_${env}.current_internal.dim_product_v1;

CREATE OR REPLACE VIEW fct_sales
COMMENT 'Gold Actueel: verkoopfeiten van de laatste succesvolle business load.'
AS SELECT * FROM contoso_gold_${env}.current_internal.fct_sales_v1;

-- -----------------------------------------------------------------------------
-- Freshness monitoring: maakt zichtbaar hoe oud de actieve versie is.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_gold_freshness
COMMENT 'Actualiteit van elke Gold Actueel dataset; alarmeert bij bevroren marts.'
AS
SELECT 'dim_customer' AS entity, _as_of_delivery_id, _as_of_timestamp, _batch_id,
       round((unix_timestamp(current_timestamp()) - unix_timestamp(_as_of_timestamp)) / 3600.0, 2) AS freshness_hours
FROM dim_customer LIMIT 1
UNION ALL
SELECT 'dim_product', _as_of_delivery_id, _as_of_timestamp, _batch_id,
       round((unix_timestamp(current_timestamp()) - unix_timestamp(_as_of_timestamp)) / 3600.0, 2)
FROM dim_product LIMIT 1
UNION ALL
SELECT 'fct_sales', _as_of_delivery_id, _as_of_timestamp, _batch_id,
       round((unix_timestamp(current_timestamp()) - unix_timestamp(_as_of_timestamp)) / 3600.0, 2)
FROM fct_sales LIMIT 1;
