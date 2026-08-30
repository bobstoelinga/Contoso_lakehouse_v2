-- =============================================================================
-- 40_raw_vault.sql
-- Data Vault 2.0 — Raw Vault.
--
-- Ontwerpbesluiten (zie docs/00_besluitenlog.md):
--  * Satellites zijn INSERT-ONLY. Fysieke end-dating (UPDATE) schaalt niet in
--    Delta. load_end_date / is_current worden via een view afgeleid met LEAD().
--    De fysieke tabel heet <naam>_h; de view heet <naam> en is het contract
--    voor alle downstream lagen.
--  * Hash keys: SHA-256 over genormaliseerde business keys
--    (upper(trim(x)), NULL -> '^^', separator '||'). Zie dv_hash() in het framework.
--  * Multi-source: hub_* bevat bk_collision_code zodat sleutels uit verschillende
--    bronsystemen niet botsen.
--  * Liquid clustering in plaats van partitionering (beheerbaar bij honderden tabellen).
-- =============================================================================
USE CATALOG contoso_vault_${env};
USE SCHEMA raw_vault;

-- -----------------------------------------------------------------------------
-- HUBS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_customer (
  hk_customer        STRING    NOT NULL COMMENT 'SHA-256 over bk_collision_code || customer_key',
  customer_key       STRING    NOT NULL,
  bk_collision_code  STRING    NOT NULL COMMENT 'Onderscheidt gelijke sleutels uit verschillende bronsystemen',
  load_date          TIMESTAMP NOT NULL,
  record_source      STRING    NOT NULL,
  _batch_id          STRING    NOT NULL,
  CONSTRAINT pk_hub_customer PRIMARY KEY (hk_customer) RELY
)
USING DELTA
CLUSTER BY (hk_customer)
COMMENT 'Hub: unieke klant-business keys.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS hub_product (
  hk_product         STRING    NOT NULL,
  product_key        STRING    NOT NULL,
  bk_collision_code  STRING    NOT NULL,
  load_date          TIMESTAMP NOT NULL,
  record_source      STRING    NOT NULL,
  _batch_id          STRING    NOT NULL,
  CONSTRAINT pk_hub_product PRIMARY KEY (hk_product) RELY
)
USING DELTA
CLUSTER BY (hk_product)
COMMENT 'Hub: unieke product-business keys.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS hub_order (
  hk_order           STRING    NOT NULL,
  order_key          STRING    NOT NULL,
  bk_collision_code  STRING    NOT NULL,
  load_date          TIMESTAMP NOT NULL,
  record_source      STRING    NOT NULL,
  _batch_id          STRING    NOT NULL,
  CONSTRAINT pk_hub_order PRIMARY KEY (hk_order) RELY
)
USING DELTA
CLUSTER BY (hk_order)
COMMENT 'Hub: unieke order-business keys.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

-- -----------------------------------------------------------------------------
-- LINKS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lnk_order_customer (
  hk_order_customer  STRING    NOT NULL,
  hk_order           STRING    NOT NULL,
  hk_customer        STRING    NOT NULL,
  load_date          TIMESTAMP NOT NULL,
  record_source      STRING    NOT NULL,
  _batch_id          STRING    NOT NULL,
  CONSTRAINT pk_lnk_order_customer PRIMARY KEY (hk_order_customer) RELY,
  CONSTRAINT fk_loc_order    FOREIGN KEY (hk_order)    REFERENCES hub_order(hk_order) RELY,
  CONSTRAINT fk_loc_customer FOREIGN KEY (hk_customer) REFERENCES hub_customer(hk_customer) RELY
)
USING DELTA
CLUSTER BY (hk_order, hk_customer)
COMMENT 'Link: relatie order <-> klant.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS lnk_order_product (
  hk_order_product   STRING    NOT NULL,
  hk_order           STRING    NOT NULL,
  hk_product         STRING    NOT NULL,
  order_line_number  INT       NOT NULL COMMENT 'Degenerate key: onderscheidt orderregels',
  load_date          TIMESTAMP NOT NULL,
  record_source      STRING    NOT NULL,
  _batch_id          STRING    NOT NULL,
  CONSTRAINT pk_lnk_order_product PRIMARY KEY (hk_order_product) RELY,
  CONSTRAINT fk_lop_order   FOREIGN KEY (hk_order)   REFERENCES hub_order(hk_order) RELY,
  CONSTRAINT fk_lop_product FOREIGN KEY (hk_product) REFERENCES hub_product(hk_product) RELY
)
USING DELTA
CLUSTER BY (hk_order, hk_product)
COMMENT 'Link: orderregel = order <-> product.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

-- -----------------------------------------------------------------------------
-- SATELLITES (insert-only, fysiek)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_customer_h (
  hk_customer       STRING    NOT NULL,
  load_date         TIMESTAMP NOT NULL,
  hashdiff          STRING    NOT NULL,
  record_source     STRING    NOT NULL,
  _batch_id         STRING    NOT NULL,
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
  is_deleted        BOOLEAN,
  CONSTRAINT pk_sat_customer PRIMARY KEY (hk_customer, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_customer)
COMMENT 'Satellite (insert-only): beschrijvende klantattributen.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS sat_product_h (
  hk_product          STRING    NOT NULL,
  load_date           TIMESTAMP NOT NULL,
  hashdiff            STRING    NOT NULL,
  record_source       STRING    NOT NULL,
  _batch_id           STRING    NOT NULL,
  product_name        STRING,
  product_category    STRING,
  product_subcategory STRING,
  brand               STRING,
  unit_cost           DECIMAL(18,4),
  unit_price          DECIMAL(18,4),
  is_discontinued     BOOLEAN,
  is_deleted          BOOLEAN,
  CONSTRAINT pk_sat_product PRIMARY KEY (hk_product, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_product)
COMMENT 'Satellite (insert-only): beschrijvende productattributen.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS sat_order_h (
  hk_order      STRING    NOT NULL,
  load_date     TIMESTAMP NOT NULL,
  hashdiff      STRING    NOT NULL,
  record_source STRING    NOT NULL,
  _batch_id     STRING    NOT NULL,
  order_status  STRING,
  order_date    DATE,
  currency_code STRING,
  CONSTRAINT pk_sat_order PRIMARY KEY (hk_order, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_order)
COMMENT 'Satellite (insert-only): orderkop-attributen.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS sat_order_line_h (
  hk_order_product STRING    NOT NULL,
  load_date        TIMESTAMP NOT NULL,
  hashdiff         STRING    NOT NULL,
  record_source    STRING    NOT NULL,
  _batch_id        STRING    NOT NULL,
  quantity         INT,
  unit_price       DECIMAL(18,4),
  discount_amount  DECIMAL(18,4),
  net_amount       DECIMAL(18,4),
  ship_date        DATE,
  delivery_date    DATE,
  CONSTRAINT pk_sat_order_line PRIMARY KEY (hk_order_product, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_order_product)
COMMENT 'Link-satellite (insert-only): orderregel-attributen.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

-- -----------------------------------------------------------------------------
-- STATUS / RECORD-TRACKING SATELLITES (delete-detectie bij snapshotbronnen)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_customer_status_h (
  hk_customer   STRING    NOT NULL,
  load_date     TIMESTAMP NOT NULL,
  status        STRING    NOT NULL COMMENT 'I (nieuw) | U (aanwezig) | D (afwezig in snapshot)',
  record_source STRING    NOT NULL,
  _batch_id     STRING    NOT NULL,
  CONSTRAINT pk_sat_customer_status PRIMARY KEY (hk_customer, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_customer)
COMMENT 'Record-tracking satellite: detecteert sleutels die uit de snapshot verdwijnen.';

CREATE TABLE IF NOT EXISTS sat_product_status_h LIKE sat_customer_status_h;

-- -----------------------------------------------------------------------------
-- EFFECTIVITY SATELLITE (welke relatie is op enig moment geldig)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_eff_order_customer_h (
  hk_order_customer STRING    NOT NULL,
  hk_order          STRING    NOT NULL COMMENT 'Driving key: per order geldt maximaal één klant',
  load_date         TIMESTAMP NOT NULL,
  is_active         BOOLEAN   NOT NULL,
  record_source     STRING    NOT NULL,
  _batch_id         STRING    NOT NULL,
  CONSTRAINT pk_sat_eff_oc PRIMARY KEY (hk_order_customer, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_order)
COMMENT 'Effectivity satellite: maakt een oude order-klant relatie ongeldig bij overboeking.';

-- -----------------------------------------------------------------------------
-- VIEWS: end-dating zonder UPDATE. Dit is het contract voor Gold.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW sat_customer AS
SELECT *,
       lead(load_date) OVER (PARTITION BY hk_customer ORDER BY load_date) AS load_end_date,
       lead(load_date) OVER (PARTITION BY hk_customer ORDER BY load_date) IS NULL AS is_current
FROM sat_customer_h;

CREATE OR REPLACE VIEW sat_product AS
SELECT *,
       lead(load_date) OVER (PARTITION BY hk_product ORDER BY load_date) AS load_end_date,
       lead(load_date) OVER (PARTITION BY hk_product ORDER BY load_date) IS NULL AS is_current
FROM sat_product_h;

CREATE OR REPLACE VIEW sat_order AS
SELECT *,
       lead(load_date) OVER (PARTITION BY hk_order ORDER BY load_date) AS load_end_date,
       lead(load_date) OVER (PARTITION BY hk_order ORDER BY load_date) IS NULL AS is_current
FROM sat_order_h;

CREATE OR REPLACE VIEW sat_order_line AS
SELECT *,
       lead(load_date) OVER (PARTITION BY hk_order_product ORDER BY load_date) AS load_end_date,
       lead(load_date) OVER (PARTITION BY hk_order_product ORDER BY load_date) IS NULL AS is_current
FROM sat_order_line_h;

CREATE OR REPLACE VIEW sat_eff_order_customer AS
SELECT *,
       lead(load_date) OVER (PARTITION BY hk_order_customer ORDER BY load_date) AS load_end_date,
       lead(load_date) OVER (PARTITION BY hk_order_customer ORDER BY load_date) IS NULL AS is_current
FROM sat_eff_order_customer_h;
