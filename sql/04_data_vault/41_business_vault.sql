-- =============================================================================
-- 41_business_vault.sql
-- Business Vault: computed satellites + PIT tabel.
-- Zelfde insert-only principe als de raw vault.
-- =============================================================================
USE CATALOG contoso_vault_${env};
USE SCHEMA business_vault;

CREATE TABLE IF NOT EXISTS sat_customer_bv_h (
  hk_customer     STRING    NOT NULL,
  load_date       TIMESTAMP NOT NULL,
  hashdiff        STRING    NOT NULL,
  record_source   STRING    NOT NULL,
  _batch_id       STRING    NOT NULL,
  full_address    STRING,
  customer_type   STRING    COMMENT 'B2B | B2C | UNKNOWN, afgeleid van customer_segment',
  tenure_band     STRING    COMMENT 'NEW | ESTABLISHED | LOYAL | UNKNOWN',
  is_contactable  BOOLEAN,
  CONSTRAINT pk_sat_customer_bv PRIMARY KEY (hk_customer, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_customer)
COMMENT 'Business Vault satellite: afgeleide klantkenmerken.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE TABLE IF NOT EXISTS sat_order_line_bv_h (
  hk_order_product STRING    NOT NULL,
  load_date        TIMESTAMP NOT NULL,
  hashdiff         STRING    NOT NULL,
  record_source    STRING    NOT NULL,
  _batch_id        STRING    NOT NULL,
  gross_amount     DECIMAL(18,4),
  net_amount_calc  DECIMAL(18,4),
  discount_rate    DECIMAL(9,6),
  lead_time_days   INT,
  is_cancelled     BOOLEAN,
  CONSTRAINT pk_sat_order_line_bv PRIMARY KEY (hk_order_product, load_date) RELY
)
USING DELTA
CLUSTER BY (hk_order_product)
COMMENT 'Business Vault satellite: berekende orderregelbedragen en doorlooptijd.'
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

CREATE OR REPLACE VIEW sat_customer_bv AS
SELECT *,
       lead(load_date) OVER (PARTITION BY hk_customer ORDER BY load_date) AS load_end_date,
       lead(load_date) OVER (PARTITION BY hk_customer ORDER BY load_date) IS NULL AS is_current
FROM sat_customer_bv_h;

CREATE OR REPLACE VIEW sat_order_line_bv AS
SELECT *,
       lead(load_date) OVER (PARTITION BY hk_order_product ORDER BY load_date) AS load_end_date,
       lead(load_date) OVER (PARTITION BY hk_order_product ORDER BY load_date) IS NULL AS is_current
FROM sat_order_line_bv_h;

-- -----------------------------------------------------------------------------
-- PIT: versnelt het samenvoegen van meerdere satellites op één moment.
-- Snapshotbeleid staat in meta_dv_entity (snapshot_frequency / retention).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_customer (
  hk_customer            STRING    NOT NULL,
  snapshot_date          TIMESTAMP NOT NULL,
  sat_customer_load_date TIMESTAMP,
  sat_customer_bv_load_date TIMESTAMP,
  _batch_id              STRING    NOT NULL,
  CONSTRAINT pk_pit_customer PRIMARY KEY (hk_customer, snapshot_date) RELY
)
USING DELTA
CLUSTER BY (snapshot_date, hk_customer)
COMMENT 'Point-in-Time tabel voor hub_customer over sat_customer en sat_customer_bv.';
