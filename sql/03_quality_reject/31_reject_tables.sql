-- =============================================================================
-- 31_reject_tables.sql
-- Reject laag. Eén rij per afgekeurd bronrecord, met ALLE faalredenen en de
-- volledige originele payload zodat herverwerking mogelijk blijft.
-- =============================================================================
USE CATALOG contoso_reject_${env};
USE SCHEMA sales;

-- Generieke reject-structuur; identiek voor elk bronobject zodat de engine
-- één schrijfpad heeft. De brondata blijft als JSON behouden.
CREATE OR REPLACE TABLE rj_customers (
  reject_id         STRING    NOT NULL COMMENT 'UUID',
  _delivery_id      STRING    NOT NULL,
  _delivery_date    DATE      NOT NULL,
  _batch_id         STRING    NOT NULL,
  _run_id           STRING    NOT NULL,
  _record_source    STRING    NOT NULL,
  source_object_id  STRING    NOT NULL,
  business_key      STRING              COMMENT 'Samengestelde business key, indien beschikbaar',
  payload           STRING    NOT NULL  COMMENT 'Volledig bronrecord als JSON',
  failed_rules      ARRAY<STRUCT<
                       rule_id: STRING,
                       rule_name: STRING,
                       severity: STRING,
                       reason_code: STRING,
                       reason_text: STRING>> NOT NULL,
  reject_status     STRING    NOT NULL DEFAULT 'OPEN'
                    COMMENT 'OPEN | IN_REVIEW | RESOLVED | WONT_FIX | RESUBMITTED',
  resolved_at       TIMESTAMP,
  resolved_by       STRING,
  resubmitted_batch_id STRING,
  rejected_at       TIMESTAMP NOT NULL,
  CONSTRAINT pk_rj_customers PRIMARY KEY (reject_id) RELY
)
USING DELTA
PARTITIONED BY (_delivery_date)
COMMENT 'Reject: afgekeurde klantrecords, herverwerkbaar.'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE OR REPLACE TABLE rj_products LIKE rj_customers;
ALTER TABLE rj_products SET TBLPROPERTIES (comment = 'Reject: afgekeurde productrecords, herverwerkbaar.');

CREATE OR REPLACE TABLE rj_orders LIKE rj_customers;
ALTER TABLE rj_orders SET TBLPROPERTIES (comment = 'Reject: afgekeurde orderregels, herverwerkbaar.');

-- Operationeel overzicht voor data stewards.
CREATE OR REPLACE VIEW v_open_rejects
COMMENT 'Alle openstaande rejects over alle bronobjecten heen.'
AS
SELECT source_object_id, _delivery_id, _delivery_date, r.reason_code, r.reason_text, count(*) AS reject_count
FROM (
  SELECT * FROM rj_customers WHERE reject_status = 'OPEN'
  UNION ALL SELECT * FROM rj_products  WHERE reject_status = 'OPEN'
  UNION ALL SELECT * FROM rj_orders    WHERE reject_status = 'OPEN'
)
LATERAL VIEW explode(failed_rules) AS r
GROUP BY ALL;
