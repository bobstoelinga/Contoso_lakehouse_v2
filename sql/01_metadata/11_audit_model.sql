-- =============================================================================
-- 11_audit_model.sql
-- Runtime status. Dit bepaalt of vervolgstappen mogen starten.
-- =============================================================================
USE CATALOG contoso_meta_${env};
USE SCHEMA audit;

-- -----------------------------------------------------------------------------
-- 1. Levering (= één datumfolder in het landing volume)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_delivery (
  delivery_id           STRING    NOT NULL COMMENT 'SALES|2026-08-30',
  source_system_id      STRING    NOT NULL,
  delivery_date         DATE      NOT NULL COMMENT 'Afgeleid van de folder naam',
  delivery_folder       STRING    NOT NULL COMMENT 'Volledig volumepad van de datumfolder',
  expected_object_count INT       NOT NULL COMMENT 'Aantal verplichte objecten volgens metadata',
  loaded_object_count   INT       NOT NULL DEFAULT 0,
  delivery_status       STRING    NOT NULL COMMENT 'DETECTED | IN_PROGRESS | COMPLETE | FAILED | QUARANTINED | SUPERSEDED | LATE_ARRIVAL',
  delivery_sequence_number BIGINT NOT NULL
      COMMENT 'Verwerkingsvolgorde. Levering N+1 mag pas starten als N COMPLETE is.',
  first_seen_at         TIMESTAMP NOT NULL,
  completed_at          TIMESTAMP,
  superseded_at         TIMESTAMP,
  superseded_by         STRING,
  supersede_reason      STRING,
  supersede_approval_reference STRING,
  quarantined_at        TIMESTAMP,
  quarantine_reason     STRING,
  released_at           TIMESTAMP,
  released_by           STRING,
  release_reason        STRING,
  release_approval_reference STRING,
  CONSTRAINT pk_delivery PRIMARY KEY (delivery_id) RELY
)
USING DELTA
COMMENT 'Eén rij per logische levering. De gate voor alle vervolgverwerking.'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  delta.enableChangeDataFeed = true
);

-- -----------------------------------------------------------------------------
-- 2. Status per object binnen een levering
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_delivery_object (
  delivery_id           STRING    NOT NULL,
  source_object_id      STRING    NOT NULL,
  object_status         STRING    NOT NULL COMMENT 'PENDING | RUNNING | SUCCESS | FAILED | SKIPPED',
  files_processed       BIGINT    NOT NULL DEFAULT 0,
  rows_ingested         BIGINT    NOT NULL DEFAULT 0,
  bronze_table_version  BIGINT    COMMENT 'Delta versie van de bronze tabel na de load',
  new_columns_detected  ARRAY<STRING> COMMENT 'Door schema evolution toegevoegde kolommen',
  started_at            TIMESTAMP,
  ended_at              TIMESTAMP,
  error_message         STRING,
  CONSTRAINT pk_delivery_object PRIMARY KEY (delivery_id, source_object_id) RELY
)
USING DELTA
COMMENT 'Bronze laadstatus per object per levering.'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  delta.enableChangeDataFeed = true
);

-- -----------------------------------------------------------------------------
-- 3. Load runs (alle lagen)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_load_run (
  run_id                STRING    NOT NULL COMMENT 'UUID per uitgevoerde stap',
  batch_id              STRING    NOT NULL COMMENT 'Groepeert alle stappen van één end-to-end run',
  delivery_id           STRING,
  metadata_version      STRING    NOT NULL COMMENT 'SHA-256 fingerprint van de metadatarelease',
  layer                 STRING    NOT NULL COMMENT 'BRONZE | QUALITY | RAW_VAULT | BUSINESS_VAULT | GOLD_HIST | GOLD_CURR',
  entity_id             STRING    NOT NULL,
  run_status            STRING    NOT NULL COMMENT 'RUNNING | SUCCESS | FAILED | SKIPPED',
  rows_read             BIGINT    DEFAULT 0,
  rows_inserted         BIGINT    DEFAULT 0,
  rows_updated          BIGINT    DEFAULT 0,
  rows_rejected         BIGINT    DEFAULT 0,
  load_date             TIMESTAMP NOT NULL COMMENT 'Data Vault load_date; identiek voor de hele batch',
  started_at            TIMESTAMP NOT NULL,
  ended_at              TIMESTAMP,
  duration_seconds      DOUBLE,
  databricks_job_run_id STRING,
  error_message         STRING,
  CONSTRAINT pk_load_run PRIMARY KEY (run_id) RELY
)
USING DELTA
COMMENT 'Legacy snapshotregistratie; nieuwe runs schrijven append-only events.'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  delta.enableChangeDataFeed = true
);

CREATE TABLE IF NOT EXISTS audit_load_run_event (
  event_id              STRING    NOT NULL,
  run_id                STRING    NOT NULL,
  batch_id              STRING    NOT NULL,
  delivery_id           STRING,
  metadata_version      STRING    NOT NULL,
  layer                 STRING    NOT NULL,
  entity_id             STRING    NOT NULL,
  run_status            STRING    NOT NULL COMMENT 'RUNNING | SUCCESS | FAILED',
  rows_read             BIGINT    NOT NULL,
  rows_inserted         BIGINT    NOT NULL,
  rows_updated          BIGINT    NOT NULL,
  rows_rejected         BIGINT    NOT NULL,
  load_date             TIMESTAMP NOT NULL,
  started_at            TIMESTAMP NOT NULL,
  ended_at              TIMESTAMP NOT NULL,
  duration_seconds      DOUBLE,
  databricks_job_run_id STRING,
  error_message         STRING,
  CONSTRAINT pk_load_run_event PRIMARY KEY (event_id) RELY
)
USING DELTA
COMMENT 'Append-only load-run events; veilig voor gelijktijdige Serverless taken.';

CREATE OR REPLACE VIEW v_load_run_status AS
SELECT run_id, batch_id, delivery_id, metadata_version, layer, entity_id,
       run_status, rows_read, rows_inserted, rows_updated, rows_rejected,
       load_date, started_at, ended_at, duration_seconds, databricks_job_run_id,
       error_message
FROM audit_load_run_event
QUALIFY row_number() OVER (PARTITION BY run_id ORDER BY ended_at DESC, event_id DESC) = 1;

-- -----------------------------------------------------------------------------
-- 4. Metadatareleases
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_metadata_version (
  metadata_version      STRING    NOT NULL COMMENT 'SHA-256 fingerprint van alle seed-metadata',
  deployed_at           TIMESTAMP NOT NULL,
  CONSTRAINT pk_metadata_version PRIMARY KEY (metadata_version) RELY
)
USING DELTA
COMMENT 'Onveranderlijke registratie van elke gedeployde metadatarelease.';

-- -----------------------------------------------------------------------------
-- 5. Kwaliteitsresultaten
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_dq_result (
  dq_result_id          STRING    NOT NULL,
  run_id                STRING    NOT NULL,
  batch_id              STRING    NOT NULL,
  delivery_id           STRING    NOT NULL,
  source_object_id      STRING    NOT NULL,
  rule_id               STRING    NOT NULL,
  rule_name             STRING    NOT NULL,
  severity              STRING    NOT NULL,
  rows_evaluated        BIGINT    NOT NULL,
  rows_passed           BIGINT    NOT NULL,
  rows_failed           BIGINT    NOT NULL,
  failed_pct            DOUBLE    NOT NULL,
  threshold_pct         DOUBLE,
  threshold_breached    BOOLEAN   NOT NULL DEFAULT false,
  evaluated_at          TIMESTAMP NOT NULL,
  CONSTRAINT pk_dq_result PRIMARY KEY (dq_result_id) RELY
)
USING DELTA
COMMENT 'Meetresultaat per kwaliteitsregel per run.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 5. Gold Actueel publicaties (publish-by-pointer)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_gold_publication (
  publication_id        STRING    NOT NULL,
  gold_entity_id        STRING    NOT NULL,
  batch_id              STRING    NOT NULL,
  delivery_id           STRING,
  physical_slot         STRING    NOT NULL COMMENT 'Naam van de fysieke tabel: <table>_v1 of <table>_v2',
  publication_status    STRING    NOT NULL COMMENT 'BUILDING | ACTIVE | SUPERSEDED | FAILED',
  row_count             BIGINT,
  built_at              TIMESTAMP NOT NULL,
  published_at          TIMESTAMP COMMENT 'Moment waarop de view naar dit slot ging wijzen',
  superseded_at         TIMESTAMP,
  CONSTRAINT pk_gold_publication PRIMARY KEY (publication_id) RELY
)
USING DELTA
COMMENT 'Welke fysieke versie van een Gold Actueel dataset momenteel actief is.';

-- -----------------------------------------------------------------------------
-- 6. Actieve release per Gold-publicatiegroep
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_gold_publication_group (
  publication_group_id STRING    NOT NULL,
  batch_id             STRING    NOT NULL,
  delivery_id          STRING,
  release_status       STRING    NOT NULL COMMENT 'ACTIVE | FAILED',
  published_at         TIMESTAMP NOT NULL,
  CONSTRAINT pk_gold_publication_group PRIMARY KEY (publication_group_id) RELY
)
USING DELTA
COMMENT 'Atomische consumer-pointer per Gold Actueel publicatiegroep.';

-- -----------------------------------------------------------------------------
-- 7. Views voor de orchestratie-gates
-- -----------------------------------------------------------------------------

-- Is een levering compleet? (alle verplichte objecten SUCCESS in dezelfde datumfolder)
CREATE OR REPLACE VIEW v_delivery_readiness
COMMENT 'Gate: leveringen waarvan alle verplichte bronobjecten succesvol in Bronze staan.'
AS
SELECT
  d.delivery_id,
  d.source_system_id,
  d.delivery_date,
  d.delivery_folder,
  d.delivery_sequence_number,
  d.expected_object_count,
  count_if(o.object_status = 'SUCCESS')                                    AS success_count,
  count_if(o.object_status = 'FAILED')                                     AS failed_count,
  count_if(o.object_status IN ('PENDING', 'RUNNING'))                      AS pending_count,
  (count_if(o.object_status = 'SUCCESS') >= d.expected_object_count
     AND count_if(o.object_status = 'FAILED') = 0)                         AS is_ready,
  max(o.ended_at)                                                          AS last_object_completed_at
FROM audit_delivery d
LEFT JOIN audit_delivery_object o USING (delivery_id)
GROUP BY ALL;

-- De eerstvolgende levering die verwerkt mag worden, per bronsysteem.
-- Garandeert chronologische verwerking: essentieel voor correcte SCD2 historie.
CREATE OR REPLACE VIEW v_next_processable_delivery
COMMENT 'Gate: laagste nog niet verwerkte levering per bronsysteem, mits compleet.'
AS
WITH open_deliveries AS (
  SELECT r.*, d.delivery_status
  FROM v_delivery_readiness r
  JOIN audit_delivery d USING (delivery_id)
  WHERE d.delivery_status NOT IN ('QUARANTINED', 'SUPERSEDED')
    AND r.expected_object_count = (
      SELECT count(*)
      FROM contoso_meta_${env}.metadata.meta_source_object
      WHERE source_system_id = r.source_system_id
        AND is_mandatory_in_delivery
        AND is_active
    )
    AND NOT EXISTS (
      SELECT 1 FROM audit_gold_publication_group pg
      WHERE pg.delivery_id = r.delivery_id
        AND pg.release_status = 'ACTIVE')
)
SELECT *
FROM open_deliveries
QUALIFY row_number() OVER (
  PARTITION BY source_system_id ORDER BY delivery_sequence_number
) = 1;

-- Laatste succesvolle business load per Gold entiteit.
CREATE OR REPLACE VIEW v_active_gold_publication
COMMENT 'Actieve (laatst succesvolle) publicatie per Gold Actueel entiteit.'
AS
SELECT gold_entity_id, batch_id, delivery_id, physical_slot, row_count, published_at
FROM audit_gold_publication
WHERE publication_status = 'ACTIVE'
QUALIFY row_number() OVER (PARTITION BY gold_entity_id ORDER BY published_at DESC) = 1;
