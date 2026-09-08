-- =============================================================================
-- 10_metadata_model.sql
-- Het metadata model. Dit is de enige plek waar pipelines geconfigureerd worden.
-- Geen enkele ETL-stap bevat hardcoded bron-, mapping- of afhankelijkheidslogica.
-- =============================================================================
USE CATALOG contoso_meta_${env};
USE SCHEMA metadata;

-- -----------------------------------------------------------------------------
-- 1. Bronsystemen
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_source_system (
  source_system_id      STRING    NOT NULL COMMENT 'Technische sleutel, bv. SALES',
  source_system_name    STRING    NOT NULL,
  landing_volume_path   STRING    NOT NULL COMMENT '/Volumes/<cat>/<schema>/<volume>/...',
  delivery_folder_format STRING   NOT NULL COMMENT "Datumpatroon van de leveringsfolder, bv. yyyy-MM-dd",
  delivery_frequency    STRING    NOT NULL COMMENT 'DAILY | HOURLY | ON_DEMAND',
  is_active             BOOLEAN   NOT NULL DEFAULT true,
  valid_from            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  valid_to              TIMESTAMP,
  CONSTRAINT pk_source_system PRIMARY KEY (source_system_id) RELY
)
USING DELTA
COMMENT 'Bronsystemen en hun landingconventie.'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  delta.enableChangeDataFeed = true
);

CREATE TABLE IF NOT EXISTS meta_source_connector (
  source_object_id STRING NOT NULL,
  connector_type STRING NOT NULL COMMENT 'HTTP_JSON | HTTP_CSV | JDBC | LAKEFLOW_CONNECT',
  endpoint_url STRING NOT NULL,
  response_format STRING NOT NULL DEFAULT 'JSON',
  records_key STRING,
  next_link_key STRING,
  request_options MAP<STRING,STRING> COMMENT 'HTTP: timeout_seconds, max_retries, retry_delay_seconds, total_timeout_seconds',
  is_active BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_source_connector PRIMARY KEY (source_object_id) RELY
)
USING DELTA
COMMENT 'Connectorinstellingen; credentials verwijzen uitsluitend naar Secrets of UC Connections.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 2. Bronobjecten + laadstrategie
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_source_object (
  source_object_id      STRING    NOT NULL COMMENT 'bv. SALES.ORDERS',
  source_system_id      STRING    NOT NULL,
  object_name           STRING    NOT NULL COMMENT 'orders | customers | products',
  file_pattern          STRING    NOT NULL COMMENT 'bv. orders*.parquet',
  file_format           STRING    NOT NULL COMMENT 'parquet | csv | json',
  reader_options        MAP<STRING,STRING>  COMMENT 'Extra Auto Loader / reader opties',

  -- laadstrategie
  load_strategy         STRING    NOT NULL COMMENT 'INCREMENTAL_APPEND | INCREMENTAL_MERGE | SNAPSHOT_SCD2 | FULL_OVERWRITE',
  business_key_columns  ARRAY<STRING> NOT NULL COMMENT 'Natuurlijke sleutel in de bron',
  change_tracking_columns ARRAY<STRING> COMMENT 'Kolommen die een wijziging aanduiden (hashdiff-scope)',
  deleted_flag_column   STRING              COMMENT 'Kolom die een logische delete markeert',
  delete_semantics      STRING    NOT NULL DEFAULT 'NONE' COMMENT 'NONE | SOFT_DELETE_FLAG | SNAPSHOT_ABSENCE | CDC_TOMBSTONE',
  absence_means_delete  BOOLEAN   NOT NULL DEFAULT false COMMENT 'Een ontbrekende sleutel in een complete snapshot is een delete',
  schema_contract_version STRING  NOT NULL DEFAULT '1.0',
  late_arrival_window_days INT    NOT NULL DEFAULT 30,
  freshness_sla_hours   INT       NOT NULL DEFAULT 26,
  backfill_strategy     STRING    NOT NULL DEFAULT 'FULL_RELOAD' COMMENT 'FULL_RELOAD | REPLAY_FROM_LANDING | REPROCESS_FROM_BRONZE',
  is_mandatory_in_delivery BOOLEAN NOT NULL DEFAULT true COMMENT 'Blokkeert de leverings-gate indien afwezig',
  schema_drift_policy   STRING    NOT NULL DEFAULT 'STRICT'
                        COMMENT 'STRICT | ALLOW_NEW_COLUMNS_WITH_APPROVAL | RESCUE',
  schema_drift_approval_required BOOLEAN NOT NULL DEFAULT false,
  owner_team            STRING    NOT NULL COMMENT 'Operationeel verantwoordelijke domeinteam',
  criticality           STRING    NOT NULL DEFAULT 'MEDIUM' COMMENT 'LOW | MEDIUM | HIGH',
  processing_route      STRING    NOT NULL DEFAULT 'RAW_VAULT'
                        COMMENT 'RAW_VAULT | REFERENCE_DATA',

  -- bronze doel
  bronze_catalog        STRING    NOT NULL,
  bronze_schema         STRING    NOT NULL,
  bronze_table          STRING    NOT NULL,
  bronze_partition_columns ARRAY<STRING>,

  -- auto loader
  checkpoint_path       STRING    NOT NULL,
  schema_location_path  STRING    NOT NULL,
  schema_evolution_mode STRING    NOT NULL DEFAULT 'addNewColumns'
                        COMMENT 'addNewColumns | rescue | failOnNewColumns | none',
  max_files_per_trigger INT       DEFAULT 1000,

  -- quality / reject doel
  quality_catalog       STRING,
  quality_schema        STRING,
  quality_table         STRING,
  reject_catalog        STRING,
  reject_schema         STRING,
  reject_table          STRING,
  quality_filter_expression STRING
                        COMMENT 'Optionele Spark SQL-filter vóór Quality-projectie',

  -- alleen voor processing_route = REFERENCE_DATA
  reference_catalog     STRING,
  reference_schema      STRING,
  reference_table       STRING,

  load_order            INT       NOT NULL DEFAULT 100,
  is_active             BOOLEAN   NOT NULL DEFAULT true,
  updated_at            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  updated_by            STRING    NOT NULL DEFAULT current_user(),
  CONSTRAINT pk_source_object PRIMARY KEY (source_object_id) RELY,
  CONSTRAINT fk_source_object_system FOREIGN KEY (source_system_id)
    REFERENCES meta_source_system(source_system_id) RELY
)
USING DELTA
COMMENT 'Bronobjecten, laadstrategie en fysieke doellocaties per laag.'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  delta.enableChangeDataFeed = true
);

-- -----------------------------------------------------------------------------
-- 2a. Referentietabellen voor governance en beheer
-- -----------------------------------------------------------------------------

-- Gedenormaliseerde retry- en prioriteitsconfiguratie, één bron van waarheid
-- voor meta_dependency en andere herhaalbare faalscenario's.
CREATE TABLE IF NOT EXISTS meta_retry_policy (
  retry_policy_id     STRING NOT NULL COMMENT 'bv. STANDARD, CRITICAL, NO_RETRY',
  max_retries         INT    NOT NULL DEFAULT 3,
  backoff_type        STRING NOT NULL DEFAULT 'EXPONENTIAL_BACKOFF'
                      COMMENT 'EXPONENTIAL_BACKOFF | FIXED_DELAY | NONE',
  initial_delay_sec   INT    NOT NULL DEFAULT 60,
  max_delay_sec       INT    NOT NULL DEFAULT 3600,
  priority            INT    NOT NULL DEFAULT 100 COMMENT 'Lagere waarde = eerder gepland',
  description         STRING,
  is_active           BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_retry_policy PRIMARY KEY (retry_policy_id) RELY
)

USING DELTA
COMMENT 'Referentietabel voor retry- en prioriteitsbeleid; voorkomt duplicatie in meta_dependency.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 2b. Versiebeheer van uitvoerbare SQL per ETL-component
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_sql_script (
  script_id          STRING    NOT NULL,
  source_object_id   STRING,
  target_layer       STRING    NOT NULL COMMENT 'BRONZE | QUALITY | VAULT | GOLD',
  script_path        STRING    NOT NULL COMMENT 'Pad in de Git-repository',
  script_body        STRING    NOT NULL COMMENT 'Goedgekeurde uitvoerbare SQL',
  script_version     STRING    NOT NULL,
  script_checksum    STRING    NOT NULL,
  script_status      STRING    NOT NULL COMMENT 'DRAFT | APPROVED | ACTIVE | RETIRED',
  change_reference   STRING,
  approved_by        STRING,
  approved_at        TIMESTAMP,
  is_active          BOOLEAN   NOT NULL DEFAULT true,
  created_at         TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  created_by         STRING    NOT NULL DEFAULT current_user(),
  CONSTRAINT pk_meta_sql_script PRIMARY KEY (script_id) RELY
)
USING DELTA
COMMENT 'Gecontroleerde SQL-implementaties per ETL-component; alleen ACTIVE wordt door runtime gebruikt.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 2b. Onderhoudsbeleid voor Delta-tabellen
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_table_maintenance_policy (
  policy_id                 STRING    NOT NULL,
  catalog_name              STRING    NOT NULL,
  schema_name               STRING,
  table_name                STRING,
  maintenance_tier          STRING    NOT NULL COMMENT 'HOT | WARM | COLD | AUDIT',
  optimize_mode             STRING    NOT NULL COMMENT 'SCHEDULED | DISABLED',
  optimize_interval_hours   INT       NOT NULL,
  min_files_before_optimize BIGINT    NOT NULL DEFAULT 20,
  vacuum_retain_hours       INT       NOT NULL,
  priority                  INT       NOT NULL DEFAULT 100,
  is_active                 BOOLEAN   NOT NULL DEFAULT true,
  CONSTRAINT pk_maintenance_policy PRIMARY KEY (policy_id) RELY
)
USING DELTA
COMMENT 'Overervend onderhoudsbeleid per catalog, schema of tabel; laagste priority wint.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 2c. Governancebeleid per bronsysteem
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_data_governance_policy (
  source_system_id       STRING    NOT NULL,
  data_owner             STRING    NOT NULL,
  data_steward           STRING    NOT NULL,
  data_domain            STRING    NOT NULL,
  pii_classification     STRING    NOT NULL COMMENT 'PUBLIC | INTERNAL | CONFIDENTIAL | RESTRICTED',
  retention_days         INT       NOT NULL,
  cost_center            STRING    NOT NULL,
  sla_tier               STRING    NOT NULL COMMENT 'CRITICAL | STANDARD | BEST_EFFORT',
  is_active              BOOLEAN   NOT NULL DEFAULT true,
  CONSTRAINT pk_data_governance_policy PRIMARY KEY (source_system_id) RELY,
  CONSTRAINT fk_governance_source FOREIGN KEY (source_system_id)
    REFERENCES meta_source_system(source_system_id) RELY
)
USING DELTA
COMMENT 'Eigenaarschap, classificatie, retentie en SLA voor ieder bronsysteem.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 2d. Gold data-productcontracten
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_gold_data_product (
  publication_group_id   STRING    NOT NULL,
  data_product_name      STRING    NOT NULL,
  data_owner             STRING    NOT NULL,
  data_steward           STRING    NOT NULL,
  consumer_group         STRING    NOT NULL,
  refresh_sla_hours      INT       NOT NULL,
  compatibility_policy   STRING    NOT NULL COMMENT 'BACKWARD_COMPATIBLE | VERSIONED_BREAKING_CHANGE',
  deprecation_date       DATE,
  is_active              BOOLEAN   NOT NULL DEFAULT true,
  CONSTRAINT pk_gold_data_product PRIMARY KEY (publication_group_id) RELY
)
USING DELTA
COMMENT 'Consumercontract en ownership per atomisch Gold-publicatiegroep.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- Formele goedkeuringsregistratie voor schema-drift, zodat de policy
-- ALLOW_NEW_COLUMNS_WITH_APPROVAL afdwingbaar wordt in plaats van declaratief.
CREATE TABLE IF NOT EXISTS meta_schema_drift_approval (
  approval_id         STRING    NOT NULL COMMENT 'Unieke sleutel, bv. SDA-2026-0001',
  source_object_id    STRING    NOT NULL,
  from_schema_version STRING    COMMENT 'Contractversie vóór drift',
  to_schema_version   STRING    COMMENT 'Contractversie ná drift',
  detected_columns    ARRAY<STRING> COMMENT 'Nieuwe of gewijzigde kolommen',
  drift_type          STRING    NOT NULL COMMENT 'ADD_COLUMN | RENAME | DROP | TYPE_CHANGE',
  status              STRING    NOT NULL DEFAULT 'PENDING'
                      COMMENT 'PENDING | APPROVED | REJECTED | APPLIED',
  requested_by        STRING    NOT NULL DEFAULT current_user(),
  requested_at        TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  approved_by         STRING,
  approved_at         TIMESTAMP,
  applied_at          TIMESTAMP,
  notes               STRING,
  CONSTRAINT pk_schema_drift_approval PRIMARY KEY (approval_id) RELY,
  CONSTRAINT fk_sda_object FOREIGN KEY (source_object_id)
    REFERENCES meta_source_object(source_object_id) RELY
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  delta.enableChangeDataFeed = true
);

-- -----------------------------------------------------------------------------
-- 3. Afhankelijkheden (nooit hardcoded in code of workflow)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_dependency (
  dependency_id         STRING  NOT NULL,
  entity_id             STRING  NOT NULL COMMENT 'De stap die wacht',
  entity_layer          STRING  NOT NULL COMMENT 'BRONZE | QUALITY | RAW_VAULT | BUSINESS_VAULT | GOLD_HIST | GOLD_CURR',
  depends_on_entity_id  STRING  NOT NULL COMMENT 'De stap waarop gewacht wordt',
  depends_on_layer      STRING  NOT NULL,
  dependency_type       STRING  NOT NULL COMMENT 'DELIVERY_COMPLETE | UPSTREAM_SUCCESS | SAME_DELIVERY',
  is_blocking           BOOLEAN NOT NULL DEFAULT true,
  priority              INT     NOT NULL DEFAULT 100 COMMENT 'Lagere waarde wordt eerder ingepland',
  retry_policy          STRING  NOT NULL DEFAULT 'EXPONENTIAL_BACKOFF'
                              COMMENT 'NONE | FIXED_DELAY | EXPONENTIAL_BACKOFF',
  max_retries           INT     NOT NULL DEFAULT 3,
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_dependency PRIMARY KEY (dependency_id) RELY
)
USING DELTA
COMMENT 'Afhankelijkheidsgraaf tussen entiteiten en lagen. Bepaalt runtime volgorde.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 4. Kwaliteitsregels
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_quality_rule (
  rule_id               STRING  NOT NULL,
  source_object_id      STRING  NOT NULL,
  rule_name             STRING  NOT NULL,
  rule_type             STRING  NOT NULL
      COMMENT 'NOT_NULL | UNIQUE | RANGE | REGEX | ALLOWED_VALUES | REFERENTIAL | CUSTOM_SQL | DATA_TYPE',
  target_columns        ARRAY<STRING> NOT NULL,
  rule_expression       STRING  NOT NULL COMMENT 'Spark SQL boolean expressie; TRUE = record voldoet',
  evaluation_scope      STRING  NOT NULL DEFAULT 'ROW'
      COMMENT 'ROW = per rij | DATASET = window/aggregatie | CROSS_DATASET = join met andere tabel',
  severity              STRING  NOT NULL COMMENT 'ERROR = reject | WARNING = doorlaten met vlag',
  reject_reason_code    STRING  NOT NULL,
  reject_reason_text    STRING  NOT NULL,
  execution_order       INT     NOT NULL DEFAULT 100,
  threshold_pct         DOUBLE  COMMENT 'Max % afgekeurde records voordat de hele batch faalt',
  on_threshold_breach   STRING  NOT NULL DEFAULT 'FAIL_BATCH'
      COMMENT 'FAIL_BATCH | QUARANTINE_BATCH | WARN_ONLY',
  is_blocking           BOOLEAN NOT NULL DEFAULT true,
  rule_group            STRING  NOT NULL DEFAULT 'CORE',
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_quality_rule PRIMARY KEY (rule_id) RELY,
  CONSTRAINT fk_quality_rule_object FOREIGN KEY (source_object_id)
    REFERENCES meta_source_object(source_object_id) RELY
)
USING DELTA
COMMENT 'Declaratieve kwaliteitsregels; uitgevoerd als Spark SQL expressies.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 5. Bron-doel mappings (kolomniveau)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_mapping (
  mapping_id            STRING  NOT NULL,
  source_object_id      STRING  NOT NULL,
  target_layer          STRING  NOT NULL COMMENT 'BRONZE | QUALITY | GOLD_HIST | GOLD_CURR',
  target_entity         STRING  NOT NULL COMMENT 'Doeltabel (zonder catalog/schema)',
  source_column         STRING           COMMENT 'NULL bij een puur afgeleide kolom',
  source_expression     STRING           COMMENT 'Spark SQL expressie; wint van source_column',
  target_column         STRING  NOT NULL,
  target_data_type      STRING  NOT NULL,
  is_business_key       BOOLEAN NOT NULL DEFAULT false,
  is_nullable           BOOLEAN NOT NULL DEFAULT true,
  default_value         STRING,
  ordinal_position      INT     NOT NULL,
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_mapping PRIMARY KEY (mapping_id) RELY
)
USING DELTA
COMMENT 'Bron-doel mapping op kolomniveau, inclusief transformatie-expressies.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 6. Data Vault entiteiten
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_dv_entity (
  dv_entity_id          STRING  NOT NULL COMMENT 'bv. HUB_CUSTOMER',
  dv_entity_type        STRING  NOT NULL COMMENT 'HUB | LINK | SATELLITE | LINK_SATELLITE | PIT | BRIDGE',
  dv_zone               STRING  NOT NULL COMMENT 'RAW_VAULT | BUSINESS_VAULT',
  target_catalog        STRING  NOT NULL,
  target_schema         STRING  NOT NULL,
  target_table          STRING  NOT NULL,
  hash_key_column       STRING  NOT NULL COMMENT 'bv. hk_customer / hk_order_customer',
  parent_entity_ids     ARRAY<STRING> COMMENT 'Hubs waarnaar een LINK of SATELLITE verwijst',
  business_key_columns  ARRAY<STRING> COMMENT 'Business keys van een HUB',
  hashdiff_column       STRING        COMMENT 'Alleen voor SATELLITE',
  is_multi_active       BOOLEAN NOT NULL DEFAULT false,
  multi_active_key      ARRAY<STRING>,
  record_source_expr    STRING  NOT NULL DEFAULT "'SALES'",
  load_order            INT     NOT NULL COMMENT 'Hubs < Links < Satellites',
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_dv_entity PRIMARY KEY (dv_entity_id) RELY
)
USING DELTA
COMMENT 'Definitie van Data Vault entiteiten (raw en business vault).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 7. Data Vault mappings (bron -> DV kolom)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_dv_mapping (
  dv_mapping_id         STRING  NOT NULL,
  dv_entity_id          STRING  NOT NULL,
  source_object_id      STRING  NOT NULL COMMENT 'Quality-tabel als bron',
  source_expression     STRING  NOT NULL COMMENT 'Spark SQL expressie op de quality-tabel',
  target_column         STRING  NOT NULL,
  target_data_type      STRING  NOT NULL,
  column_role           STRING  NOT NULL
      COMMENT 'HASH_KEY | BUSINESS_KEY | HASHDIFF | DESCRIPTIVE | DEGENERATE | DRIVING_KEY | LOAD_DATE | RECORD_SOURCE',
  is_in_hashdiff        BOOLEAN NOT NULL DEFAULT false,
  ordinal_position      INT     NOT NULL,
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_dv_mapping PRIMARY KEY (dv_mapping_id) RELY,
  CONSTRAINT fk_dv_mapping_entity FOREIGN KEY (dv_entity_id)
    REFERENCES meta_dv_entity(dv_entity_id) RELY
)
USING DELTA
COMMENT 'Kolommapping van Quality naar Data Vault, inclusief hashdiff-scope.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- -----------------------------------------------------------------------------
-- 8. Gold entiteiten
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_gold_entity (
  gold_entity_id        STRING  NOT NULL,
  source_system_id      STRING  NOT NULL DEFAULT 'SALES'
      COMMENT 'Bronsysteem of data product dat deze Gold-entiteit publiceert',
  gold_layer            STRING  NOT NULL COMMENT 'HISTORICAL | CURRENT',
  entity_type           STRING  NOT NULL COMMENT 'DIMENSION | FACT | AGGREGATE',
  target_catalog        STRING  NOT NULL,
  target_schema         STRING  NOT NULL,
  target_table          STRING  NOT NULL,
  select_sql            STRING  NOT NULL COMMENT 'Parametriseerbare SELECT over de (business) vault',
  business_key_columns  ARRAY<STRING> NOT NULL,
  scd_type              STRING  NOT NULL COMMENT 'SCD1 | SCD2 | SNAPSHOT',
  partition_columns     ARRAY<STRING>,
  cluster_columns       ARRAY<STRING> COMMENT 'Liquid clustering keys; vervangt ZORDER',
  depends_on_gold_entity_ids ARRAY<STRING>,
  publish_mode          STRING  NOT NULL DEFAULT 'ATOMIC_SWAP'
      COMMENT 'ATOMIC_SWAP (view-pointer) | MERGE | OVERWRITE',
  publication_group_id  STRING  COMMENT 'Alle entiteiten in dezelfde groep switchen samen of niet',
  publish_status        STRING  NOT NULL DEFAULT 'READY' COMMENT 'READY | ACTIVE | FAILED',
  pointer_table         STRING  COMMENT 'Publieke view die BI-consumenten gebruiken',
  staging_table         STRING  COMMENT 'Een fysiek slot met suffix _v1 of _v2',
  load_order            INT     NOT NULL,
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_gold_entity PRIMARY KEY (gold_entity_id) RELY
)
USING DELTA
COMMENT 'Definitie van Gold Historisch en Gold Actueel entiteiten.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
