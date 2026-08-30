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
TBLPROPERTIES (delta.enableChangeDataFeed = true);

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
  is_mandatory_in_delivery BOOLEAN NOT NULL DEFAULT true COMMENT 'Blokkeert de leverings-gate indien afwezig',

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
TBLPROPERTIES (delta.enableChangeDataFeed = true);

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
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_dependency PRIMARY KEY (dependency_id) RELY
)
USING DELTA
COMMENT 'Afhankelijkheidsgraaf tussen entiteiten en lagen. Bepaalt runtime volgorde.';

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
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_quality_rule PRIMARY KEY (rule_id) RELY,
  CONSTRAINT fk_quality_rule_object FOREIGN KEY (source_object_id)
    REFERENCES meta_source_object(source_object_id) RELY
)
USING DELTA
COMMENT 'Declaratieve kwaliteitsregels; uitgevoerd als Spark SQL expressies.';

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
COMMENT 'Bron-doel mapping op kolomniveau, inclusief transformatie-expressies.';

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
COMMENT 'Definitie van Data Vault entiteiten (raw en business vault).';

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
COMMENT 'Kolommapping van Quality naar Data Vault, inclusief hashdiff-scope.';

-- -----------------------------------------------------------------------------
-- 8. Gold entiteiten
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta_gold_entity (
  gold_entity_id        STRING  NOT NULL,
  gold_layer            STRING  NOT NULL COMMENT 'HISTORICAL | CURRENT',
  entity_type           STRING  NOT NULL COMMENT 'DIMENSION | FACT | AGGREGATE',
  target_catalog        STRING  NOT NULL,
  target_schema         STRING  NOT NULL,
  target_table          STRING  NOT NULL,
  select_sql            STRING  NOT NULL COMMENT 'Parametriseerbare SELECT over de (business) vault',
  business_key_columns  ARRAY<STRING> NOT NULL,
  scd_type              STRING  NOT NULL COMMENT 'SCD1 | SCD2 | SNAPSHOT',
  partition_columns     ARRAY<STRING>,
  zorder_columns        ARRAY<STRING>,
  depends_on_gold_entity_ids ARRAY<STRING>,
  publish_mode          STRING  NOT NULL DEFAULT 'ATOMIC_SWAP'
      COMMENT 'ATOMIC_SWAP (view-pointer) | MERGE | OVERWRITE',
  publication_group_id  STRING  COMMENT 'Alle entiteiten in dezelfde groep switchen samen of niet',
  load_order            INT     NOT NULL,
  is_active             BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT pk_gold_entity PRIMARY KEY (gold_entity_id) RELY
)
USING DELTA
COMMENT 'Definitie van Gold Historisch en Gold Actueel entiteiten.';
