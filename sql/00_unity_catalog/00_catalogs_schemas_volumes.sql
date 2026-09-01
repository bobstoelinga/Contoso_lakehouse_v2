-- =============================================================================
-- 00_catalogs_schemas_volumes.sql
-- Unity Catalog structuur voor het Contoso Lakehouse.
-- Parameters: ${env}  (dev | tst | prd)  -> catalognamen krijgen een suffix.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Landing (Volumes)
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS raw_${env}
  COMMENT 'Landing zone: onbewerkte bestandsleveringen per bronsysteem.';

CREATE SCHEMA IF NOT EXISTS raw_${env}.sales
  COMMENT 'Bronsysteem SALES (Contoso): orders, customers, products.';

-- Externe volume: bestanden worden door het bronsysteem aangeleverd.
-- Folderconventie: /Volumes/raw_${env}/sales/landing/<yyyy-MM-dd>/<object>.parquet
CREATE EXTERNAL VOLUME IF NOT EXISTS raw_${env}.sales.landing
  LOCATION 'abfss://landing@${storage_account}.dfs.core.windows.net/${landing_path}'
  COMMENT 'Landing volume voor het bronsysteem SALES. Eén datumfolder = één levering.';

-- Interne volume voor Auto Loader checkpoints en schema locations.
CREATE VOLUME IF NOT EXISTS raw_${env}.sales.checkpoints
  COMMENT 'Auto Loader checkpoint- en schemalocaties per bronobject.';

-- Quarantaine voor onleesbare / corrupte bestanden (badRecordsPath).
CREATE VOLUME IF NOT EXISTS raw_${env}.sales.quarantine
  COMMENT 'Bestanden/records die niet leesbaar waren tijdens Bronze ingest.';

-- -----------------------------------------------------------------------------
-- 2. Metadata (control framework)
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_meta_${env}
  COMMENT 'Metadata-gedreven control framework: configuratie, status en audit.';

CREATE SCHEMA IF NOT EXISTS contoso_meta_${env}.metadata
  COMMENT 'Configuratietabellen: bronobjecten, mappings, DQ-regels, DV-mappings.';

CREATE SCHEMA IF NOT EXISTS contoso_meta_${env}.audit
  COMMENT 'Runtime: leveringen, load runs, batch status, DQ resultaten.';

-- -----------------------------------------------------------------------------
-- 3. Bronze
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_bronze_${env}
  COMMENT 'Bronze: 1:1 landing van bronbestanden, incrementeel, schema evolution.';

CREATE SCHEMA IF NOT EXISTS contoso_bronze_${env}.sales
  COMMENT 'Bronze tabellen van het bronsysteem SALES.';

-- -----------------------------------------------------------------------------
-- 4. Quality
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_quality_${env}
  COMMENT 'Quality: gevalideerde en getypeerde records, klaar voor de Data Vault.';

CREATE SCHEMA IF NOT EXISTS contoso_quality_${env}.sales
  COMMENT 'Goedgekeurde records per bronobject.';

-- -----------------------------------------------------------------------------
-- 5. Reject
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_reject_${env}
  COMMENT 'Reject: afgekeurde records inclusief regel- en runcontext.';

CREATE SCHEMA IF NOT EXISTS contoso_reject_${env}.sales
  COMMENT 'Afgekeurde records per bronobject.';

-- -----------------------------------------------------------------------------
-- 6. Data Vault
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_vault_${env}
  COMMENT 'Data Vault 2.0: raw vault en business vault.';

CREATE SCHEMA IF NOT EXISTS contoso_vault_${env}.raw_vault
  COMMENT 'Hubs, Links en Satellites zonder businessregels.';

CREATE SCHEMA IF NOT EXISTS contoso_vault_${env}.business_vault
  COMMENT 'Business Vault: computed satellites, PIT- en Bridgetabellen.';

-- -----------------------------------------------------------------------------
-- 7. Gold
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_gold_${env}
  COMMENT 'Gold: dimensioneel model voor consumptie.';

CREATE SCHEMA IF NOT EXISTS contoso_gold_${env}.historical
  COMMENT 'Gold Historisch: volledige SCD2 historie.';

CREATE SCHEMA IF NOT EXISTS contoso_gold_${env}.current
  COMMENT 'Gold Actueel: laatste succesvolle business load (publish-by-pointer).';

CREATE SCHEMA IF NOT EXISTS contoso_gold_${env}.current_internal
  COMMENT 'Interne, versiebeheerde fysieke Gold Actueel tabellen (_v1/_v2 slots).';
