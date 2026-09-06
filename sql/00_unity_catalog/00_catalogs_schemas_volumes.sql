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

CREATE SCHEMA IF NOT EXISTS raw_${env}.cbs
  COMMENT 'Landing voor openbare CBS StatLine-leveringen.';
CREATE EXTERNAL VOLUME IF NOT EXISTS raw_${env}.cbs.landing
  LOCATION 'abfss://landing@${storage_account}.dfs.core.windows.net/cbs'
  COMMENT 'Immutable CBS API-deliveries per extractdatum.';
CREATE VOLUME IF NOT EXISTS raw_${env}.cbs.checkpoints
  COMMENT 'Auto Loader checkpoint- en schemalocaties voor CBS-bronobjecten.';
CREATE VOLUME IF NOT EXISTS raw_${env}.cbs.quarantine
  COMMENT 'Onleesbare of corrupte CBS-bestanden.';

CREATE SCHEMA IF NOT EXISTS raw_${env}.ecb
  COMMENT 'Landing voor openbare ECB Data API-leveringen.';
CREATE EXTERNAL VOLUME IF NOT EXISTS raw_${env}.ecb.landing
  LOCATION 'abfss://landing@${storage_account}.dfs.core.windows.net/ecb'
  COMMENT 'Immutable ECB API-deliveries per extractdatum.';
CREATE VOLUME IF NOT EXISTS raw_${env}.ecb.checkpoints
  COMMENT 'Auto Loader checkpoint- en schemalocaties voor ECB-bronobjecten.';
CREATE VOLUME IF NOT EXISTS raw_${env}.ecb.quarantine
  COMMENT 'Onleesbare of corrupte ECB-bestanden.';

CREATE SCHEMA IF NOT EXISTS raw_${env}.nager
  COMMENT 'Landing voor openbare Nager.Date-feestdagenleveringen.';
CREATE EXTERNAL VOLUME IF NOT EXISTS raw_${env}.nager.landing
  LOCATION 'abfss://landing@${storage_account}.dfs.core.windows.net/nager'
  COMMENT 'Immutable Nager.Date API-deliveries per extractdatum.';
CREATE VOLUME IF NOT EXISTS raw_${env}.nager.checkpoints
  COMMENT 'Auto Loader checkpoint- en schemalocaties voor Nager-bronobjecten.';
CREATE VOLUME IF NOT EXISTS raw_${env}.nager.quarantine
  COMMENT 'Onleesbare of corrupte Nager-bestanden.';

CREATE SCHEMA IF NOT EXISTS raw_${env}.sharepoint
  COMMENT 'Landing voor ongestructureerde bestandsaanleveringen uit SharePoint.';
CREATE VOLUME IF NOT EXISTS raw_${env}.sharepoint.landing
  COMMENT 'Immutable landing van door de SharePoint-connector opgehaalde bestanden.';
CREATE VOLUME IF NOT EXISTS raw_${env}.sharepoint.checkpoints
  COMMENT 'Auto Loader checkpoint- en schemalocaties voor SharePoint-bronobjecten.';
CREATE VOLUME IF NOT EXISTS raw_${env}.sharepoint.quarantine
  COMMENT 'Onleesbare of corrupte SharePoint-bestanden.';

CREATE SCHEMA IF NOT EXISTS raw_${env}.fabric_sales
  COMMENT 'Landing voor immutable Fabric SQL orderregel-snapshots.';
CREATE EXTERNAL VOLUME IF NOT EXISTS raw_${env}.fabric_sales.landing
  LOCATION 'abfss://landing@${storage_account}.dfs.core.windows.net/fabric_sales'
  COMMENT 'Immutable Fabric SQL orderregel-snapshots per extractdatum.';
CREATE VOLUME IF NOT EXISTS raw_${env}.fabric_sales.checkpoints
  COMMENT 'Auto Loader checkpoint- en schemalocaties voor Fabric Sales.';
CREATE VOLUME IF NOT EXISTS raw_${env}.fabric_sales.quarantine
  COMMENT 'Onleesbare of corrupte Fabric Sales-bestanden.';

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
CREATE SCHEMA IF NOT EXISTS contoso_bronze_${env}.cbs;
CREATE SCHEMA IF NOT EXISTS contoso_bronze_${env}.ecb;
CREATE SCHEMA IF NOT EXISTS contoso_bronze_${env}.nager;
CREATE SCHEMA IF NOT EXISTS contoso_bronze_${env}.sharepoint;
CREATE SCHEMA IF NOT EXISTS contoso_bronze_${env}.fabric_sales;

-- -----------------------------------------------------------------------------
-- 4. Quality
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_quality_${env}
  COMMENT 'Quality: gevalideerde en getypeerde records, klaar voor de Data Vault.';

CREATE SCHEMA IF NOT EXISTS contoso_quality_${env}.sales
  COMMENT 'Goedgekeurde records per bronobject.';
CREATE SCHEMA IF NOT EXISTS contoso_quality_${env}.cbs;
CREATE SCHEMA IF NOT EXISTS contoso_quality_${env}.ecb;
CREATE SCHEMA IF NOT EXISTS contoso_quality_${env}.nager;
CREATE SCHEMA IF NOT EXISTS contoso_quality_${env}.sharepoint;
CREATE SCHEMA IF NOT EXISTS contoso_quality_${env}.fabric_sales;

-- -----------------------------------------------------------------------------
-- 5. Reject
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS contoso_reject_${env}
  COMMENT 'Reject: afgekeurde records inclusief regel- en runcontext.';

CREATE SCHEMA IF NOT EXISTS contoso_reject_${env}.sales
  COMMENT 'Afgekeurde records per bronobject.';
CREATE SCHEMA IF NOT EXISTS contoso_reject_${env}.cbs;
CREATE SCHEMA IF NOT EXISTS contoso_reject_${env}.ecb;
CREATE SCHEMA IF NOT EXISTS contoso_reject_${env}.nager;
CREATE SCHEMA IF NOT EXISTS contoso_reject_${env}.sharepoint;
CREATE SCHEMA IF NOT EXISTS contoso_reject_${env}.fabric_sales;

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
