-- =============================================================================
-- 01_grants.sql — Unity Catalog rechtenmodel (least privilege).
-- Groepen worden verondersteld te bestaan in de account console.
-- =============================================================================

-- Service principal die de workflows draait.
GRANT USE CATALOG, USE SCHEMA, READ VOLUME            ON CATALOG raw_${env}             TO `svc_contoso_etl`;
GRANT WRITE VOLUME                                    ON VOLUME  raw_${env}.sales.checkpoints TO `svc_contoso_etl`;
GRANT WRITE VOLUME                                    ON VOLUME  raw_${env}.sales.quarantine  TO `svc_contoso_etl`;

GRANT ALL PRIVILEGES ON CATALOG contoso_meta_${env}    TO `svc_contoso_etl`;
GRANT ALL PRIVILEGES ON CATALOG contoso_bronze_${env}  TO `svc_contoso_etl`;
GRANT ALL PRIVILEGES ON CATALOG contoso_quality_${env} TO `svc_contoso_etl`;
GRANT ALL PRIVILEGES ON CATALOG contoso_reject_${env}  TO `svc_contoso_etl`;
GRANT ALL PRIVILEGES ON CATALOG contoso_vault_${env}   TO `svc_contoso_etl`;
GRANT ALL PRIVILEGES ON CATALOG contoso_gold_${env}    TO `svc_contoso_etl`;

-- Data engineers: lezen overal, schrijven alleen in metadata-configuratie.
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG contoso_bronze_${env}  TO `grp_data_engineers`;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG contoso_quality_${env} TO `grp_data_engineers`;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG contoso_reject_${env}  TO `grp_data_engineers`;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG contoso_vault_${env}   TO `grp_data_engineers`;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG contoso_gold_${env}    TO `grp_data_engineers`;
GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY ON SCHEMA contoso_meta_${env}.metadata TO `grp_data_engineers`;

-- Analisten: uitsluitend Gold.
GRANT USE CATALOG ON CATALOG contoso_gold_${env}                   TO `grp_bi_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA contoso_gold_${env}.current     TO `grp_bi_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA contoso_gold_${env}.historical  TO `grp_bi_analysts`;

-- Reject bevat brondata: alleen data stewards.
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG contoso_reject_${env} TO `grp_data_stewards`;
