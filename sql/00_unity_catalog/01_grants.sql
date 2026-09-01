-- =============================================================================
-- 01_grants.sql — Unity Catalog rechtenmodel (least privilege).
-- Groepen worden verondersteld te bestaan in de account console.
-- =============================================================================

-- Service principal die de workflows draait.
GRANT USE CATALOG, READ VOLUME                        ON CATALOG raw_${env}             TO `${service_principal}`;
GRANT USE SCHEMA                                      ON SCHEMA  raw_${env}.sales        TO `${service_principal}`;
GRANT WRITE VOLUME                                    ON VOLUME  raw_${env}.sales.checkpoints TO `${service_principal}`;
GRANT WRITE VOLUME                                    ON VOLUME  raw_${env}.sales.quarantine  TO `${service_principal}`;

GRANT ALL PRIVILEGES ON CATALOG contoso_meta_${env}    TO `${service_principal}`;
GRANT ALL PRIVILEGES ON CATALOG contoso_bronze_${env}  TO `${service_principal}`;
GRANT ALL PRIVILEGES ON CATALOG contoso_quality_${env} TO `${service_principal}`;
GRANT ALL PRIVILEGES ON CATALOG contoso_reject_${env}  TO `${service_principal}`;
GRANT ALL PRIVILEGES ON CATALOG contoso_vault_${env}   TO `${service_principal}`;
GRANT ALL PRIVILEGES ON CATALOG contoso_gold_${env}    TO `${service_principal}`;

-- Data engineers: lezen overal, schrijven alleen in metadata-configuratie.
GRANT USE CATALOG, SELECT ON CATALOG contoso_bronze_${env}  TO `${data_engineers_group}`;
GRANT USE CATALOG, SELECT ON CATALOG contoso_quality_${env} TO `${data_engineers_group}`;
GRANT USE CATALOG, SELECT ON CATALOG contoso_reject_${env}  TO `${data_engineers_group}`;
GRANT USE CATALOG, SELECT ON CATALOG contoso_vault_${env}   TO `${data_engineers_group}`;
GRANT USE CATALOG, SELECT ON CATALOG contoso_gold_${env}    TO `${data_engineers_group}`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA contoso_meta_${env}.metadata TO `${data_engineers_group}`;

-- Analisten: uitsluitend Gold.
GRANT USE CATALOG ON CATALOG contoso_gold_${env}                   TO `${bi_analysts_group}`;
GRANT USE SCHEMA ON SCHEMA contoso_gold_${env}.current             TO `${bi_analysts_group}`;
GRANT USE SCHEMA ON SCHEMA contoso_gold_${env}.historical          TO `${bi_analysts_group}`;

-- Reject bevat brondata: alleen data stewards.
GRANT USE CATALOG, SELECT ON CATALOG contoso_reject_${env} TO `${data_stewards_group}`;
GRANT USE SCHEMA ON SCHEMA contoso_reject_${env}.sales TO `${data_stewards_group}`;
