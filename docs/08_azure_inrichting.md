# Azure-inrichting en technische specificaties

**Datum:** 8 september 2026  
**Status:** actuele prototype-inventarisatie voor `dev`

Dit document beschrijft wat in Azure en Azure Databricks nodig is om het
Contoso Lakehouse en de Control Room te laten werken. Waarden met de status
`geverifieerd` zijn afkomstig uit de repositoryconfiguratie, de actieve
Databricks API of de Azure-resourceweergave. Secrets worden nooit opgenomen.

## 1. Azure-resourceinventaris

| Onderdeel | Naam of waarde | Status | Functie |
|---|---|---|---|
| Azure-subscription | `Azure subscription 1` | Portalweergave | Container voor alle resources |
| Resource group | `Databricks` | Portalweergave | Groepering van Databricks-gerelateerde resources |
| Azure Databricks workspace | `https://adb-7405619535862062.2.azuredatabricks.net/` | Geverifieerd in `databricks.yml` | Compute, Workflows, Unity Catalog en SQL |
| Storage account | `contosolake3` | Portalweergave en bundleconfiguratie | Landingdata en lakehouse-opslag |
| Event Grid system topic | `contosolake3-2b5969ec-b604-49f1-88e4-ba4beaf931b0` | Portalweergave | Eventintegratie voor storage-events |
| Access Connector for Azure Databricks | `ac-databricks-contoso-dev` | Portalweergave | Managed identity voor toegang tot Azure Storage/Unity Catalog |
| Databricks App | `contoso-control-room` | Geverifieerd via Databricks Apps API | Operationele monitoring en gecontroleerde wijzigingen |

De portalweergave toonde daarnaast een tweede resourcegroep-item met de naam
`Databricks`. De exacte Azure-resource-ID's, regio, SKU en networkinginstellingen
zijn in deze sessie niet via Azure Resource Manager uitgelezen en moeten vóór
productieacceptatie in de Azure Portal of met `az resource list` worden
vastgelegd.

## 2. Azure Storage en landingzone

De storage account `contosolake3` levert de landingzone voor immutable brondata.
De logische indeling per omgeving is:

```text
/Volumes/raw_<env>/sales/landing/<yyyy-MM-dd>/
/Volumes/control_<env>/platform/checkpoints/<source>/bronze/<table>/_checkpoint
/Volumes/control_<env>/platform/checkpoints/<source>/bronze/<table>/_schema
```

In de bundleconfiguratie zijn de omgevingsspecifieke landingpaden:

| Omgeving | `landing_path` | Catalogusprefix |
|---|---|---|
| `dev` | `sales` | `contoso_*_dev` |
| `tst` | `sales/tst` | `contoso_*_tst` |
| `prd` | `sales/prd` | `contoso_*_prd` |

Benodigde Azure/Unity Catalog-inrichting:

1. Storage account en container voor Unity Catalog/lakehouse-opslag.
2. Azure Databricks Access Connector met managed identity.
3. Storage credentials en external locations in Unity Catalog.
4. Landingvolume voor bronbestanden.
5. Managed volumes voor Auto Loader checkpoints, schema locations en quarantine.
6. Storage-eventintegratie via Event Grid voor file-arrival-scenario's.
7. Least-privilege storage-rollen voor de Databricks identity.

## 3. Azure Databricks en Unity Catalog

De oplossing gebruikt één Azure Databricks-workspace met omgevingsgescheiden
catalogi:

```text
contoso_meta_<env>      metadata en audit
contoso_bronze_<env>    Bronze Delta-tabellen
contoso_quality_<env>   Quality-tabellen
contoso_reject_<env>    Reject-tabellen
contoso_vault_<env>     Raw Vault en Business Vault
contoso_gold_<env>      Gold Historisch en Gold Actueel
```

Voor `dev` is de actieve metadata-catalogus `contoso_meta_dev`. De actieve
Unity Catalog-metastore die via Databricks is uitgelezen heeft ID
`2557188a-d12b-45ff-9ab5-a5aa79308484`. De catalogus gebruikt een managed
storage-root; het volledige storage-accountpad staat in Databricks en wordt
hier bewust niet herhaald als operationele secret/configuratie.

Binnen `contoso_meta_dev` zijn minimaal deze schema's nodig:

- `metadata`: bron-, connector-, mapping-, DQ-, Data Vault- en Gold-configuratie
- `audit`: deliveries, runs, work-items, qualityresultaten en onboarding-drafts

## 4. Identities en rechten

| Identity | Gebruik | Benodigde rechten |
|---|---|---|
| `22fb388c-33eb-46a3-8031-0cb4db20209d` | Databricks bundle/service principal voor ETL | Uitvoeren van workflows en metadata-/lakehouse-acties volgens UC-grants |
| `5a3b49f3-ab1c-48b6-ac9e-a42d14b56e92` | Service principal van Databricks App `contoso-control-room` | `CAN_USE` op SQL Warehouse; `USE_CATALOG` op `contoso_meta_dev`; `USE_SCHEMA` + `SELECT` op `metadata` en `audit`; `MODIFY` uitsluitend op `audit_onboarding_draft` |
| `grp_data_engineers` | Engineers | Beheer via Git/DAB; productie niet rechtstreeks wijzigen |
| `grp_bi_analysts` | BI-consumenten | Alleen publieke Gold-views |
| `grp_data_stewards` | Data stewards | Reject-data volgens expliciete grants |

Het SQL Warehouse dat door de Control Room gebruikt wordt:

```text
Warehouse ID: 06c37b8b16646405
HTTP path:    /sql/1.0/warehouses/06c37b8b16646405
```

De App had aanvankelijk wel toegang tot het Warehouse, maar nog geen catalogus-
en draftschrijfrechten. Die rechten zijn daarna gericht toegevoegd. De app kan
nu alleen onboarding-drafts opslaan; zij kan geen productiemetadata rechtstreeks
wijzigen.

## 5. Deployment en runtime

De bundle gebruikt deze Azure Databricks-targets:

| Target | Mode | Workspace | Run-as |
|---|---|---|---|
| `dev` | development | dezelfde Azure Databricks workspace | gebruiker tijdens ontwikkeling |
| `tst` | production | dezelfde workspace | service principal |
| `prd` | production | dezelfde workspace | service principal |

Deploymentvolgorde:

1. `databricks bundle validate -t dev`
2. `databricks bundle deploy -t dev`
3. Setup-job voor Unity Catalog, schema's, volumes en metadata-seeds
4. Validatiejob voor metadata en DDL
5. Pipeline- en bronjobs
6. Databricks App deployment voor de Control Room

De Control Room draait als Databricks App op:

<https://contoso-control-room-7405619535862062.2.azure.databricksapps.com>

De app leest operationele data via Databricks SQL en start voor operatoracties
bestaande Databricks Jobs. SQL-scriptwijzigingen worden via GitHub-branch en Pull
Request beheerd; directe productiepush vanuit de app is niet toegestaan.

## 6. Secrets en configuratie

De volgende waarden moeten als secret-backed environment variables of Databricks
secret worden ingericht en mogen niet in Git worden opgeslagen:

- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN` of Workspace OAuth-configuratie
- `GITHUB_TOKEN` voor het openen van Pull Requests vanuit de SQL-editor
- Connectorcredentials voor HTTP/JDBC-bronnen

## 7. Nog te bevestigen Azure-specificaties

Voor productie-readiness moeten de volgende gegevens nog rechtstreeks uit Azure
Resource Manager worden toegevoegd:

- subscription-ID en tenant-ID
- Azure-regio van workspace, storage en resource group
- Databricks workspace SKU en pricing tier
- Storage redundancy/replication, TLS- en firewallinstellingen
- Private Endpoint/VNet injection en publieke netwerktoegang
- Access Connector managed identity object-ID
- Storage RBAC-rollen en scopes
- Event Grid subscription filters en dead-lettering
- Log Analytics/diagnostic settings en retentie
- Azure budget, tags en cost-center

Deze punten zijn bewust als open controlepunten opgenomen; ze zijn niet uit de
beschikbare lokale Azure-authenticatie afgeleid.

## 8. SQL-scriptregistry

SQL die onderdeel is van de ETL-keten wordt geregistreerd in
`contoso_meta_<env>.metadata.meta_sql_script`. GitHub blijft de versiebron,
maar de registry bevat de exacte uitvoerbare inhoud, versie, checksum,
bronobjectkoppeling en status.

Voor Bronze worden de `CREATE TABLE`-blokken per `source_object_id` geregistreerd.
De setup-job leest de actieve Bronze-versie uit deze registry, controleert de
checksum en voert daarna de SQL uit. Daardoor kan de Control Room in het
detailscherm precies het relevante blok tonen en wijzigen, zonder alle andere
bronnen in hetzelfde SQL-bestand te tonen.

Een wijziging doorloopt altijd deze statussen:

```text
DRAFT -> APPROVED -> ACTIVE -> RETIRED
```

De app maakt de wijziging als Pull Request in GitHub. Pas na merge, bundle
deployment en een succesvolle setup-/validatierun wordt de nieuwe versie actief.
