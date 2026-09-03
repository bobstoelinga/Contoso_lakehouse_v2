# Projectverslag — Contoso Lakehouse v2

**Versie:** 1.0  
**Datum:** 3 september 2026  
**Status:** conceptueel ontwerp en dev-validatie  
**Repository:** `Contoso_lakehouse_v2`  
**Branch:** `main`  
**Laatste commit:** `a1e4f5e`

## 1. Managementsamenvatting

Dit project ontwikkelde een conceptuele, metadata-gedreven ETL-oplossing voor Contoso Sales op Databricks. Het ontwerp gebruikt Unity Catalog, Delta Lake, Auto Loader, Quality/Reject, Data Vault 2.0 en historische en actuele Gold-datamarts.

De oplossing is technisch substantieel uitgewerkt en meerdere end-to-end paden zijn in een dev-omgeving gevalideerd. Belangrijke ontwerpcontroles zijn aanwezig: een delivery-gate, chronologische verwerking, schema-driftbeleid, idempotente loads, metadata-validatie, audit-events en atomische publicatie van actuele Gold-data.

**Productieadvies:** nog niet productieklaar. De basis is geschikt voor verdere hardening en een gecontroleerde testomgeving, maar productieacceptatie moet wachten op een volledige positieve stresstest, de negatieve proeven, bewezen SCD2/delete-verwerking, operationalisering van governance en formele security-, SLA-, recovery- en kostencontroles.

## 2. Projectomschrijving

Het doel was een generiek ETL-framework te ontwerpen waarbij nieuwe bronobjecten zoveel mogelijk via metadata worden toegevoegd, zonder nieuwe notebooks of workflowlogica te schrijven.

De oorspronkelijke vraag beschreef de volgende keten:

`Volume -> Bronze -> Quality/Reject -> Data Vault -> Gold Historisch -> Gold Actueel`

Orders, Customers en Products komen uit een bron systeem. Bestanden worden per bron en ontvangstdatum aangeleverd. Auto Loader verwerkt de bestanden incrementeel naar Bronze. Vervolgverwerking mag pas starten wanneer alle verplichte objecten van dezelfde levering beschikbaar en succesvol geladen zijn.

## 3. Requirements

### Functionele requirements

- Bestanden per bron en datumfolder ontvangen.
- Auto Loader incrementeel vanuit Volume naar Bronze laten lezen.
- Nieuwe bronkolommen gecontroleerd kunnen verwerken via schema evolution.
- Een delivery-gate afdwingen voor alle verplichte objecten van dezelfde levering.
- Metadata gebruiken voor bronobjecten, mappings, kwaliteitsregels, afhankelijkheden en Data Vault-mappings.
- Kwaliteitsfouten vastleggen in Reject met reden en originele payload.
- Gevalideerde gegevens naar Raw Vault en Business Vault laden.
- Historische Gold-datamarts met SCD2-historie bouwen.
- Actuele Gold-datamarts alleen na een volledig succesvolle business load publiceren.
- Bij een fout de vorige actuele Gold-release actief houden.
- Runs, statussen, fouten, row counts en metadata-versies auditen.

### Niet-functionele requirements

- Schaalbaar naar tientallen bronsystemen en honderden tabellen.
- Idempotent herstarten na fouten.
- Geen hardcoded entiteitsafhankelijkheden in workflows.
- Herleidbaarheid naar levering, batch, bronbestand en metadatarelease.
- Beheersbare Delta-performance zonder handmatig partitiebeheer.
- Scheiding van omgevingen, rollen en consumerrechten.

## 4. Opgeleverde architectuur

### Lagen

- **Landing/Volume:** external volume op ADLS Gen2 met datumfolders.
- **Bronze:** 1-op-1 brondata met technische metadata, Auto Loader en checkpoints.
- **Quality:** getypeerde en gevalideerde records volgens metadata.
- **Reject:** afgekeurde records, alle faalredenen en originele JSON-payload.
- **Raw Vault:** hubs, links en insert-only satellites.
- **Business Vault:** afgeleide satellites en PIT-ondersteuning.
- **Gold Historisch:** dimensioneel model met volledige historie.
- **Gold Actueel:** publieke views op fysieke v1/v2-slots met een groepsreleasepointer.
- **Metadata/Audit:** configuratie, afhankelijkheden, DQ-resultaten, runs, deliveries en publicaties.

### Belangrijkste ontwerpbesluiten

- Auto Loader detecteert bestanden; `foreachBatch` verwerkt per delivery en chronologisch.
- Een delivery krijgt een volgnummer; een latere delivery wacht op de oudste nog niet afgehandelde delivery.
- Satellites zijn fysiek insert-only; `load_end_date` en `is_current` worden via views bepaald.
- SHA-256, normalisatie, null-token, separator en conventieversie zijn centraal vastgelegd.
- Multi-source hubs gebruiken een collision code.
- Kwaliteitsregels worden in een gecombineerde evaluatiepass verwerkt.
- Schema drift faalt bewust onder het ingestbeleid en wordt geaudit.
- Metadataexpressies worden vooraf met `EXPLAIN` gevalideerd.
- Bronze gebruikt begrensde Serverless fan-out; auditstatussen zijn append-only events.
- Gold Actueel publiceert per publication group via een atomische releasepointer.
- Elke run verwijst naar een deterministische metadatarelease.
- Onderhoud, OPTIMIZE, VACUUM en freshness-monitoring zijn als ontwerpcomponent opgenomen.

## 5. Implementatie-overzicht

| Onderdeel | Implementatie |
|---|---|
| Bundle en omgevingen | `databricks.yml`, targets voor dev/tst/prd |
| Catalogs, schemas, volumes, grants | `sql/00_unity_catalog` |
| Metadata model | `sql/01_metadata/10_metadata_model.sql` |
| Audit model en gates | `sql/01_metadata/11_audit_model.sql` |
| Metadata migraties | `sql/01_metadata/12_metadata_migrations.sql` |
| Monitoring | `sql/01_metadata/13_monitoring_queries.sql` |
| Seed metadata | `metadata/seed` |
| Python framework | `src/contoso_lakehouse` |
| Databricks notebooks | `notebooks` |
| Jobs | `workflows` |
| Lokale regressietests | `tests/test_metadata_consistency.py` |

## 6. Mijlpalen en resultaten

1. Eerste architectuur ontworpen voor Volume, Bronze, Quality, Reject, Vault en Gold.
2. Kritische review uitgevoerd op schaalbaarheid, afhankelijkheden, performance, schema evolution en actuele marts.
3. P0/P1-bevindingen verwerkt in metadata, SQL, Python en workflows.
4. Unity Catalog external location, file-arrival-trigger en DDL-uitvoering hersteld en gevalideerd.
5. Serverless-onverenigbare sessieconfiguratie vervangen door schema evolution op de MERGE-operatie.
6. Bronze-fan-out en append-only audit-events toegevoegd.
7. Gold Actueel gewijzigd naar atomische publication groups.
8. Metadatarelease-fingerprint aan audit toegevoegd.
9. Delivery-gate en remediation voor geblokkeerde demo-leveringen gevalideerd.
10. Een volledige delivery op 9 september en een tweede op 10 september doorliepen metadata, Bronze, gate, Quality, Raw Vault, Business Vault, Gold Historisch en Gold Actueel.
11. Een stresstestdelivery met 100.000 Customers, 50.000 Products, 10.000 Employees, 1.000.000 Orders en 20.000 Returns is gegenereerd in 140 Parquet-bestanden.
12. De lokale regressiesuite is uitgebreid naar 69 geslaagde tests.

## 7. Testplan en uitgevoerde tests

### Lokale tests

Uitgevoerd:

```text
python -m pytest -q
69 passed
```

De tests controleren onder meer hash-conventies, veilige identifiers, placeholder-resolutie, DQ-thresholds, parallelle execution waves, cyclusdetectie, audit-fouten, reject-clearing, schema-driftbeleid, Gold-publicatievoorwaarden, seed-versies en metadata-contracten.

### End-to-end dev-validatie

Bewezen:

- Metadata-validatie vóór verwerking.
- Bronze-fan-out met vijf objecten.
- Delivery-gate.
- Quality-blokkade bij ongeldige data.
- Geen Vault- of Gold-verwerking na Quality-falen.
- Raw Vault, Business Vault, Gold Historisch en Gold Actueel bij valide deliveries.
- Atomische `SALES_MART`-publicatie.
- Behoud van de vorige Current Gold-release bij een mislukte build.
- Gecontroleerd superseden met reden, goedkeurder en referentie.
- Registratie van technische fouten en herstelacties.

### Nog uit te voeren of niet geaccepteerd

- Tien opeenvolgende productierepresentatieve deliveries.
- Positieve `change_set=1`-verwerking met aantoonbare wijzigingen en deletes.
- Controle van SCD2, hashdiffs, delete-status en actuele Gold na die wijzigingen.
- Incomplete delivery en later arriverend bestand.
- Chronologie met N+1 gereed terwijl N onvolledig is.
- DQ- en reject-herverwerking.
- Nieuwe niet-gemapte kolom en typewijziging.
- Herstart op elk belangrijk breekpunt.
- Gold-buildfout met bewijs dat alle publieke views de oude groep blijven tonen.
- Fan-out en audit-concurrency op enterprise-schaal.
- Performance-, SLA- en kostenmeting op representatieve productievolumes.

## 8. Productie-readiness

### Status per domein

| Domein | Beoordeling | Toelichting |
|---|---|---|
| Architectuur | Groen/amber | Sterke basis; conceptueel passend voor de doelstelling. |
| Metadata-gedreven ontwerp | Groen | Seed, mappings, regels, afhankelijkheden en DV/Gold-definities aanwezig. |
| Delivery-gate | Groen/amber | Werkt in dev; uitgebreide failure-matrix nog uitvoeren. |
| Data Vault | Amber | Kernmodel werkt; effectivity satellite-loadlogica staat nog open. |
| Gold Actueel | Groen/amber | Atomische groepspublicatie ontworpen en deels bewezen; foutinjectie nog formeel testen. |
| Schema evolution | Amber | Beleid en retries aanwezig; governanceproces voor nieuwe kolommen moet worden ingericht. |
| Schaalbaarheid | Amber | Fan-out is begrensd; tien-delivery en enterprise-volume benchmark ontbreken. |
| Governance | Rood/amber | Owner, PII, retentie, SLA en cost center ontbreken nog als metadata-contract. |
| Operationeel beheer | Amber | Monitoring en maintenance bestaan; runbooks, on-call en rejectproces moeten worden belegd. |
| Security | Amber | UC-grants zijn ontworpen; formele autorisatie- en secretscan moet nog worden uitgevoerd. |
| Disaster recovery | Rood/amber | Backup/restore, replay, cross-region en RPO/RTO zijn niet aangetoond. |
| Kostenbeheersing | Amber | Serverless gekozen; DBU- en Azure-usage moeten nog worden gemeten en begrensd. |

### Besluit

**Niet vrijgeven voor productie.** Het project is geschikt als architectuurprototype en als basis voor een gecontroleerde testomgeving. Productieacceptatie vereist minimaal:

1. volledige testmatrix inclusief tien-delivery stresstest;
2. bewezen SCD2, deletes, effectivity en idempotente recovery;
3. formele security-, privacy-, governance- en autorisatiegoedkeuring;
4. ingevulde omgevingsparameters en productie-identiteiten;
5. SLA, RPO/RTO, monitoring, alerting, runbooks en on-call-proces;
6. gemeten DBU-, opslag- en egresskosten met budgetlimieten;
7. gecontroleerde CI/CD-promotie naar `tst` en `prd`;
8. formele businessacceptatie van Gold-contracten en freshness.

## 9. Openstaande acties

| Prioriteit | Actie | Eigenaar bij overdracht |
|---|---|---|
| P0 | Volledige end-to-end acceptatietest afronden | Data engineering |
| P0 | Production security, privacy en grants testen | Platform/security |
| P0 | RPO/RTO, restore en replay aantonen | Platform/operations |
| P1 | CDC en partial snapshot laadstrategieën toevoegen | Data engineering |
| P1 | Effectivity satellite-loadlogica toevoegen | Data Vault engineering |
| P1 | Reject-herverwerking als werkproces inrichten | Data operations/data stewards |
| P1 | Governancevelden toevoegen: owner, PII, retentie, SLA, cost center | Data governance |
| P1 | DBU/Azure-kosten meten en budgetalerts instellen | FinOps/platform |
| P2 | Delta control-table lookup voor zeer grote fan-outs | Platform engineering |
| P2 | Metadata-SCD2 alleen toevoegen als runtime-configuratie buiten Git nodig is | Architecture board |

## 10. Tijd en kosten

De lokale Copilot-sessiehistorie registreert voor het v1- en v2-traject samen 17 interactieve sessies met 383 turns. De som van de geregistreerde sessievensters is ongeveer **31 uur en 55 minuten**. De kalenderdoorlooptijd liep van 29 augustus tot 2 september 2026, ongeveer **4 dagen en 11 uur**.

Dit is geen betrouwbare factuurmeting: pauzes kunnen zijn inbegrepen en werk buiten Copilot ontbreekt. De lokale opslag bevat geen tokengebruik, modelprijzen of Copilot-factuurgegevens. Ook zijn de Databricks DBU- en Azure-verbruikskosten niet vastgelegd; deze moeten uit Databricks system billing en Azure Cost Management worden gehaald.

## 11. Overdracht

De technische besluiten en runtimebevindingen staan in [00_besluitenlog.md](00_besluitenlog.md). Dit document is de samenvatting voor besluitvorming en overdracht. De aanbevolen volgende stap is het afronden van de acceptatietestmatrix en het herbeoordelen van de productie-gate op basis van meetresultaten, securitybewijs en operationele eigenaarschap.
