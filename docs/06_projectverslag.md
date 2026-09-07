# Eindverslag - Contoso Lakehouse v2

**Versie:** 2.0  
**Datum:** 7 september 2026  
**Status:** afgerond prototype, gevalideerd in `dev`  
**Technologie:** Azure Databricks, Unity Catalog, Delta Lake, Databricks Workflows en Microsoft Fabric SQL Database als bron

## 1. Managementsamenvatting

Dit project realiseert een **werkend, aantoonbaar gevalideerd metadata-gedreven Lakehouse-prototype met enterprise-ontwerpprincipes**. Het prototype verwerkt bestandsleveringen en externe pull-bronnen via Landing Volume, Bronze, Quality/Reject, Data Vault of Reference Data, historisch Gold en actueel Gold.

De runtime en governance-laag zijn Azure Databricks, Unity Catalog en Delta Lake. Microsoft Fabric is aangesloten als werkende SQL-bron: een Fabric SQL Database wordt met JDBC uitgelezen, schrijft een immutable Parquet-levering naar het Databricks Landing Volume en wordt vervolgens door dezelfde metadata-gedreven keten verwerkt. Dit is dus geen native Fabric Lakehouse-implementatie.

De kernketen en de belangrijkste foutscenario's zijn in `dev` bewezen. De Fabric Sales-delivery `FABRIC_SALES|2026-09-06` doorliep succesvol Bronze, delivery-gate, Quality, Reference Data, Gold Historisch en Gold Actueel. Een onafhankelijke read-only controle telde 542 rijen in elk van deze lagen. De lokale regressiesuite eindigde met **98 geslaagde tests**.

De oplossing is geschikt als gevalideerd architectuurprototype en als basis voor een gecontroleerde testomgeving. Productieacceptatie vereist nog bewijzen voor volume, recovery, beveiliging, governance, kosten en operationeel beheer.

## 2. Aanleiding en doelstelling

De opdracht was een metadata-gedreven Lakehouse-oplossing te ontwerpen met de lagen Volume, Brons, Quality, Data Vault, Historische Datamart en Actuele Datamart. Orders, Customers en Products vormen samen een levering. Vervolgverwerking mag pas starten wanneer alle verplichte objecten voor dezelfde ontvangstdatum succesvol zijn geladen.

Het doel was een herbruikbaar framework, niet een eenmalige Sales-pipeline: nieuwe bronobjecten moeten zo veel mogelijk met metadata worden toegevoegd, zonder nieuwe notebooks of workflowlogica te bouwen.

```text
Bronbestand, API of Fabric SQL
  -> immutable Landing Volume
  -> Bronze
  -> metadata-gestuurde delivery-gate
  -> Quality / Reject
  -> Raw Vault of Reference Data
  -> Business Vault
  -> Gold Historisch
  -> Gold Actueel
```

## 3. Scope en afbakening

### Binnen scope

- Metadata voor systemen, objecten, connectors, mappings, afhankelijkheden, kwaliteitsregels, Data Vault en Gold.
- Landing via Unity Catalog Volumes en incrementele Bronze-ingest via Auto Loader, checkpoints en Delta Lake.
- Quality-controles, traceerbare rejects en auditeerbare deliveries en runs.
- Data Vault 2.0-principes voor historiserende Sales- en CBS-data.
- De expliciete `REFERENCE_DATA`-route voor ECB, Nager en Fabric Sales.
- Historische en actuele Gold-datamarts met atomische publicatie.
- Pull-extracten voor CBS, ECB, Nager en Fabric SQL; file-arrival voor Sales.

### Buiten scope

- Native Fabric Lakehouse-, Fabric Data Factory- of Fabric Notebook-runtime.
- Formele productie-SLA's, RPO/RTO-bewijs, 24x7-on-call en volledige FinOps.
- Business-mastering en semantische multi-source integratie.
- Een generieke CDC-implementatie voor alle connectoren.

## 4. Requirements en realisatie

| Requirement | Realisatie | Bewijsstatus |
|---|---|---|
| Leveringen per bron en datumfolder | Bron-specifieke immutable Landing Volumes | Gevalideerd in dev |
| Incrementeler Bronze-load | Auto Loader met checkpoints en metadata per object | Gevalideerd in dev |
| Nieuwe bronkolommen accepteren | `RESCUE`, `addNewColumns` en Delta `MERGE WITH SCHEMA EVOLUTION` | Gevalideerd met Fabric Sales |
| Onvolledige levering blokkeren | Delivery-gate op verplichte objecten en chronologische selectie | Gevalideerd in dev |
| Metadata-gedreven besturing | Objecten, mappings, DQ, routes, DV en Gold in Git-beheerde seedmetadata | Gevalideerd |
| Rejects opslaan | Payload, alle faalredenen en herstelstatus in `rj_*` | DDL en gedrag gevalideerd |
| Historiseren | Raw Vault, Business Vault en SCD2 Gold Historisch | Gevalideerd in dev |
| Actuele datamart | Current views op v1/v2-slots met groepsreleasepointer | Gevalideerd in dev |
| Oude versie behouden bij fout | Pointer wijzigt pas na volledige succesvolle build | Ontwerp en foutscenario gevalideerd |
| Herleidbaarheid | Audit-events, delivery, batch, bronbestand en metadata-versie | Gevalideerd in dev |

## 5. Architectuur

| Laag | Verantwoordelijkheid |
|---|---|
| Landing Volume | Ongewijzigde, immutable bronbestanden per datum |
| Bronze | Alle bronkolommen plus technische lineage, zonder businessfilter |
| Quality | Typen, contracteren en valideren volgens metadata |
| Reject | Ongeldige records met originele payload en alle faalredenen |
| Raw Vault | Hubs, links en historiserende satellites zonder interpretatie |
| Business Vault | Computed satellites, PIT en versioned referentiedata |
| Gold Historisch | Historisch dimensioneel contract met SCD2-informatie |
| Gold Actueel | Laatste volledig succesvolle release per publicatiegroep |
| Metadata/Audit | Besturing, versiebeheer, status, kwaliteit en monitoring |

### Metadata als besturingslaag

De bestanden in `metadata/seed` zijn de versieerbare bron van waarheid. De setup-job synchroniseert deze idempotent naar Unity Catalog. Het model bevat systemen, objecten, connectors, mappings, kwaliteitsregels, afhankelijkheden, Data Vault-entiteiten, Gold-entiteiten en auditmetadata.

Elke metadatarelease krijgt een deterministische SHA-256-fingerprint. Audit-runs bewaren die versie, zodat incidentonderzoek en herverwerking aan de exacte Git/DAB-configuratie kunnen worden gekoppeld.

### Orchestratie en delivery-gate

De workflow bevat alleen technische lagen; inhoudelijke entiteitsafhankelijkheden staan in metadata en worden topologisch geordend.

```text
validate_metadata
  -> plan_bronze_fanout
  -> bronze_ingest
  -> delivery_gate
  -> quality
  -> raw_vault en/of reference_data
  -> business_vault
  -> gold_historical
  -> gold_current
```

Bronze gebruikt een begrensde Databricks `for_each_task` per bronobject. De gate opent alleen voor een complete levering met succesvolle verplichte objecten. Een latere levering wacht op de oudste nog niet afgehandelde levering van hetzelfde bronsysteem. Dit beschermt de tijdsvolgorde van snapshot- en SCD2-verwerking.

De end-to-end pipeline kan standaard drie gelijktijdige runs uitvoeren. Deze
paralleliteit is bedoeld voor onafhankelijke bronsystemen. De delivery-gate
handhaaft de chronologische seriele verwerking binnen ieder bronsysteem; voor
een hoger volume moeten runtimes, Delta-concurrency en kosten eerst worden
gemeten.

### Data Vault en Gold

De Raw Vault gebruikt SHA-256, centrale normalisatie, een null-token en separator. Multi-source hubs gebruiken een collision code. Satellites zijn fysiek insert-only; `load_end_date` en `is_current` worden via views afgeleid. Zo wordt historisch Delta-data niet bij iedere load herschreven.

Gold Actueel gebruikt per publicatiegroep twee fysieke slots (`_v1` en `_v2`). Publieke views volgen een enkele releasepointer. Bij een mislukte build blijft die pointer ongewijzigd en zien BI-consumenten de vorige consistente release.

### Brontypen

| Bronsysteem | Aanleverpatroon | Status |
|---|---|---|
| `SALES` | Push: ERP-export naar Landing | Actief |
| `CBS` | Pull: OData naar Landing | Actief |
| `ECB` | Pull: HTTP/CSV naar Landing | Actief |
| `NAGER` | Pull: HTTP/JSON naar Landing | Actief |
| `FABRIC_SALES` | Pull: JDBC uit Fabric SQL naar Landing | Actief en gevalideerd |
| `SHAREPOINT` | Voorbereid in metadata | Inactief |

Wanneer een immutable delivery al in Landing staat, kan de generieke pipeline direct vanaf Bronze worden gestart. Dit is het gecontroleerde herstelpad na een fout in Quality, Vault of Gold, zonder de bron opnieuw te belasten.

## 6. Belangrijkste ontwerpkeuzes

1. **Landing is immutable.** Extracties schrijven eerst naar staging en publiceren pas na succesvolle voltooiing naar de datumfolder.
2. **Bronze bewaart de bron.** Bij `RESCUE` blijven onbekende velden fysiek in Bronze via schema evolution; Quality bepaalt het expliciete businesscontract.
3. **Metadata is privileged code.** Metadata is Git-beheerd, identifiers worden gevalideerd en eindgebruikers krijgen in productie geen schrijfrechten.
4. **Quality werkt in een gecombineerde pass.** Dit voorkomt N tabelscans voor N kwaliteitsregels.
5. **Audit is append-only.** Parallelle Serverless-taken schrijven events; statusviews leiden daaruit de actuele status af.
6. **Routes zijn expliciet.** `RAW_VAULT` en `REFERENCE_DATA` voorkomen dat referentiebronnen onnodig als hubs, links en satellites worden gemodelleerd.
7. **Gold Actueel is een groepscontract.** De publicatiegroep voorkomt nieuwe dimensies naast oude feiten.

## 7. Validatie en herstelbevindingen

### Lokale en deploymentvalidatie

De regressiesuite controleert onder meer hashconventies, veilige identifiers, placeholderresolutie, DQ-drempels, afhankelijkheidsgrafen, schema-drift, Gold-publicatie, connectoren en DDL-dekking.

```text
py -m pytest -q
98 passed
```

Daarnaast is lokaal geborgd dat de zelfstandige CBS-, ECB- en Fabric Sales-laadjobs
eerst hun pull-/extracttaak uitvoeren en pas daarna de generieke pipeline starten.
De pull-laadjobs zijn bovendien voorzien van Databricks schedules: in `dev`
standaard gepauzeerd en in `tst`/`prd` activeerbaar via de bundle-variabele
`pull_schedule_pause_status`.

De Databricks Asset Bundle is gevalideerd en naar `dev` gedeployed. De idempotente setup-job is na relevante DDL- en metadatawijzigingen succesvol uitgevoerd.

Op 7 september 2026 is de stressleveringsgenerator aangescherpt na een instabiele run. De write-stap valideerde eerder exact het aantal part-files per object en kon daardoor falen bij adaptive Spark-planning. De generator accepteert nu elk positief aantal geschreven part-files, verwijdert vooraf eventuele stagingresten en geeft expliciete feedback over het daadwerkelijk geschreven aantal bestanden per object. Daarnaast geeft het notebook nu een duidelijkere melding bij hergebruik van een bestaande `delivery_date`.

De hervatting van Fase 2 van de Sales-stresstest is op 7 september 2026 operationeel geblokkeerd. Een lokale `databricks bundle validate --target dev` bereikte de dev-workspace, maar eindigde met HTTP 403 `Invalid access token`; er is daardoor geen job of datawijziging gestart. Vernieuw eerst de lokaal geconfigureerde Databricks-authenticatie en valideer de bundle opnieuw. Start daarna de pipeline voor `SALES`: de chronologische gate moet eerst `SALES|2026-09-10` verwerken en pas vervolgens `SALES|2026-09-11` met `change_set=1`. Controleer na beide succesvolle runs de SCD2-versies, deletes, hashdiffs en de actieve `SALES_MART`-publicatiegroep.

Na vernieuwde CLI-authenticatie valideerde de Asset Bundle op 7 september 2026 succesvol voor `dev`. Pipelinerun `775524928887640` startte voor `SALES` en eindigde technisch succesvol na 244 seconden. Metadata-validatie en alle vijf Bronze-objecttaken slaagden; de delivery-gate gaf `SKIPPED` terug en zette de conditionele vervolgroute correct uit. Daardoor startten Quality, Vault en Gold niet. De gate retourneert uitsluitend `SKIPPED` wanneer `v_next_processable_delivery` geen complete, nog niet verwerkte Sales-delivery bevat. De eerder genoemde folders `2026-09-10` en `2026-09-11` zijn in deze runtime dus niet beschikbaar als verwerkbare wachtrij of voldoen niet aan het actuele metadata-contract; vervolgonderzoek vereist read-only inspectie van `audit_delivery`, `audit_delivery_object` en de Gold-publicatiegroep.

### Bewezen scenario's

- Valide Sales-deliveries doorlopen de volledige Raw Vault- en Gold-keten.
- Een ongeldige delivery wordt bij Quality geblokkeerd; Vault en Gold starten dan niet.
- De chronologische gate verwerkt geen nieuwere delivery zolang een oudere nog geblokkeerd is.
- CBS, ECB en Nager zijn als brongebonden data-producten ingericht.
- ECB filtert expliciet niet-castbare SDMX-observaties, maar blokkeert nul- en negatieve wisselkoersen nog steeds hard.
- Gold-entiteiten zijn aan `source_system_id` gebonden. Een niet-Sales-delivery kan daardoor geen `SALES_MART` publiceren.
- `FABRIC_SALES|2026-09-06` is volledig verwerkt met 542 rijen in Bronze, Quality, Reference Data, historisch Gold en actueel Gold.

### Fabric Sales: feitelijke herstelcyclus

De eerste Fabric Sales-run faalde terecht in Bronze: de live metadata voor onbekende velden was niet met de Git-seed gesynchroniseerd en vereiste mapping-goedkeuring. Na deploy en setup stond de runtime weer op `RESCUE`.

De volgende run bereikte Quality en toonde dat `total_due_amount` voor alle 542 regels `NULL` was. Read-only onderzoek bevestigde dat `subtotal_amount`, `tax_amount` en `freight_amount` gevuld waren. De Quality-mapping leidt daarom het totaal deterministisch af als:

$$
\text{total\_due\_amount} = \text{subtotal\_amount} + \text{tax\_amount} + \text{freight\_amount}
$$

De harde DQ-controle voor ontbrekende of negatieve totalen blijft actief. Een daaropvolgende run vond de ontbrekende rejecttabel `contoso_reject_dev.fabric_sales.rj_order_lines`; deze is idempotent aan de DDL en het steward-overzicht toegevoegd. Na deploy en setup was de volgende run volledig succesvol.

Deze cyclus toont gecontroleerd herstel: brondata is niet aangepast; alle correcties waren metadata- of DDL-gedreven, lokaal getest, gedeployed en in de runtime opnieuw bewezen.

## 8. Enterprise-beoordeling

### Sterke punten

- Nieuwe objecten zijn grotendeels configureerbaar zonder pipelinecode.
- Bronsystemen en Gold-publicaties zijn bewust gescheiden.
- Immutable Landing, delivery-gate en chronologie beperken gedeeltelijke en out-of-order historisatie.
- Metadata-validatie, DQ en traceerbare rejects maken beheer op grotere schaal realistisch.
- De atomische Gold-pointer beschermt consumenten tegen inconsistente marts.

### Grenzen en resterende risico's

- Task values voor fan-out hebben een limiet. Bij veel objecten is een Delta control-tabel als plannerinput nodig.
- `SNAPSHOT_SCD2` voor Reference Data mist nog een formeel snapshot-compleetheidscontract en expire-logica voor verdwenen sleutels.
- Multi-source integratie vereist een aparte Business Vault-run met matching, mastering, freshness- en versiecontracten.
- Nieuwe Bronze-kolommen zijn technisch veilig op te slaan, maar vragen nog een formeel governanceproces voor classificatie, mapping en Gold-publicatie.
- De uitgevoerde tests bewijzen gedrag, niet productiecapaciteit, kostenplafond of herstelbaarheid binnen een afgesproken RPO/RTO.

## 9. Productie-readiness

| Domein | Status | Benodigde vervolgstap |
|---|---|---|
| Functionele kernketen | Groen | Behouden als acceptatiebaseline |
| Metadata en orkestratie | Groen/amber | Schaaltest en plannerfallback toevoegen |
| Schema evolution | Groen/amber | Governance- en approvalproces operationaliseren |
| Data Vault | Amber | Effectivity- en delete-scenario's aantonen |
| Gold Actueel | Groen/amber | Foutinjectie over alle views herhalen |
| Security en privacy | Amber | Least-privilege, PII-classificatie en secretscan formaliseren |
| Operations en recovery | Amber/rood | Runbooks, replay en RPO/RTO testen |
| Performance en kosten | Amber | Volume-, DBU- en storagebenchmark uitvoeren |

Het advies is: **nog niet vrijgeven voor productie**, maar het resultaat wel accepteren als werkend en aantoonbaar gevalideerd prototype. Een volgende fase moet gericht zijn op het bewijzen en operationaliseren van niet-functionele productiekwaliteit, niet op ontbrekende basisfunctionaliteit.

## 10. Conclusie

De doelstelling is gerealiseerd. Er staat een metadata-gedreven Lakehouse-prototype waarin bronnen via Landing gecontroleerd worden verwerkt, datakwaliteit en afwijzingen traceerbaar zijn, historisatie via Data Vault of versioned Reference Data plaatsvindt en Gold Actueel uitsluitend na een volledige succesvolle run atomisch wordt gepubliceerd.

De uitvoering toont bovendien dat het ontwerp onder realistische fouten beheersbaar blijft. Schema-drift, een onvolledig bedragcontract en een ontbrekende rejectvoorziening zijn onderzocht, structureel hersteld en opnieuw gevalideerd. Daarmee is dit meer dan een architectuurtekening: een **werkend, aantoonbaar gevalideerd metadata-gedreven Lakehouse-prototype met enterprise-ontwerpprincipes**.

Voor technische verdieping wordt verwezen naar [architectuur](01_architecture.md), [workflowontwerp](05_workflow_design.md) en de [besluitenlog](00_besluitenlog.md).

## 11. Architectuurreview 7 september 2026

Op verzoek is een kritische review uitgevoerd vanuit het perspectief Principal Data Architect (Databricks, Delta Lake, Data Vault 2.0, Fabric). Kernbevindingen:

- **Schaalbaarheid**: delivery-gate en chronologische catch-up serialiseren te sterk voor honderden objecten; Gold-MERGE doet full-scans zonder incrementeel venster.
- **Beheer**: metadata mist versie-/approvalproces (policy `ALLOW_NEW_COLUMNS_WITH_APPROVAL` heeft geen approval-mechanisme), retry-configuratie is gedenormaliseerd, cycle-detectie op de afhankelijkheidsgraaf ontbreekt.
- **Performance**: Auto Loader-checkpoints liggen in de raw Volume (retentierisico); `mergeSchema`-reader-optie is overbodig; liquid clustering verkiezen boven ZORDER op hash-keys.
- **Afhankelijkheden**: `SAME_DELIVERY` is gedefinieerd maar nergens toegepast; out-of-order leveringen (late levering die in het verleden moet invoegen) zijn ongedefinieerd.
- **Metadata-tekortkomingen**: delete-semantiek (hard/soft/absence), late-arrival-window, freshness-SLA, schema-contractversie en backfill-strategie ontbreken.
- **Laadstrategieën**: `INCREMENTAL_CDC` en `PARTIAL_SNAPSHOT` zijn enterprise-kritisch en moeten vóór verdere brononboarding worden geïmplementeerd; scope van `SNAPSHOT_SCD2` (welke laag, absence-detectie) moet expliciet.
- **Schema evolution**: alleen additief gedekt; rename/drop en remediatie vanuit `_rescued_data` ontbreken; bij schaal is een voorgestelde-metadata-diff-generator nodig.
- **Gold Actueel**: slotwisseling is per entiteit (inconsistentievenster binnen een publication group); soft deletes verdwijnen stilletjes uit de actuele mart; rollback beperkt tot één versie.
- **Fabric**: hybride landschap vereist expliciete keuzes rond OneLake-shortcuts, tweede security-perimeter en eigenaarschap van de actuele mart.

Besluit: fundament (metadata-gedrevenheid, gate, rejects, atomische publicatie) is enterprise-waardig; prioriteit ligt bij delete-/rename-semantiek, CDC + PARTIAL_SNAPSHOT, metadata-CI/CD en tiering van de gate.

## 12. Verwerking review — 7 september 2026

Eerste, laag-risico reeks verbeteringen is doorgevoerd:

- **`meta_source_object.json`**: alle elf bronobjecten hebben nu `delete_semantics`, `absence_means_delete`, `schema_contract_version`, `late_arrival_window_days`, `freshness_sla_hours` en `backfill_strategy`. Delete-semantiek is alleen `SOFT_DELETE_FLAG` waar een `is_deleted`-vlag bestaat; `absence_means_delete` alleen `true` bij volledige gecureerde snapshots (SALES-dimensies, SharePoint, Fabric), niet bij append-only of referentiefeeds (CBS, ECB, Nager). Checkpoints van de SALES-objecten zijn verplaatst van `raw_${env}` naar `control_${env}` om het retentierisico op de landingzone te dichten.
- **`meta_dependency.json`**: de vier kern-links (`LNK_ORDER_CUSTOMER`, `LNK_ORDER_PRODUCT`) gebruiken nu `SAME_DELIVERY` in plaats van `UPSTREAM_SUCCESS`, zodat hubs uit dezelfde levering worden gecombineerd.
- **`10_metadata_model.sql`**: twee nieuwe tabellen toegevoegd — `meta_retry_policy` (referentie voor retry/prioriteit, tegen duplicatie in `meta_dependency`) en `meta_schema_drift_approval` (formele PENDING/APPROVED/REJECTED-workflow, waarmee de bestaande policy `ALLOW_NEW_COLUMNS_WITH_APPROVAL` afdwingbaar wordt).
- **`02_metadata_model.md`**: nieuwe velden en tabellen gedocumenteerd.

Nog openstaand (bewust niet in deze ronde): implementatie van `INCREMENTAL_CDC` en `PARTIAL_SNAPSHOT`, incrementeel venster in Gold-MERGE, gate-tiering op `criticality`, en cycle-detectie op de afhankelijkheidsgraaf.

## 13. Verwerking resterende reviewpunten — 7 september 2026 (tweede ronde)

De vier bewust uitgestelde punten zijn nu alsnog doorgevoerd en getest (86/86 groen):

- **Laadstrategieën `INCREMENTAL_CDC` en `PARTIAL_SNAPSHOT`** geïmplementeerd in [bronze.py](../src/contoso_lakehouse/bronze.py). CDC voegt `_cdc_op` toe aan de MERGE-match zodat I/U/D apart landt; SCD2-resolutie blijft downstream. PARTIAL_SNAPSHOT deelt het APPEND-pad maar respecteert `absence_means_delete=false`, zodat een deelsnapshot nooit als delete wordt geïnterpreteerd. De test die beweerde dat CDC onbekend was, is omgebouwd tot een test die het correcte MERGE-gedrag bewijst.
- **Incrementeel venster in Gold-MERGE** in [gold.py](../src/contoso_lakehouse/gold.py): `load_historical` accepteert nu `incremental_since`; bij SCD2-entiteiten filtert de subquery op `load_date >= incremental_since`, waardoor de full-scan-per-run verdwijnt. Zonder waarde valt de loader veilig terug op de volledige set (backfill).
- **Gate-tiering op `criticality`** in [orchestration.py](../src/contoso_lakehouse/orchestration.py): naast `require_delivery_complete` is er `require_delivery_critical_complete`, die alleen HIGH-criticaliteitsobjecten afdwingt. Eén traag MEDIUM/LOW-object blokkeert de keten niet meer.
- **Governancevelden in het dataclass-model** in [metadata.py](../src/contoso_lakehouse/metadata.py): `SourceObject` draagt nu `delete_semantics`, `absence_means_delete`, `schema_contract_version`, `late_arrival_window_days`, `freshness_sla_hours` en `backfill_strategy`, zodat de loaders de seed-waarden daadwerkelijk kunnen gebruiken.

Correctie op paragraaf 11: Gold Actueel publiceert al atomisch per publicatiegroep via één gedeelde releasepointer (`publish_group`), waardoor het eerdergenoemde inconsistentievenster binnen een groep niet bestaat.

## 14. Liquid clustering metadata-gedreven gemaakt — 7 september 2026

- De Gold-DDL gebruikte al `CLUSTER BY`, maar de metadata heette nog `zorder_columns` en was niet leidend. Dat is nu rechtgetrokken.
- [meta_gold_entity.json](../metadata/seed/meta_gold_entity.json): alle 23 Gold-entiteiten gebruiken `cluster_columns`; `zorder_columns` is volledig verdwenen. Feiten behouden `partition_columns` op datum; dimensies gebruiken uitsluitend clustering op hun hash-key.
- [metadata.py](../src/contoso_lakehouse/metadata.py): `GoldEntity` draagt nu `partition_columns` en `cluster_columns`, zodat de setup-notebook de DDL metadata-gedreven kan genereren.
- Bestaande tabellen vereisen een eenmalige `ALTER TABLE ... CLUSTER BY` of rebuild; liquid clustering werkt incrementeel en vereist daarna minder `OPTIMIZE`-onderhoud dan ZORDER.