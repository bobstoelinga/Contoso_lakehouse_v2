# Besluitenverslag — Contoso Lakehouse v2

Verslag van de ontwerpsessie: wat is gevraagd, wat is gebouwd, welke besluiten
zijn genomen en welke punten nog openstaan.

- **Project:** Contoso Lakehouse v2 — metadata-gedreven Lakehouse op Databricks
- **Datum:** 30 augustus 2026
- **Repository:** `Contoso_lakehouse_v2` (git, branch `main`)
- **Rollen:** opdrachtgever (gebruiker), Principal Data Architect (assistent)

---

## 1. Opdracht

Ontwerp een metadata-gedreven Lakehouse architectuur in Databricks op basis van
de Contoso datamarts, met de lagen Volume → Bronze → Quality → Reject →
Data Vault → Gold Historisch → Gold Actueel.

Vastgestelde uitgangspunten:

| Uitgangspunt | Invulling |
|---|---|
| Alles als Delta tabel | Elke laag is een managed Delta tabel in Unity Catalog |
| Metadata-gedreven ETL | Bronobjecten, laadstrategie, afhankelijkheden, kwaliteitsregels, bron-doel mappings en Data Vault mappings staan in metadata |
| Levering = datumfolder | `/Volumes/raw/sales/yyyy-MM-dd/{orders,customers,products}.parquet` |
| Bronze via Auto Loader | Incrementeel, schema evolution actief, checkpoints verplicht |
| Leverings-gate | Vervolgverwerking start pas als Orders, Customers én Products van dezelfde datumfolder succesvol zijn geladen |
| Data Vault 2.0 | Hubs, Links, Satellites + Business Vault, historisatie volgens DV2.0 |
| Gold Actueel | Alleen de laatste succesvolle business load; bij falen blijft de vorige versie actief |
| Orchestratie | Databricks Workflows; afhankelijkheden nooit hardcoded |

## 2. Verloop van de sessie

1. **Ontwerp v1** — Eerste volledige uitwerking: Unity Catalog structuur,
   metadata model, Delta tabellen, seed-metadata voor Orders/Customers/Products.
2. **Architectuurreview** — Kritische review op schaalbaarheid, beheer,
   performance, afhankelijkheden, metadata-tekortkomingen, laadstrategieën,
   schema evolution en Gold Actueel, gericht op een enterprise-omgeving met
   tientallen bronsystemen en honderden tabellen.
3. **Ontwerp v2** — De P0- en P1-bevindingen zijn verwerkt in de code en de
   metadata; de resterende punten staan in [§6](#6-openstaande-punten).
4. **Inrichtingssessie (31 augustus 2026)** — Gebruikers-OAuth is lokaal
   gevalideerd met het profiel `contoso-dev`; de bundle is succesvol gedeployed
   naar `dev`. De setup legde aanvankelijk de ontbrekende Unity Catalog external
   location voor de Azure-landingzone bloot; die voorwaarde is daarna ingericht
   en gevalideerd. De setup moet nogmaals worden uitgevoerd.
5. **Validatiesessie (1 september 2026)** — `python -m pytest -q` slaagde eerst
   met 37 tests en na nieuwe regressiedekking met 38 tests. `bundle validate` en
   `setup_lakehouse` voor `dev` zijn geslaagd. De eerste pipeline-run faalde in
   Bronze op een Serverless-onverenigbare sessieconfiguratie; na herstel bereikte
   de tweede run Quality en faalde daar op een niet-opgeloste metadata-placeholder.
   Na herstel doorliep de derde run Bronze, Quality, Raw Vault, Business Vault en
   Gold Historisch; alleen de Gold Actueel factprojectie bevatte nog een typefout.
   De vierde run werd correct door Quality geblokkeerd op een dubbele `customer_key`
   in de reeds bestaande demo-levering `SALES|2026-08-29`; een nieuwe datumfolder
   is gekozen voor de schone eindvalidatie.

---

## 3. Genomen ontwerpbesluiten

### B-01 — Auto Loader detecteert, batch verwerkt per levering
**Probleem:** Auto Loader is bestandsgericht en kent geen levering. Eén
micro-batch kan bestanden uit meerdere datumfolders bevatten (backfill, storing),
waardoor de leverings-gate niet afdwingbaar is en SCD2-historie corrupt raakt.

**Besluit:** De stream schrijft niet rechtstreeks weg. In `foreachBatch` wordt de
micro-batch gesplitst op `_delivery_date` en **chronologisch** per levering
verwerkt en geregistreerd.
→ [src/contoso_lakehouse/bronze.py](../src/contoso_lakehouse/bronze.py)

### B-02 — Expliciete volgordegarantie op leveringen
**Besluit:** `audit_delivery` krijgt `delivery_sequence_number`. De view
`v_next_processable_delivery` levert per bronsysteem de laagste nog niet
verwerkte levering; levering N+1 wacht op N.
→ [sql/01_metadata/11_audit_model.sql](../sql/01_metadata/11_audit_model.sql)

**Afweging:** Doorstroomsnelheid is opgeofferd aan historische correctheid. Bij
snapshotbronnen met SCD2 is out-of-order verwerking niet herstelbaar.

### B-03 — Satellites zijn insert-only
**Probleem:** Fysieke end-dating (`load_end_date`, `is_current` bijwerken)
vereist UPDATEs op historische rijen. In Delta betekent dat het herschrijven van
parquet-bestanden bij elke load — de duurste operatie, en de grootste kostenpost
bij honderden satellites.

**Besluit:** De fysieke tabel (`sat_*_h`) is insert-only. `load_end_date` en
`is_current` komen uit een view met `LEAD(load_date)`. De view (`sat_*`) is het
contract voor alle downstream lagen; Gold merkt het verschil niet.
→ [sql/04_data_vault/40_raw_vault.sql](../sql/04_data_vault/40_raw_vault.sql)

### B-04 — Hash-conventie centraal en versioneerd
**Besluit:** SHA-256 (geen MD5), normalisatie `upper(trim(cast(x as string)))`,
NULL-token `^^`, separator `||`, conventieversie 1. Vastgelegd op één plek;
wijzigen ervan vereist een volledige rebuild van de vault.
→ [src/contoso_lakehouse/hashing.py](../src/contoso_lakehouse/hashing.py)

### B-05 — Multi-source hubs met collision code
**Probleem:** Bij tientallen bronsystemen is een hub per definitie multi-source;
klantnummers botsen tussen CRM, ERP en webshop.

**Besluit:** Elke hub bevat `bk_collision_code`, dat meegaat in de hash key.

### B-06 — Quality: één pass over alle regels
**Probleem:** Eén evaluatie per regel geeft N full scans per tabel.

**Besluit:** Alle regels worden als extra boolean kolommen toegevoegd in één
pass; het meten gebeurt in één aggregatie. Er is onderscheid tussen `ROW`- en
`DATASET`-regels (`evaluation_scope`), omdat window-functies niet in een
`WHERE`-clausule kunnen.
→ [src/contoso_lakehouse/quality.py](../src/contoso_lakehouse/quality.py)

### B-07 — Reject is herverwerkbaar, niet alleen een logboek
**Besluit:** Een reject-rij bevat *alle* faalredenen (`ARRAY<STRUCT<...>>`), de
volledige originele payload als JSON, en een levenscyclus
(`OPEN → IN_REVIEW → RESOLVED | WONT_FIX | RESUBMITTED`).
→ [sql/03_quality_reject/31_reject_tables.sql](../sql/03_quality_reject/31_reject_tables.sql)

### B-08 — Gold Actueel: publish-by-pointer per publication group
**Probleem:** Per-entiteit publiceren levert bij een gedeeltelijke fout
"nieuwe dimensies met oude feiten" op — precies de inconsistentie die vermeden
moest worden.

**Besluit:** Elke Gold Actueel dataset heeft twee fysieke slots (`_v1`/`_v2`) in
`current_internal`; de publieke objecten in `current` zijn views. Vóór iedere
pointerwissel moet de volledige `publication_group_id` (`SALES_MART`) nog de
auditstatus `BUILDING` hebben. Daarna promoot één Delta `MERGE` de
groep-releasepointer naar de nieuwe batch. De publieke views leiden elk hun
fysieke slot af uit die ene pointer; bij een incomplete of gefaalde groep
blijft de pointer en daarmee de vorige consistente release ongewijzigd.
→ [src/contoso_lakehouse/gold.py](../src/contoso_lakehouse/gold.py)

**Consumer-contract:** BI-consumenten gebruiken uitsluitend de publieke views
in `current`. Fysieke slots en audit-tabellen zijn implementatiedetails en
krijgen geen leesrechten voor BI-rollen.

### B-09 — Freshness expliciet zichtbaar
**Besluit:** Elke Gold Actueel tabel draagt `_as_of_delivery_id`,
`_as_of_timestamp` en `_batch_id`. De view `v_gold_freshness` maakt zichtbaar hoe
oud de actieve versie is, zodat een bevroren mart opvalt.

### B-10 — Metadata is een privileged, read-only input in productie
**Probleem:** `rule_expression`, `source_expression` en `select_sql` worden in
gegenereerde SQL opgenomen. Wie `MODIFY` heeft op de metadata, heeft effectief
code-executie als de ETL service principal — een privilege-escalatie.

**Besluit:** De seed in `metadata/seed` is de bron van waarheid en wordt via
Git/DAB gedeployed. Identifiers worden gevalideerd met `safe_identifier()` als
tweede verdedigingslinie. In productie krijgen gebruikers geen `MODIFY`.
→ [src/contoso_lakehouse/sqlutil.py](../src/contoso_lakehouse/sqlutil.py)

### B-11 — Afhankelijkheden als graaf, met cyclusdetectie
**Besluit:** `meta_dependency` wordt topologisch gesorteerd per laag. Cycli en
wees-verwijzingen worden vóór het laden gedetecteerd, niet halverwege een run.
De Workflow-graaf bevat alleen de *lagen*; de volgorde binnen een laag komt uit
de metadata.
→ [src/contoso_lakehouse/orchestration.py](../src/contoso_lakehouse/orchestration.py)

### B-12 — Metadata-validatie als eerste pipelinetaak
**Besluit:** Elke DQ-regel, mapping-expressie en Gold-SELECT wordt met `EXPLAIN`
gecompileerd vóór uitvoering. Dit is bij honderden tabellen de belangrijkste
kwaliteitsmaatregel.
→ [src/contoso_lakehouse/validation.py](../src/contoso_lakehouse/validation.py),
[notebooks/99_validate_metadata.py](../notebooks/99_validate_metadata.py)

### B-13 — Schema evolution: retries verplicht, drift zichtbaar
**Besluit:** `addNewColumns` laat de stream bewust falen bij een nieuwe kolom;
de ingest-taak heeft daarom `max_retries: 3`. Nieuwe bronkolommen zonder
`meta_mapping`-rij worden geregistreerd in `audit_delivery_object.new_columns_detected`.

### B-14 — Liquid clustering in plaats van partitionering
**Besluit:** Vault- en Gold-tabellen gebruiken `CLUSTER BY`. Handmatig
partitiebeheer is bij honderden tabellen niet houdbaar en dagpartities leveren
na jaren duizenden kleine partities op.

### B-15 — Idempotentie op `batch_id`
**Besluit:** Elke Data Vault load verwijdert eerst de rijen van de eigen
`_batch_id` en schrijft daarna. Een herstart van een gefaalde run levert geen
dubbele satellite-rijen op.

### B-16 — Onderhoud is onderdeel van het ontwerp
**Besluit:** Een wekelijkse maintenance-job doet `OPTIMIZE` en `VACUUM` over alle
lagen en signaleert verlopen Gold-slots. Superseded slots blijven minimaal
24 uur bestaan zodat lopende BI-queries hun bron niet verliezen.

### B-17 — Landing is een external volume op ADLS Gen2
**Besluit:** Bestandsaanlevering van buiten Databricks gebeurt uitsluitend via
het external volume `raw_<env>.sales.landing`. Managed volumes (`checkpoints`,
`quarantine`) blijven alleen toegankelijk vanuit Databricks en worden niet als
integratiepunt gebruikt.

**Configuratie dev:** storage account `contosolake3`, container `landing`,
subfolder `sales`; de volume-URL is
`abfss://landing@contosolake3.dfs.core.windows.net/sales`.

**Vereiste:** vóór de setup moet een metastorebeheerder een Unity Catalog storage
credential en external location op
`abfss://landing@contosolake3.dfs.core.windows.net/` aanmaken. De identity van
de credential krijgt minimaal Azure RBAC `Storage Blob Data Contributor` op de
container. De uitvoerende gebruiker krijgt in Unity Catalog `CREATE EXTERNAL
VOLUME` op de external location.

**Ingericht (31 augustus 2026):** de managed identity van Access Connector
`ac-databricks-contoso-dev` heeft op storage account `contosolake3` de rollen
`Storage Blob Data Contributor`, `Storage Queue Data Contributor`, `Storage
Account Contributor` en `EventGrid EventSubscription Contributor`. De
credentialtest moet opnieuw worden uitgevoerd nadat Azure RBAC is gepropageerd.

**Ingericht voor file events (31 augustus 2026):** naast de drie
storage-accountrollen heeft `ac-databricks-contoso-dev` nu ook `EventGrid Data
Contributor` op resource group `Databricks`. De storage-accountfirewall moet
nog de optie `Allow trusted Microsoft services to access this resource`
toestaan. `EventGrid EventSubscription Contributor` op alleen het storage
account vervangt de resource-grouprol niet.

**Diagnose en herstel (31 augustus 2026):** de validatie van file events faalde
omdat de Azure resource provider `Microsoft.EventGrid` voor de subscription niet
was geregistreerd. De provider is geregistreerd en heeft de status `Registered`.
Het storage account is geschikt: `StorageV2`, `Standard_RAGRS`, hierarchical
namespace ingeschakeld en publieke netwerktoegang ingeschakeld. De
external-locationtest moet opnieuw worden uitgevoerd.

**Gevalideerd (31 augustus 2026):** de Databricks external location is na de
Azure-inrichting opnieuw getest; toegang tot storage en `File Events Read` zijn
geslaagd. Het landing external volume kan nu door de lakehouse-setup worden
aangemaakt.

### B-18 — Lokale deploy gebruikt gebruikers-OAuth in dev
**Besluit:** Deploy en setup in `dev` worden uitgevoerd met het lokale
Databricks-profiel `contoso-dev` en gebruikers-OAuth. De geconfigureerde
Databricks service principal is uitsluitend de `run_as`-identity voor `tst` en
`prd`; zijn OAuth-machine-to-machine-credentials worden niet opgeslagen in de
repository.

### B-19 — Herstelde inrichtingsdefecten
**Besluit:** De file-arrival-trigger gebruikt een volume-URL met afsluitende
slash, zoals door de Databricks Jobs API vereist. De setup-notebook verwijdert
SQL-commentaarregels per statement voordat DDL wordt uitgevoerd; hierdoor wordt
het eerste `CREATE CATALOG`-statement niet meer ten onrechte overgeslagen.

### B-20 — Actuele marts gebruiken een atomische groepsreleasepointer
**Besluit:** Een Gold-publicatiegroep wordt niet langer door een reeks
`CREATE OR REPLACE VIEW`-acties gepubliceerd. Nadat alle entiteiten de status
`BUILDING` hebben, zet één Delta `MERGE` in
`audit_gold_publication_group` de actieve batch voor de groep. De publieke
views leiden hun fysieke slot uit die pointer af. Daardoor zien consumenten
steeds een consistente groep van dimensies en feiten.
→ [src/contoso_lakehouse/gold.py](../src/contoso_lakehouse/gold.py),
[sql/05_gold/51_gold_current.sql](../sql/05_gold/51_gold_current.sql)

### B-21 — Audit runs refereren aan een immutable metadatarelease
**Besluit:** De volledige set seed-metadata krijgt bij deployment een
deterministische SHA-256-fingerprint. Die release wordt in
`audit_metadata_version` geregistreerd en iedere load-run bewaart de gebruikte
`metadata_version`. Dit maakt herstel en onderzoek reproduceerbaar tegen de
exacte Git/DAB-metadatarelease. Een idempotente migratie vult de nieuwe velden
ook aan in reeds bestaande Delta-tabellen.
→ [src/contoso_lakehouse/seed.py](../src/contoso_lakehouse/seed.py),
[sql/01_metadata/12_metadata_migrations.sql](../sql/01_metadata/12_metadata_migrations.sql)

### B-22 — Bronze schaalt via begrensde Serverless task fan-out
**Besluit:** Eén notebookdriver start niet langer alle Auto Loader-streams
serieel en gebruikt ook geen Python-threads voor gelijktijdige Spark-jobs. Een
planner levert één gedeelde `batch_id` en de actieve bronobjecten aan een
Databricks `for_each_task`. Elke iteratie draait als afzonderlijke Serverless
taak; `bronze_parallelism` begrenst de gelijktijdigheid per bronsysteem. De
delivery-gate start pas nadat alle iteraties gereed zijn.
→ [notebooks/06_plan_bronze_fanout.py](../notebooks/06_plan_bronze_fanout.py),
[workflows/pipeline.job.yml](../workflows/pipeline.job.yml)

**Schaalgrens:** Task values kunnen maximaal 48 KB aan `for_each`-input
doorgeven. Bij meer objecten dan daarin passen, gebruikt de planner een Delta
control-tabel als lookup in plaats van de objectlijst als task value.

### B-23 — Parallelle Serverless-runs loggen append-only
**Probleem:** gelijktijdige Bronze-taken werkten dezelfde gepartitioneerde
`audit_load_run`-tabel bij. Hierdoor ontstonden Delta optimistic-concurrency
conflicten op de gedeelde `BRONZE`-partitie, hoewel de brontabellen onafhankelijk
waren.

**Besluit:** elke statusovergang schrijft een immutable rij naar
`audit_load_run_event`. `v_load_run_status` projecteert daaruit de actuele status
per `run_id`; gates en dashboards lezen alleen die view. Parallelle Serverless
taken doen hierdoor uitsluitend append-operaties op de auditlaag.
→ [src/contoso_lakehouse/audit.py](../src/contoso_lakehouse/audit.py),
[sql/01_metadata/11_audit_model.sql](../sql/01_metadata/11_audit_model.sql)

### B-24 — Serverless-schema-evolution staat op de MERGE-operatie
**Probleem:** `spark.databricks.delta.schema.autoMerge.enabled` is op Databricks
Serverless niet beschikbaar. Daardoor faalden alle parallelle Bronze-taken,
ondanks correcte landingbestanden en metadata.

**Besluit:** Bronze gebruikt `MERGE WITH SCHEMA EVOLUTION` in plaats van de
sessieconfiguratie. Een regressietest verbiedt de Serverless-onverenigbare
configuratie. De daaropvolgende pipeline-run bevestigde een succesvolle Bronze-
fan-out en delivery gate; Quality werd vervolgens bereikt.
→ [src/contoso_lakehouse/bronze.py](../src/contoso_lakehouse/bronze.py),
[tests/test_metadata_consistency.py](../tests/test_metadata_consistency.py)

### B-25 — Kwaliteitsregels moeten volledig omgevingsresolubaar zijn
**Probleem:** referentiële DQ-regels gebruikten `{quality_schema}`, terwijl
alleen catalog-placeholders worden ondersteund. De placeholder kwam daardoor
ongewijzigd in de gegenereerde SQL terecht en Quality faalde met een parsefout.

**Besluit:** de regels verwijzen naar het contractuele schema `sales` en een
regressietest detecteert naamachtige, niet-opgeloste placeholders. Hierdoor
blijft environment-resolutie beperkt tot fysieke catalognamen.
→ [metadata/seed/meta_quality_rule.json](../metadata/seed/meta_quality_rule.json),
[tests/test_metadata_consistency.py](../tests/test_metadata_consistency.py)

### B-26 — Gold Actueel volgt het fysieke factcontract
**Probleem:** de metadataquery voor `GC_FCT_SALES` bevatte de typefout
`customer_h+s` en projecteerde slechts een deel van de kolommen die in de
fysieke Gold Actueel facttabel zijn gedefinieerd. De derde end-to-end-run faalde
daardoor uitsluitend in `gold_current`; de releasepointer bleef ongewijzigd.

**Besluit:** de metadataquery projecteert nu alle factcontractkolommen uit de
historische mart, met `customer_hk`. Een regressietest bewaakt die kolommenset.
→ [metadata/seed/meta_gold_entity.json](../metadata/seed/meta_gold_entity.json),
[tests/test_metadata_consistency.py](../tests/test_metadata_consistency.py)

### B-27 — Kwaliteitsfouten blijven traceerbaar per onveranderlijke levering
**Bevinding:** de eerder aangemaakte folder `SALES|2026-08-29` bevat een dubbele
`customer_key` en overschrijdt de drempel van regel `DQ-CUST-002`. De
Quality-gate heeft de vervolglagen daardoor correct geblokkeerd.

**Besluit:** de levering blijft ongewijzigd beschikbaar voor kwaliteitsanalyse.
De demo-generator schrijft voor de eindvalidatie naar de nieuwe datumfolder
`2026-09-01`, zodat de proef geen historische brondata muteert.

### B-28 — Chronologische delivery-gate blokkeert latere leveringen correct
**Bevinding:** de nieuwe, valide demo-levering `SALES|2026-09-01` is succesvol
naar landing geschreven. De daaropvolgende pipeline-run koos echter terecht
eerst `SALES|2026-08-29`: die is de laagste nog niet succesvol gepubliceerde
levering en faalt op `DQ-CUST-002`. De nieuwe folder wordt dus niet verwerkt
voordat de oudere levering is hersteld of expliciet als `SUPERSEDED` is gemarkeerd.

**Besluit:** voor de `dev`-eindvalidatie wordt de historische testlevering via
het reject-herverwerkingsproces hersteld, of na expliciete goedkeuring als
`SUPERSEDED` gemarkeerd. De view `v_next_processable_delivery` is toegevoegd
aan het monitoringsscript om deze blokkade operationeel zichtbaar te maken.

### B-29 — Projectverslag is de operationele beslis- en validatiehistorie
**Besluit (1 september 2026):** relevante testresultaten, runtimebevindingen,
configuratiewijzigingen, ontwerp- en beheerbesluiten, en open acties worden
bij iedere werkstap vastgelegd in dit besluitenverslag. Hierdoor blijven
technische keuzes, hun onderbouwing en de geverifieerde uitkomst traceerbaar
voor later beheer, review en overdracht.

### B-30 — Dev gebruikt gecontroleerd superseden voor de geblokkeerde demo-levering
**Overwogen opties:** (1) herstel `SALES|2026-08-29` via het rejectproces en
verwerk de levering opnieuw; (2) markeer de onvoltooide demo-levering als
`SUPERSEDED` en verwerk daarna `SALES|2026-09-01`.

**Besluit (1 september 2026):** voor de `dev`-eindvalidatie is optie 2 gekozen.
Dit verandert geen brondata en maakt de beslissing traceerbaar met tijdstip,
reden, goedkeurder en referentie in `audit_delivery`. Voor `tst` en `prd`
blijft optie 1 de standaard; superseden vereist daar formele goedkeuring.
→ [notebooks/07_supersede_delivery.py](../notebooks/07_supersede_delivery.py),
[workflows/delivery_remediation.job.yml](../workflows/delivery_remediation.job.yml)

### B-31 — Beheerjobs gebruiken hetzelfde gedeployde frameworkpad
**Bevinding:** de eerste uitvoering van de nieuwe `supersede_delivery`-job
faalde met `ModuleNotFoundError` omdat de notebook het bundle-`src`-pad niet
aan `sys.path` toevoegde. De audit-tabel is hierdoor niet gewijzigd.

**Besluit:** iedere notebook die frameworkmodules importeert ontvangt
`repo_root` uit `${workspace.file_path}` en voegt `${repo_root}/src` toe aan
`sys.path`. De remediation-notebook heeft hiervoor regressiedekking.

### B-32 — Bronze-compleet is niet gelijk aan end-to-end gepubliceerd
**Bevinding:** de tweede remediation-poging blokkeerde `SALES|2026-08-29`
omdat de status `COMPLETE` was. Die status beschrijft correct dat alle verplichte
bronobjecten in Bronze zijn geladen, maar zegt niets over Quality, Vault of Gold.

**Besluit:** een levering mag alleen niet als `SUPERSEDED` worden gemarkeerd
als er een actieve Gold-publicatiegroep voor bestaat. Een Bronze-complete maar
Quality-geblokkeerde levering kan na expliciete goedkeuring wel worden
gesuperseded. De auditvelden bewaren de volledige beheersreden.

### B-33 — Alleen een atomische Gold-release blokkeert superseden
**Bevinding:** de derde remediation-poging behandelde een losse succesvolle
`GOLD_CURR`-entiteitsbuild als een gepubliceerde levering. Bij een gefaalde
publicatiegroep kan zo'n build bestaan zonder dat consumenten de nieuwe data
zien, omdat de groepspointer niet naar de batch is gewisseld.

**Besluit:** de supersede-guard controleert uitsluitend een `ACTIVE`-record in
`audit_gold_publication_group` voor de levering. Hierdoor zijn gepubliceerde
releases beschermd, terwijl gedeeltelijke builds na goedkeuring herstelbaar of
over te slaan blijven.

### B-34 — Geblokkeerde dev-levering gecontroleerd gesuperseded
**Uitvoering (1 september 2026):** de remediation-job heeft
`SALES|2026-08-29` succesvol als `SUPERSEDED` gemarkeerd met reden "Dubbele
customer_key in historische demo-levering blokkeert dev-eindvalidatie",
goedkeurder `stoelingabob@gmail.com` en referentie `B-30`.

**Status bij onderbreking:** de nieuwe, valide levering `SALES|2026-09-01`
staat in landing klaar. Bij hervatting start de pipeline opnieuw; de
chronologische gate kan dan deze levering selecteren voor de resterende
end-to-end-validatie tot en met Gold Actueel.

### B-35 — Stresstest dekt alle huidige Sales-tabellen van volume tot Gold
**Besluit (2 september 2026):** voer een positieve end-to-end stresstest uit
als reeks van tien opeenvolgende dagleveringen. Per levering bevat de landing
100.000 `customers`, 50.000 `products`, 10.000 `employees`, 1.000.000
`orders`-regels en 20.000 `returns`, verdeeld over minimaal tien bestanden per
object en honderd bestanden voor Orders. De reeks bevat daarmee 11,8 miljoen
bronrijen en belast zowel bestandsfan-out als alle volledige Gold-rebuilds.

| Brontabel | Wijziging per volgende levering | Verplichte downstream-dekking |
|---|---:|---|
| `customers` | 2% gewijzigd of verwijderd | `HUB_CUSTOMER`, `SAT_CUSTOMER`, `SAT_CUSTOMER_BV`, `GH_DIM_CUSTOMER`, `GC_DIM_CUSTOMER` |
| `products` | 1% gewijzigd of verwijderd | `HUB_PRODUCT`, `SAT_PRODUCT`, `GH_DIM_PRODUCT`, `GC_DIM_PRODUCT` |
| `employees` | 0,5% gewijzigd of verwijderd | `HUB_EMPLOYEE`, `SAT_EMPLOYEE`, `GH_DIM_EMPLOYEE`, `GC_DIM_EMPLOYEE` |
| `orders` | nieuwe regels en statuswijzigingen | `HUB_ORDER`, `LNK_ORDER_CUSTOMER`, `LNK_ORDER_PRODUCT`, `LNK_ORDER_EMPLOYEE`, `SAT_ORDER`, `SAT_ORDER_LINE`, `SAT_ORDER_LINE_BV`, `GH_FCT_SALES`, `GC_FCT_SALES` |
| `returns` | 2% van de orderregels | `HUB_RETURN`, `LNK_RETURN_ORDER_PRODUCT`, `LNK_RETURN_EMPLOYEE`, `SAT_RETURN`, `GH_FCT_RETURNS`, `GC_FCT_RETURNS` |

Gebruik uitsluitend geldige sleutels en referenties in deze positieve stroom.
De test slaagt wanneer iedere levering binnen de afgesproken SLA de volledige
`SALES_MART`-publicatiegroep activeert, Gold-rijtotalen en bedragen herleidbaar
zijn tot Quality, geen dubbele business keys of satelliteversies ontstaan en
alle Current-Gold views dezelfde actieve `batch_id` en `delivery_id` tonen.

Naast de positieve volumestroom zijn de volgende gecontroleerde proeven
verplicht. Elke proef gebruikt een eigen, chronologisch latere deliverydatum,
zodat eerdere resultaten en Auto Loader-checkpoints niet worden gemuteerd.

| Proef | Injectie | Verwachte controle |
|---|---|---|
| Delivery-gate | Eén verplicht bestand ontbreekt of arriveert later | Geen Quality/Vault/Gold-run; na aanvulling opent uitsluitend de volledige levering. |
| Chronologie | Datum $N+1$ is volledig, datum $N$ onvolledig | De gate verwerkt $N+1$ niet voordat $N$ is gepubliceerd of gecontroleerd gesuperseded. |
| Quality en Reject | Geldige levering met gerichte null, duplicate en referentiële fout | Correcte regelresultaten, payload en reden in Reject; `FAIL_BATCH` blokkeert vervolg en `QUARANTINE_BATCH` bewaart de reden. |
| Schema drift | Nieuwe niet-gemapte bronkolom | Bronze registreert de nieuwe kolom en faalt volgens het driftbeleid, zonder gedeeltelijke vervolgverwerking. |
| Raw Vault | Nieuwe en bestaande business keys, gewijzigde hashdiffs en relaties | Hubs en links zijn uniek op hun hash key; satellites zijn insert-only en voegen alleen gewijzigde hashdiffs toe. |
| SCD2 en deletes | Gewijzigde en verdwenen Customers, Products en Employees | Historische Gold-rijen krijgen correcte `valid_from`, `valid_to` en `is_current`; Current Gold toont alleen de actuele, niet-verwijderde versie. |
| Business Vault | Gewijzigde adressen, bedragen, korting, status en leverdatum | Afgeleide customer- en order-line-satellites wijzigen alleen wanneer hun eigen hashdiff verandert; berekende Gold-waarden sluiten aan op Quality. |
| Herstart/idempotentie | Onderbreek na Bronze, Quality en per Vault-zone; herstart met dezelfde batch | Geen dubbele Bronze-, Quality- of Vault-rijen; audit eindigt in `SUCCESS` voor de herstelde run. |
| Gold-publicatie | Forceer fout tijdens een Current-Gold-build | Geen groepspointerwissel; alle publieke Current-views houden de vorige gezamenlijke `batch_id` en `delivery_id`. |
| Fan-out en audit | Honderd Order-bestanden en gelijktijdige Bronze-objecten | Geen audit-concurrencyconflicten; alle verplichte objecten zijn exact eenmaal `SUCCESS` voordat de gate opent. |

**Opschalen naar productie:** vervang het startprofiel bij beschikbare
piekmetingen door minimaal drie maal de grootste verwachte daglevering, met
dezelfde bestandsverdeling en wijzigingspercentages. Test incomplete leveringen,
DQ-drempeloverschrijding, schema drift, herstart na een fout en een mislukte
Gold-build apart; die moeten respectievelijk de gate sluiten, quarantainen,
veilig falen, idempotent herstellen en de voorgaande Gold-release zichtbaar
houden.

**Kostenraming:** de pipeline gebruikt Serverless Jobs; er zijn geen permanente
job-clusters. De kosten zijn daarom hoofdzakelijk
`verbruikte Serverless-DBU-uren x de contractprijs per DBU-uur`, aangevuld met
ADLS-opslag, read/write/list-transacties en eventueel netwerk-egress. Meet eerst
één representatieve levering met 1.000.000 orderregels en alle vijf objecten.
Vermenigvuldig de gemeten DBU-kosten met tien voor de positieve reeks en tel
20% reservering voor negatieve herstelproeven en run-to-runvariatie op. Gebruik
de werkelijk gefactureerde usage-records uit Databricks system billing als
bron; de DBU-prijs verschilt per Azure-regio en enterprisecontract. Begrens
kosten vooraf met een testbudget, `max_concurrent_runs: 1` en de bestaande
`bronze_parallelism: 4`.

**Resultatenlog:** na iedere uitvoering worden hieronder uitsluitend gemeten
waarden genoteerd: datumfolder, job-run-id, duur per laag, verbruikte
Serverless-DBU-uren, ingelezen/afgekeurde/gepubliceerde rijtotalen, actieve
Gold `batch_id` en `delivery_id`, en de uitkomst van elke negatieve proef.

| Uitvoering | Status | Gemeten resultaat |
|---|---|---|
| 2 september 2026, generatorrun `74226457869886` | Veilig gestopt | Geen data geschreven. Na 49 seconden meldde de generator dat `/Volumes/raw_dev/sales/landing/2026-09-02` al bestond; overschrijven is correct geweigerd. |
| 2 september 2026, generatorrun `315473211182482` | Veilig gestopt | Geen data geschreven. Na 24 seconden meldde de generator dat `/Volumes/raw_dev/sales/landing/2026-09-03` al bestond; overschrijven is correct geweigerd. |
| 2 september 2026, generatorrun `826080059378600` | Technisch gefaald | Geen bronbestand geschreven. Serverless Spark Connect ondersteunt write-modus `errorifexists` niet; de generator is hersteld naar `overwrite` voor uitsluitend de tijdelijke stagingfolder. |
| 2 september 2026, generatorrun `816898969720412` | Technisch gefaald | Geen bronbestand geschreven. Serverless vereiste een `INT` als `element_at`-index, terwijl de index uit `range.id` een `BIGINT` was; de generator cast de vier indexen nu expliciet naar `INT`. |
| 2 september 2026, generatorrun `291249352899112` | Succes | Levering `2026-09-04` is in 104 seconden geschreven met 100.000 Customers, 50.000 Products, 10.000 Employees, 1.000.000 Orders en 20.000 Returns. De 140 verwachte Parquet-bestanden zijn aangemaakt; DBU-usage is nog op te halen uit system billing. |
| 2 september 2026, pipelinerun `112566148879705` | Bewust geblokkeerd | Bronze (vijf objecten) en de delivery-gate slaagden. Quality blokkeerde Orders op `order_date_not_future`; de eerste poging faalde na 69 seconden en de identieke automatische retry is gestopt om onnodige DBU-kosten te voorkomen. Geen Vault- of Gold-taak is gestart. De generator gebruikt voortaan een afzonderlijke, gevalideerde `business_date`. |
| 2 september 2026, remediationrun `555230881632015` | Succes | `SALES|2026-09-04` is gecontroleerd als `SUPERSEDED` geregistreerd. Reden: de representatieve stresstest was bewust geblokkeerd door `order_date_not_future`; de positieve opvolglevering gebruikt een gescheiden `business_date`. Goedgekeurd door `stoelingabob@gmail.com`, referentie `B-35`. |
| 2 september 2026, generatorrun `842841385799462` | Succes | Positieve opvolglevering `2026-09-05` is in 85 seconden geschreven met de standaardvolumes en `business_date=2024-09-02`. Daarna volgt verwerking door `contoso_lakehouse_pipeline`. |
| 2 september 2026, pipelinerun `839588986339164` | Gefaald op Quality | De gate selecteerde aantoonbaar `SALES|2026-09-05`; metadata-validatie, Bronze-fan-out met vijf objecten en de gate slaagden. Quality faalde na twee pogingen op `order_date_not_future`; Raw Vault, Business Vault, Gold Historisch en Gold Actueel zijn correct niet gestart. Read-only query `01f1a68b-626e-154e-b760-4df1b3ce9742` toonde 1.000.000 Bronze-Orders met `min_order_date=NULL`, `max_order_date=NULL` en nul toekomstige datums. Oorzaak: datumgeneratie met een kolomargument gaf onder Spark Connect NULL terug; hersteld met expliciete Spark SQL-expressies. |
| 2 september 2026, remediationrun `832642036027967` | Succes | `SALES|2026-09-05` is gecontroleerd als `SUPERSEDED` geregistreerd wegens de bewezen NULL `order_date`-waarden. Goedgekeurd door `stoelingabob@gmail.com`, referentie `B-35`; de gecorrigeerde positieve levering krijgt een nieuwe datumfolder. |
| 2 september 2026, generatorrun `193601309737521` | Succes | Gecorrigeerde positieve levering `2026-09-06` is in 83 seconden geschreven met de standaardvolumes en `business_date=2024-09-02`. De nieuwe Spark SQL-datumexpressies worden in de volgende pipeline-run gevalideerd. |
| 2 september 2026, generatorrun `457189887708000` | Succes | Delivery `SALES|2026-09-09` is gegenereerd met standaardvolumes en ISO-datumstrings, passend bij het bestaande Auto Loader-schema. |
| 2 september 2026, pipelinerun `138888199822102` | Succes | Delivery `SALES|2026-09-09` volledig verwerkt: metadata, Bronze, gate, Quality, Raw Vault, Business Vault, Gold Historisch en Gold Current. De volledige `SALES_MART`-publication group is atomisch `ACTIVE`. |
| 2 september 2026, pipelinerun `321288909336916` | Succes | Delivery `SALES|2026-09-10` volledig verwerkt met dezelfde end-to-end dekking. |
| 2 september 2026, generatorrun `355108901666945` | Succes | Fase 2-testdata `SALES|2026-09-11` gegenereerd met `change_set=1`; generator ondersteunt deterministische wijzigingen, deletes, nieuwe orderkeys en statuswijzigingen. |
| 2 september 2026, pipelineruns `991506170448924` en `331340688817919` | Niet geldig als Fase 2-acceptatie | De runs eindigden technisch succesvol, maar de chronologische gate verwerkte opnieuw `SALES|2026-09-09` in plaats van de gewijzigde delivery. De runs bewijzen daarom geen SCD2/delete-verwerking voor `change_set=1`. |
| 2 september 2026, technische fixes | Opgelost | Gold-schema-migraties toegevoegd voor bestaande facttabellen; de delivery-gate vereist nu een actieve volledige Gold-publication group en sluit legacy deliveries met een verouderd objectcontract uit. Lokale regressiesuite: 69 tests geslaagd. |

---

## 4. Opgeleverde artefacten

| Onderdeel | Locatie |
|---|---|
| Unity Catalog structuur + grants | [sql/00_unity_catalog](../sql/00_unity_catalog) |
| Metadata model (8 configuratietabellen) | [sql/01_metadata/10_metadata_model.sql](../sql/01_metadata/10_metadata_model.sql) |
| Audit model + gate-views | [sql/01_metadata/11_audit_model.sql](../sql/01_metadata/11_audit_model.sql) |
| Bronze Delta tabellen | [sql/02_bronze/20_bronze_tables.sql](../sql/02_bronze/20_bronze_tables.sql) |
| Quality + Reject tabellen | [sql/03_quality_reject](../sql/03_quality_reject) |
| Raw Vault + Business Vault | [sql/04_data_vault](../sql/04_data_vault) |
| Gold Historisch + Gold Actueel | [sql/05_gold](../sql/05_gold) |
| Seed-metadata (Contoso Sales) | [metadata/seed](../metadata/seed) |
| Python framework | [src/contoso_lakehouse](../src/contoso_lakehouse) |
| Databricks notebooks | [notebooks](../notebooks) |
| Workflows (Asset Bundle) | [databricks.yml](../databricks.yml), [workflows](../workflows) |
| Metadata-consistentietests | [tests/test_metadata_consistency.py](../tests/test_metadata_consistency.py) |
| Operationele monitoringsqueries | [sql/01_metadata/13_monitoring_queries.sql](../sql/01_metadata/13_monitoring_queries.sql) |

## 5. Review-bevindingen en status

| # | Bevinding | Prio | Status |
|---|---|---|---|
| 1 | Auto Loader onverenigbaar met leverings-gate | P0 | Opgelost (B-01, B-02) |
| 2 | Multi-source hubs ontbraken | P0 | Opgelost (B-05) |
| 3 | Hash-conventie niet vastgelegd | P0 | Opgelost (B-04) |
| 4 | SQL-injectie / privilege-escalatie via metadata | P0 | Opgelost (B-10) |
| 5 | Fysieke end-dating op satellites | P1 | Opgelost (B-03) |
| 6 | Cross-entity inconsistentie in Gold Actueel | P1 | Opgelost (B-08) |
| 7 | Quality: N full scans, geen reject-DDL | P1 | Opgelost (B-06, B-07) |
| 8 | Geen cyclusdetectie op afhankelijkheden | P1 | Opgelost (B-11) |
| 9 | Geen metadata-validatie | P1 | Opgelost (B-12) |
| 10 | Delete-detectie ontbrak in de vault | P2 | Opgelost — status- en effectivity satellites toegevoegd |
| 11 | Geen onderhouds- en retentiebeleid | P2 | Opgelost (B-16) |
| 12 | Geen freshness-monitoring | P2 | Opgelost (B-09) |
| 13 | Reproduceerbare metadata-versie per load-run | P1 | Opgelost (B-21) |
| 14 | `INCREMENTAL_CDC` en `PARTIAL_SNAPSHOT` laadstrategieën | P2 | **Open** |
| 15 | Begrensde Serverless-paralleliteit voor Bronze | P2 | Opgelost (B-22) |
| 16 | Ownership, PII-classificatie, SLA in de metadata | P2 | **Open** |
| 17 | Unity Catalog external location voor ADLS-landing ontbreekt | P0 | Opgelost (B-17) |
| 18 | File-arrival-trigger vereiste afsluitende slash | P1 | Opgelost (B-19) |
| 19 | DDL-runner sloeg statements na SQL-commentaar over | P1 | Opgelost (B-19) |
| 20 | Bestaande Gold-tabellen liepen achter op de actuele DDL | P1 | Opgelost — idempotente schema-migraties toegevoegd |
| 21 | Delivery-gate sloot deliveries met gedeeltelijke Gold-success ten onrechte af | P1 | Opgelost — gate gebruikt volledige actieve publication group |
| 22 | Historische deliveries met oud metadata-contract blokkeerden de huidige gate | P1 | Opgelost — actuele mandatory-object count is onderdeel van gate-selectie |
| 23 | Stressgenerator ondersteunde geen reproduceerbare wijzigingen en deletes | P2 | Opgelost — `change_set` toegevoegd; runtime-validatie van gewijzigde delivery staat open |

## 6. Openstaande punten

1. **Metadata-SCD2 (P2).** Git/DAB-releases en `metadata_version` maken runs
   reproduceerbaar. Bitemporale, runtime-wijzigbare metadata is alleen nodig
   als productieconfiguratie buiten Git mag worden aangepast.
2. **CDC-laadstrategie (P2).** Vereist voor ERP/CRM-bronnen met een change feed.
3. **Fan-out lookup voor zeer grote inventories (P2).** Boven de 48 KB
   task-valuegrens moet de Serverless `for_each` zijn inputs uit een Delta
   control-tabel ophalen in plaats van uit een task value.
4. **Governance-metadata (P2).** `data_owner`, `pii_classification`,
   `retention_days`, `sla_minutes`, `cost_center`.
5. **Beleidskeuze reject-herverwerking.** De structuur is er; het proces (wie
   beoordeelt, binnen welke termijn, en hoe wordt teruggevoerd) is nog niet belegd.
6. **Effectivity satellite wordt nog niet geladen.** De tabel bestaat, de
   loadlogica voor het `DRIVING_KEY`-patroon moet nog worden toegevoegd.

## 7. Vervolgstappen

1. Fase 2 afronden: laat de gate eerst `SALES|2026-09-10` verwerken en daarna
   `SALES|2026-09-11` met `change_set=1`; accepteer pas na controle van SCD2,
   deletes, hashdiffs en Current Gold.
2. De positieve reeks uitbreiden tot tien opeenvolgende deliveries volgens B-35,
   met echte wijzigingen en deletes vanaf de tweede delivery.
3. Daarna de negatieve proeven uitvoeren: incomplete delivery, chronologie,
   Quality/Reject, schema drift, herstart/idempotentie, Gold-publicatiefout en
   fan-out/audit-concurrency.
4. De resterende P2-punten prioriteren: CDC, governance-metadata, de grote
   fan-out inventory, reject-herverwerking en effectivity satellites.
