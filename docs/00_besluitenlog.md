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

### B-35 — Review 1 september 2026: Quarantine-pad bevat een P0-defect
**Bevinding:** `AuditLogger.quarantine_delivery()` is in
`refresh_delivery_status()` genest door zijn inspringing. Het is daardoor geen
methode van `AuditLogger`. Een regel met `on_threshold_breach =
QUARANTINE_BATCH` roept een `AttributeError` op nadat Quality al data heeft
weggeschreven, in plaats van de levering gecontroleerd als `QUARANTINED` te
registreren.

**Aanbeveling (P0):** maak `quarantine_delivery()` onmiddellijk een publieke
methode van `AuditLogger`, voeg een unit-test voor de gegenereerde `UPDATE` toe
en voer daarna een end-to-end-proef met een echte `QUARANTINE_BATCH`-regel uit.
Voor productie mag de remediation-job pas worden vrijgegeven nadat die proef de
status, reden en heropening volledig traceerbaar toont.
→ [src/contoso_lakehouse/audit.py](../src/contoso_lakehouse/audit.py),
[src/contoso_lakehouse/quality.py](../src/contoso_lakehouse/quality.py)

### B-36 — Review 1 september 2026: Quality is niet retry-idempotent
**Bevinding:** iedere Quality-run schrijft zowel passed- als reject-records met
`mode("append")`. Bij een taakretry of een opnieuw gestarte batch worden de
records van dezelfde `delivery_id` opnieuw weggeschreven. De Vault leest daarna
de hele levering; deduplicatie is daar slechts deels en entiteitspecifiek
aanwezig. Rejects krijgen bovendien een nieuwe UUID en zijn daardoor altijd
dubbel.

**Aanbeveling (P1):** introduceer een stabiele `quality_run_id` per
`delivery_id + metadata_version` of een deterministische recordfingerprint.
Schrijf Quality en Reject atomisch/idempotent via `MERGE`, of verwijder eerst
uitsluitend de eigen `delivery_id` en `metadata_version` binnen een transactie.
Leg de gekozen semantiek vast voor reruns met gewijzigde metadata, zodat een
historische kwaliteitsuitkomst reproduceerbaar blijft.
→ [src/contoso_lakehouse/quality.py](../src/contoso_lakehouse/quality.py)

### B-37 — Review 1 september 2026: Metadata en DDL zijn nog niet volledig inventory-schaalbaar
**Bevinding:** bron- en transformatielogica zijn metadata-gedreven, maar de
fysieke Quality/Reject-, Vault- en Gold-DDL bevat nog expliciete Contoso
entiteiten. Ook `v_open_rejects` bevat een vaste `UNION ALL` over drie
rejecttabellen. Een nieuw bronobject vereist dus alsnog DDL- en
monitoringwijzigingen; dit strijdt met de beheerambitie voor honderden tabellen.

**Aanbeveling (P1):** voeg een provisioningstap toe die vanuit metadata de
fysieke laagcontracten, publieke views en monitoringinventaris genereert.
Gebruik één generieke rejecttabel met `source_object_id`, of genereer de view
per inventory-versie. Beheer schemawijzigingen als compatibele contractmigraties
en valideer de gegenereerde DDL in CI vóór deployment.
→ [sql/03_quality_reject/31_reject_tables.sql](../sql/03_quality_reject/31_reject_tables.sql),
[sql/04_data_vault/40_raw_vault.sql](../sql/04_data_vault/40_raw_vault.sql),
[sql/05_gold/51_gold_current.sql](../sql/05_gold/51_gold_current.sql)

### B-38 — P0- en Quality-retryherstel gevalideerd
**Uitvoering (1 september 2026):** `quarantine_delivery()` is hersteld als
publieke methode van `AuditLogger`; de Quality-engine kan een overschreden
`QUARANTINE_BATCH`-regel daardoor traceerbaar op de levering registreren. Vóór
een nieuwe succesvolle Quality-uitvoer verwijdert de engine uitsluitend de
bestaande Quality- en Reject-records voor dezelfde `delivery_id` en hetzelfde
bronobject. Een retry dupliceert deze outputs daarom niet meer.

**Validatie:** de gerichte regressiesuite voor quarantine en Quality-retry
slaagde met 5 tests. Een volledige pipeline-proef met `QUARANTINE_BATCH` blijft
nodig om de statusovergang in Databricks zelf te bevestigen.
→ [src/contoso_lakehouse/audit.py](../src/contoso_lakehouse/audit.py),
[src/contoso_lakehouse/quality.py](../src/contoso_lakehouse/quality.py),
[tests/test_metadata_consistency.py](../tests/test_metadata_consistency.py)

### B-39 — Omgevingen delen één workspace, maar niet hun data- of control-plane
**Besluit (1 september 2026):** `dev`, `tst` en `prd` blijven bewust in dezelfde
Databricks-workspace. Unity Catalog dwingt de scheiding af: de catalog is de
omgevingsgrens, het schema is de functionele-laaggrens en een Gold-tabel of
-view is de consumptiegrens. Eigen external volumes en bundle-rootpaden maken
deze rechtenstructuur operationeel. Dit houdt beheer, observability en
kostencontrole centraal.

**Landingisolatie:** `landing_path` is een expliciete bundlevariabele. De
bestaande developmentlocatie blijft `sales`; `tst` en `prd` gebruiken
respectievelijk `sales/tst` en `sales/prd`. De setup geeft de waarde door aan de
external-volume-DDL. Per productieomgeving blijft een eigen storage credential
en external location verplicht, ook wanneer dezelfde storage account wordt
gebruikt.

**Promotievoorwaarde:** `dev` gebruikt gebruikers-OAuth; `tst` en `prd` draaien
alleen onder de service principal en ontvangen deploys vanuit CI/CD. Wijzigingen
promoveren uitsluitend na een schone end-to-end-proef in `tst`, goedgekeurde
metadatarelease en geslaagde bundlevalidatie. De standaard-CLI-token is lokaal
ongeldig; `databricks bundle validate -t dev --profile contoso-dev` is wel
succesvol gevalideerd.
→ [databricks.yml](../databricks.yml),
[notebooks/00_setup_lakehouse.py](../notebooks/00_setup_lakehouse.py),
[sql/00_unity_catalog/00_catalogs_schemas_volumes.sql](../sql/00_unity_catalog/00_catalogs_schemas_volumes.sql)

### B-40 — Shared workspace hanteert Unity Catalog als autorisatiegrens
**Besluit (1 september 2026):** een gedeelde runtime- of deploymentidentity is
toegestaan wanneer die identity bewust voor meerdere omgevingen wordt beheerd.
De omgevingsscheiding voor gebruikers en consumers wordt niet door de workspace
of identitynaam afgedwongen, maar door Unity Catalog grants op catalog en
schema. Voor Gold geldt aanvullend `SELECT` op expliciet goedgekeurde tabellen
en views.

**Beheermaatregel:** leg bij iedere promotie `SHOW GRANTS`-resultaten vast voor
de betrokken catalog, schema's, volumes en Gold-objecten. Een aparte runtime
service principal per omgeving blijft beschikbare defense-in-depth, maar is
geen releaseblokkade binnen dit gekozen model.

### B-41 — Gold-consumptie is op objectniveau beperkt
**Besluit (1 september 2026):** omgevingsscheiding is een Unity Catalog-model:
catalogrechten vormen de omgevingsgrens, schemarechten de functionele grens en
Gold-objectrechten de consumptiegrens. De BI-groep krijgt alleen `USE CATALOG`
en `USE SCHEMA` op Gold. `SELECT` is expliciet toegekend op de drie historische
tabellen en vier publieke actuele Gold-views. De fysieke slots in
`current_internal` krijgen geen BI-rechten.

**Uitvoering:** het nieuwe script met Gold-objectgrants draait na de Gold-DDL.
De setup blijft daardoor idempotent en een nieuw object is pas leesbaar na een
expliciete, gereviewde grantwijziging. Dit is tevens een releasecontrole totdat
metadata-gedreven provisioning is gerealiseerd.
→ [sql/00_unity_catalog/01_grants.sql](../sql/00_unity_catalog/01_grants.sql),
[sql/00_unity_catalog/02_gold_consumer_grants.sql](../sql/00_unity_catalog/02_gold_consumer_grants.sql),
[notebooks/00_setup_lakehouse.py](../notebooks/00_setup_lakehouse.py)

### B-42 — Historische Gold-laadstrategie is insert-only
**Bevinding (1 september 2026):** twee development-pipeline-runs bereikten
succesvol Bronze, Quality, Raw Vault en Business Vault. De daaropvolgende Gold
Historisch-taak bleef langdurig actief zonder foutmelding; beide runs zijn
gecontroleerd geannuleerd voordat Gold Actueel kon starten. De loader las bij
elke run de volledige Vault-historie en voerde `WHEN MATCHED THEN UPDATE SET *`
uit, ook voor onveranderlijke SCD2-versies.

**Besluit:** Gold Historisch volgt dezelfde immutable-SCD2-semantiek als de
Vault: een rij met dezelfde business key en `valid_from` wordt nooit door een
reguliere batch gewijzigd. De `MERGE` voegt daarom alleen niet-bestaande versies
in. Correcties op een al gepubliceerde historische versie vragen een expliciete,
auditeerbare rebuild of contractmigratie. Dit voorkomt Delta-file rewrites bij
elke rerun en beperkt write amplification aanzienlijk.

**Validatie:** de nieuwe unit-test bevestigt dat de gegenereerde SQL uitsluitend
`WHEN NOT MATCHED THEN INSERT` bevat. De geoptimaliseerde bundle moet nog in
`dev` end-to-end tot en met Gold Actueel worden gevalideerd.
→ [src/contoso_lakehouse/gold.py](../src/contoso_lakehouse/gold.py),
[tests/test_metadata_consistency.py](../tests/test_metadata_consistency.py)

### B-43 — Retouren, medewerkers en kalender verrijken de Sales-mart
**Besluit (1 september 2026):** `returns` is volledig als end-to-end feit
gemodelleerd. De bron behoudt de relatie met de oorspronkelijke orderregel en
product; Gold publiceert `fct_returns` met retourdatum, retourreden en
terugbetalingsbedrag. De medewerker die een verkoop of retour behandelt komt uit
de SCD2-bron `employees` en wordt via expliciete Vault-links gekoppeld aan beide
feiten. `dim_date` is een vaste, gegenereerde kalender voor 2020-2030; verkoop
bevat order-, verzend- en leverdatumkeys en retouren bevatten een retourdatumkey.

**Uitvoeringswaarborg:** Quality verwerkt klanten, producten en medewerkers vóór
orders en retouren, zodat alle referentiële regels tegen dezelfde levering
valideren. Alle nieuwe actuele Gold-objecten nemen deel aan dezelfde atomische
`SALES_MART`-publicatiegroep.

**Validatie:** de lokale metadata-consistentiesuite is geslaagd met 64 tests.
De volgende operationele stap is `bundle run setup_lakehouse -t dev` gevolgd
door een nieuwe volledige pipeline-run; de Gold-selects moeten daar met
Databricks `EXPLAIN` en de fysieke Delta-contracten worden gevalideerd.
→ [metadata/seed](../metadata/seed),
[sql/04_data_vault/40_raw_vault.sql](../sql/04_data_vault/40_raw_vault.sql),
[sql/05_gold](../sql/05_gold),
[notebooks/01_generate_demo_delivery.py](../notebooks/01_generate_demo_delivery.py)

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
| 20 | `QUARANTINE_BATCH` kan de delivery-status niet zetten | P0 | Opgelost (B-38) |
| 21 | Quality- en rejectschrijfsels zijn niet idempotent bij retry | P1 | Opgelost (B-38) |
| 22 | Fysieke contracten en reject-monitoring zijn deels hardcoded | P1 | **Open** (B-37) |
| 23 | Omgevingen deelden dezelfde ADLS-landinglocatie | P0 | Opgelost (B-39) |
| 24 | Autorisatiegrens voor gedeelde workspace expliciet vastgelegd | P1 | Opgelost (B-40) |
| 25 | Gold-consumenten konden schema-breed lezen | P1 | Opgelost (B-41) |
| 26 | Gold Historisch herschreef onveranderlijke SCD2-versies bij iedere run | P1 | Opgelost in code; dev E2E open (B-42) |

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
7. **`QUARANTINE_BATCH` end-to-end valideren (P1).** Herstelde unit-tests
   bewaken de code; een Databricks-proef moet de delivery-status, reden en
   gecontroleerde vrijgave nog aantonen.
8. **Metadata-gedreven provisioning (P1).** DDL, publieke views en monitoring
   genereren vanuit een gevalideerde inventory voordat tientallen bronsystemen
   worden aangesloten.
9. **Single-workspace grants valideren (P1).** Leg per doelgroep met
   `SHOW GRANTS` vast dat catalog-, schema-, volume- en Gold-objectrechten
   overeenkomen met het autorisatiemodel.
10. **Gold Historisch end-to-end valideren (P1).** Bevestig in `dev` dat de
   insert-only strategie de pipeline doorlaat tot en met Gold Actueel en de
   groepspointer publiceert.

## 7. Vervolgstappen

1. `databricks bundle run setup_lakehouse -t dev --profile contoso-dev` opnieuw
   uitvoeren; de bundle is al succesvol gedeployed en de landing-voorwaarde is
   gevalideerd.
2. Testlevering in `/Volumes/raw_dev/sales/landing/<yyyy-MM-dd>/` plaatsen en de
   pipeline end-to-end valideren.
3. De resterende P2-punten prioriteren: CDC, governance-metadata, de grote
   fan-out inventory, reject-herverwerking en effectivity satellites.
4. Een end-to-end-validatie van `QUARANTINE_BATCH` uitvoeren, inclusief
   gecontroleerde vrijgave vanuit quarantaine.
5. Metadata-gedreven provisioning als releasevoorwaarde voor de
   enterprise-uitrol ontwerpen en realiseren.
6. De `tst`-promotie met de service principal uitvoeren en de Unity Catalog
   grants voor catalog, schema, volume en Gold-objecten als release-evidence
   vastleggen.
7. De geoptimaliseerde Gold Historisch-laag naar `dev` deployen en de volledige
   pipeline-run inclusief Gold Actueel verifiëren.
