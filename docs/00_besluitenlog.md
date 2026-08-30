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
`current_internal`; de publieke objecten in `current` zijn views. Alle entiteiten
met dezelfde `publication_group_id` (`SALES_MART`) switchen in één stap. Faalt er
één, dan wordt niets gepubliceerd en blijft de vorige versie actief.
→ [src/contoso_lakehouse/gold.py](../src/contoso_lakehouse/gold.py)

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
| 13 | SCD2 op het metadatamodel zelf | P1 | **Open** |
| 14 | `INCREMENTAL_CDC` en `PARTIAL_SNAPSHOT` laadstrategieën | P2 | **Open** |
| 15 | Stream-groepering per bronsysteem (schaal > 1000 tabellen) | P2 | **Open** |
| 16 | Ownership, PII-classificatie, SLA in de metadata | P2 | **Open** |

## 6. Openstaande punten

1. **Metadata-historie (P1).** Zonder `valid_from`/`valid_to` op de
   configuratietabellen is een load van maanden geleden niet reproduceerbaar.
   Voorstel: SCD2 op alle `meta_*` tabellen + `metadata_version` in `audit_load_run`.
2. **CDC-laadstrategie (P2).** Vereist voor ERP/CRM-bronnen met een change feed.
3. **Stream-groepering (P2).** Eén stream per tabel loopt vast rond duizenden
   tabellen; nodig zijn `ingest_group_id` en `stream_isolation_level`.
4. **Governance-metadata (P2).** `data_owner`, `pii_classification`,
   `retention_days`, `sla_minutes`, `cost_center`.
5. **Beleidskeuze reject-herverwerking.** De structuur is er; het proces (wie
   beoordeelt, binnen welke termijn, en hoe wordt teruggevoerd) is nog niet belegd.
6. **Effectivity satellite wordt nog niet geladen.** De tabel bestaat, de
   loadlogica voor het `DRIVING_KEY`-patroon moet nog worden toegevoegd.

## 7. Vervolgstappen

1. Bevestigen van de openstaande punten en prioritering.
2. Workspace-hosts en storage account invullen in [databricks.yml](../databricks.yml).
3. `databricks bundle deploy -t dev` en `bundle run setup_lakehouse -t dev`.
4. Testlevering in `/Volumes/raw_dev/sales/landing/<yyyy-MM-dd>/` plaatsen en de
   pipeline end-to-end valideren.
