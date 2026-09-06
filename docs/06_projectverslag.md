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
- Bronobjecten kiezen expliciet tussen `RAW_VAULT` en `REFERENCE_DATA`; de laatste
	route schrijft na Quality een versioned referentietabel in Business Vault.

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
13. De publieke connectoren CBS StatLine, ECB en Nager.Date zijn gehard met
	metadata-gestuurde request- en totale object-timeouts, exponential backoff,
	task-timeouts, staging-cleanup en `LANDING`-audit-events. Een mislukte
	extractie publiceert geen delivery naar het landingvolume en kan Auto Loader
	daarom niet activeren.
14. ECB-wisselkoersen en Nager-vakantiedagen zijn als zelfstandige Gold
    referentieproducten toegevoegd. Beide behouden historie vanuit Business
    Vault en publiceren een actuele dimensie atomisch per brongebonden
    publication group; CBS blijft een zelfstandig Raw-Vault-feitenproduct.
15. Op 4 september is de CBS StatLine-extractie voor delivery `CBS|2026-09-05`
	succesvol naar Landing gepubliceerd. De eerste daaropvolgende Bronze-run
	faalde omdat de DDL voor `raw_dev.cbs` wel het landingvolume maar niet het
	vereiste interne `checkpoints`-volume aanmaakte. De DDL is uitgebreid met
	idempotente `checkpoints`- en `quarantine`-volumes voor CBS, ECB en Nager;
	de herstel-setup is daarna succesvol afgerond. De CBS-pipeline moet nu
	opnieuw vanaf Bronze worden uitgevoerd en volledig worden geverifieerd.
16. De herstart van de CBS-pipeline bereikte Quality en bewees daarmee dat
	Bronze na de volumeherstelactie functioneert. Quality faalde vervolgens
	bij het schrijven van Rejects: de business key gebruikte de ruwe CBS-namen
	`Vormen`, `Wijken` en `Perioden`, terwijl de Quality-projectie alleen de
	gemapte doelkolommen bevat. Reject-business-keys worden nu afgeleid uit de
	als business key gemarkeerde `QUALITY`-mappings; de gerichte lokale
	regressies zijn succesvol met 79 tests. De gewijzigde pipeline moet nog
	opnieuw naar dev worden gedeployed en end-to-end worden uitgevoerd.
17. De gecorrigeerde bundle is gevalideerd en naar dev gedeployed. De volledige
	CBS-pipeline eindigde op 4 september succesvol: metadata-validatie, Bronze,
	delivery-gate, Quality, Raw Vault, Business Vault, Gold Historisch en Gold
	Actueel zijn uitgevoerd. De gate verwerkte chronologisch de oudste
	beschikbare delivery `CBS|2026-09-04`; de succesvol geëxtraheerde delivery
	`CBS|2026-09-05` wacht als volgende op verwerking.

## 7. Testplan en uitgevoerde tests

### Lokale tests

Uitgevoerd:

```text
python -m pytest -q
92 passed
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
- Per bronobject een expliciete verwerkingsroute: `RAW_VAULT` of `REFERENCE_DATA`.
- `REFERENCE_DATA` gaat na Quality naar een versioned referentietabel in Business Vault;
	bestaande objecten behouden door de standaardwaarde hun `RAW_VAULT`-route.
- CBS StatLine-tabel `86204NED` beoordeeld als historisch feitenobject en daarmee
	aangewezen voor `RAW_VAULT`, niet voor de referentieroute.
- Gold Historisch en Gold Actueel zijn per `source_system_id` gefilterd; een
	CBS-delivery kan daardoor geen `SALES_MART`-entiteiten bouwen of publiceren.

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

### Cross-source integratierisico's

De routekeuze per bronobject voorkomt niet automatisch dat data uit meerdere
bronnen inhoudelijk veilig gecombineerd wordt. Voor CBS en toekomstige
bronsystemen gelden daarom de volgende voorwaarden:

- Een `delivery_id` is uitsluitend binnen één bronsysteem betekenisvol.
	Gebruik geen `SAME_DELIVERY`-afhankelijkheid tussen bijvoorbeeld `SALES` en
	`CBS`; leg in plaats daarvan een expliciet freshness- of versiecontract vast.
- Een Gold-mart die bronnen combineert moet een eigen data-product en
	publicatiegroep krijgen. Publiceer pas wanneer alle benoemde bronversies
	beschikbaar zijn; combineer nooit een nieuwe Sales-delivery stilzwijgend met
	een willekeurige actuele CBS-versie.
- De Raw-Vault collision code bevat het bronsysteem. Dezelfde natuurlijke sleutel
	uit twee systemen vormt daardoor bewust twee hubs. Integratie vereist een
	expliciete matching-, mastering- of same-as-regel in Business Vault.
- Een Data Vault-entiteit met mappings uit meerdere bronsystemen wordt niet door
	de brongebonden planner geladen. Multi-source integratie vereist een afzonderlijke
	integratierun na de bronloads, met expliciete afhankelijkheden en testgevallen.
- Een reference-load sluit momenteel geen sleutels af die in een volledige nieuwe
	snapshot ontbreken. Voeg vóór gebruik van `SNAPSHOT_SCD2` voor referentiedata
	een snapshot-compleetheidscontract en ontbrekende-sleutelafhandeling toe.
- CBS `86204NED` is een jaartabel; een nieuw jaar kan een nieuwe StatLine-tabelcode
	krijgen. Houd daarom het logische data product stabiel, maar versieer de fysieke
	datasetcode, het kolomcontract en de comparability/trend-break metadata.

### Aanbevolen vervolgaanpak voor CBS

1. **Bouw eerst een zelfstandig CBS-data product.** Onboard `86204NED` als
	`CBS_JEUGDZORG_MART` met een eigen Landing-volume, OData-extract, Bronze- en
	Quality-contract, Raw Vault, historische Gold-tabel en eigen publicatiegroep.
	Gebruik een volledige, immutable extract per publicatiemoment omdat CBS cijfers
	achteraf reviseert.
2. **Beheer het statistiekcontract expliciet.** Leg naast de datasetcode de
	CBS-publicatie- en extracttijd, cijferstatus (voorlopig/nader voorlopig/definitief),
	maatregel, peilperiode en eventuele trendbreuk vast. Een jaarlijkse nieuwe
	StatLine-code wordt zo een nieuwe bronversie van hetzelfde logische product.
3. **Voeg geen Sales-CBS-join toe zonder businessvraag.** `86204NED` en de
	Contoso Sales-case hebben geen natuurlijke gemeenschappelijke bedrijfsentiteit.
	Een technische join op gemeente is geen voldoende reden voor een gecombineerd
	data product.
4. **Maak cross-source integratie pas daarna expliciet.** Alleen wanneer een
	concrete analysevraag een combinatie vereist, definieer je een nieuw data
	product met een eigen integratierun, matchingregels, freshness-SLA en een
	releasecontract waarin de gekozen Sales- en CBS-versies zijn vastgezet.

### Aanbevolen externe bron voor de Sales-case: ECB-wisselkoersen

De ECB Data API is een geschiktere eerste externe bron voor een echte
Sales-verrijking dan CBS `86204NED`. De bestaande order- en retourdata bevatten
al `currency_code` en geldbedragen. Dagelijkse ECB-referentiekoersen kunnen
daarom tijdsafhankelijk aan `order_date`, `ship_date` of `return_date` worden
gekoppeld.

- Configureer de bron als `REFERENCE_DATA`, bijvoorbeeld `ECB.EXCHANGE_RATE`.
	De API levert SDMX-data; per reeks is de koers gekoppeld aan valuta en
	observatiedatum.
- Bewaar in Landing en Quality minimaal `currency_code`, `rate_date`,
	`rate_to_eur`, bronreeks, publicatietijd en extracttijd. De ECB-reeks
	`EXR/D.USD.EUR.SP00.A` is bijvoorbeeld een dagelijkse USD-koers ten opzichte
	van EUR; de conversierichting moet als metadata worden vastgelegd.
- Laad de reeks als versioned referentiedata naar Business Vault. Verrijk
	orderregels in een computed satellite met de laatst bekende koers op of vóór
	de transactiedatum. Op niet-publicatiedagen, zoals weekenden en feestdagen,
	bestaat immers niet altijd een nieuwe koers.
- Leg vooraf de boekhoudkundige regel vast: een historische omzetkoers wordt
	doorgaans bij verwerking vastgezet; een latere ECB-correctie leidt alleen na
	expliciete goedkeuring tot een herwaardering. Zonder die regel kunnen eerdere
	Gold-totalen ongemerkt wijzigen.

### Deploymentstatus externe bronnen

Op 4 september 2026 is de Databricks Asset Bundle met het OAuth-profiel
`databricks_oauth` succesvol gevalideerd en naar `dev` gedeployed. De Gold
historietabellen en Current-views voor CBS, ECB en Nager zijn in Unity Catalog
aanwezig. Een eerste CBS-extractie faalde voordat netwerkverkeer plaatsvond,
omdat de notebook `request_options` las zonder deze kolom in zijn configuratie-
SELECT op te nemen. Dit is hersteld en gedekt met een metadatacontracttest.

Voor bestaande omgevingen is `request_options` bovendien opgenomen in de
idempotente schema-aware setupmigratie. De herstel-setup is gestart om de
ontbrekende kolom, indien van toepassing, toe te voegen, de seedmetadata te
laden en de metadata opnieuw te valideren. De oude CBS-, ECB-, Nager- en
validatieruns zijn gecontroleerd geannuleerd om geen verouderde metadata naast
de herstelrun te gebruiken. Ten tijde van dit verslag wacht de herstel-setup
nog op voltooiing; de volledige lokale regressiesuite is groen met 92 tests.

Op 5 september 2026 is Nager.Date voor delivery `NAGER|2026-09-05` succesvol
geextraheerd naar `/Volumes/raw_dev/nager/landing/2026-09-05/holidays_nl`.
Het immutable manifest bevat 11 records en de levering bevat alle verwachte
JSON-bestanden plus `_SUCCESS`. Een handmatige herhaalrun eindigde op de
bestaande delivery-check; dat is inhoudelijk correct om overschrijven te
voorkomen, maar de job markeert de run daardoor nog als `FAILED`. De workflow
is daarom gedeployed met een lege datumdefault, zodat een run zonder expliciete
datum de actuele UTC-datum gebruikt. Open actie: een bestaande complete
delivery moet in de extractor als idempotent succes worden geregistreerd.

Deze open actie is op 5 september 2026 opgelost. De publieke extractor
controleert nu het immutable manifest: een bestaande, complete delivery eindigt
als succesvolle no-op (`EXISTS`) en een bestaande folder zonder manifest faalt
als onvolledig. Daardoor start een herhaalrun veilig de brongebonden keten naar
Gold zonder Landing-data te overschrijven.

Op 5 september 2026 is de ECB Quality-filter aangescherpt naar
`try_cast(OBS_VALUE AS decimal(18,8)) IS NOT NULL`. SDMX gebruikt lege of
niet-castbare observaties voor momenten zonder gepubliceerde koers; die zijn
geen koersrecords en worden daarom voor de Quality-projectie uitgesloten. De
regel `ecb_rate_positive` behoudt een harde drempel van 0 procent voor de
overblijvende koersrecords: nul- en negatieve waarden falen de batch nog steeds.
De metadatawijziging is gedekt met een gerichte regressietest.

Op 5 september 2026 zijn ECB, CBS en Nager als drie afzonderlijke end-to-end
laadprocessen ingericht. Elke bron heeft een eigen Databricks-job met een
eigen deliverydatum, immutable Landing-extract en brongebonden vervolgketen
naar Gold. De jobs starten achtereenvolgens de gedeelde extractjob met een vast
bronobject en de pipeline met het bijbehorende bronsysteem. Daardoor kunnen
planning, retries, audit en foutafhandeling per extern data-product worden
beheerd zonder dat een bron een ander data-product publiceert.

Op 5 september 2026 is vastgesteld dat de oorspronkelijke ECB-SDMX-CSV meer
kolommen bevat dan het Quality-contract. Bronze comprimeerde die onbekende
kolommen bij `RESCUE` ten onrechte naar `_rescued_data` en verwijderde ze uit
de fysieke Bronzelaag. Dit is aangepast: Bronze bewaart alle bronkolommen met
schema evolution en technische lineagekolommen. De Quality-laag bepaalt daarna
via mappings welke velden onderdeel zijn van het gevalideerde datacontract.

Op 5 september 2026 is `ECB|2026-09-05` aantoonbaar end-to-end verwerkt. De
zelfstandige ECB-job en alle onderliggende taken zijn succesvol: extract,
Bronze, delivery gate, Quality, Reference Data, Gold Historisch en Gold
Actueel. De 2.417 lege SDMX-observaties worden door de geactiveerde
`quality_filter_expression` vóór de Quality-projectie uitgesloten; valide
koersrecords zijn naar de versioned referentietabel en de Gold-publicatie
verwerkt.

Op 5 september 2026 is de daadwerkelijke Gold-output in dev gecontroleerd.
Alle actieve Gold-entiteiten voor Sales, CBS, ECB en Nager bevatten rijen in
hun historische tabel en hebben voor de actuele laag een actieve publicatie.
De vier SharePoint-Gold-entiteiten hebben bewust nog geen output, omdat de
SharePoint-bronobjecten in metadata inactief staan.

Op 6 september 2026 is in de Fabric SQL-database `Contoso_database` de
bronview `SalesLT.vw_databricks_sales_order_line` ingericht. De view levert
een expliciet orderregelcontract met één rij per `SalesOrderDetailID`, een
deterministische productbeschrijving en `source_last_modified_at` als
extract-watermark. Direct identificeerbare contact- en adresgegevens zijn
voorlopig niet opgenomen. Voor de extractie is in Entra-tenant
`4kjwn2.onmicrosoft.com` (tenant-ID `f9350d85-6f0e-42fd-8385-a59c2f09ec1a`)
de single-tenant service principal `spn-databricks-fabric-reader` aangemaakt,
met client-ID `73d92d95-9287-4778-917c-bb86e63e5473`. De service principal is
lid van de dedicated Entra security group
`grp-fabric-contoso-databricks-readers`
(`ad49fc75-cf5c-4fc9-94e9-f4fd9c7ceb7f`). Het Fabric-item `Contoso_database`
staat in workspace `retail_lakehouse_dev`
(`0aaf238c-abe9-4497-9943-87a6a6d23ebd`). Op 6 september 2026 is de Fabric
tenantpolicy `Service principals can call Fabric public APIs` beperkt tot
uitsluitend `grp-fabric-contoso-databricks-readers`. De groep heeft vervolgens
de Viewer-rol in `retail_lakehouse_dev` gekregen, via de Fabric-portal
gevalideerd met de Power BI-workspace-API. De jaarlijkse Entra-clientcredential
kan niet vanuit de geautomatiseerde omgeving worden opgeslagen: secretwaarden
worden daar vóór verwerking geredigeerd. De secret scope `fabric-extract`
bevat `client-id`, `client-secret` en `tenant-id`; op 6 september 2026 is de
client-secretwaarde daarom na handmatige secretcreatie rechtstreeks in de
Databricks CLI ingevoerd. Credentials worden niet in Git of documentatie
opgenomen.

De JDBC-proef bevestigde vervolgens Entra-authenticatie en verbinding met
`Contoso_database`. Een eerste externe databasegebruiker, aangemaakt op basis
van de service-principal-weergavenaam, bleek niet met de JDBC-login-SID overeen
te komen en had daardoor geen effectief `SELECT`-recht. Fabric SQL koppelt de
JDBC-login aan het application/client-ID. De databasegebruiker is daarom
vervangen door de JDBC-loginnaam
`73d92d95-9287-4778-917c-bb86e63e5473@f9350d85-6f0e-42fd-8385-a59c2f09ec1a`,
met de client-ID als externe SID en met uitsluitend `SELECT` op
`SalesLT.vw_databricks_sales_order_line`; de SID-koppeling is vervolgens in de
database bevestigd. De laatste JDBC-leesproef op de view geeft nog
`Invalid object name`, de SQL Server-melding die ook bij ontbrekende effectieve
objectrechten optreedt. De effectieve objecttoegang van de application-ID is
daarmee nog open als technische validatiepunt; er zijn bewust geen bredere
schema- of database-rechten toegekend. Op 6 september 2026 is de service
principal aanvullend rechtstreeks als Viewer toegevoegd aan workspace
`retail_lakehouse_dev`, naast het bestaande groepslidmaatschap. De onmiddellijke
JDBC-herproef faalde nog; na Fabric-autorisatiepropagatie moet uitsluitend de
read-only JDBC-proef worden herhaald. Een aanvullende proef met de ingebouwde
database-rol `db_datareader` voor de JDBC-loginnaam is op 6 september 2026
succesvol toegekend, maar loste de `Invalid object name`-fout niet op. Daarmee
is vastgesteld dat de fout niet door de object-, schema- of database-
leespermissie wordt veroorzaakt. De service-principal-token wordt door de
Fabric SQL-database nog niet aan de verwachte databaseprincipal gekoppeld;
verdere oplossing vereist een ondersteund Fabric SQL-service-principal-
authenticatiepatroon of Microsoft-supportonderzoek. Een aanvullende directe
Fabric-workspacerol `Contributor` voor de service principal loste de JDBC-fout
evenmin op; er is bewust niet verder opgeschaald naar `Admin`.
Een aansluitende JDBC-proef met de gebruiker geformatteerd als
`application-id@tenant-id` authenticeerde eveneens, maar gaf nog steeds
`Invalid object name` voor de contractview. Daarmee is ook de JDBC-
gebruikersnaamvariant als oorzaak uitgesloten.

Op 6 september 2026 is de oorzaak van de JDBC-storing vastgesteld en opgelost.
De eerdere proeven gebruikten ten onrechte het SQL analytics endpoint
`*.datawarehouse.fabric.microsoft.com` en de logische naam
`Contoso_database`. De Fabric SQL-database vereist het eigen TDS-endpoint
`qugtl6ion76ufa4fuwoc6cpmdi-rqr26cxjvolujgkdq6tknur6xu.database.fabric.microsoft.com`,
TLS-validatie voor `*.database.windows.net` en de fysieke databasenaam
`Contoso_database-a2e53891-642a-4a0c-9a7f-c72e1351ff57`. De serverless
Databricks JDBC-proef is daarna succesvol uitgevoerd: de technische identity
las 542 rijen uit `SalesLT.vw_databricks_sales_order_line`.

Dit platte broncontract volgt na Bronze en Quality de bestaande
`REFERENCE_DATA`-route. Daarmee wordt Raw Vault bewust overgeslagen, terwijl
de versioned referentietabel in Business Vault en de atomische Gold Current-
publicatie behouden blijven. De Databricks JDBC-extractor en de metadata voor
deze route zijn de resterende implementatiestappen.

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
| P0 | Herstel-setup, bronextracties en brongebonden volledige pipeline-runs afronden en Gold-publicaties controleren | Data engineering/platform |
| P1 | CDC en partial snapshot laadstrategieën toevoegen | Data engineering |
| P1 | Effectivity satellite-loadlogica toevoegen | Data Vault engineering |
| P1 | Reject-herverwerking als werkproces inrichten | Data operations/data stewards |
| P1 | Governancevelden toevoegen: owner, PII, retentie, SLA, cost center | Data governance |
| P1 | DBU/Azure-kosten meten en budgetalerts instellen | FinOps/platform |
| P1 | CBS `86204NED` onboarden: landingvolume, OData-extract, objectcontract en Vault/Gold-metadata | Data engineering/architecture |
| P1 | Freshness- en versiecontracten ontwerpen voor Gold-data producten die bronnen combineren | Data architecture |
| P1 | Multi-source Business-Vault-integratierun ontwerpen, inclusief matching/masteringregels | Data Vault engineering |
| P1 | Snapshot-compleetheid en expire-logica toevoegen aan `REFERENCE_DATA` | Data engineering |
| P1 | ECB-wisselkoersbron ontwerpen en onboarden voor tijdsafhankelijke Sales-bedragverrijking | Data engineering/finance |
| P2 | Delta control-table lookup voor zeer grote fan-outs | Platform engineering |
| P2 | Metadata-SCD2 alleen toevoegen als runtime-configuratie buiten Git nodig is | Architecture board |

## 10. Tijd en kosten

De lokale Copilot-sessiehistorie registreert voor het v1- en v2-traject samen 17 interactieve sessies met 383 turns. De som van de geregistreerde sessievensters is ongeveer **31 uur en 55 minuten**. De kalenderdoorlooptijd liep van 29 augustus tot 2 september 2026, ongeveer **4 dagen en 11 uur**.

Dit is geen betrouwbare factuurmeting: pauzes kunnen zijn inbegrepen en werk buiten Copilot ontbreekt. De lokale opslag bevat geen tokengebruik, modelprijzen of Copilot-factuurgegevens. Ook zijn de Databricks DBU- en Azure-verbruikskosten niet vastgelegd; deze moeten uit Databricks system billing en Azure Cost Management worden gehaald.

## 11. Overdracht

De technische besluiten en runtimebevindingen staan in [00_besluitenlog.md](00_besluitenlog.md). Dit document is de samenvatting voor besluitvorming en overdracht. De aanbevolen volgende stap is het afronden van de acceptatietestmatrix en het herbeoordelen van de productie-gate op basis van meetresultaten, securitybewijs en operationele eigenaarschap.
