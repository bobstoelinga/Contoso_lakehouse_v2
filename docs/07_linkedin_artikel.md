# LinkedIn-artikel

## Van idee naar metadata-gedreven ETL: een Databricks-concept ontwikkeld met AI

Een metadata-gedreven ETL-platform ontwerpen klinkt overzichtelijk totdat de eerste echte vragen op tafel komen.

Wat gebeurt er als drie bestanden niet tegelijk binnenkomen? Hoe voorkom je dat een nieuwe bronkolom stilletjes verdwijnt? Wat doe je met afgekeurde records? En hoe zorg je ervoor dat een actuele datamart nooit een mix toont van nieuwe dimensies en oude feiten?

Voor een Contoso Sales-case heb ik samen met AI een conceptuele lakehouse-oplossing uitgewerkt op Databricks, met Unity Catalog, Delta Lake, Auto Loader en Data Vault 2.0. Dit artikel beschrijft een leer- en ontwikkelproject, geen productieadvies.

**Transparant:** dit artikel en het bijbehorende conceptuele project zijn ontwikkeld met ondersteuning van AI. AI hielp bij analyse, uitwerking en documentatie. De ontwerpkeuzes, controles en uiteindelijke beoordeling heb ik zelf gedaan.

## Meer informatie

Het volledige project en de technische documentatie zijn beschikbaar op [GitHub](https://github.com/bobstoelinga/Contoso_lakehouse_v2).

Lees ook het [volledige projectverslag](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/main/docs/06_projectverslag.md) met de requirements, architectuur, mijlpalen, testresultaten, openstaande acties en productie-readiness.

## Het uitgangspunt

De gewenste keten was:

`Volume -> Bronze -> Quality/Reject -> Data Vault -> Gold Historisch -> Gold Actueel`

Orders, Customers en Products worden per ontvangstdatum aangeleverd. Alle objecten, mappings, kwaliteitsregels en afhankelijkheden moesten metadata-gedreven zijn. Het doel was niet alleen een pipeline die vandaag werkt, maar een framework waarin nieuwe bronobjecten zoveel mogelijk zonder nieuwe notebooklogica kunnen worden toegevoegd.

## De architectuurkeuzes

### 1. Een delivery is meer dan een bestand

Auto Loader denkt in bestanden. De business denkt in leveringen.

Daarom wordt een micro-batch gesplitst per deliverydatum en chronologisch verwerkt. Een delivery-gate controleert of alle verplichte bronobjecten succesvol in Bronze staan. Een latere levering mag niet vóór een oudere onvolledige levering worden verwerkt wanneer dat de historische juistheid kan aantasten.

### 2. Metadata is het besturingsmodel

Bronobjecten, laadstrategieën, mappings, DQ-regels, Data Vault-entiteiten en Gold-definities staan in metadata. De workflow bevat vooral de lagen; de inhoudelijke volgorde komt uit de afhankelijkheidsgraaf.

Daarbij is metadata niet zomaar een configuratietabel. SQL-expressies in metadata kunnen uitvoeringsrechten krijgen. Daarom is gekozen voor Git als bron van waarheid, deployment via Databricks Asset Bundles, read-only metadata in productie en validatie van identifiers en SQL-expressies.

### 3. Data Vault als historische ruggengraat

Hubs en links gebruiken SHA-256 met een vastgelegde hash-conventie. De bronidentiteit wordt meegenomen in de collision code, zodat dezelfde business key uit verschillende systemen niet onbedoeld samenvalt.

Satellites zijn fysiek insert-only. Historische einddatums worden in views afgeleid. Dat voorkomt voortdurende updates op historische Delta-bestanden en past beter bij schaalbare historisatie.

### 4. Quality moet herstelbaar zijn

Een afgekeurde rij is geen losse foutmelding. De rejectlaag bewaart de volledige payload, alle faalredenen en een status voor opvolging. Daardoor blijft zichtbaar waarom een record is afgewezen en kan een organisatie later een gecontroleerd herstelproces uitvoeren.

### 5. Actueel publiceren als één geheel

De actuele datamart gebruikt twee fysieke slots. Een nieuwe release wordt volledig opgebouwd in het inactieve slot. Pas wanneer alle entiteiten van de publication group succesvol zijn opgebouwd, wordt één releasepointer gewijzigd.

Een fout in één fact of dimensie laat de vorige complete release actief. Dat is een klein technisch detail met grote gevolgen voor de betrouwbaarheid van BI-consumenten.

## AI als ontwikkelpartner

AI heeft in dit project geholpen bij het uitwerken van Python-frameworkcode, SQL-DDL, metadata-seeds, workflows, tests en documentatie. De waarde zat vooral in het snel verkennen van ontwerpopties en het zichtbaar maken van consequenties.

Maar AI vervangt geen architectuurverantwoordelijkheid. De lastigste problemen kwamen juist naar voren tijdens validatie:

- Serverless ondersteunde een aanvankelijk gekozen sessieconfiguratie niet.
- Een metadata-placeholder veroorzaakte een SQL-parsefout.
- Een typefout in een Gold-query blokkeerde alleen de actuele factpublicatie.
- Een chronologische gate verwerkte bewust een oudere geblokkeerde levering vóór een nieuwere valide levering.
- Een stresstestgenerator produceerde eerst onbruikbare datumwaarden onder Spark Connect.

Elke fout leidde tot een codefix, regressietest en een nieuw architectuurbesluit. Dat is voor mij de kern van AI-ondersteund ontwikkelen: snel bouwen, maar iedere aanname laten botsen met een test, runtime of expliciet contract.

## Wat is aangetoond?

Tijdens de ontwikkeling zijn in de beschikbare dev-omgeving onder andere de volgende onderdelen getest:

- metadata-validatie vóór verwerking;
- Bronze-fan-out met meerdere bronobjecten;
- delivery-gates en chronologische verwerking;
- Quality-blokkade en Reject-registratie;
- Raw Vault, Business Vault en Gold Historisch;
- atomische publicatie van de actuele `SALES_MART`;
- gecontroleerde superseding van een geblokkeerde demo-delivery;
- een lokale regressiesuite met 69 geslaagde tests;
- een grote testdelivery met 1.000.000 Orders en in totaal 1.180.000 records.

## Zijn we productieklaar?

Nee, nog niet.

Dat is geen teleurstellende conclusie, maar een nuttige grens. Het ontwerp is een sterke conceptuele en technische basis. Voor productie ontbreken nog bewijs en operationalisering voor onder meer:

- tien opeenvolgende representatieve deliveries;
- SCD2-, delete- en effectivity-validatie met echte wijzigingen;
- alle negatieve herstelproeven;
- CDC en partial snapshots;
- governancevelden voor owner, PII, retentie, SLA en kostenplaats;
- formele security-, disaster-recovery- en RPO/RTO-tests;
- runbooks, alerting, on-call en reject-herverwerking;
- gemeten DBU-, opslag- en egresskosten.

Een groen architectuurdiagram is dus niet hetzelfde als een productieplatform. Productierijpheid ontstaat wanneer ontwerp, code, data, beveiliging, operatie en kosten gezamenlijk zijn bewezen.

## De belangrijkste les

Metadata-gedreven betekent niet dat alles automatisch veilig en schaalbaar is. Metadata moet zelf worden beheerd als product: versieerbaar, valideerbaar, beveiligd, traceerbaar en voorzien van duidelijke eigenaars.

AI versnelt het denk- en ontwikkelproces aanzienlijk. De menselijke rol verschuift daardoor niet naar de achtergrond. Die wordt juist belangrijker bij het bepalen van grenzen, het beoordelen van risico’s en het weigeren van een productie-release zolang de bewijslast niet compleet is.

Dit project eindigde daarom niet met de vraag “werkt de pipeline?”, maar met de betere vraag:

**Onder welke voorwaarden mogen we erop vertrouwen?**

#Databricks #DeltaLake #DataVault #DataEngineering #Lakehouse #MetadataDriven #AI #DataArchitecture
