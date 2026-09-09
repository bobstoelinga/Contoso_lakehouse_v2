# LinkedIn-artikel

## Samen met AI een lakehouse bouwen: de waarde zat in wat er daarna misging

In dit praktijkexperiment heb ik samen met AI een metadata-gedreven lakehouse voor een Contoso Sales-case ontworpen, uitgevoerd en gedocumenteerd. Het is gebouwd op Databricks met Unity Catalog, Delta Lake, Auto Loader en Data Vault 2.0.

AI versnelde de analyse, uitwerking, tests en documentatie. Ik bepaalde de requirements, stelde de kritische vragen, koos de grenzen en beoordeelde de uitkomst. Dat onderscheid is belangrijk: AI kan opties genereren en patronen helpen uitwerken, maar productierisico's wegen en een release accepteren blijft mensenwerk.

Het volledige project en de technische documentatie staan op [GitHub](https://github.com/bobstoelinga/Contoso_lakehouse_v2). Het [projectverslag](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/main/docs/06_praktijk_experiment_verslag.md) bevat de requirements, besluiten, testresultaten en openstaande acties.

## De opdracht

De gewenste straat was helder:

`Volume -> Bronze -> Quality/Reject -> Data Vault -> Gold Historisch -> Gold Actueel`

Orders, Customers en Products moesten per ontvangstdatum worden verwerkt. Alle objecten, mappings, kwaliteitsregels en afhankelijkheden moesten metadata-gedreven zijn. Een actuele datamart mocht pas wijzigen wanneer een volledige, consistente nieuwe versie klaarstond.

Dat klinkt overzichtelijk. Tot de echte vragen komen: wat als drie bestanden niet tegelijk arriveren? Wat gebeurt er met een nieuwe bronkolom? Wanneer blokkeert een kwaliteitsprobleem de rest van de keten? En hoe voorkom je dat een actuele mart nieuwe dimensies naast oude feiten toont?

## Wat we samen hebben ontworpen

### Een delivery is meer dan een bestand

Auto Loader ziet bestanden; de business ziet leveringen. Daarom wordt een micro-batch per deliverydatum gesplitst en chronologisch verwerkt. Een delivery-gate laat verwerking pas door wanneer alle verplichte bronobjecten succesvol in Bronze staan.

Een complete Landing-delivery is idempotent. Een herhaalrun overschrijft geen immutable data, maar kan de vervolgketen veilig hervatten. Een bestaande folder zonder manifest faalt expliciet.

### Metadata bestuurt de keten

Bronobjecten, laadstrategieën, mappings, DQ-regels, Data Vault-entiteiten en Gold-definities staan in metadata. De workflow orkestreert lagen; de inhoudelijke volgorde komt uit een afhankelijkheidsgraaf.

Ik koos Git als bron van waarheid, Databricks Asset Bundles voor deployment en validatie van identifiers en SQL-expressies. Dat is geen administratie. Metadata met uitvoerbare SQL is onderdeel van het aanvals- en foutoppervlak en moet dus als product worden beheerd.

### Bronze bewaart, Quality contracteert

Schema evolution bleek een nuttige proef. Bronze bewaart nu de volledige bronstructuur plus technische lineage. Quality projecteert daarna alleen het expliciet gemapte contract. Nieuwe velden verdwijnen dus niet stilletjes en blijven beschikbaar voor analyse, contractuitbreiding en onderzoek.

### Historie en actuele data krijgen elk hun eigen taak

Hubs en links gebruiken SHA-256 met een vaste conventie en een brongebonden collision code. Satellites zijn insert-only. Historische einddatums worden in views afgeleid.

Voor Gold Actueel wordt een nieuwe release eerst in een inactief slot gebouwd. Pas als de complete publication group is geslaagd, wisselt een releasepointer. Faalt één fact of dimensie, dan blijft de vorige consistente versie zichtbaar voor BI.

## Waar het praktijkexperiment werkelijk werd getest

Het interessantste deel was niet het eerste diagram. Dat waren de fouten die het diagram moesten overleven.

- Serverless ondersteunde een gekozen sessieconfiguratie niet.
- Een metadata-placeholder veroorzaakte een SQL-parsefout.
- Een fout in een Gold-query blokkeerde uitsluitend de actuele factpublicatie.
- Een chronologische gate hield terecht een nieuwere levering tegen achter een oudere, onvolledige delivery.
- Een stresstestgenerator produceerde eerst onbruikbare datumwaarden onder Spark Connect.
- De ECB-SDMX-feed bevatte 2.417 lege observaties. Die moesten in Bronze behouden blijven, maar niet als geldige koers naar Quality. De oplossing was een metadata-gedreven Quality-filter, niet het versoepelen van de kwaliteitsdrempel.
- Een metadata-attribuut bestond al in de Python-code en seedbestanden, maar niet in de fysieke metadatatabel. Pas na een schema-aware migratie had de configuratie daadwerkelijk effect.

Bij elk incident hielp AI mogelijke oorzaken, codewijzigingen en tests te formuleren. Ik hield de architectuur scherp door de kernvraag steeds terug te brengen tot: wat is hier feitelijk bewezen, en wat alleen aannemelijk?

## Wat in dev is bewezen

- Metadata-validatie vóór verwerking.
- Bronze-fan-out, delivery-gates en chronologische verwerking.
- Quality-blokkade, Reject-registratie en gecontroleerde remediation.
- Raw Vault, Business Vault, Gold Historisch en atomisch Gold Actueel.
- Zelfstandige extract- en laadprocessen voor CBS, ECB en Nager.Date.
- Volledige schema-evolutie en technische lineage voor de uitgebreide ECB-SDMX-structuur.
- `ECB|2026-09-05` volledig van extract tot Gold verwerkt, met 262.410 actuele koersrecords.
- Gold-output gecontroleerd voor alle actieve Sales-, CBS-, ECB- en Nager-producten.
- Een lokale regressiesuite met 101 geslaagde tests.
- Een testdelivery met 1.000.000 Orders en in totaal 1.180.000 records.

Een koppeling met een interne Microsoft Fabric-databaseview is bewust buiten deze demonstratiescope gehouden. Die zou vooral Entra-identity, secretbeheer en cross-platformconnectiviteit aantonen. De kern van de metadata-gedreven keten is al met bestanden en publieke API's end-to-end bewezen.

## Zijn we productieklaar?

Nee.

Dat is geen tekortkoming van een demo, maar de juiste conclusie op basis van de beschikbare bewijslast. Voor productie zijn onder meer nog nodig:

- tien opeenvolgende representatieve deliveries;
- SCD2-, delete- en effectivity-validatie met echte wijzigingen;
- negatieve herstelproeven, CDC en partial snapshots;
- governance voor eigenaar, PII, retentie, SLA en kostenplaats;
- formele security-, disaster-recovery- en RPO/RTO-tests;
- runbooks, alerting, on-call en reject-herverwerking;
- gemeten DBU-, opslag- en egresskosten.

## Mijn belangrijkste les uit dit project

AI kan het denk- en ontwikkelproces versnellen: alternatieven vergelijken, repetitieve uitwerking doen, tests formuleren en een fout als aanwijzing behandelen in plaats van als eindpunt.

Maar ik moet nog steeds het doel bepalen, aannames uitdagen, risico's wegen en accepteren of weigeren. Juist wanneer de code er overtuigend uitziet, is die menselijke rol het belangrijkst.

Dit project eindigde daarom niet met de vraag: “Werkt de pipeline?”

Maar met de betere vraag:

**Onder welke voorwaarden mogen we erop vertrouwen?**

#Databricks #DeltaLake #DataVault #DataEngineering #Lakehouse #MetadataDriven #AI #DataArchitecture
