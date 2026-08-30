# Architectuur

## Lagen en dataflow

```mermaid
flowchart TD
    subgraph Landing
      V["/Volumes/raw/sales/landing/yyyy-MM-dd/<br/>orders · customers · products"]
    end
    subgraph Bronze
      B1[br_orders]
      B2[br_customers]
      B3[br_products]
    end
    G{{"Delivery gate<br/>alle verplichte objecten<br/>SUCCESS in dezelfde folder"}}
    subgraph Quality
      Q1[qa_orders]
      Q2[qa_customers]
      Q3[qa_products]
    end
    R[(Reject<br/>rj_*)]
    subgraph RawVault["Raw Vault"]
      H1[hub_customer]
      H2[hub_product]
      H3[hub_order]
      L1[lnk_order_customer]
      L2[lnk_order_product]
      S1[sat_customer]
      S2[sat_product]
      S3[sat_order]
      S4[sat_order_line]
    end
    subgraph BusinessVault["Business Vault"]
      BV1[sat_customer_bv]
      BV2[sat_order_line_bv]
      P1[pit_customer]
    end
    GH["Gold Historisch<br/>dim_*_hist · fct_*_hist"]
    GC["Gold Actueel<br/>views -> _v1/_v2 slots"]

    V -->|Auto Loader| B1 & B2 & B3
    B1 & B2 & B3 --> G
    G --> Q1 & Q2 & Q3
    Q1 & Q2 & Q3 -.afgekeurd.-> R
    Q2 --> H1
    Q3 --> H2
    Q1 --> H3
    H1 & H3 --> L1
    H2 & H3 --> L2
    H1 --> S1
    H2 --> S2
    H3 --> S3
    L2 --> S4
    S1 --> BV1
    S4 --> BV2
    BV1 --> P1
    BV1 & BV2 & S2 & S3 --> GH
    GH --> GC
```

## Laagverantwoordelijkheden

| Laag | Doel | Wat er níét gebeurt |
|---|---|---|
| **Volume** | Onbewerkte bestanden bewaren; één datumfolder = één levering | Geen transformatie, geen validatie |
| **Bronze** | 1:1 opslag + technische kolommen; incrementeel via Auto Loader met schema evolution | Geen typering, geen filtering, geen deduplicatie |
| **Quality** | Typeren en valideren volgens `meta_mapping` en `meta_quality_rule` | Geen businesslogica, geen joins tussen entiteiten |
| **Reject** | Afgekeurde records met alle faalredenen en de originele payload | Geen automatische correctie |
| **Raw Vault** | Historisatie zonder interpretatie (DV2.0) | Geen berekeningen, geen businessregels |
| **Business Vault** | Afgeleide attributen, PIT en Bridge | Geen presentatielogica |
| **Gold Historisch** | Dimensioneel model met volledige SCD2 historie | Geen filtering op actualiteit |
| **Gold Actueel** | Snapshot van de laatste succesvolle business load | Geen historie |

## Technische kolommen

Elke laag draagt een vaste set technische kolommen zodat herkomst en run altijd
te herleiden zijn.

| Kolom | Bronze | Quality | Vault | Gold Hist | Gold Actueel |
|---|:-:|:-:|:-:|:-:|:-:|
| `_delivery_id` / `_delivery_date` | ✓ | ✓ | | | |
| `_batch_id` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `_record_source` / `record_source` | ✓ | ✓ | ✓ | ✓ | |
| `load_date` | | | ✓ | | |
| `_source_file_*` | ✓ | | | | |
| `_rescued_data` | ✓ | | | | |
| `_quality_status` / `_warning_codes` | | ✓ | | | |
| `valid_from` / `valid_to` / `is_current` | | | via view | ✓ | |
| `_as_of_delivery_id` / `_as_of_timestamp` | | | | | ✓ |

## Fouttolerantie

| Scenario | Gedrag |
|---|---|
| Eén bestand ontbreekt in de levering | Gate blijft dicht; vervolgstappen worden overgeslagen (`SKIPPED`), geen halve mart |
| Nieuwe kolom in de bron | Stream faalt bewust, retry pikt het nieuwe schema op; kolom komt in Bronze, niet in Gold |
| Type-wijziging in de bron | Waarde landt in `_rescued_data`; DQ-signaal op rescued data |
| DQ-drempel overschreden | Batch faalt (`FAIL_BATCH`); Quality-tabel blijft ongewijzigd |
| Data Vault load faalt | Idempotente herstart op `_batch_id`; geen dubbele rijen |
| Gold Actueel bouw faalt | Niets gepubliceerd; vorige versie blijft actief en consistent |
| Herstart na storing | Leveringen worden chronologisch ingehaald op volgnummer |
