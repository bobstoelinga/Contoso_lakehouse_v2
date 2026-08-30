# Unity Catalog structuur

## Catalogs en schema's

```
raw_<env>
└── sales
    ├── landing      (external volume)  /Volumes/raw_<env>/sales/landing/<yyyy-MM-dd>/
    ├── checkpoints  (managed volume)   Auto Loader checkpoints en schema locations
    └── quarantine   (managed volume)   onleesbare bestanden

contoso_meta_<env>
├── metadata         configuratie (meta_*)
└── audit            runtime status (audit_*, v_*)

contoso_bronze_<env>
└── sales            br_orders, br_customers, br_products

contoso_quality_<env>
└── sales            qa_orders, qa_customers, qa_products

contoso_reject_<env>
└── sales            rj_orders, rj_customers, rj_products, v_open_rejects

contoso_vault_<env>
├── raw_vault        hub_*, lnk_*, sat_*_h (+ views sat_*)
└── business_vault   sat_*_bv_h (+ views), pit_customer

contoso_gold_<env>
├── historical       dim_*_hist, fct_*_hist
├── current          views: dim_customer, dim_product, fct_sales, v_gold_freshness
└── current_internal fysieke slots: *_v1, *_v2
```

## Folderstructuur van een levering

```
/Volumes/raw_<env>/sales/landing/
├── 2026-08-28/
│   ├── orders.parquet
│   ├── customers.parquet
│   └── products.parquet
├── 2026-08-29/
│   ├── orders.parquet
│   ├── customers.parquet
│   └── products.parquet
└── 2026-08-30/
    └── ...
```

De datumfolder is de enige bron van de `delivery_id` (`SALES|2026-08-30`) en van
`delivery_sequence_number`. Bestanden worden herkend via
`meta_source_object.file_pattern`.

## Rechtenmodel

| Principal | Rechten |
|---|---|
| `svc_contoso_etl` (service principal) | `ALL PRIVILEGES` op alle verwerkingscatalogs; `READ VOLUME` op landing, `WRITE VOLUME` op checkpoints/quarantine |
| `grp_data_engineers` | `SELECT` op alle lagen; `MODIFY` op de metadata **alleen in dev** |
| `grp_bi_analysts` | `SELECT` uitsluitend op `contoso_gold_<env>.current` en `.historical` |
| `grp_data_stewards` | `SELECT` op de reject-catalog (bevat brondata) |

In `tst` en `prd` heeft niemand `MODIFY` op `contoso_meta_*.metadata`: metadata
wordt uitsluitend via Git en de Asset Bundle gedeployed. Zie besluit B-10 in
[00_besluitenlog.md](00_besluitenlog.md).

## Naamgevingsconventies

| Object | Conventie | Voorbeeld |
|---|---|---|
| Bronze tabel | `br_<object>` | `br_orders` |
| Quality tabel | `qa_<object>` | `qa_orders` |
| Reject tabel | `rj_<object>` | `rj_orders` |
| Hub | `hub_<entiteit>` | `hub_customer` |
| Link | `lnk_<a>_<b>` | `lnk_order_product` |
| Satellite (fysiek) | `sat_<parent>_h` | `sat_customer_h` |
| Satellite (view) | `sat_<parent>` | `sat_customer` |
| Business Vault satellite | `sat_<parent>_bv_h` | `sat_customer_bv_h` |
| Gold Historisch | `dim_*_hist` / `fct_*_hist` | `fct_sales_hist` |
| Gold Actueel (publiek) | `dim_*` / `fct_*` | `fct_sales` |
| Gold Actueel (slot) | `<tabel>_v1` / `_v2` | `fct_sales_v2` |
| Technische kolom | prefix `_` | `_batch_id` |
