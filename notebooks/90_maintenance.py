# Databricks notebook source
# MAGIC %md
# MAGIC # 90 — Onderhoud
# MAGIC OPTIMIZE en VACUUM over alle lagen, plus opschonen van verlopen Gold Actueel
# MAGIC slots. Tabellen worden uit de metadata en Unity Catalog afgeleid; er staat
# MAGIC geen tabelnaam in dit notebook.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")
dbutils.widgets.text("vacuum_retain_hours", "168")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import Settings

settings = Settings(env=dbutils.widgets.get("env"))
retain_hours = int(dbutils.widgets.get("vacuum_retain_hours"))

CATALOGS = [
    settings.bronze_catalog,
    settings.quality_catalog,
    settings.reject_catalog,
    settings.vault_catalog,
    settings.gold_catalog,
    settings.meta_catalog,
]

# COMMAND ----------

for catalog in CATALOGS:
    schemas = [r.schema_name for r in spark.sql(
        f"SELECT schema_name FROM {catalog}.information_schema.schemata "
        f"WHERE schema_name <> 'information_schema'"
    ).collect()]
    for schema in schemas:
        tables = spark.sql(
            f"""
            SELECT table_name FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}' AND table_type = 'MANAGED'
            """
        ).collect()
        for t in tables:
            fqn = f"{catalog}.{schema}.{t.table_name}"
            try:
                spark.sql(f"OPTIMIZE {fqn}")
                spark.sql(f"VACUUM {fqn} RETAIN {retain_hours} HOURS")
                print(f"OK  {fqn}")
            except Exception as exc:
                print(f"SKIP {fqn}: {exc}")

# COMMAND ----------

# MAGIC %md ## Verlopen Gold Actueel publicaties opruimen
# MAGIC Een superseded slot blijft minimaal 24 uur bewaard zodat lopende BI-queries
# MAGIC niet halverwege hun bron kwijtraken.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT gold_entity_id, physical_slot, publication_status, superseded_at
        FROM {settings.meta_catalog}.audit.audit_gold_publication
        WHERE publication_status = 'SUPERSEDED'
          AND superseded_at < current_timestamp() - INTERVAL 24 HOURS
        """
    )
)
