# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — Data Vault (Raw + Business Vault)
# MAGIC Genereert alle hub-, link- en satellite-loads uit `meta_dv_entity` en
# MAGIC `meta_dv_mapping`. Er is geen entiteit-specifieke code.
# MAGIC
# MAGIC Satellites zijn insert-only; `load_end_date` en `is_current` komen uit de
# MAGIC bijbehorende views. De volgorde komt uit de afhankelijkheidsgraaf.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("delivery_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.dropdown("zone", "RAW_VAULT", ["RAW_VAULT", "BUSINESS_VAULT"])
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.datavault import VaultLoader
from contoso_lakehouse.metadata import MetadataRepository
from contoso_lakehouse.orchestration import Orchestrator

env = dbutils.widgets.get("env")
zone = dbutils.widgets.get("zone")

settings = Settings(env=env)
ctx = RunContext.create(
    settings,
    batch_id=dbutils.widgets.get("batch_id"),
    delivery_id=dbutils.widgets.get("delivery_id"),
)
repo = MetadataRepository(spark, settings)
orch = Orchestrator(spark, repo, ctx)
loader = VaultLoader(spark, repo, ctx)

# COMMAND ----------

# MAGIC %md ## Uitvoervolgorde bepalen
# MAGIC Topologische sortering van `meta_dependency`; hubs vóór links vóór satellites.

# COMMAND ----------

layer = "BUSINESS_VAULT" if zone == "BUSINESS_VAULT" else "RAW_VAULT"
order = orch.execution_order(layer)
in_zone = {e.dv_entity_id for e in repo.dv_entities() if e.dv_zone == zone and e.dv_entity_type != "PIT"}
plan = [e for e in order if e in in_zone] + sorted(in_zone - set(order))
print(plan)

# COMMAND ----------

for entity_id in plan:
    orch.require_upstream_success(entity_id, layer)
    inserted = loader.load(entity_id)
    print(f"{entity_id}: {inserted} rijen toegevoegd")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT entity_id, run_status, rows_inserted, duration_seconds
        FROM {settings.meta_catalog}.audit.audit_load_run
        WHERE batch_id = '{ctx.batch_id}' AND layer = '{layer}'
        ORDER BY started_at
        """
    )
)
