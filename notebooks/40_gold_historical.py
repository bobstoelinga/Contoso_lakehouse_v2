# Databricks notebook source
# MAGIC %md
# MAGIC # 40 — Gold Historisch
# MAGIC SCD2 MERGE vanuit de (business) vault op basis van
# MAGIC `meta_gold_entity.select_sql` waar `gold_layer = 'HISTORICAL'`.
# MAGIC Volledige historie blijft behouden.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("delivery_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.gold import GoldLoader
from contoso_lakehouse.metadata import MetadataRepository
from contoso_lakehouse.orchestration import Orchestrator

settings = Settings(env=dbutils.widgets.get("env"))
ctx = RunContext.create(
    settings,
    batch_id=dbutils.widgets.get("batch_id"),
    delivery_id=dbutils.widgets.get("delivery_id"),
)
repo = MetadataRepository(spark, settings)
orch = Orchestrator(spark, repo, ctx)
loader = GoldLoader(spark, repo, ctx)

# COMMAND ----------

for entity in repo.gold_entities():
    if entity.gold_layer != "HISTORICAL":
        continue
    orch.require_upstream_success(entity.gold_entity_id, "GOLD_HIST")
    rows = loader.load_historical(entity)
    print(f"{entity.gold_entity_id}: {rows} rijen verwerkt")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT entity_id, run_status, rows_inserted, duration_seconds
        FROM {settings.meta_catalog}.audit.audit_load_run
        WHERE batch_id = '{ctx.batch_id}' AND layer = 'GOLD_HIST'
        ORDER BY started_at
        """
    )
)
