# Databricks notebook source
# MAGIC %md
# MAGIC # 20 — Quality & Reject
# MAGIC Voert alle regels uit `meta_quality_rule` in één pass uit per bronobject.
# MAGIC Goedgekeurde records gaan naar de Quality-tabel, afgekeurde naar Reject
# MAGIC inclusief alle faalredenen en de originele payload.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_system_id", "SALES")
dbutils.widgets.text("delivery_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.metadata import MetadataRepository
from contoso_lakehouse.orchestration import Orchestrator
from contoso_lakehouse.quality import QualityEngine

env = dbutils.widgets.get("env")
delivery_id = dbutils.widgets.get("delivery_id")
batch_id = dbutils.widgets.get("batch_id")
source_system_id = dbutils.widgets.get("source_system_id")

settings = Settings(env=env)
ctx = RunContext.create(settings, batch_id=batch_id, delivery_id=delivery_id)
repo = MetadataRepository(spark, settings)
orch = Orchestrator(spark, repo, ctx)
engine = QualityEngine(spark, repo, ctx)

# COMMAND ----------

# MAGIC %md ## Volgorde uit de metadata
# MAGIC Referentiële regels op Orders vereisen dat Customers en Products al in de
# MAGIC Quality-laag staan. Die volgorde komt uit `meta_source_object.load_order`.

# COMMAND ----------

for obj in repo.source_objects():
    if obj.source_system_id != source_system_id:
        continue
    orch.require_upstream_success(obj.source_object_id, "QUALITY")
    print(f"--- {obj.source_object_id} ---")
    stats = engine.run(obj.source_object_id, delivery_id)
    print(stats)

# COMMAND ----------

# MAGIC %md ## Kwaliteitsresultaat van deze batch

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_object_id, rule_name, severity, rows_evaluated, rows_failed,
               round(failed_pct, 3) AS failed_pct, threshold_pct, threshold_breached
        FROM {settings.meta_catalog}.audit.audit_dq_result
        WHERE batch_id = '{ctx.batch_id}'
        ORDER BY source_object_id, rows_failed DESC
        """
    )
)
