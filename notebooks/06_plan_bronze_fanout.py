# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Plan Bronze delivery
# MAGIC Levert een gedeelde batch-ID en de actieve bronobjecten aan één Serverless
# MAGIC Bronze-taak. De objecten behouden hun eigen checkpoint en auditstatus.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_system_id", "SALES")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import json
import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.metadata import MetadataRepository

env = dbutils.widgets.get("env")
source_system_id = dbutils.widgets.get("source_system_id")
settings = Settings(env=env)
ctx = RunContext.create(settings)
repo = MetadataRepository(spark, settings)

active_objects = [
    {"source_object_id": obj.source_object_id}
    for obj in repo.source_objects()
    if obj.source_system_id == source_system_id
]
if not active_objects:
    raise ValueError(f"Geen actieve bronobjecten voor {source_system_id}.")

audit = f"{settings.meta_catalog}.audit"
has_pending_delivery = spark.sql(f"""
SELECT EXISTS (
  SELECT 1
  FROM {audit}.audit_delivery_manifest m
  LEFT JOIN {audit}.audit_delivery d ON d.delivery_id = m.delivery_id
  WHERE m.source_system_id = '{source_system_id}'
    AND m.manifest_status = 'CLOSED'
    AND coalesce(d.delivery_status, 'DETECTED') NOT IN ('COMPLETE', 'QUARANTINED', 'SUPERSEDED')
) AS has_pending_delivery
""").first().has_pending_delivery

bronze_inputs = active_objects if has_pending_delivery else []

dbutils.jobs.taskValues.set("batch_id", ctx.batch_id)
dbutils.jobs.taskValues.set(
    "bronze_object_ids",
    json.dumps([item["source_object_id"] for item in bronze_inputs]),
)
print(
    f"batch_id={ctx.batch_id}; bronze-objecten={len(bronze_inputs)}; "
    f"pending_delivery={has_pending_delivery}"
)
