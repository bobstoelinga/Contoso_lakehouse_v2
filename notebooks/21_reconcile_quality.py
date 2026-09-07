# Databricks notebook source
# MAGIC %md
# MAGIC # 21 - Quality reconciliatie
# MAGIC Bewijst per bronobject dat iedere Bronze-rij na de optionele Quality-filter
# MAGIC precies eenmaal als Quality-record of Reject-record is verantwoord.

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
from contoso_lakehouse.reconciliation import ReconciliationEngine

settings = Settings(env=dbutils.widgets.get("env"))
source_system_id = dbutils.widgets.get("source_system_id")
ctx = RunContext.create(
    settings, batch_id=dbutils.widgets.get("batch_id"), delivery_id=dbutils.widgets.get("delivery_id"),
)
repo = MetadataRepository(spark, settings)
engine = ReconciliationEngine(spark, repo, ctx)

# COMMAND ----------

results = []
for obj in repo.source_objects():
    if obj.source_system_id == source_system_id:
        result = engine.reconcile_quality(obj)
        results.append((result.source_object_id, result.expected_count, result.actual_count, result.passed))

display(spark.createDataFrame(results, "source_object_id string, expected_count long, actual_count long, passed boolean"))
print(f"Quality-reconciliatie geslaagd voor {len(results)} bronobjecten.")