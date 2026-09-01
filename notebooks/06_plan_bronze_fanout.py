# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Plan Bronze fan-out
# MAGIC Levert een gedeelde batch-ID en de actieve bronobjecten aan een Serverless
# MAGIC `for_each`-taak. Elke iteratie krijgt een eigen compute-isolatie.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_system_id", "SALES")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.metadata import MetadataRepository

env = dbutils.widgets.get("env")
source_system_id = dbutils.widgets.get("source_system_id")
settings = Settings(env=env)
ctx = RunContext.create(settings)
repo = MetadataRepository(spark, settings)

bronze_inputs = [
    {"source_object_id": obj.source_object_id}
    for obj in repo.source_objects()
    if obj.source_system_id == source_system_id
]
if not bronze_inputs:
    raise ValueError(f"Geen actieve bronobjecten voor {source_system_id}.")

dbutils.jobs.taskValues.set("batch_id", ctx.batch_id)
dbutils.jobs.taskValues.set("bronze_inputs", bronze_inputs)
print(f"batch_id={ctx.batch_id}; bronze-taken={len(bronze_inputs)}")
