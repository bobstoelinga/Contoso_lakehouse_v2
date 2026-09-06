# Databricks notebook source
# MAGIC %md
# MAGIC # 31 - Reference data
# MAGIC Laadt objecten met `processing_route = REFERENCE_DATA` vanuit Quality naar
# MAGIC versioned referentietabellen in Business Vault. Raw Vault wordt bewust
# MAGIC overgeslagen.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_system_id", "CBS")
dbutils.widgets.text("delivery_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.metadata import MetadataRepository
from contoso_lakehouse.reference import ReferenceDataLoader

settings = Settings(env=dbutils.widgets.get("env"))
ctx = RunContext.create(
    settings,
    batch_id=dbutils.widgets.get("batch_id"),
    delivery_id=dbutils.widgets.get("delivery_id"),
)
repo = MetadataRepository(spark, settings)
loader = ReferenceDataLoader(spark, repo, ctx)

for obj in repo.reference_objects(dbutils.widgets.get("source_system_id")):
    loader.load(obj.source_object_id)
    print(f"{obj.source_object_id}: referencehistorie bijgewerkt")