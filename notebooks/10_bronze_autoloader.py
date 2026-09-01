# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Bronze ingest (Auto Loader)
# MAGIC Volledig metadata-gedreven. Het notebook kent geen enkel bronobject:
# MAGIC het leest `meta_source_object` en start per object een Auto Loader stream
# MAGIC met schema evolution en checkpointing.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_system_id", "SALES")
dbutils.widgets.text("source_object_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")
dbutils.widgets.dropdown("mode", "once", ["once", "continuous"])

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.bronze import BronzeLoader
from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.metadata import MetadataRepository

env = dbutils.widgets.get("env")
source_system_id = dbutils.widgets.get("source_system_id")
source_object_id = dbutils.widgets.get("source_object_id")
batch_id = dbutils.widgets.get("batch_id")
once = dbutils.widgets.get("mode") == "once"

settings = Settings(env=env)
ctx = RunContext.create(
    settings=settings,
    batch_id=batch_id,
)
repo = MetadataRepository(spark, settings)
loader = BronzeLoader(spark, repo, ctx)

print(f"batch_id={ctx.batch_id}  load_date={ctx.load_date.isoformat()}")

# COMMAND ----------

# MAGIC %md ## Landingpad ophalen uit de metadata

# COMMAND ----------

system = spark.sql(
    f"""
    SELECT landing_volume_path FROM {settings.meta_catalog}.metadata.meta_source_system
    WHERE source_system_id = '{source_system_id}' AND is_active
    """
).collect()[0]
landing_path = settings.resolve(system.landing_volume_path)
print(landing_path)

# COMMAND ----------

# MAGIC %md ## Eén bronobject laden
# MAGIC De Serverless `for_each`-taak bepaalt de paralleliteit; dit notebook voert
# MAGIC precies één metadata-gedefinieerd object uit.

# COMMAND ----------

obj = repo.source_object(source_object_id)
if obj.source_system_id != source_system_id:
    raise ValueError(f"{source_object_id} hoort niet bij bronsysteem {source_system_id}.")
print(f"--- {obj.source_object_id} ({obj.load_strategy}) ---")
loader.load(source_object_id, landing_path, once=once)

# COMMAND ----------

# MAGIC %md ## Status van de leveringen

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT * FROM {settings.meta_catalog}.audit.v_delivery_readiness
        WHERE source_system_id = '{source_system_id}'
        ORDER BY delivery_sequence_number DESC LIMIT 20
        """
    )
)
