# Databricks notebook source
# MAGIC %md
# MAGIC # 41 — Gold Actueel (publish-by-pointer)
# MAGIC Bouwt elke Gold Actueel dataset in het inactieve slot en zet daarna alle
# MAGIC views van dezelfde `publication_group_id` in één stap om.
# MAGIC
# MAGIC Faalt een van de entiteiten, dan wordt er niets gepubliceerd en blijft de
# MAGIC vorige — consistente — versie actief.

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

settings = Settings(env=dbutils.widgets.get("env"))
ctx = RunContext.create(
    settings,
    batch_id=dbutils.widgets.get("batch_id"),
    delivery_id=dbutils.widgets.get("delivery_id"),
)
repo = MetadataRepository(spark, settings)
loader = GoldLoader(spark, repo, ctx)

# COMMAND ----------

# MAGIC %md ## Actieve versie vóór publicatie

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {settings.meta_catalog}.audit.v_active_gold_publication"))

# COMMAND ----------

# MAGIC %md ## Bouwen en publiceren per publication group

# COMMAND ----------

loader.run_current_layer()

# COMMAND ----------

# MAGIC %md ## Resultaat en actualiteit

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {settings.meta_catalog}.audit.v_active_gold_publication"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {settings.gold_catalog}.current.v_gold_freshness"))
