# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Delivery gate
# MAGIC Bepaalt of vervolgverwerking mag starten. Deze taak is de enige plek waar
# MAGIC de leverings-afhankelijkheid wordt afgedwongen — en die komt volledig uit
# MAGIC `meta_dependency` en `audit_delivery`.
# MAGIC
# MAGIC Uitkomst wordt als taskValue doorgegeven zodat de Workflow kan vertakken.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_system_id", "SALES")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.metadata import MetadataRepository
from contoso_lakehouse.orchestration import Orchestrator

env = dbutils.widgets.get("env")
source_system_id = dbutils.widgets.get("source_system_id")

settings = Settings(env=env)
ctx = RunContext(settings=settings)
repo = MetadataRepository(spark, settings)
orch = Orchestrator(spark, repo, ctx)

# COMMAND ----------

# MAGIC %md ## Graafvalidatie
# MAGIC Cycli en wees-verwijzingen worden hier afgevangen, niet tijdens het laden.

# COMMAND ----------

orch.validate_graph()
print("Afhankelijkheidsgraaf is geldig.")

# COMMAND ----------

# MAGIC %md ## Eerstvolgende verwerkbare levering
# MAGIC Chronologisch: levering N+1 wacht op N. Anders raakt SCD2-historie corrupt.

# COMMAND ----------

delivery_id = orch.next_delivery(source_system_id)

display(
    spark.sql(
        f"""
        SELECT * FROM {settings.meta_catalog}.audit.v_next_processable_delivery
        WHERE source_system_id = '{source_system_id}'
        """
    )
)

# COMMAND ----------

if delivery_id is None:
    print("Geen complete levering beschikbaar; vervolgstappen worden overgeslagen.")
    dbutils.jobs.taskValues.set("gate_open", "false")
    dbutils.jobs.taskValues.set("delivery_id", "")
    dbutils.notebook.exit("SKIPPED")

orch.require_delivery_complete(delivery_id)
print(f"Gate open voor {delivery_id}")

dbutils.jobs.taskValues.set("gate_open", "true")
dbutils.jobs.taskValues.set("delivery_id", delivery_id)
dbutils.jobs.taskValues.set("batch_id", ctx.batch_id)
dbutils.notebook.exit(delivery_id)
