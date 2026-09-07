# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Dead-letter work-item herplannen
# MAGIC Operatoractie met verplichte reden en approval-reference. De volgende
# MAGIC pipeline-run claimt het heropende work-item; dit notebook voert zelf niets uit.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("delivery_id", "")
dbutils.widgets.text("layer", "")
dbutils.widgets.text("entity_id", "")
dbutils.widgets.text("reason", "")
dbutils.widgets.text("approved_by", "")
dbutils.widgets.text("approval_reference", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext, Settings

settings = Settings(env=dbutils.widgets.get("env"))
delivery_id = dbutils.widgets.get("delivery_id").strip()
layer = dbutils.widgets.get("layer").strip()
entity_id = dbutils.widgets.get("entity_id").strip()
reason = dbutils.widgets.get("reason").strip()
approved_by = dbutils.widgets.get("approved_by").strip()
approval_reference = dbutils.widgets.get("approval_reference").strip()

if not all((delivery_id, layer, entity_id, reason, approved_by, approval_reference)):
    raise ValueError("delivery_id, layer, entity_id, reason, approved_by en approval_reference zijn verplicht.")

audit = AuditLogger(spark, RunContext.create(settings, delivery_id=delivery_id))
audit.requeue_dead_letter(delivery_id, layer, entity_id, approved_by, reason, approval_reference)
print(f"Dead-letter work-item heropend: {delivery_id} {layer}.{entity_id}")