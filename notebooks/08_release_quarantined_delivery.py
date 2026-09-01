# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Quarantaine gecontroleerd vrijgeven
# MAGIC Heropent uitsluitend een door Quality gequarantaineerde levering. Reden,
# MAGIC goedkeurder en referentie maken de beheeractie volledig auditeerbaar.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("delivery_id", "")
dbutils.widgets.text("reason", "")
dbutils.widgets.text("approved_by", "")
dbutils.widgets.text("approval_reference", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import Settings
from contoso_lakehouse.sqlutil import sql_string

settings = Settings(env=dbutils.widgets.get("env"))
delivery_id = dbutils.widgets.get("delivery_id").strip()
reason = dbutils.widgets.get("reason").strip()
approved_by = dbutils.widgets.get("approved_by").strip()
approval_reference = dbutils.widgets.get("approval_reference").strip()

if not all((delivery_id, reason, approved_by, approval_reference)):
    raise ValueError("delivery_id, reason, approved_by en approval_reference zijn verplicht.")

audit_delivery = f"{settings.meta_catalog}.audit.audit_delivery"
row = spark.sql(
    f"""
    SELECT delivery_status FROM {audit_delivery}
    WHERE delivery_id = {sql_string(delivery_id)}
    """
).first()
if row is None:
    raise ValueError(f"Onbekende delivery_id: {delivery_id}")
if row.delivery_status != "QUARANTINED":
    raise ValueError(
        f"Alleen een QUARANTINED levering mag worden vrijgegeven; huidige status: {row.delivery_status}"
    )

spark.sql(
    f"""
    UPDATE {audit_delivery}
    SET delivery_status = 'COMPLETE',
        released_at = current_timestamp(),
        released_by = {sql_string(approved_by)},
        release_reason = {sql_string(reason)},
        release_approval_reference = {sql_string(approval_reference)}
    WHERE delivery_id = {sql_string(delivery_id)}
      AND delivery_status = 'QUARANTINED'
    """
)
print(f"Levering uit quarantaine vrijgegeven: {delivery_id}")