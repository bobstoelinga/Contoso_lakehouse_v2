# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Levering gecontroleerd superseden
# MAGIC Beheeractie voor een onherstelbare of bewust overgeslagen levering.
# MAGIC Een volledige auditreden, goedkeurder en referentie zijn verplicht.

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
    SELECT d.delivery_status,
           EXISTS (
             SELECT 1
                         FROM {settings.meta_catalog}.audit.audit_gold_publication_group g
                         WHERE g.delivery_id = d.delivery_id
                             AND g.release_status = 'ACTIVE'
           ) AS gold_published
    FROM {audit_delivery} d
    WHERE d.delivery_id = {sql_string(delivery_id)}
    """
).first()
if row is None:
    raise ValueError(f"Onbekende delivery_id: {delivery_id}")
if row.gold_published:
    raise ValueError("Een in Gold Actueel gepubliceerde levering mag niet als SUPERSEDED worden gemarkeerd.")

spark.sql(
    f"""
    UPDATE {audit_delivery}
    SET delivery_status = 'SUPERSEDED',
        superseded_at = current_timestamp(),
        superseded_by = {sql_string(approved_by)},
        supersede_reason = {sql_string(reason)},
        supersede_approval_reference = {sql_string(approval_reference)}
    WHERE delivery_id = {sql_string(delivery_id)}
    """
)
print(f"Levering als SUPERSEDED gemarkeerd: {delivery_id}")