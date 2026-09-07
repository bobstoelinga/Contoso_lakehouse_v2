# Databricks notebook source
# MAGIC %md
# MAGIC # 43 - Operationele SLO-monitor
# MAGIC Faalt wanneer control-plane signalen actie vereisen: manifest-SLA's,
# MAGIC dead letters, verlopen work-itemleases of verlopen Gold-leases.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import Settings

settings = Settings(env=dbutils.widgets.get("env"))
audit = f"{settings.meta_catalog}.audit"

# COMMAND ----------

breaches = spark.sql(f"""
WITH source_sla AS (
  SELECT source_system_id, min(freshness_sla_hours) AS freshness_sla_hours
  FROM {settings.meta_catalog}.metadata.meta_source_object
  WHERE is_active
  GROUP BY source_system_id
), manifest_breaches AS (
  SELECT 'MANIFEST_SLA' AS breach_type, d.delivery_id,
         concat('Manifest is niet CLOSED binnen ', sla.freshness_sla_hours, ' uur') AS detail
  FROM {audit}.audit_delivery d
  JOIN source_sla sla USING (source_system_id)
  LEFT JOIN {audit}.audit_delivery_manifest m USING (delivery_id)
  WHERE d.delivery_status NOT IN ('QUARANTINED', 'SUPERSEDED')
    AND coalesce(m.manifest_status, 'OPEN') <> 'CLOSED'
    AND d.first_seen_at < current_timestamp() - make_interval(0, 0, 0, 0, sla.freshness_sla_hours)
), delivery_breaches AS (
  SELECT 'DELIVERY_SLA' AS breach_type, d.delivery_id,
         concat('Delivery niet gepubliceerd binnen ', sla.freshness_sla_hours, ' uur') AS detail
  FROM {audit}.audit_delivery d
  JOIN source_sla sla USING (source_system_id)
  LEFT ANTI JOIN {audit}.audit_gold_publication_group g
    ON g.delivery_id = d.delivery_id AND g.release_status = 'ACTIVE'
  WHERE d.delivery_status = 'COMPLETE'
    AND d.completed_at < current_timestamp() - make_interval(0, 0, 0, 0, sla.freshness_sla_hours)
), work_item_breaches AS (
  SELECT CASE WHEN work_status = 'DEAD_LETTER' THEN 'DEAD_LETTER' ELSE 'EXPIRED_WORK_LEASE' END AS breach_type,
         delivery_id, concat(layer, '.', entity_id, ': ', coalesce(last_error, 'geen foutmelding')) AS detail
  FROM {audit}.audit_work_item
  WHERE work_status = 'DEAD_LETTER'
     OR (work_status = 'RUNNING' AND lease_expires_at < current_timestamp())
), gold_lease_breaches AS (
  SELECT 'EXPIRED_GOLD_LEASE' AS breach_type, NULL AS delivery_id,
         concat('Publicatiegroep ', publication_group_id, ' heeft een verlopen lease') AS detail
  FROM {audit}.audit_gold_publication_lease
  WHERE released_at IS NULL AND expires_at < current_timestamp()
)
SELECT * FROM manifest_breaches
UNION ALL SELECT * FROM delivery_breaches
UNION ALL SELECT * FROM work_item_breaches
UNION ALL SELECT * FROM gold_lease_breaches
ORDER BY breach_type, delivery_id
""")

display(breaches)
count = breaches.count()
if count:
    raise RuntimeError(f"Operationele SLO-monitor vond {count} blokkades: {[row.asDict() for row in breaches.collect()]}")
print("Operationele SLO-monitor: geen blokkades.")