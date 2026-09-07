# Databricks notebook source
# MAGIC %md
# MAGIC # 42 - Monitor actieve bronnen tot Gold
# MAGIC Controleert of iedere actieve bron met actieve Gold Actueel-entiteiten een
# MAGIC actieve Gold-publicatie heeft met rijen. De taak faalt hard bij ontbrekende
# MAGIC of lege publicaties.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("expected_delivery_date", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import Settings

settings = Settings(env=dbutils.widgets.get("env"))
expected_delivery_date = dbutils.widgets.get("expected_delivery_date").strip()
expected_delivery_filter = ""
if expected_delivery_date:
    expected_delivery_filter = (
        "AND p.delivery_id = concat(expected.source_system_id, '|', "
        f"'{expected_delivery_date}')"
    )

# COMMAND ----------

gold_status = spark.sql(f"""
WITH expected AS (
  SELECT DISTINCT
    s.source_system_id,
    ge.publication_group_id,
    ge.gold_entity_id
  FROM {settings.meta_catalog}.metadata.meta_source_system s
  JOIN {settings.meta_catalog}.metadata.meta_gold_entity ge
    ON coalesce(ge.source_system_id, 'SALES') = s.source_system_id
  WHERE s.is_active
    AND ge.is_active
    AND ge.gold_layer = 'CURRENT'
    AND ge.publication_group_id IS NOT NULL
), active_publications AS (
  SELECT
    expected.source_system_id,
    expected.publication_group_id,
    expected.gold_entity_id,
    p.delivery_id,
    p.batch_id,
    p.physical_slot,
    p.row_count,
    p.published_at,
    CASE
      WHEN p.gold_entity_id IS NULL THEN 'MISSING_PUBLICATION'
      WHEN coalesce(p.row_count, 0) = 0 THEN 'EMPTY_PUBLICATION'
      ELSE 'OK'
    END AS gold_status
  FROM expected
  LEFT JOIN {settings.meta_catalog}.audit.v_active_gold_publication p
    ON p.gold_entity_id = expected.gold_entity_id
   {expected_delivery_filter}
)
SELECT *
FROM active_publications
ORDER BY source_system_id, publication_group_id, gold_entity_id
""")

display(gold_status)

# COMMAND ----------

failures = gold_status.where("gold_status <> 'OK'")
failure_count = failures.count()
if failure_count:
    display(failures)
    details = [row.asDict() for row in failures.collect()]
    raise RuntimeError(f"Gold-monitoring faalde voor {failure_count} actieve publicaties: {details}")

summary = gold_status.groupBy("source_system_id").count().orderBy("source_system_id")
display(summary)
print("Alle actieve bronnen met Gold Actueel-entiteiten hebben een actieve, niet-lege Gold-publicatie.")