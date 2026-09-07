# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Registreer delivery-manifests
# MAGIC Leest gesloten rootmanifests uit Landing en registreert ze centraal in
# MAGIC audit. Push-bronnen hoeven daardoor geen Databricks API aan te roepen.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_system_id", "SALES")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import json
import re
import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext, Settings

settings = Settings(env=dbutils.widgets.get("env"))
source_system_id = dbutils.widgets.get("source_system_id")
config = spark.sql(f"""
SELECT landing_volume_path
FROM {settings.meta_catalog}.metadata.meta_source_system
WHERE source_system_id = '{source_system_id}' AND is_active
""").collect()
if len(config) != 1:
    raise ValueError(f"Geen actief bronsysteem gevonden: {source_system_id}.")

landing_root = settings.resolve(config[0].landing_volume_path)
expected_objects = spark.sql(f"""
SELECT count(*) AS n
FROM {settings.meta_catalog}.metadata.meta_source_object
WHERE source_system_id = '{source_system_id}' AND is_active AND is_mandatory_in_delivery
""").first().n
requires_complete_snapshot = spark.sql(f"""
SELECT exists(
  SELECT 1 FROM {settings.meta_catalog}.metadata.meta_source_object
  WHERE source_system_id = '{source_system_id}' AND is_active AND absence_means_delete
) AS required
""").first().required

for item in dbutils.fs.ls(landing_root):
    delivery_date = item.name.rstrip("/")
    if not item.isDir() or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", delivery_date):
        continue
    manifest_path = f"{landing_root}/{delivery_date}/_manifest.json"
    try:
        manifest = json.loads(dbutils.fs.head(manifest_path))
    except Exception:
        continue

    delivery_id = f"{source_system_id}|{delivery_date}"
    if manifest.get("delivery_id") != delivery_id or manifest.get("status") != "CLOSED":
        raise ValueError(f"Ongeldig manifest voor {delivery_id}: delivery_id en status CLOSED zijn verplicht.")
    snapshot_complete = bool(manifest.get("is_snapshot_complete", False))
    if requires_complete_snapshot and not snapshot_complete:
        raise ValueError(f"{delivery_id}: is_snapshot_complete is verplicht voor snapshot-deletes.")
    file_count = int(manifest.get("file_count", expected_objects))
    if file_count < expected_objects:
        raise ValueError(f"{delivery_id}: file_count is kleiner dan het verplichte objectaantal.")

    audit = AuditLogger(spark, RunContext.create(settings, delivery_id=delivery_id))
    audit.close_delivery_manifest(
        delivery_id, source_system_id, manifest_path, expected_objects, file_count,
        snapshot_complete, source_watermark=manifest.get("source_watermark"),
    )
    print(f"Manifest geregistreerd: {delivery_id}")