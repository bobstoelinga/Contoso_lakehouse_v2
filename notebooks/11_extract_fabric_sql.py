# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Fabric SQL extract
# MAGIC Publiceert een volledige JDBC-snapshot atomisch naar immutable Landing.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_object_id", "FABRIC_SALES.ORDER_LINES")
dbutils.widgets.text("delivery_date", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import json
import sys
from datetime import datetime, timezone

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext, Settings

settings = Settings(env=dbutils.widgets.get("env"))
source_object_id = dbutils.widgets.get("source_object_id")
delivery_date = dbutils.widgets.get("delivery_date") or datetime.now(timezone.utc).date().isoformat()

config = spark.sql(f"""
SELECT s.source_system_id, sys.landing_volume_path, c.endpoint_url, c.request_options
FROM {settings.meta_catalog}.metadata.meta_source_object s
JOIN {settings.meta_catalog}.metadata.meta_source_connector c USING (source_object_id)
JOIN {settings.meta_catalog}.metadata.meta_source_system sys USING (source_system_id)
WHERE s.source_object_id = '{source_object_id}'
  AND c.connector_type = 'JDBC' AND s.is_active AND c.is_active
""").collect()
if len(config) != 1:
    raise ValueError(f"Geen actieve JDBC-configuratie voor {source_object_id}.")

row = config[0]
options = row.request_options or {}
source_query = options.get("source_query")
secret_scope = options.get("secret_scope", "fabric-extract")
if not source_query:
    raise ValueError("JDBC request_options.source_query ontbreekt.")

landing_root = settings.resolve(row.landing_volume_path)
object_folder = source_object_id.rsplit(".", 1)[-1].lower()
target = f"{landing_root}/{delivery_date}/{object_folder}"
staging = f"{landing_root}/_staging/{source_object_id.replace('.', '_').lower()}_{delivery_date}"
delivery_id = f"{row.source_system_id}|{delivery_date}"
ctx = RunContext.create(settings=settings, delivery_id=delivery_id)
audit = AuditLogger(spark, ctx)

with audit.run("LANDING", source_object_id) as stats:
    try:
        try:
            files = {file.name for file in dbutils.fs.ls(target)}
        except Exception:
            files = set()
        if "_manifest.json" in files:
            dbutils.notebook.exit("EXISTS")
        dbutils.fs.rm(staging, True)
        frame = (spark.read.format("jdbc").option("url", row.endpoint_url)
            .option("query", source_query)
            .option(
                "user",
                f"{dbutils.secrets.get(secret_scope, 'client-id')}@"
                f"{dbutils.secrets.get(secret_scope, 'tenant-id')}",
            )
            .option("password", dbutils.secrets.get(secret_scope, "client-secret"))
            .option("tenantId", dbutils.secrets.get(secret_scope, "tenant-id"))
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())
        rows_written = frame.count()
        frame.write.mode("overwrite").parquet(staging)
        dbutils.fs.put(f"{staging}/_manifest.json", json.dumps({"source_object_id": source_object_id, "delivery_id": delivery_id, "rows_written": rows_written, "extracted_at_utc": datetime.now(timezone.utc).isoformat()}, sort_keys=True), True)
        dbutils.fs.mv(staging, target, True)
        stats["rows_read"] = rows_written
        stats["rows_inserted"] = rows_written
    finally:
        dbutils.fs.rm(staging, True)