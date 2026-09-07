# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Public API extract
# MAGIC Leest de connectorconfiguratie uit metadata en publiceert pas na een
# MAGIC volledige extractie een immutable delivery naar het landing volume.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_object_id", "")
dbutils.widgets.text("delivery_date", "")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

import json
import sys
from datetime import datetime, timezone
from functools import partial

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.cbs import with_odata_page_size
from contoso_lakehouse.connector import csv_rows, fetch_json, fetch_text, iter_json_pages, without_null_only_fields
from contoso_lakehouse.context import RunContext, Settings

settings = Settings(env=dbutils.widgets.get("env"))
source_object_id = dbutils.widgets.get("source_object_id")
delivery_date = dbutils.widgets.get("delivery_date") or datetime.now(timezone.utc).date().isoformat()

config = spark.sql(f"""
SELECT s.source_system_id, s.load_strategy, sys.landing_volume_path, c.connector_type, c.endpoint_url,
       c.records_key, c.next_link_key, c.request_options
FROM {settings.meta_catalog}.metadata.meta_source_object s
JOIN {settings.meta_catalog}.metadata.meta_source_connector c
  ON c.source_object_id = s.source_object_id
JOIN {settings.meta_catalog}.metadata.meta_source_system sys
    ON sys.source_system_id = s.source_system_id
WHERE s.source_object_id = '{source_object_id}' AND s.is_active AND c.is_active
""").collect()
if len(config) != 1:
    raise ValueError(f"Geen actieve connectorconfiguratie voor {source_object_id}.")

row = config[0]
expected_objects = spark.sql(f"""
SELECT count(*) AS n FROM {settings.meta_catalog}.metadata.meta_source_object
WHERE source_system_id = '{row.source_system_id}' AND is_active AND is_mandatory_in_delivery
""").first().n
landing_root = settings.resolve(row.landing_volume_path)
object_folder = source_object_id.rsplit(".", 1)[-1].lower()
target = f"{landing_root}/{delivery_date}/{object_folder}"
staging = f"{landing_root}/_staging/{source_object_id.replace('.', '_').lower()}_{delivery_date}"
delivery_id = f"{row.source_system_id}|{delivery_date}"
ctx = RunContext.create(settings=settings, delivery_id=delivery_id)
audit = AuditLogger(spark, ctx)

request_options = row.request_options or {}


def request_option(name, default, converter, minimum):
    value = request_options.get(name, default)
    try:
        value = converter(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"request_options.{name} is ongeldig: {value!r}") from exc
    if value < minimum:
        raise ValueError(f"request_options.{name} moet minimaal {minimum} zijn.")
    return value


timeout_seconds = request_option("timeout_seconds", 60, int, 1)
max_retries = request_option("max_retries", 2, int, 0)
retry_delay_seconds = request_option("retry_delay_seconds", 1.0, float, 0.0)
total_timeout_seconds = request_option("total_timeout_seconds", 900, int, 1)
fetch_json_with_policy = partial(
    fetch_json,
    timeout_seconds=timeout_seconds,
    max_retries=max_retries,
    retry_delay_seconds=retry_delay_seconds,
    total_timeout_seconds=total_timeout_seconds,
)
fetch_text_with_policy = partial(
    fetch_text,
    timeout_seconds=timeout_seconds,
    max_retries=max_retries,
    retry_delay_seconds=retry_delay_seconds,
    total_timeout_seconds=total_timeout_seconds,
)

with audit.run("LANDING", source_object_id) as stats:
    try:
        try:
            existing_files = {file.name for file in dbutils.fs.ls(target)}
        except Exception as exc:
            if "FileNotFoundException" not in str(exc):
                raise
        else:
            if "_manifest.json" not in existing_files:
                raise ValueError(f"Bestaande delivery is onvolledig: {target}")
            print(f"{source_object_id}: delivery bestaat al en wordt niet opnieuw gepubliceerd.")
            dbutils.notebook.exit("EXISTS")

        dbutils.fs.rm(staging, True)
        rows_written = 0
        if row.connector_type == "HTTP_JSON":
            records_key = row.records_key if row.records_key is not None else "value"
            next_link_key = row.next_link_key if row.next_link_key is not None else "@odata.nextLink"
            endpoint = (
                with_odata_page_size(row.endpoint_url)
                if row.source_system_id == "CBS"
                else row.endpoint_url
            )
            for page in iter_json_pages(
                endpoint,
                fetch=fetch_json_with_policy,
                records_key=records_key,
                next_link_key=next_link_key,
            ):
                if page:
                    spark.createDataFrame(without_null_only_fields(page)).write.mode("append").json(staging)
                    rows_written += len(page)
        elif row.connector_type == "HTTP_CSV":
            records = csv_rows(fetch_text_with_policy(row.endpoint_url))
            if records:
                spark.createDataFrame(records).write.mode("overwrite").option("header", "true").csv(staging)
                rows_written = len(records)
        else:
            raise ValueError(f"Niet-ondersteund connector_type: {row.connector_type}")

        dbutils.fs.put(f"{staging}/_manifest.json", json.dumps({
            "source_object_id": source_object_id,
            "source_system_id": row.source_system_id,
            "delivery_id": delivery_id,
            "endpoint_url": row.endpoint_url,
            "rows_written": rows_written,
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        }, sort_keys=True), True)
        dbutils.fs.mv(staging, target, True)
        audit.close_delivery_manifest(
            delivery_id, row.source_system_id, f"{target}/_manifest.json", expected_objects,
            file_count=1, snapshot_complete=row.load_strategy == "SNAPSHOT_SCD2",
        )
        stats["rows_read"] = rows_written
        stats["rows_inserted"] = rows_written
        print(f"{source_object_id}: {rows_written} records gepubliceerd naar {target}")
    finally:
        # Een fout mag geen deel-extract achterlaten dat later alsnog gepubliceerd wordt.
        dbutils.fs.rm(staging, True)