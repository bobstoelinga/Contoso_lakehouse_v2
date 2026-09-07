# Databricks notebook source
# MAGIC %md
# MAGIC # 90 — Policy-driven onderhoud
# MAGIC Resolveert onderhoudsbeleid uit metadata, schrijft eerst een auditeerbaar
# MAGIC plan en voert alleen verschuldigde acties uit.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])

# COMMAND ----------

import sys
import uuid

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import Settings
from contoso_lakehouse.sqlutil import safe_identifier, sql_string

settings = Settings(env=dbutils.widgets.get("env"))
dry_run = dbutils.widgets.get("dry_run") == "true"
audit = f"{settings.meta_catalog}.audit"
maintenance_run_id = str(uuid.uuid4())

spark.sql(f"""
INSERT INTO {audit}.audit_maintenance_run VALUES (
    {sql_string(maintenance_run_id)}, {str(dry_run).lower()}, current_timestamp(), NULL, 'RUNNING')
""")

policy_rows = spark.sql(f"""
SELECT * FROM {settings.meta_catalog}.metadata.meta_table_maintenance_policy
WHERE is_active
ORDER BY priority, catalog_name, schema_name, table_name
""").collect()
if not policy_rows:
        raise ValueError("Geen actieve onderhoudspolicies geconfigureerd.")

# COMMAND ----------

def resolved_policy(catalog: str, schema: str, table: str):
    candidates = []
    for policy in policy_rows:
        if settings.resolve(policy.catalog_name) != catalog:
            continue
        if policy.schema_name and policy.schema_name != schema:
            continue
        if policy.table_name and policy.table_name != table:
            continue
        specificity = int(policy.schema_name is not None) + int(policy.table_name is not None)
        candidates.append((policy.priority, -specificity, policy))
    return min(candidates, default=None, key=lambda item: (item[0], item[1]))[2] if candidates else None


def log_action(table_fqn, policy, action_type, status, reason, detail, error=None):
    completed_at = "current_timestamp()" if status in ("EXECUTED", "FAILED") else "NULL"
    spark.sql(f"""
    INSERT INTO {audit}.audit_maintenance_action VALUES (
      {sql_string(str(uuid.uuid4()))}, {sql_string(maintenance_run_id)}, {sql_string(table_fqn)},
      {sql_string(policy.policy_id)}, {sql_string(action_type)}, {sql_string(status)},
      {sql_string(reason)}, {detail.numFiles}, {detail.sizeInBytes}, {sql_string(error)},
      current_timestamp(), {completed_at})
    """)


try:
    catalogs = sorted({settings.resolve(policy.catalog_name) for policy in policy_rows})
    for catalog in catalogs:
        schemas = spark.sql(
            f"SELECT schema_name FROM {safe_identifier(catalog)}.information_schema.schemata "
            "WHERE schema_name <> 'information_schema'"
        ).collect()
        for schema_row in schemas:
            schema = schema_row.schema_name
            tables = spark.sql(f"""
                SELECT table_name FROM {safe_identifier(catalog)}.information_schema.tables
                WHERE table_schema = {sql_string(schema)} AND table_type = 'MANAGED'
            """).collect()
            for table_row in tables:
                table = table_row.table_name
                policy = resolved_policy(catalog, schema, table)
                if policy is None:
                    continue
                table_fqn = f"{catalog}.{schema}.{table}"
                detail = spark.sql(f"DESCRIBE DETAIL {safe_identifier(table_fqn)}").first()
                if policy.optimize_mode == "DISABLED":
                    log_action(table_fqn, policy, "OPTIMIZE", "SKIPPED", "Policy disabled", detail)
                    continue
                recent = spark.sql(f"""
                    SELECT count(*) AS n FROM {audit}.audit_maintenance_action
                    WHERE table_fqn = {sql_string(table_fqn)} AND action_type = 'OPTIMIZE'
                      AND action_status = 'EXECUTED'
                      AND completed_at >= current_timestamp() - INTERVAL {policy.optimize_interval_hours} HOURS
                """).first().n
                if detail.numFiles < policy.min_files_before_optimize or recent:
                    log_action(table_fqn, policy, "OPTIMIZE", "SKIPPED", "Not due", detail)
                    continue
                log_action(table_fqn, policy, "OPTIMIZE", "PLANNED", "Policy due", detail)
                log_action(table_fqn, policy, "VACUUM", "PLANNED", "Policy due", detail)
                if dry_run:
                    continue
                try:
                    spark.sql(f"OPTIMIZE {safe_identifier(table_fqn)}")
                    log_action(table_fqn, policy, "OPTIMIZE", "EXECUTED", "Policy due", detail)
                    spark.sql(f"VACUUM {safe_identifier(table_fqn)} RETAIN {policy.vacuum_retain_hours} HOURS")
                    log_action(table_fqn, policy, "VACUUM", "EXECUTED", "Policy due", detail)
                except Exception as exc:
                    log_action(table_fqn, policy, "OPTIMIZE", "FAILED", "Execution failed", detail, str(exc)[:4000])
                    raise
except Exception:
    spark.sql(f"""
    UPDATE {audit}.audit_maintenance_run
    SET run_status = 'FAILED', ended_at = current_timestamp()
    WHERE maintenance_run_id = {sql_string(maintenance_run_id)}
    """)
    raise
else:
    spark.sql(f"""
    UPDATE {audit}.audit_maintenance_run
    SET run_status = 'SUCCESS', ended_at = current_timestamp()
    WHERE maintenance_run_id = {sql_string(maintenance_run_id)}
    """)

# COMMAND ----------

display(
    spark.sql(
        f"""
                SELECT table_fqn, policy_id, action_type, action_status, reason, num_files, size_in_bytes, error_message
                FROM {audit}.audit_maintenance_action
                WHERE maintenance_run_id = {sql_string(maintenance_run_id)}
                ORDER BY table_fqn, action_type, created_at
        """
    )
)
