# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Metadata validatie
# MAGIC Draait vóór elke deploy en als eerste taak van de pipeline. Vangt de meest
# MAGIC voorkomende oorzaak van productie-incidenten af bij honderden tabellen:
# MAGIC ongeldige metadata.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")
dbutils.widgets.dropdown("validation_mode", "FULL", ["FULL", "PREFLIGHT"])

# COMMAND ----------

import sys

sys.path.insert(0, f"{dbutils.widgets.get('repo_root')}/src")

from contoso_lakehouse.context import RunContext, Settings
from contoso_lakehouse.metadata import MetadataRepository
from contoso_lakehouse.orchestration import Orchestrator
from contoso_lakehouse.validation import MetadataValidator

settings = Settings(env=dbutils.widgets.get("env"))
ctx = RunContext.create(settings)
repo = MetadataRepository(spark, settings)
validator = MetadataValidator(spark, repo, settings)
validation_mode = dbutils.widgets.get("validation_mode")
metadata_version = spark.sql(
    f"SELECT metadata_version FROM {settings.meta_catalog}.audit.audit_metadata_version "
    "ORDER BY deployed_at DESC LIMIT 1"
).first().metadata_version

if validation_mode == "PREFLIGHT":
    validated = spark.sql(
        f"""
        SELECT count(*) AS n
        FROM {settings.meta_catalog}.audit.audit_metadata_validation
        WHERE metadata_version = '{metadata_version}'
          AND validation_status = 'SUCCESS'
        """
    ).first().n
    if not validated:
        raise ValueError(
            f"Geen geslaagde volledige metadata-preflight voor release {metadata_version}."
        )
    print(f"Volledige metadata-preflight aanwezig voor {metadata_version}.")
    dbutils.notebook.exit("PREFLIGHT_OK")

# COMMAND ----------

# MAGIC %md ## 1. Graaf: cycli en wees-verwijzingen

# COMMAND ----------

Orchestrator(spark, repo, ctx).validate_graph()
print("Graaf OK")

# COMMAND ----------

# MAGIC %md ## 2. Expressies compileren
# MAGIC Elke DQ-regel, mapping-expressie en Gold-SELECT wordt met EXPLAIN getest.

# COMMAND ----------

issues = validator.validate_all()
if issues:
    for issue in issues:
        print(f"[{issue.category}] {issue.entity}: {issue.message}")
    raise ValueError(f"{len(issues)} metadata-problemen gevonden.")
spark.sql(
        f"""
        INSERT INTO {settings.meta_catalog}.audit.audit_metadata_validation VALUES (
            '{metadata_version}', 'SUCCESS', current_timestamp(),
            'metadata-validator-v1', '{ctx.job_run_id}')
        """
)
print("Alle expressies compileren.")
