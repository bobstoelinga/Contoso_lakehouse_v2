# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Metadata validatie
# MAGIC Draait vóór elke deploy en als eerste taak van de pipeline. Vangt de meest
# MAGIC voorkomende oorzaak van productie-incidenten af bij honderden tabellen:
# MAGIC ongeldige metadata.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

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
print("Alle expressies compileren.")
