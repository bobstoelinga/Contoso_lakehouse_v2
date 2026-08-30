# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup Lakehouse
# MAGIC Eenmalige/idempotente opbouw van Unity Catalog, de metadatatabellen en alle
# MAGIC Delta tabellen. Voert de SQL scripts in volgorde uit en laadt daarna de
# MAGIC metadata seed.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("storage_account", "contosolake")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

env = dbutils.widgets.get("env")
storage_account = dbutils.widgets.get("storage_account")
repo_root = dbutils.widgets.get("repo_root")

# COMMAND ----------

import sys
from pathlib import Path

sys.path.insert(0, f"{repo_root}/src")

from contoso_lakehouse.context import Settings
from contoso_lakehouse.seed import load_seed

settings = Settings(env=env)

# COMMAND ----------

# MAGIC %md ## 1. DDL uitvoeren
# MAGIC Elk script wordt op statement-niveau uitgevoerd met parametersubstitutie.

# COMMAND ----------

SCRIPTS = [
    "sql/00_unity_catalog/00_catalogs_schemas_volumes.sql",
    "sql/00_unity_catalog/01_grants.sql",
    "sql/01_metadata/10_metadata_model.sql",
    "sql/01_metadata/11_audit_model.sql",
    "sql/02_bronze/20_bronze_tables.sql",
    "sql/03_quality_reject/30_quality_tables.sql",
    "sql/03_quality_reject/31_reject_tables.sql",
    "sql/04_data_vault/40_raw_vault.sql",
    "sql/04_data_vault/41_business_vault.sql",
    "sql/05_gold/50_gold_historical.sql",
    "sql/05_gold/51_gold_current.sql",
]

PARAMS = {"${env}": env, "${storage_account}": storage_account}


def run_script(relative_path: str) -> None:
    text = Path(f"{repo_root}/{relative_path}").read_text(encoding="utf-8")
    for placeholder, value in PARAMS.items():
        text = text.replace(placeholder, value)
    for statement in (s.strip() for s in text.split(";")):
        if not statement or statement.startswith("--"):
            continue
        spark.sql(statement)


for script in SCRIPTS:
    print(f"-> {script}")
    run_script(script)

# COMMAND ----------

# MAGIC %md ## 2. Metadata seed laden

# COMMAND ----------

counts = load_seed(spark, settings, f"{repo_root}/metadata/seed")
display(spark.createDataFrame(list(counts.items()), "table string, records int"))

# COMMAND ----------

# MAGIC %md ## 3. Metadata valideren
# MAGIC Faalt bij cycli, wees-verwijzingen of ongeldige expressies.

# COMMAND ----------

dbutils.notebook.run("99_validate_metadata", 600, {"env": env, "repo_root": repo_root})
