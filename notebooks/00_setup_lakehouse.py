# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup Lakehouse
# MAGIC Eenmalige/idempotente opbouw van Unity Catalog, de metadatatabellen en alle
# MAGIC Delta tabellen. Voert de SQL scripts in volgorde uit en laadt daarna de
# MAGIC metadata seed.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("storage_account", "contosolake")
dbutils.widgets.text("landing_path", "sales")
dbutils.widgets.text("service_principal", "svc_contoso_etl")
dbutils.widgets.text("data_engineers_group", "grp_data_engineers")
dbutils.widgets.text("bi_analysts_group", "grp_bi_analysts")
dbutils.widgets.text("data_stewards_group", "grp_data_stewards")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

env = dbutils.widgets.get("env")
storage_account = dbutils.widgets.get("storage_account")
landing_path = dbutils.widgets.get("landing_path")
service_principal = dbutils.widgets.get("service_principal")
data_engineers_group = dbutils.widgets.get("data_engineers_group")
bi_analysts_group = dbutils.widgets.get("bi_analysts_group")
data_stewards_group = dbutils.widgets.get("data_stewards_group")
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
    "sql/01_metadata/12_metadata_migrations.sql",
    "sql/02_bronze/20_bronze_tables.sql",
    "sql/03_quality_reject/30_quality_tables.sql",
    "sql/03_quality_reject/31_reject_tables.sql",
    "sql/04_data_vault/40_raw_vault.sql",
    "sql/04_data_vault/41_business_vault.sql",
    "sql/05_gold/50_gold_historical.sql",
    "sql/05_gold/51_gold_current.sql",
    "sql/00_unity_catalog/02_gold_consumer_grants.sql",
]

PARAMS = {
    "${env}": env,
    "${storage_account}": storage_account,
    "${landing_path}": landing_path,
    "${service_principal}": service_principal,
    "${data_engineers_group}": data_engineers_group,
    "${bi_analysts_group}": bi_analysts_group,
    "${data_stewards_group}": data_stewards_group,
}


def split_sql_statements(text: str) -> list[str]:
    statements: list[str] = []
    statement: list[str] = []
    in_string = False
    position = 0

    while position < len(text):
        character = text[position]
        next_character = text[position + 1] if position + 1 < len(text) else ""

        if not in_string and character == "-" and next_character == "-":
            position = text.find("\n", position)
            if position < 0:
                break
            statement.append("\n")
        elif character == "'":
            statement.append(character)
            if in_string and next_character == "'":
                statement.append(next_character)
                position += 1
            else:
                in_string = not in_string
        elif character == ";" and not in_string:
            sql = "".join(statement).strip()
            if sql:
                statements.append(sql)
            statement = []
        else:
            statement.append(character)
        position += 1

    sql = "".join(statement).strip()
    if sql:
        statements.append(sql)
    return statements


def run_script(relative_path: str) -> None:
    text = Path(f"{repo_root}/{relative_path}").read_text(encoding="utf-8")
    for placeholder, value in PARAMS.items():
        text = text.replace(placeholder, value)
    for statement in split_sql_statements(text):
        spark.sql(statement)


for script in SCRIPTS:
    print(f"-> {script}")
    run_script(script)

# COMMAND ----------

# MAGIC %md ## 2. Schema-aware metadata-migraties

# COMMAND ----------

MIGRATIONS = {
    "metadata.meta_source_object": {
        "schema_drift_policy": "STRING",
        "owner_team": "STRING",
        "criticality": "STRING",
    },
    "metadata.meta_dependency": {
        "priority": "INT",
        "retry_policy": "STRING",
        "max_retries": "INT",
    },
    "metadata.meta_quality_rule": {
        "is_blocking": "BOOLEAN",
        "rule_group": "STRING",
    },
    "metadata.meta_gold_entity": {
        "publish_status": "STRING",
        "pointer_table": "STRING",
        "staging_table": "STRING",
    },
    "audit.audit_load_run": {"metadata_version": "STRING"},
    "audit.audit_delivery": {
        "superseded_at": "TIMESTAMP",
        "superseded_by": "STRING",
        "supersede_reason": "STRING",
        "supersede_approval_reference": "STRING",
        "quarantined_at": "TIMESTAMP",
        "quarantine_reason": "STRING",
        "released_at": "TIMESTAMP",
        "released_by": "STRING",
        "release_reason": "STRING",
        "release_approval_reference": "STRING",
    },
}

GOLD_MIGRATIONS = {
    "historical.fct_sales_hist": {
        "employee_hk": "STRING",
        "order_date_key": "INT",
        "ship_date_key": "INT",
        "delivery_date_key": "INT",
    },
    "historical.fct_returns_hist": {
        "employee_hk": "STRING",
        "return_date_key": "INT",
    },
}


def apply_migrations() -> None:
    for table, columns in MIGRATIONS.items():
        fqn = f"{settings.meta_catalog}.{table}"
        existing = {row.col_name.lower() for row in spark.sql(f"DESCRIBE {fqn}").collect()}
        missing = [f"{name} {data_type}" for name, data_type in columns.items() if name not in existing]
        if missing:
            spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({', '.join(missing)})")
            print(f"Gemigreerd: {fqn}: {', '.join(missing)}")

    for table, columns in GOLD_MIGRATIONS.items():
        fqn = f"{settings.gold_catalog}.{table}"
        existing = {row.col_name.lower() for row in spark.sql(f"DESCRIBE {fqn}").collect()}
        missing = [f"{name} {data_type}" for name, data_type in columns.items() if name not in existing]
        if missing:
            spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({', '.join(missing)})")
            print(f"Gemigreerd: {fqn}: {', '.join(missing)}")


apply_migrations()

# COMMAND ----------

# MAGIC %md ## 3. Metadata seed laden

# COMMAND ----------

counts = load_seed(spark, settings, f"{repo_root}/metadata/seed")
display(spark.createDataFrame(list(counts.items()), "table string, records int"))

# COMMAND ----------

# MAGIC %md ## 4. Metadata valideren
# MAGIC Faalt bij cycli, wees-verwijzingen of ongeldige expressies.

# COMMAND ----------

dbutils.notebook.run("99_validate_metadata", 600, {"env": env, "repo_root": repo_root})
