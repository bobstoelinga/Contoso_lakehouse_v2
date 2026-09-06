# Databricks notebook source
# MAGIC %md
# MAGIC # 98 - Fabric SQL JDBC connectivity test
# MAGIC Verifieert uitsluitend de technische verbinding met de contractview.

# COMMAND ----------

dbutils.widgets.text(
    "host",
    "qugtl6ion76ufa4fuwoc6cpmdi-rqr26cxjvolujgkdq6tknur6xu.database.fabric.microsoft.com",
)
dbutils.widgets.text("database", "Contoso_database-a2e53891-642a-4a0c-9a7f-c72e1351ff57")
dbutils.widgets.text("source_view", "SalesLT.vw_databricks_sales_order_line")
dbutils.widgets.text("secret_scope", "fabric-extract")

# COMMAND ----------

host = dbutils.widgets.get("host")
database = dbutils.widgets.get("database")
source_view = dbutils.widgets.get("source_view")
secret_scope = dbutils.widgets.get("secret_scope")

client_id = dbutils.secrets.get(secret_scope, "client-id")
client_secret = dbutils.secrets.get(secret_scope, "client-secret")
tenant_id = dbutils.secrets.get(secret_scope, "tenant-id")
service_principal_user = f"{client_id}@{tenant_id}"

jdbc_url = (
    f"jdbc:sqlserver://{host}:1433;databaseName={database};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;authentication=ActiveDirectoryServicePrincipal"
)

row_count = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("query", f"SELECT COUNT(*) AS row_count FROM {source_view}")
    .option("user", service_principal_user)
    .option("password", client_secret)
    .option("tenantId", tenant_id)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
    .collect()[0]
    .row_count
)

result = f"Fabric JDBC connection succeeded: {source_view} contains {row_count} rows."
print(result)
dbutils.notebook.exit(result)