# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Genereer demo-levering
# MAGIC Schrijft een kleine, consistente Sales-levering naar de landingzone voor
# MAGIC end-to-end validatie. Een bestaande datumfolder wordt nooit overschreven.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("delivery_date", "2026-08-31")
dbutils.widgets.text("repo_root", "/Workspace/Repos/contoso/Contoso_lakehouse_v2")

# COMMAND ----------

from datetime import date, datetime

from pyspark.sql import types as T

delivery_date = dbutils.widgets.get("delivery_date")
delivery_day = datetime.strptime(delivery_date, "%Y-%m-%d").date()
business_date = min(delivery_day, date.today()).isoformat()
landing_path = f"/Volumes/raw_{dbutils.widgets.get('env')}/sales/landing/{delivery_date}"

try:
    dbutils.fs.ls(landing_path)
    raise ValueError(f"Demo-levering bestaat al en wordt niet overschreven: {landing_path}")
except Exception as exc:
    if "FileNotFoundException" not in str(exc) and "does not exist" not in str(exc):
        raise

customers = [
    ("C-1001", "Contoso Retail BV", "sales@contoso.example", "+31201234567", "Damrak 1", "Amsterdam", "NH", "1012LG", "NL", "RETAIL", "2021-04-12", False),
    ("C-1002", "Northwind Traders", "orders@northwind.example", "+31701234567", "Hoofdstraat 2", "Den Haag", "ZH", "2511AA", "NL", "WHOLESALE", "2018-10-01", False),
]
products = [
    ("P-2001", "Laptop Pro 14", "ELECTRONICS", "LAPTOPS", "CONTOSO", 800.00, 1200.00, False, False),
    ("P-2002", "Wireless Mouse", "ELECTRONICS", "ACCESSORIES", "CONTOSO", 15.00, 35.00, False, False),
]
employees = [
    ("E-5001", "Avery", "Jansen", "Sales Representative", "Amsterdam", "2020-01-15", False),
    ("E-5002", "Robin", "de Vries", "Returns Specialist", "Den Haag", "2022-06-01", False),
]
orders = [
    ("O-3001", 1, "C-1001", "P-2001", "E-5001", business_date, business_date, business_date, "SHIPPED", 1, 1200.00, 0.00, 1200.00, "EUR"),
    ("O-3002", 1, "C-1002", "P-2002", "E-5001", business_date, None, None, "OPEN", 2, 35.00, 5.00, 65.00, "EUR"),
]
returns = [
    ("R-4001", "O-3001", 1, "P-2001", "E-5002", business_date, "RECEIVED", "DAMAGED", 1, 1200.00, "EUR"),
]

customer_schema = T.StructType([
    T.StructField("customer_key", T.StringType(), False), T.StructField("customer_name", T.StringType(), False),
    T.StructField("email", T.StringType(), True), T.StructField("phone", T.StringType(), True),
    T.StructField("address_line1", T.StringType(), True), T.StructField("city", T.StringType(), True),
    T.StructField("state_province", T.StringType(), True), T.StructField("postal_code", T.StringType(), True),
    T.StructField("country", T.StringType(), True), T.StructField("customer_segment", T.StringType(), True),
    T.StructField("customer_since", T.StringType(), True), T.StructField("is_deleted", T.BooleanType(), False),
])
product_schema = T.StructType([
    T.StructField("product_key", T.StringType(), False), T.StructField("product_name", T.StringType(), False),
    T.StructField("product_category", T.StringType(), True), T.StructField("product_subcategory", T.StringType(), True),
    T.StructField("brand", T.StringType(), True), T.StructField("unit_cost", T.DoubleType(), True),
    T.StructField("unit_price", T.DoubleType(), True), T.StructField("is_discontinued", T.BooleanType(), False),
    T.StructField("is_deleted", T.BooleanType(), False),
])
employee_schema = T.StructType([
    T.StructField("employee_key", T.StringType(), False), T.StructField("first_name", T.StringType(), False),
    T.StructField("last_name", T.StringType(), False), T.StructField("job_title", T.StringType(), True),
    T.StructField("office_city", T.StringType(), True), T.StructField("hire_date", T.StringType(), True),
    T.StructField("is_deleted", T.BooleanType(), False),
])
order_schema = T.StructType([
    T.StructField("order_key", T.StringType(), False), T.StructField("order_line_number", T.IntegerType(), False),
    T.StructField("customer_key", T.StringType(), False), T.StructField("product_key", T.StringType(), False), T.StructField("employee_key", T.StringType(), False),
    T.StructField("order_date", T.StringType(), False), T.StructField("ship_date", T.StringType(), True),
    T.StructField("delivery_date", T.StringType(), True), T.StructField("order_status", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), False), T.StructField("unit_price", T.DoubleType(), True),
    T.StructField("discount_amount", T.DoubleType(), True), T.StructField("net_amount", T.DoubleType(), True),
    T.StructField("currency_code", T.StringType(), True),
])
return_schema = T.StructType([
    T.StructField("return_key", T.StringType(), False), T.StructField("order_key", T.StringType(), False),
    T.StructField("order_line_number", T.IntegerType(), False), T.StructField("product_key", T.StringType(), False), T.StructField("employee_key", T.StringType(), False),
    T.StructField("return_date", T.StringType(), False), T.StructField("return_status", T.StringType(), False),
    T.StructField("return_reason_code", T.StringType(), True), T.StructField("return_quantity", T.IntegerType(), False),
    T.StructField("refund_amount", T.DoubleType(), True), T.StructField("currency_code", T.StringType(), True),
])

def write_delivery_file(data, schema, object_name: str) -> None:
    temporary_path = f"{landing_path}/_staging_{object_name}"
    spark.createDataFrame(data, schema).coalesce(1).write.parquet(temporary_path)
    part_file = next(
        file.path for file in dbutils.fs.ls(temporary_path)
        if file.name.startswith("part-") and file.name.endswith(".parquet")
    )
    dbutils.fs.mv(part_file, f"{landing_path}/{object_name}.parquet")
    dbutils.fs.rm(temporary_path, recurse=True)


write_delivery_file(customers, customer_schema, "customers")
write_delivery_file(products, product_schema, "products")
write_delivery_file(employees, employee_schema, "employees")
write_delivery_file(orders, order_schema, "orders")
write_delivery_file(returns, return_schema, "returns")
print(f"Demo-levering geschreven naar {landing_path}")