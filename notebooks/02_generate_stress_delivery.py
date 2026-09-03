# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Genereer representatieve stresslevering
# MAGIC Schrijft een valide Sales-levering met configureerbare volumes naar de
# MAGIC landingzone. Een bestaande datumfolder wordt nooit overschreven.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("delivery_date", "2026-09-02")
dbutils.widgets.text("business_date", "2024-09-02")
dbutils.widgets.text("customer_count", "100000")
dbutils.widgets.text("product_count", "50000")
dbutils.widgets.text("employee_count", "10000")
dbutils.widgets.text("order_count", "1000000")
dbutils.widgets.text("return_count", "20000")
dbutils.widgets.text("change_set", "0")

# COMMAND ----------

from datetime import date, datetime

from pyspark.sql import functions as F


def widget_count(name: str) -> int:
    value = int(dbutils.widgets.get(name))
    if value < 1:
        raise ValueError(f"{name} moet minimaal 1 zijn.")
    return value


def widget_change_set() -> int:
    value = int(dbutils.widgets.get("change_set"))
    if value < 0:
        raise ValueError("change_set mag niet negatief zijn.")
    return value


delivery_date = dbutils.widgets.get("delivery_date")
datetime.strptime(delivery_date, "%Y-%m-%d")
business_date = datetime.strptime(dbutils.widgets.get("business_date"), "%Y-%m-%d").date()
if business_date > date.today():
    raise ValueError("business_date mag niet in de toekomst liggen.")
business_date = business_date.isoformat()
customer_count = widget_count("customer_count")
product_count = widget_count("product_count")
employee_count = widget_count("employee_count")
order_count = widget_count("order_count")
return_count = widget_count("return_count")
change_set = widget_change_set()
if return_count > order_count:
    raise ValueError("return_count kan niet groter zijn dan order_count.")

landing_path = f"/Volumes/raw_{dbutils.widgets.get('env')}/sales/landing/{delivery_date}"

try:
    dbutils.fs.ls(landing_path)
    raise ValueError(f"Stresslevering bestaat al en wordt niet overschreven: {landing_path}")
except Exception as exc:
    if "FileNotFoundException" not in str(exc) and "does not exist" not in str(exc):
        raise


def write_delivery_files(df, object_name: str, file_count: int) -> None:
    temporary_path = f"{landing_path}/_staging_{object_name}"
    df.repartition(file_count).write.mode("overwrite").parquet(temporary_path)
    part_files = sorted(
        file.path for file in dbutils.fs.ls(temporary_path)
        if file.name.startswith("part-") and file.name.endswith(".parquet")
    )
    if len(part_files) != file_count:
        raise RuntimeError(f"{object_name}: verwacht {file_count} bestanden, kreeg {len(part_files)}.")
    for index, part_file in enumerate(part_files, start=1):
        dbutils.fs.mv(part_file, f"{landing_path}/{object_name}-{index:05d}.parquet")
    dbutils.fs.rm(temporary_path, recurse=True)


customers = spark.range(customer_count).select(
    F.format_string("C-%06d", F.col("id") + 1).alias("customer_key"),
    F.when(F.lit(change_set) > 0, F.format_string("Customer %06d v%d", F.col("id") + 1, F.lit(change_set)))
     .otherwise(F.format_string("Customer %06d", F.col("id") + 1)).alias("customer_name"),
    F.format_string("customer%06d@contoso.example", F.col("id") + 1).alias("email"),
    F.format_string("+3120%07d", F.col("id") + 1).alias("phone"),
    F.format_string("Salesstraat %d", (F.col("id") % 500) + 1).alias("address_line1"),
    F.element_at(F.array(F.lit("Amsterdam"), F.lit("Rotterdam"), F.lit("Utrecht")), ((F.col("id") % 3) + 1).cast("int")).alias("city"),
    F.element_at(F.array(F.lit("NH"), F.lit("ZH"), F.lit("UT")), ((F.col("id") % 3) + 1).cast("int")).alias("state_province"),
    F.format_string("%04dAB", 1000 + (F.col("id") % 9000)).alias("postal_code"),
    F.lit("NL").alias("country"),
    F.element_at(F.array(F.lit("RETAIL"), F.lit("WHOLESALE"), F.lit("ONLINE"), F.lit("CORPORATE")), ((F.col("id") % 4) + 1).cast("int")).alias("customer_segment"),
    F.lit(business_date).alias("customer_since"),
    ((F.lit(change_set) > 0) & ((F.col("id") % 50) == 0)).alias("is_deleted"),
)

products = spark.range(product_count).select(
    F.format_string("P-%06d", F.col("id") + 1).alias("product_key"),
    F.format_string("Product %06d", F.col("id") + 1).alias("product_name"),
    F.element_at(F.array(F.lit("ELECTRONICS"), F.lit("OFFICE"), F.lit("HOME")), ((F.col("id") % 3) + 1).cast("int")).alias("product_category"),
    F.lit("STANDARD").alias("product_subcategory"), F.lit("CONTOSO").alias("brand"),
    ((F.col("id") % 500) + 10).cast("double").alias("unit_cost"),
    ((F.col("id") % 500) + 20 + F.when(F.lit(change_set) > 0, 1).otherwise(0)).cast("double").alias("unit_price"),
    F.lit(False).alias("is_discontinued"),
    ((F.lit(change_set) > 0) & ((F.col("id") % 100) == 0)).alias("is_deleted"),
)

employees = spark.range(employee_count).select(
    F.format_string("E-%05d", F.col("id") + 1).alias("employee_key"),
    F.when(F.lit(change_set) > 0, F.format_string("Employee%05d v%d", F.col("id") + 1, F.lit(change_set)))
     .otherwise(F.format_string("Employee%05d", F.col("id") + 1)).alias("first_name"),
    F.lit("Contoso").alias("last_name"), F.lit("Sales Representative").alias("job_title"),
    F.lit("Amsterdam").alias("office_city"),
    F.lit(business_date).alias("hire_date"),
    ((F.lit(change_set) > 0) & ((F.col("id") % 200) == 0)).alias("is_deleted"),
)

orders = (
    spark.range(order_count).select(
        F.col("id"), F.format_string("O-%09d", F.col("id") + 1 + (F.lit(change_set) * order_count)).alias("order_key"),
        F.lit(1).cast("int").alias("order_line_number"),
        F.format_string("C-%06d", (F.col("id") % customer_count) + 1).alias("customer_key"),
        F.format_string("P-%06d", (F.col("id") % product_count) + 1).alias("product_key"),
        F.format_string("E-%05d", (F.col("id") % employee_count) + 1).alias("employee_key"),
        F.lit(business_date).alias("order_date"),
        ((F.col("id") % 5) + 1).cast("int").alias("quantity"),
        ((F.col("id") % 500) + 20).cast("double").alias("unit_price"),
    )
    .withColumn("discount_amount", (F.col("id") % 4).cast("double"))
    .withColumn("net_amount", F.col("quantity") * F.col("unit_price") - F.col("discount_amount"))
    .withColumn("ship_date", F.when((F.col("id") % 4 == 0) | ((F.lit(change_set) > 0) & (F.col("id") % 100 == 0)), F.lit(None)).otherwise(F.date_add("order_date", 2)))
    .withColumn("delivery_date", F.when(F.col("ship_date").isNull(), F.lit(None)).otherwise(F.date_add("ship_date", 2)))
    .withColumn("order_status", F.when(F.col("ship_date").isNull(), F.lit("OPEN")).otherwise(F.lit("SHIPPED")))
    .withColumn("currency_code", F.lit("EUR")).drop("id")
)

returns = orders.limit(return_count).select(
    F.format_string("R-%09d", F.monotonically_increasing_id() + 1 + (F.lit(change_set) * return_count)).alias("return_key"),
    "order_key", "order_line_number", "product_key", "employee_key",
    F.lit(business_date).alias("return_date"),
    F.lit("RECEIVED").alias("return_status"), F.lit("DAMAGED").alias("return_reason_code"),
    F.lit(1).cast("int").alias("return_quantity"), F.col("unit_price").alias("refund_amount"),
    F.lit("EUR").alias("currency_code"),
)

write_delivery_files(customers, "customers", 10)
write_delivery_files(products, "products", 10)
write_delivery_files(employees, "employees", 10)
write_delivery_files(orders, "orders", 100)
write_delivery_files(returns, "returns", 10)

print(
    f"Stresslevering geschreven naar {landing_path}: customers={customer_count}, "
    f"products={product_count}, employees={employee_count}, orders={order_count}, "
    f"returns={return_count}, change_set={change_set}, business_date={business_date}"
)