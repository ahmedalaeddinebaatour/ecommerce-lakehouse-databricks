# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime, timezone

CATALOG = "ecommerce_lakehouse"

# ---- Ingestion Bronze : Customers ----
customers_schema = (
    "customer_id STRING, customer_unique_id STRING, customer_city STRING, "
    "customer_state STRING, customer_zip_code STRING, signup_date DATE"
)

df_customers_raw = (
    spark.read
    .option("header", True)
    .schema(customers_schema)
    .csv(f"/Volumes/{CATALOG}/bronze/landing_zone/customers/")
)

df_customers_bronze = (
    df_customers_raw
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_batch_id", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
)

(df_customers_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.customers"))

print(f"✅ Table {CATALOG}.bronze.customers créée : {df_customers_bronze.count()} lignes")
display(spark.table(f"{CATALOG}.bronze.customers").limit(5))

# COMMAND ----------

from datetime import datetime, timezone

# ---- Ingestion Bronze : Orders ----
orders_schema = (
    "order_id STRING, customer_id STRING, order_status STRING, "
    "order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP, "
    "order_delivered_timestamp TIMESTAMP, order_estimated_delivery_date TIMESTAMP"
)

df_orders_raw = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(f"/Volumes/{CATALOG}/bronze/landing_zone/orders/")
)

df_orders_bronze = (
    df_orders_raw
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_batch_id", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
)

(df_orders_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_status")
    .saveAsTable(f"{CATALOG}.bronze.orders"))

print(f"✅ Table {CATALOG}.bronze.orders créée : {df_orders_bronze.count()} lignes")
display(spark.table(f"{CATALOG}.bronze.orders").limit(5))