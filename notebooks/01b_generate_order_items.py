# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from faker import Faker
from pyspark.sql import Row
from pyspark.sql import functions as F
import random

fake = Faker("fr_FR")
Faker.seed(42)
random.seed(42)

CATALOG = "ecommerce_lakehouse"

# ============================================================
# Génération des ORDER_ITEMS (produits commandés + prix)
# ============================================================
product_catalog = [
    ("PROD-001", "Smartphone", "Electronics", 899.90),
    ("PROD-002", "Laptop", "Electronics", 1299.00),
    ("PROD-003", "Casque Audio", "Electronics", 79.90),
    ("PROD-004", "T-Shirt", "Fashion", 19.90),
    ("PROD-005", "Jean", "Fashion", 49.90),
    ("PROD-006", "Chaussures", "Fashion", 89.90),
    ("PROD-007", "Canapé", "Furniture", 599.00),
    ("PROD-008", "Table", "Furniture", 249.00),
    ("PROD-009", "Livre Cuisine", "Books", 24.90),
    ("PROD-010", "Roman", "Books", 14.90),
    ("PROD-011", "Aspirateur", "Home Appliances", 199.00),
    ("PROD-012", "Cafetière", "Home Appliances", 59.90),
    ("PROD-013", "Ballon Foot", "Sports", 29.90),
    ("PROD-014", "Tapis Yoga", "Sports", 34.90),
    ("PROD-015", "Montre Connectée", "Electronics", 149.90),
]

seller_ids = [f"SELLER-{i:03d}" for i in range(1, 51)]

# On relit les order_id valides directement depuis la table SILVER
# (déjà nettoyée : plus de nulls, plus de UNKNOWN_STATUS)
# -> Bonne pratique : ce notebook est autonome, il ne dépend
#    d'aucune variable en mémoire d'un autre notebook.
valid_order_ids = [
    row.order_id for row in
    spark.table(f"{CATALOG}.silver.orders").select("order_id").distinct().collect()
]

print(f"Nombre de order_id valides récupérés depuis silver.orders : {len(valid_order_ids)}")

order_item_rows = []
item_counter = 1
for order_id in valid_order_ids:
    nb_items = random.choices([1, 2, 3], weights=[70, 20, 10], k=1)[0]
    for _ in range(nb_items):
        product = random.choice(product_catalog)
        quantity = random.randint(1, 3)
        freight_value = round(random.uniform(5.0, 35.0), 2)

        order_item_rows.append(Row(
            order_item_id=f"ITEM-{item_counter:08d}",
            order_id=order_id,
            product_id=product[0],
            product_name=product[1],
            product_category=product[2],
            seller_id=random.choice(seller_ids),
            price=product[3],
            quantity=quantity,
            freight_value=freight_value
        ))
        item_counter += 1

df_order_items = spark.createDataFrame(order_item_rows)

print(f"✅ {df_order_items.count()} lignes d'articles générées pour {len(valid_order_ids)} commandes")
df_order_items.show(5, truncate=False)

# COMMAND ----------

# Écriture en CSV dans le Volume (landing zone)
ITEMS_LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing_zone/order_items/"

(df_order_items
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(ITEMS_LANDING_PATH))

print(f"✅ Fichier CSV écrit dans : {ITEMS_LANDING_PATH}")
display(dbutils.fs.ls(ITEMS_LANDING_PATH))

# COMMAND ----------

from datetime import datetime, timezone

# ---- Ingestion Bronze : Order Items ----
order_items_schema = (
    "order_item_id STRING, order_id STRING, product_id STRING, "
    "product_name STRING, product_category STRING, seller_id STRING, "
    "price DOUBLE, quantity INT, freight_value DOUBLE"
)

df_items_raw = (
    spark.read
    .option("header", True)
    .schema(order_items_schema)
    .csv(ITEMS_LANDING_PATH)
)

df_items_bronze = (
    df_items_raw
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_batch_id", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
)

(df_items_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.order_items"))

print(f"✅ Table {CATALOG}.bronze.order_items créée : {df_items_bronze.count()} lignes")
display(spark.table(f"{CATALOG}.bronze.order_items").limit(5))

# COMMAND ----------

# ============================================================
# Passage en Silver : order_items
# ============================================================

df_items_bronze_read = spark.table(f"{CATALOG}.bronze.order_items")

# Contrôle d'intégrité référentielle : chaque order_item doit
# référencer un order_id qui existe réellement dans silver.orders
valid_orders_ids_df = spark.table(f"{CATALOG}.silver.orders").select("order_id")

df_items_with_check = df_items_bronze_read.join(
    valid_orders_ids_df.withColumnRenamed("order_id", "_ref_order_id"),
    df_items_bronze_read.order_id == F.col("_ref_order_id"),
    "left"
)

df_items_valid = df_items_with_check.filter(F.col("_ref_order_id").isNotNull()).drop("_ref_order_id")
df_items_orphans = df_items_with_check.filter(F.col("_ref_order_id").isNull()).drop("_ref_order_id")

print(f"Lignes order_items valides (order_id existe) : {df_items_valid.count()}")
print(f"Lignes orphelines (order_id introuvable)     : {df_items_orphans.count()}")

# Enrichissement : calcul du total ligne (price * quantity)
df_items_silver = df_items_valid.withColumn(
    "total_item_value", F.round(F.col("price") * F.col("quantity"), 2)
).withColumn("_processed_timestamp", F.current_timestamp())

(df_items_silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.silver.order_items"))

print(f"\n✅ silver.order_items créée : {df_items_silver.count()} lignes")
df_items_silver.select("order_item_id", "order_id", "price", "quantity", "total_item_value").show(5)

# COMMAND ----------

