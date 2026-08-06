# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from faker import Faker
from pyspark.sql import Row
from pyspark.sql import functions as F
import random

# Reproductibilité : mêmes données générées à chaque exécution
fake = Faker("fr_FR")
Faker.seed(42)
random.seed(42)

NB_CUSTOMERS = 5000

brazilian_states = ["SP", "RJ", "MG", "BA", "PR", "RS", "PE", "CE", "SC", "GO"]

rows = []
for i in range(1, NB_CUSTOMERS + 1):
    rows.append(Row(
        customer_id=f"CUST-{i:06d}",
        customer_unique_id=fake.uuid4(),
        customer_city=fake.city(),
        customer_state=random.choice(brazilian_states),
        customer_zip_code=fake.postcode(),
        signup_date=fake.date_between(start_date="-3y", end_date="-1d")
    ))

df_customers = spark.createDataFrame(rows)

# Injection volontaire de quelques anomalies pour tester la Data Quality plus tard
# (5 lignes avec customer_id null, 5 lignes dupliquées)
df_anomalies_null = df_customers.limit(5).withColumn("customer_id", F.lit(None).cast("string"))
df_anomalies_dup = df_customers.limit(5)

df_customers_final = df_customers.unionByName(df_anomalies_null).unionByName(df_anomalies_dup)

print(f"✅ {df_customers_final.count()} lignes générées (dont anomalies volontaires)")
df_customers_final.show(5, truncate=False)

# COMMAND ----------

CATALOG = "ecommerce_lakehouse"
LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing_zone/customers/"

(df_customers_final
    .coalesce(1)  # un seul fichier CSV, pratique pour une petite volumétrie de démo
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(LANDING_PATH))

print(f"✅ Fichier CSV écrit dans : {LANDING_PATH}")

# Vérification : lister les fichiers créés
display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

from datetime import timedelta

NB_ORDERS = 20000
order_statuses = ["delivered", "shipped", "processing", "canceled", "invoiced", "created", "approved"]
status_weights = [60, 10, 8, 5, 5, 7, 5]

valid_customer_ids = [row.customer_id for row in df_customers.select("customer_id").collect()]

order_rows = []
for i in range(1, NB_ORDERS + 1):
    purchase_date = fake.date_time_between(start_date="-2y", end_date="now")
    status = random.choices(order_statuses, weights=status_weights, k=1)[0]
    estimated_delivery = purchase_date + timedelta(days=random.randint(5, 20))
    approved_at = purchase_date + timedelta(hours=random.randint(1, 48))

    if status == "delivered":
        actual_delivery = purchase_date + timedelta(days=random.randint(2, 25))
    else:
        actual_delivery = None

    order_rows.append(Row(
        order_id=f"ORD-{i:07d}",
        customer_id=random.choice(valid_customer_ids),
        order_status=status,
        order_purchase_timestamp=purchase_date,
        order_approved_at=approved_at,
        order_delivered_timestamp=actual_delivery,
        order_estimated_delivery_date=estimated_delivery
    ))

df_orders = spark.createDataFrame(order_rows)

df_anom_null_id = df_orders.limit(10).withColumn("order_id", F.lit(None).cast("string"))
df_anom_bad_status = df_orders.limit(10).withColumn("order_status", F.lit("UNKNOWN_STATUS"))
df_anom_future_date = df_orders.limit(10).withColumn(
    "order_purchase_timestamp", F.date_add(F.current_timestamp(), 30)
)

df_orders_final = (df_orders
    .unionByName(df_anom_null_id)
    .unionByName(df_anom_bad_status)
    .unionByName(df_anom_future_date))

print(f"✅ {df_orders_final.count()} commandes générées (dont anomalies volontaires)")
df_orders_final.show(5, truncate=False)

# COMMAND ----------

ORDERS_LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing_zone/orders/"

(df_orders_final
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(ORDERS_LANDING_PATH))

print(f"✅ Fichier CSV écrit dans : {ORDERS_LANDING_PATH}")
display(dbutils.fs.ls(ORDERS_LANDING_PATH))

# COMMAND ----------

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

# On récupère les order_id valides (non nulls) générés précédemment
valid_order_ids = [row.order_id for row in df_orders.select("order_id").distinct().collect() if row.order_id]

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

