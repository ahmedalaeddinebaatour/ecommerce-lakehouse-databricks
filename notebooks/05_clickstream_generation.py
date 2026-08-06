# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from faker import Faker
from pyspark.sql import Row
from pyspark.sql import functions as F
import random
from datetime import datetime, timedelta, timezone

fake = Faker("fr_FR")
Faker.seed(123)
random.seed(123)

CATALOG = "ecommerce_lakehouse"

# On récupère les vrais customer_id et product_id existants
# pour simuler des events cohérents avec notre catalogue
valid_customer_ids = [
    row.customer_id for row in
    spark.table(f"{CATALOG}.silver.customers").select("customer_id").limit(500).collect()
]
valid_product_ids = [
    row.product_id for row in
    spark.table(f"{CATALOG}.silver.order_items").select("product_id").distinct().collect()
]

event_types = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "search"]
device_types = ["mobile", "desktop", "tablet"]

# Génération d'un batch d'events (simule l'arrivée d'un fichier toutes les X minutes)
def generate_clickstream_batch(nb_events=200, batch_offset_minutes=0):
    rows = []
    base_time = datetime.now(timezone.utc) - timedelta(minutes=batch_offset_minutes)
    for i in range(nb_events):
        event_time = base_time - timedelta(seconds=random.randint(0, 300))
        rows.append(Row(
            event_id=fake.uuid4(),
            customer_id=random.choice(valid_customer_ids),
            product_id=random.choice(valid_product_ids) if random.random() > 0.2 else None,
            event_type=random.choices(event_types, weights=[30, 25, 20, 10, 10, 5], k=1)[0],
            device_type=random.choice(device_types),
            session_id=fake.uuid4(),
            event_timestamp=event_time
        ))
    return spark.createDataFrame(rows)

# On génère 3 "batchs" simulant 3 arrivées successives de fichiers
df_batch_1 = generate_clickstream_batch(200, batch_offset_minutes=10)
df_batch_2 = generate_clickstream_batch(180, batch_offset_minutes=5)
df_batch_3 = generate_clickstream_batch(220, batch_offset_minutes=0)

print(f"Batch 1: {df_batch_1.count()} events")
print(f"Batch 2: {df_batch_2.count()} events")
print(f"Batch 3: {df_batch_3.count()} events")
df_batch_1.show(5, truncate=False)

# COMMAND ----------

CLICKSTREAM_LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing_zone/clickstream/"

# On écrit chaque batch dans un sous-dossier distinct pour simuler
# des fichiers distincts arrivant à des moments différents
(df_batch_1.coalesce(1).write.mode("overwrite")
    .json(f"{CLICKSTREAM_LANDING_PATH}batch_1/"))

(df_batch_2.coalesce(1).write.mode("overwrite")
    .json(f"{CLICKSTREAM_LANDING_PATH}batch_2/"))

(df_batch_3.coalesce(1).write.mode("overwrite")
    .json(f"{CLICKSTREAM_LANDING_PATH}batch_3/"))

print("✅ 3 batchs JSON écrits dans le Volume")
display(dbutils.fs.ls(CLICKSTREAM_LANDING_PATH))

# COMMAND ----------

# Simulation d'un nouveau batch qui "arrive" plus tard
df_batch_4 = generate_clickstream_batch(150, batch_offset_minutes=0)

(df_batch_4.coalesce(1).write.mode("overwrite")
    .json(f"{CLICKSTREAM_LANDING_PATH}batch_4/"))

print(f"✅ Nouveau batch_4 déposé : {df_batch_4.count()} events")

# COMMAND ----------

