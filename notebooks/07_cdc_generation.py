# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql import functions as F
import random
from datetime import datetime, timedelta, timezone

CATALOG = "ecommerce_lakehouse"

# On sélectionne 100 commandes existantes pour simuler leur historique de statuts
sample_orders = [
    row.order_id for row in
    spark.table(f"{CATALOG}.silver.orders").select("order_id").limit(100).collect()
]

# Le cycle de vie réaliste d'une commande (statuts successifs dans l'ordre)
status_lifecycle = ["created", "approved", "processing", "invoiced", "shipped", "delivered"]

def generate_cdc_events():
    """
    Pour chaque commande échantillon, génère une séquence d'événements CDC
    représentant son évolution de statut dans le temps.
    Chaque événement a un change_lsn croissant (Log Sequence Number simulé).
    """
    cdc_rows = []
    lsn_counter = 1000

    for order_id in sample_orders:
        base_time = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 10))
        # Chaque commande avance dans son cycle de vie de façon aléatoire
        # (certaines s'arrêtent à "approved", d'autres vont jusqu'à "delivered")
        nb_steps = random.randint(2, len(status_lifecycle))

        for step in range(nb_steps):
            event_time = base_time + timedelta(hours=step * random.randint(4, 24))
            cdc_rows.append(Row(
                order_id=order_id,
                order_status=status_lifecycle[step],
                operation="UPDATE" if step > 0 else "INSERT",
                change_lsn=lsn_counter,
                event_timestamp=event_time
            ))
            lsn_counter += 1

    return spark.createDataFrame(cdc_rows)

df_cdc_events = generate_cdc_events()
print(f"✅ {df_cdc_events.count()} événements CDC générés pour {len(sample_orders)} commandes")
df_cdc_events.filter(F.col("order_id") == sample_orders[0]).orderBy("change_lsn").show(truncate=False)

# COMMAND ----------

CDC_BRONZE_TABLE = f"{CATALOG}.bronze.cdc_orders_raw"

(df_cdc_events
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(CDC_BRONZE_TABLE))

print(f"✅ {CDC_BRONZE_TABLE} créée : {df_cdc_events.count()} lignes")

# COMMAND ----------

