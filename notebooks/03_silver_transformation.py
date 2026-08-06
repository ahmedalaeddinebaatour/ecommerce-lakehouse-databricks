# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "ecommerce_lakehouse"

# Lecture de la table Bronze
df_orders_bronze = spark.table(f"{CATALOG}.bronze.orders")

print(f"Nombre total de lignes en Bronze : {df_orders_bronze.count()}")

# Vue d'ensemble des statuts (on doit voir apparaître UNKNOWN_STATUS)
print("\n--- Répartition par statut ---")
df_orders_bronze.groupBy("order_status").count().orderBy(F.desc("count")).display()

# Vérification des order_id nulls
nb_null_ids = df_orders_bronze.filter(F.col("order_id").isNull()).count()
print(f"\nNombre de order_id null : {nb_null_ids}")

# Vérification des dates futures
nb_future_dates = df_orders_bronze.filter(
    F.col("order_purchase_timestamp") > F.current_timestamp()
).count()
print(f"Nombre de dates d'achat dans le futur : {nb_future_dates}")

# COMMAND ----------

# ============================================================
# ÉTAPE A : Dédoublonnage
# ============================================================
# On garde la version la plus récente de chaque commande
# (basé sur _ingest_timestamp, au cas où une commande aurait
# été ingérée plusieurs fois lors de différents batchs)

window_dedup = Window.partitionBy("order_id").orderBy(F.col("_ingest_timestamp").desc())

df_deduped = (
    df_orders_bronze
    .withColumn("_rn", F.row_number().over(window_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Lignes avant dédoublonnage : {df_orders_bronze.count()}")
print(f"Lignes après dédoublonnage : {df_deduped.count()}")

# COMMAND ----------

# ============================================================
# ÉTAPE A (version robuste) : Dédoublonnage avec tie-breaker
# ============================================================
# On ajoute order_status comme critère secondaire de tri pour
# rendre le résultat déterministe même en cas d'égalité parfaite
# sur _ingest_timestamp (cas réel rencontré ci-dessus)

window_dedup = (
    Window.partitionBy("order_id")
    .orderBy(F.col("_ingest_timestamp").desc(), F.col("order_status").asc())
)

df_deduped = (
    df_orders_bronze
    .withColumn("_rn", F.row_number().over(window_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Lignes avant dédoublonnage : {df_orders_bronze.count()}")
print(f"Lignes après dédoublonnage : {df_deduped.count()}")

# Preuve concrète : on inspecte ce qui reste pour ORD-0000001
print("\n--- Vérification sur ORD-0000001 (doit avoir 1 seule ligne maintenant) ---")
df_deduped.filter(F.col("order_id") == "ORD-0000001").select(
    "order_id", "order_status", "order_purchase_timestamp"
).show(truncate=False)

# COMMAND ----------

# ============================================================
# ÉTAPE B : Règles de Data Quality
# ============================================================

VALID_STATUSES = ["delivered", "shipped", "processing", "canceled",
                   "invoiced", "created", "approved"]

df_checked = (
    df_deduped
    .withColumn("_dq_null_order_id", F.col("order_id").isNull())
    .withColumn("_dq_null_customer_id", F.col("customer_id").isNull())
    .withColumn("_dq_invalid_status", ~F.col("order_status").isin(VALID_STATUSES))
    .withColumn(
        "_dq_future_purchase_date",
        F.col("order_purchase_timestamp") > F.current_timestamp()
    )
    .withColumn(
        "_dq_is_valid",
        ~(
            F.col("_dq_null_order_id") |
            F.col("_dq_null_customer_id") |
            F.col("_dq_invalid_status") |
            F.col("_dq_future_purchase_date")
        )
    )
)

# Séparation en deux DataFrames distincts
dq_flag_columns = [
    "_dq_null_order_id", "_dq_null_customer_id",
    "_dq_invalid_status", "_dq_future_purchase_date", "_dq_is_valid"
]

df_valid = df_checked.filter(F.col("_dq_is_valid")).drop(*dq_flag_columns)
df_quarantine = df_checked.filter(~F.col("_dq_is_valid"))

print(f"Lignes valides    : {df_valid.count()}")
print(f"Lignes en quarantaine : {df_quarantine.count()}")

print("\n--- Détail des lignes en quarantaine (raisons du rejet) ---")
df_quarantine.select(
    "order_id", "order_status",
    "_dq_null_order_id", "_dq_null_customer_id",
    "_dq_invalid_status", "_dq_future_purchase_date"
).show(30, truncate=False)

# COMMAND ----------

# ============================================================
# ÉTAPE A-bis : Traçabilité du dédoublonnage (bonne pratique)
# ============================================================
nb_before_dedup = df_orders_bronze.count()
nb_after_dedup = df_deduped.count()
nb_removed_by_dedup = nb_before_dedup - nb_after_dedup

print(f"📊 Lignes supprimées par le dédoublonnage : {nb_removed_by_dedup}")
print(f"   (à ne jamais confondre avec les rejets de Data Quality ci-dessous)")

# On log ce chiffre dans une table d'audit pour garder une trace
from datetime import datetime, timezone

audit_row = spark.createDataFrame([{
    "step": "deduplication",
    "table_name": "silver.orders",
    "rows_before": nb_before_dedup,
    "rows_after": nb_after_dedup,
    "rows_removed": nb_removed_by_dedup,
    "run_timestamp": datetime.now(timezone.utc)
}])

audit_row.write.format("delta").mode("append").saveAsTable(
    f"{CATALOG}.monitoring.pipeline_audit_log"
)

print("✅ Trace de dédoublonnage enregistrée dans monitoring.pipeline_audit_log")

# COMMAND ----------

# ============================================================
# ÉTAPE C : Enrichissement des données valides
# ============================================================
df_silver_enriched = (
    df_valid
    .withColumn("order_date", F.to_date("order_purchase_timestamp"))
    .withColumn(
        "delivery_delay_days",
        F.datediff("order_delivered_timestamp", "order_estimated_delivery_date")
    )
    .withColumn(
        "is_late_delivery",
        F.when(F.col("delivery_delay_days") > 0, True).otherwise(False)
    )
    .withColumn("_processed_timestamp", F.current_timestamp())
)

print("--- Aperçu des données enrichies ---")
df_silver_enriched.select(
    "order_id", "order_status", "order_date",
    "delivery_delay_days", "is_late_delivery"
).show(5, truncate=False)

# ============================================================
# ÉTAPE D : Écriture finale en Delta
# ============================================================
(df_silver_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.orders"))

(df_quarantine
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.monitoring.orders_quarantine"))

print(f"\n✅ silver.orders créée : {df_silver_enriched.count()} lignes")
print(f"✅ monitoring.orders_quarantine créée : {df_quarantine.count()} lignes")

# COMMAND ----------

# ============================================================
# Passage en Silver : customers (nettoyage minimal)
# ============================================================
df_customers_bronze_read = spark.table(f"{CATALOG}.bronze.customers")

window_dedup_cust = Window.partitionBy("customer_id").orderBy(F.col("_ingest_timestamp").desc())

df_customers_silver = (
    df_customers_bronze_read
    .filter(F.col("customer_id").isNotNull())   # Filtre DQ simple : pas de customer_id null
    .withColumn("_rn", F.row_number().over(window_dedup_cust))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("_processed_timestamp", F.current_timestamp())
)

(df_customers_silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.silver.customers"))

print(f"Lignes Bronze  : {df_customers_bronze_read.count()}")
print(f"✅ silver.customers créée : {df_customers_silver.count()} lignes")

# COMMAND ----------

