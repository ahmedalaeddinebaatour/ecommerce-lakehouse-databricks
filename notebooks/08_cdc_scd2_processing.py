# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "ecommerce_lakehouse"
CDC_SOURCE = f"{CATALOG}.bronze.cdc_orders_raw"
SCD2_TABLE = f"{CATALOG}.silver.orders_status_history"

# Lecture des événements CDC, triés par LSN (PAS par timestamp !)
df_cdc = spark.table(CDC_SOURCE)

print(f"Nombre d'événements CDC à traiter : {df_cdc.count()}")

# ============================================================
# Dédoublonnage préventif : si un même (order_id, change_lsn)
# apparaît plusieurs fois (re-livraison du même événement CDC),
# on ne garde qu'une occurrence
# ============================================================
window_lsn = Window.partitionBy("order_id", "change_lsn").orderBy(F.col("_ingest_timestamp").desc())

df_cdc_clean = (
    df_cdc
    .withColumn("_rn", F.row_number().over(window_lsn))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Nombre d'événements après dédoublonnage : {df_cdc_clean.count()}")

# COMMAND ----------

# ============================================================
# Préparation : trier les événements par order_id + change_lsn
# pour traiter les changements dans le bon ordre chronologique réel
# ============================================================

window_order = Window.partitionBy("order_id").orderBy("change_lsn")

df_cdc_sequenced = (
    df_cdc_clean
    .withColumn("event_sequence", F.row_number().over(window_order))
    .withColumn("max_sequence", F.max("event_sequence").over(Window.partitionBy("order_id")))
)

# Combien d'étapes de statut maximum a une commande dans notre dataset ?
max_steps = df_cdc_sequenced.agg(F.max("max_sequence")).collect()[0][0]
print(f"Nombre maximum d'étapes de statut pour une commande : {max_steps}")

# Aperçu pour vérifier le bon séquençage par LSN
df_cdc_sequenced.filter(F.col("order_id") == "ORD-0000148").select(
    "order_id", "order_status", "change_lsn", "event_sequence"
).orderBy("event_sequence").show()

# COMMAND ----------

# ============================================================
# Initialisation de la table SCD2 (si elle n'existe pas)
# ============================================================

if not spark.catalog.tableExists(SCD2_TABLE):
    # Structure vide avec le bon schéma, prête à recevoir les MERGE
    empty_schema_df = (
        df_cdc_sequenced
        .limit(0)
        .withColumn("__start_at", F.lit(None).cast("timestamp"))
        .withColumn("__end_at", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(None).cast("boolean"))
        .select("order_id", "order_status", "change_lsn", "__start_at", "__end_at", "is_current")
    )
    empty_schema_df.write.format("delta").saveAsTable(SCD2_TABLE)
    print(f"✅ Table {SCD2_TABLE} initialisée (vide)")

# ============================================================
# Traitement séquentiel : une étape à la fois, dans l'ordre du LSN
# ============================================================

for step in range(1, max_steps + 1):
    df_step = df_cdc_sequenced.filter(F.col("event_sequence") == step)
    nb_rows_this_step = df_step.count()

    if nb_rows_this_step == 0:
        continue

    scd2_table = DeltaTable.forName(spark, SCD2_TABLE)

    # 1. Fermer la version actuelle des commandes concernées par ce step
    (scd2_table.alias("t")
        .merge(
            df_step.alias("s"),
            "t.order_id = s.order_id AND t.is_current = true"
        )
        .whenMatchedUpdate(set={
            "is_current": "false",
            "__end_at": "s.event_timestamp"
        })
        .execute())

    # 2. Insérer la nouvelle version comme "current"
    df_new_version = df_step.select(
        "order_id", "order_status", "change_lsn",
        F.col("event_timestamp").alias("__start_at"),
        F.lit(None).cast("timestamp").alias("__end_at"),
        F.lit(True).alias("is_current")
    )
    df_new_version.write.format("delta").mode("append").saveAsTable(SCD2_TABLE)

    print(f"Étape {step}/{max_steps} traitée : {nb_rows_this_step} commandes mises à jour")

print(f"\n✅ SCD Type 2 complet. Total de lignes dans l'historique : {spark.table(SCD2_TABLE).count()}")

# COMMAND ----------

# ============================================================
# Vérification : historique complet de ORD-0000148
# ============================================================

print("--- Historique complet (toutes versions) ---")
spark.table(SCD2_TABLE).filter(F.col("order_id") == "ORD-0000148").orderBy("change_lsn").show(truncate=False)

print("\n--- Statut CURRENT uniquement (ce qu'un dashboard verrait) ---")
spark.table(SCD2_TABLE).filter(
    (F.col("order_id") == "ORD-0000148") & (F.col("is_current") == True)
).show(truncate=False)

# ============================================================
# Sanity check global : chaque commande doit avoir EXACTEMENT
# une seule ligne avec is_current = true
# ============================================================
print("\n--- Sanity check : nombre de versions 'current' par commande ---")
check_df = (
    spark.table(SCD2_TABLE)
    .filter(F.col("is_current") == True)
    .groupBy("order_id")
    .count()
    .filter(F.col("count") != 1)
)
nb_anomalies = check_df.count()
print(f"Nombre de commandes avec un nombre incorrect de versions 'current' : {nb_anomalies}")

# COMMAND ----------

