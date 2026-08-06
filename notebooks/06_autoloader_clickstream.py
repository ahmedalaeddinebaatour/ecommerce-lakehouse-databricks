# Databricks notebook source
CATALOG = "ecommerce_lakehouse"

SOURCE_PATH = f"/Volumes/{CATALOG}/bronze/landing_zone/clickstream/"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/bronze/landing_zone/_schemas/clickstream/"
CHECKPOINT_LOCATION = f"/Volumes/{CATALOG}/bronze/landing_zone/_checkpoints/clickstream/"
TARGET_TABLE = f"{CATALOG}.bronze.clickstream_events"

# ============================================================
# Configuration du stream Auto Loader
# ============================================================
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(SOURCE_PATH)
)

from pyspark.sql import functions as F

df_stream_enriched = (
    df_stream
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

print("✅ Stream configuré, prêt à être démarré")
df_stream_enriched.printSchema()

# COMMAND ----------

# ============================================================
# Démarrage du stream : écriture en table Delta Bronze
# trigger(availableNow=True) = traite tout ce qui est disponible
# puis s'arrête (pattern batch incrémental, pas de stream continu)
# ============================================================

query = (
    df_stream_enriched
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

print(f"✅ Stream terminé (trigger availableNow)")
print(f"Nombre de lignes dans {TARGET_TABLE} : {spark.table(TARGET_TABLE).count()}")

# COMMAND ----------

