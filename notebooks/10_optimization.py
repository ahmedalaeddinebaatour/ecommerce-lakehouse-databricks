# Databricks notebook source
from pyspark.sql import functions as F
import time

CATALOG = "ecommerce_lakehouse"

# ============================================================
# Baseline : mesurer le temps d'une requête typique AVANT optimisation
# Requête représentative : filtrer par client + agréger
# ============================================================

def measure_query_time(query_func, label):
    start = time.time()
    result = query_func()
    result.count()  # force l'exécution complète (Spark est lazy)
    elapsed = time.time() - start
    print(f"⏱️  {label} : {elapsed:.2f} secondes")
    return elapsed

# Requête test 1 : filtrage par customer_id spécifique
def query_by_customer():
    return spark.table(f"{CATALOG}.gold.fact_sales").filter(
        F.col("customer_id") == "CUST-001653"
    )

# Requête test 2 : agrégation par produit
def query_by_product():
    return (
        spark.table(f"{CATALOG}.gold.fact_sales")
        .groupBy("product_id")
        .agg(F.sum("net_revenue"))
    )

print("=== Baseline AVANT optimisation ===")
time_before_1 = measure_query_time(query_by_customer, "Filtre par customer_id")
time_before_2 = measure_query_time(query_by_product, "Agrégation par product_id")

# COMMAND ----------

# ============================================================
# Mesure corrigée : plusieurs runs pour éliminer l'effet "cold start"
# ============================================================

print("=== Warm-up (résultat ignoré) ===")
measure_query_time(query_by_customer, "Warm-up filtre")

print("\n=== Mesures fiables (après warm-up) ===")
times_customer = [measure_query_time(query_by_customer, f"Filtre customer_id (run {i+1})") for i in range(3)]
times_product = [measure_query_time(query_by_product, f"Agrégation product_id (run {i+1})") for i in range(3)]

avg_customer_before = sum(times_customer) / len(times_customer)
avg_product_before = sum(times_product) / len(times_product)

print(f"\n📊 Moyenne filtre customer_id AVANT optimisation : {avg_customer_before:.2f}s")
print(f"📊 Moyenne agrégation product_id AVANT optimisation : {avg_product_before:.2f}s")

# COMMAND ----------

CATALOG = "ecommerce_lakehouse"

# Relecture des données actuelles de fact_sales
df_fact_sales_current = spark.table(f"{CATALOG}.gold.fact_sales")

# Réécriture SANS partitionnement classique
(df_fact_sales_current
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.fact_sales"))

print(f"✅ Table réécrite sans partitionnement : {spark.table(f'{CATALOG}.gold.fact_sales').count()} lignes")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Application du Liquid Clustering (sans conflit cette fois)
# MAGIC -- ============================================================
# MAGIC
# MAGIC ALTER TABLE ecommerce_lakehouse.gold.fact_sales
# MAGIC CLUSTER BY (customer_id, product_id);
# MAGIC
# MAGIC -- Forcer une réorganisation immédiate des fichiers existants
# MAGIC OPTIMIZE ecommerce_lakehouse.gold.fact_sales;
# MAGIC
# MAGIC -- Vérification de la configuration
# MAGIC DESCRIBE DETAIL ecommerce_lakehouse.gold.fact_sales;

# COMMAND ----------

print("=== Mesures APRÈS Liquid Clustering ===")

# Warm-up
measure_query_time(query_by_customer, "Warm-up filtre")

times_customer_after = [measure_query_time(query_by_customer, f"Filtre customer_id (run {i+1})") for i in range(3)]
times_product_after = [measure_query_time(query_by_product, f"Agrégation product_id (run {i+1})") for i in range(3)]

avg_customer_after = sum(times_customer_after) / len(times_customer_after)
avg_product_after = sum(times_product_after) / len(times_product_after)

print(f"\n📊 COMPARAISON FINALE")
print(f"{'Métrique':<35}{'Avant':<12}{'Après':<12}{'Delta'}")
print(f"{'Filtre customer_id':<35}{avg_customer_before:.2f}s{'':<7}{avg_customer_after:.2f}s{'':<7}{avg_customer_after - avg_customer_before:+.2f}s")
print(f"{'Agrégation product_id':<35}{avg_product_before:.2f}s{'':<7}{avg_product_after:.2f}s{'':<7}{avg_product_after - avg_product_before:+.2f}s")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- VACUUM : suppression des fichiers physiques obsolètes
# MAGIC -- (anciennes versions de données remplacées par OPTIMIZE/CLUSTER)
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Vérifier d'abord ce qui SERAIT supprimé (mode dry-run)
# MAGIC -- DRY RUN désactivé par défaut sur Databricks récents, on utilise directement VACUUM
# MAGIC
# MAGIC -- Rétention par défaut = 7 jours (168h), on la respecte ici (bonne pratique)
# MAGIC VACUUM ecommerce_lakehouse.gold.fact_sales RETAIN 168 HOURS;

# COMMAND ----------

