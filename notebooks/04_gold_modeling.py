# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"

# Lecture des 3 tables Silver
df_orders = spark.table(f"{CATALOG}.silver.orders")
df_items = spark.table(f"{CATALOG}.silver.order_items")
df_customers = spark.table(f"{CATALOG}.silver.customers")

print(f"orders      : {df_orders.count()} lignes")
print(f"order_items : {df_items.count()} lignes")
print(f"customers   : {df_customers.count()} lignes")

# Aperçu rapide des colonnes disponibles pour construire fact_sales
df_orders.printSchema()

# COMMAND ----------

# ============================================================
# Construction de GOLD.FACT_SALES
# Grain : 1 ligne = 1 article commandé (order_item)
# ============================================================

df_fact_sales = (
    df_items
    .join(df_orders, "order_id", "inner")
    .join(
        df_customers.select("customer_id", "customer_city", "customer_state"),
        "customer_id",
        "left"
    )
    .select(
        "order_item_id",
        "order_id",
        "customer_id",
        "customer_city",
        "customer_state",
        "product_id",
        "product_name",
        "product_category",
        "seller_id",
        "order_status",
        "order_date",
        F.col("price").alias("unit_price"),
        "quantity",
        "total_item_value",
        "freight_value",
        "is_late_delivery",
        "delivery_delay_days"
    )
    .withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
)

print(f"Nombre de lignes dans fact_sales : {df_fact_sales.count()}")
df_fact_sales.show(5, truncate=False)

# Vérification rapide : le CA total doit être cohérent
total_revenue = df_fact_sales.agg(F.sum("total_item_value")).collect()[0][0]
print(f"\n💰 Chiffre d'affaires total (brut) : {total_revenue:,.2f} €")

# COMMAND ----------

# ============================================================
# Correction : ajout de colonnes métier pour le calcul du CA
# ============================================================

REVENUE_ELIGIBLE_STATUSES = ["delivered", "shipped", "invoiced", "approved", "processing"]
# Explication métier : on considère le CA "reconnu" dès qu'une commande
# est confirmée et en cours de traitement, PAS si elle est annulée
# ni si elle est juste "created" (panier non encore validé/payé)

df_fact_sales = (
    df_fact_sales
    .withColumn(
        "is_revenue_eligible",
        F.col("order_status").isin(REVENUE_ELIGIBLE_STATUSES)
    )
    .withColumn(
        "net_revenue",
        F.when(F.col("is_revenue_eligible"), F.col("total_item_value")).otherwise(F.lit(0.0))
    )
)

print("--- Vérification sur la ligne annulée ---")
df_fact_sales.filter(F.col("order_id") == "ORD-0008292").select(
    "order_id", "order_status", "total_item_value", "is_revenue_eligible", "net_revenue"
).show()

# Nouveau calcul du CA, correct cette fois
total_gross = df_fact_sales.agg(F.sum("total_item_value")).collect()[0][0]
total_net = df_fact_sales.agg(F.sum("net_revenue")).collect()[0][0]

print(f"💰 CA BRUT (toutes commandes confondues) : {total_gross:,.2f} €")
print(f"💰 CA NET (hors annulations)              : {total_net:,.2f} €")
print(f"📉 Manque à gagner dû aux annulations     : {total_gross - total_net:,.2f} €")

# COMMAND ----------

# ============================================================
# Écriture finale : GOLD.FACT_SALES (version corrigée)
# Cohérente avec le Liquid Clustering appliqué en Phase 7
# ============================================================

(df_fact_sales
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.fact_sales"))

print(f"✅ gold.fact_sales créée : {df_fact_sales.count()} lignes")

# Ré-application du Liquid Clustering (idempotent : sans effet si déjà configuré)
spark.sql(f"""
    ALTER TABLE {CATALOG}.gold.fact_sales
    CLUSTER BY (customer_id, product_id)
""")
spark.sql(f"OPTIMIZE {CATALOG}.gold.fact_sales")

print("✅ Table optimisée avec Liquid Clustering")

# COMMAND ----------

# ============================================================
# Construction de GOLD.AGG_DAILY_REVENUE (version corrigée)
# ============================================================

df_fact = spark.table(f"{CATALOG}.gold.fact_sales")

df_agg_daily = (
    df_fact
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("nb_orders"),
        F.countDistinct("customer_id").alias("nb_unique_customers"),
        F.round(F.sum("total_item_value"), 2).alias("gross_revenue"),
        F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_item_value"),
        F.sum(F.when(F.col("is_late_delivery"), 1).otherwise(0)).alias("nb_late_deliveries"),
        F.sum(F.when(~F.col("is_revenue_eligible"), 1).otherwise(0)).alias("nb_canceled_items")
    )
    .orderBy("order_date")
)

print(f"Nombre de jours distincts : {df_agg_daily.count()}")
df_agg_daily.show(10, truncate=False)

(df_agg_daily
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .saveAsTable(f"{CATALOG}.gold.agg_daily_revenue"))

print(f"\n✅ gold.agg_daily_revenue créée")

# COMMAND ----------

# ============================================================
# Construction de GOLD.DIM_CUSTOMER (avec KPI de Churn)
# ============================================================

df_customers_silver = spark.table(f"{CATALOG}.silver.customers")
df_fact = spark.table(f"{CATALOG}.gold.fact_sales")

# Agrégation des métriques d'achat par client
df_customer_metrics = (
    df_fact
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("net_revenue"), 2).alias("lifetime_value"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date")
    )
)

# On récupère la date la plus récente de tout le dataset (simulation du "aujourd'hui")
# En prod, on utiliserait F.current_date(), mais nos données sont historiques (2024-2026)
max_dataset_date = df_fact.agg(F.max("order_date")).collect()[0][0]
print(f"Date de référence utilisée pour le calcul du churn : {max_dataset_date}")

df_dim_customer = (
    df_customers_silver
    .join(df_customer_metrics, "customer_id", "left")
    .withColumn(
        "days_since_last_order",
        F.datediff(F.lit(max_dataset_date), F.col("last_order_date"))
    )
    .withColumn(
        "churn_status",
        F.when(F.col("last_order_date").isNull(), "NEVER_PURCHASED")
         .when(F.col("days_since_last_order") > 90, "AT_RISK")
         .otherwise("ACTIVE")
    )
    .withColumn(
        "customer_segment",
        F.when(F.col("lifetime_value") >= 1000, "HIGH_VALUE")
         .when(F.col("lifetime_value") >= 300, "MEDIUM_VALUE")
         .when(F.col("lifetime_value") > 0, "LOW_VALUE")
         .otherwise("NO_PURCHASE")
    )
)

print("\n--- Répartition par statut de churn ---")
df_dim_customer.groupBy("churn_status").count().show()

print("--- Répartition par segment client ---")
df_dim_customer.groupBy("customer_segment").count().show()

(df_dim_customer
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.gold.dim_customer"))

print(f"✅ gold.dim_customer créée : {df_dim_customer.count()} lignes")

# COMMAND ----------

# ============================================================
# Correction : distinguer "jamais commandé" de "commandé mais annulé"
# ============================================================

df_dim_customer_fixed = (
    df_dim_customer
    .drop("customer_segment")
    .withColumn(
        "customer_segment",
        F.when(F.col("last_order_date").isNull(), "NEVER_PURCHASED")
         .when(F.col("lifetime_value") == 0, "ZERO_REVENUE_ALL_CANCELED")
         .when(F.col("lifetime_value") >= 1000, "HIGH_VALUE")
         .when(F.col("lifetime_value") >= 300, "MEDIUM_VALUE")
         .otherwise("LOW_VALUE")
    )
)

print("--- Nouvelle répartition par segment (corrigée) ---")
df_dim_customer_fixed.groupBy("customer_segment").count().show()

# Vérification de cohérence : NEVER_PURCHASED doit être identique dans les 2 colonnes
check = df_dim_customer_fixed.filter(
    (F.col("churn_status") == "NEVER_PURCHASED") != (F.col("customer_segment") == "NEVER_PURCHASED")
).count()
print(f"Nombre d'incohérences restantes entre churn_status et customer_segment : {check}")

(df_dim_customer_fixed
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.gold.dim_customer"))

print(f"\n✅ gold.dim_customer corrigée et réécrite : {df_dim_customer_fixed.count()} lignes")

# COMMAND ----------

