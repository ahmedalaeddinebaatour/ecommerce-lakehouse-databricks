# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime, timezone

CATALOG = "ecommerce_lakehouse"

# ============================================================
# Health Check #1 : Taux de rejet Data Quality (orders)
# ============================================================

nb_silver = spark.table(f"{CATALOG}.silver.orders").count()
nb_quarantine = spark.table(f"{CATALOG}.monitoring.orders_quarantine").count()
nb_total = nb_silver + nb_quarantine

rejection_rate = nb_quarantine / nb_total if nb_total > 0 else 0

DQ_THRESHOLD = 0.05  # seuil d'alerte : 5% de rejet maximum toléré

print(f"Lignes valides (Silver)     : {nb_silver}")
print(f"Lignes en quarantaine       : {nb_quarantine}")
print(f"Taux de rejet               : {rejection_rate:.2%}")
print(f"Seuil d'alerte configuré    : {DQ_THRESHOLD:.2%}")

dq_status = "🔴 ALERTE" if rejection_rate > DQ_THRESHOLD else "🟢 OK"
print(f"\nStatut Data Quality : {dq_status}")

# COMMAND ----------

# ============================================================
# Health Check #2 : Fraîcheur des données (freshness)
# Est-ce que gold.fact_sales a été mis à jour récemment ?
# ============================================================

from pyspark.sql.functions import col

# On récupère la dernière modification de la table via son historique Delta
history_df = spark.sql(f"DESCRIBE HISTORY {CATALOG}.gold.fact_sales LIMIT 1")
last_update_timestamp = history_df.select("timestamp").collect()[0][0]

now = datetime.now(timezone.utc)
hours_since_update = (now - last_update_timestamp.replace(tzinfo=timezone.utc)).total_seconds() / 3600

FRESHNESS_THRESHOLD_HOURS = 26  # le pipeline tourne 1x/jour, on tolère 26h avant alerte

print(f"Dernière mise à jour de gold.fact_sales : {last_update_timestamp}")
print(f"Heures écoulées depuis                  : {hours_since_update:.1f}h")
print(f"Seuil de fraîcheur configuré            : {FRESHNESS_THRESHOLD_HOURS}h")

freshness_status = "🔴 ALERTE - Données périmées" if hours_since_update > FRESHNESS_THRESHOLD_HOURS else "🟢 OK"
print(f"\nStatut Fraîcheur : {freshness_status}")

# ============================================================
# Enregistrement des métriques dans une table historique
# ============================================================

health_check_row = spark.createDataFrame([{
    "check_timestamp": now,
    "dq_rejection_rate": float(rejection_rate),
    "dq_status": "ALERT" if rejection_rate > DQ_THRESHOLD else "OK",
    "freshness_hours": float(hours_since_update),
    "freshness_status": "ALERT" if hours_since_update > FRESHNESS_THRESHOLD_HOURS else "OK",
    "nb_silver_orders": int(nb_silver),
    "nb_quarantine_orders": int(nb_quarantine)
}])

(health_check_row
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.monitoring.pipeline_health_checks"))

print(f"\n✅ Health check enregistré dans monitoring.pipeline_health_checks")

# COMMAND ----------

# ============================================================
# Décision finale : faire échouer le Job si une alerte est active
# ============================================================

alerts = []

if rejection_rate > DQ_THRESHOLD:
    alerts.append(f"Taux de rejet DQ trop élevé : {rejection_rate:.2%} (seuil: {DQ_THRESHOLD:.2%})")

if hours_since_update > FRESHNESS_THRESHOLD_HOURS:
    alerts.append(f"Données périmées : {hours_since_update:.1f}h (seuil: {FRESHNESS_THRESHOLD_HOURS}h)")

if alerts:
    error_message = "🔴 HEALTH CHECK FAILED:\n" + "\n".join(f"  - {a}" for a in alerts)
    print(error_message)
    raise ValueError(error_message)  # Fait échouer le run -> déclenche la notification email
else:
    print("✅ Tous les health checks sont OK. Pipeline en bonne santé.")

# COMMAND ----------

