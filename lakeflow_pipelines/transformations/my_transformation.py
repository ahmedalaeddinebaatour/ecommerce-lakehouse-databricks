from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"

# ============================================================
# Source : lecture des événements CDC bruts déjà en Bronze
# ============================================================
@dp.table(
    name="cdc_orders_source",
    comment="Vue de préparation des événements CDC avant AUTO CDC"
)
def cdc_orders_source():
    return spark.readStream.table(f"{CATALOG}.bronze.cdc_orders_raw")

# ============================================================
# Table cible streaming (obligatoire avant un AUTO CDC)
# ============================================================
dp.create_streaming_table(
    name="orders_status_current",
    comment="Statut courant de chaque commande (SCD Type 1 - dernier état connu)"
)

dp.create_auto_cdc_flow(
    target="orders_status_current",
    source="cdc_orders_source",
    keys=["order_id"],
    sequence_by=F.col("change_lsn"),
    stored_as_scd_type=1
)

# ============================================================
# SCD Type 2 : historique complet (équivalent de notre
# implémentation manuelle dans 08_cdc_scd2_processing)
# ============================================================
dp.create_streaming_table(
    name="orders_status_history_autocdc",
    comment="Historique complet des statuts (SCD Type 2) - version AUTO CDC"
)

dp.create_auto_cdc_flow(
    target="orders_status_history_autocdc",
    source="cdc_orders_source",
    keys=["order_id"],
    sequence_by=F.col("change_lsn"),
    stored_as_scd_type=2
)