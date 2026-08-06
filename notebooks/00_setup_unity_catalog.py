# Databricks notebook source
# MAGIC %sql
# MAGIC -- Sélection du catalogue de travail
# MAGIC USE CATALOG ecommerce_lakehouse;
# MAGIC
# MAGIC -- Création des schémas de l'architecture Medallion
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Zone RAW - données brutes non transformées';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Zone CLEANED - données nettoyées, validées, dédoublonnées';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Zone BUSINESS - modèle en étoile prêt pour le BI';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS monitoring
# MAGIC COMMENT 'Tables de logs, métriques qualité, audit';
# MAGIC
# MAGIC -- Vérification : lister tous les schémas créés
# MAGIC SHOW SCHEMAS IN ecommerce_lakehouse;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création du volume dans le schéma bronze : c'est notre "landing zone"
# MAGIC -- où seront déposés les fichiers bruts avant ingestion (CSV, JSON)
# MAGIC CREATE VOLUME IF NOT EXISTS ecommerce_lakehouse.bronze.landing_zone
# MAGIC COMMENT 'Zone de dépôt des fichiers sources bruts (CSV/JSON) avant ingestion';
# MAGIC
# MAGIC -- Vérification
# MAGIC SHOW VOLUMES IN ecommerce_lakehouse.bronze;

# COMMAND ----------

