# 🛒 E-Commerce Lakehouse — Databricks End-to-End Data Engineering Project

> Pipeline Data Engineering complet reproduisant l'architecture Lakehouse d'une plateforme e-commerce, implémentant l'architecture Medallion (Bronze/Silver/Gold) sur **Databricks Free Edition**, avec ingestion batch/streaming/CDC, contrôles de qualité de données, orchestration automatisée et monitoring en production.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white)

---

## 📌 Table des matières

- [Vue d'ensemble](#-vue-densemble)
- [Architecture](#-architecture)
- [Stack technique](#-stack-technique)
- [Structure du projet](#-structure-du-projet)
- [Modèle de données](#-modèle-de-données)
- [Data Quality](#-data-quality)
- [Orchestration & Monitoring](#-orchestration--monitoring)
- [Dashboard BI](#-dashboard-bi)
- [Incident réel & résolution](#-incident-réel--résolution)
- [Installation & Setup](#-installation--setup)
- [Résultats clés](#-résultats-clés)
- [Roadmap](#-roadmap)

---

## 🎯 Vue d'ensemble

Ce projet simule un environnement Data Engineering de production pour une plateforme e-commerce fictive, couvrant l'intégralité du cycle de vie de la donnée :

```
Données brutes → Ingestion (Batch + Streaming + CDC) → Nettoyage & Data Quality 
    → Modélisation métier → Dashboard BI → Orchestration automatisée → Monitoring
```

**Pourquoi ce projet ?** Développé pour maîtriser Databricks de manière pratique et acquérir une expérience directement transférable en entreprise, avec des cas d'usage réalistes (y compris la résolution d'un incident de production réel — voir [section dédiée](#-incident-réel--résolution)).

---

## 🏗️ Architecture

```
                                   ┌────────────────────────────────────────┐
                                   │              SOURCES DE DONNÉES          │
                                   │  CSV (Faker) | JSON Clickstream | CDC    │
                                   └───────────────────┬──────────────────────┘
                                                        │
                     ┌──────────────────────────────────┼──────────────────────────────────┐
                     ▼                                  ▼                                  ▼
             (Batch Load)                      (Auto Loader Streaming)             (CDC simulé)
                     │                                  │                                  │
                     ▼                                  ▼                                  ▼
      ╔═══════════════════════════════════════════════════════════════════════════════════════╗
      ║                                  ZONE BRONZE (RAW)                                       ║
      ║   bronze.customers | bronze.orders | bronze.order_items                                  ║
      ║   bronze.clickstream_events (streaming) | bronze.cdc_orders_raw                           ║
      ║   Colonnes d'audit : _ingest_timestamp, _source_file, _batch_id                            ║
      ╚═══════════════════════════════════════╦═══════════════════════════════════════════════╝
                                                │  PySpark : dédoublonnage, Data Quality
                                                ▼
      ╔═══════════════════════════════════════════════════════════════════════════════════════╗
      ║                                  ZONE SILVER (CLEANED)                                   ║
      ║  silver.customers | silver.orders | silver.order_items                                   ║
      ║  silver.orders_status_history (SCD Type 2)                                                ║
      ║  monitoring.orders_quarantine | monitoring.pipeline_audit_log                             ║
      ╚═══════════════════════════════════════╦═══════════════════════════════════════════════╝
                                                │  Jointures, agrégations, logique métier
                                                ▼
      ╔═══════════════════════════════════════════════════════════════════════════════════════╗
      ║                                  ZONE GOLD (BUSINESS)                                    ║
      ║  gold.fact_sales (Liquid Clustering) | gold.agg_daily_revenue | gold.dim_customer         ║
      ╚═══════════════════════════════════════╦═══════════════════════════════════════════════╝
                                                │
                     ┌──────────────────────────┼──────────────────────────┐
                     ▼                          ▼                          ▼
              Databricks SQL              monitoring.pipeline_       Unity Catalog
              Dashboard (Genie)           health_checks              (gouvernance)

      ══════════════════ ORCHESTRATION & GOUVERNANCE (TRANSVERSAL) ══════════════════
      Unity Catalog (3-level namespace, Volumes, lineage)
      Databricks Workflows (DAG 4 tâches, retries, schedule quotidien, alerting email)
```

---

## 🔧 Stack technique

| Catégorie | Technologies |
|---|---|
| Plateforme | Databricks Free Edition (Serverless Compute) |
| Stockage & Format | Delta Lake, Unity Catalog, Volumes |
| Traitement | PySpark, Spark Structured Streaming, Auto Loader |
| Orchestration | Databricks Workflows (Jobs, DAG, Schedules) |
| BI | Databricks SQL, Genie AI Dashboards |
| Langages | Python, SQL |
| Génération de données | Faker (données synthétiques réalistes) |

---

## 📂 Structure du projet

```
Workspace/
├── 00_setup_unity_catalog.py         # Création catalogue, schémas, volume
├── 01_generate_synthetic_data.py     # Génération customers + orders (Faker)
├── 01b_generate_order_items.py       # Génération + ingestion order_items
├── 02b_bronze_ingestion.py           # Ingestion Bronze (customers, orders)
├── 03_silver_transformation.py       # Dédoublonnage, Data Quality, quarantaine
├── 04_gold_modeling.py               # fact_sales, agg_daily_revenue, dim_customer
├── 05_clickstream_generation.py      # Génération données streaming
├── 06_autoloader_clickstream.py      # Ingestion Auto Loader (incrémentale)
├── 07_cdc_generation.py              # Simulation d'événements CDC
├── 08_cdc_scd2_processing.py         # Implémentation SCD Type 2 (MERGE)
├── 09_pipeline_health_check.py       # Health checks automatisés (quality gate)
├── 10_optimization.py                # Liquid Clustering, VACUUM, benchmarks
└── dashboard_queries/                # Requêtes SQL du dashboard BI
```

> 📝 Note : `01b` et `02b` reflètent des itérations réelles du projet (contournement d'un incident de session Databricks) — conservés tels quels par souci de traçabilité authentique du développement.

---

## 🗃️ Modèle de données

### Tables principales par couche

| Couche | Table | Lignes | Description |
|---|---|---|---|
| Bronze | `customers` | 5 010 | Clients bruts (avec anomalies volontaires) |
| Bronze | `orders` | 20 030 | Commandes brutes, partitionnées par statut |
| Bronze | `order_items` | 27 990 | Articles commandés |
| Bronze | `clickstream_events` | 750 | Événements de navigation (via Auto Loader) |
| Bronze | `cdc_orders_raw` | 404 | Événements CDC de changement de statut |
| Silver | `customers` | 5 000 | Clients dédoublonnés et validés |
| Silver | `orders` | 19 990 | Commandes nettoyées (11 rejetées) |
| Silver | `order_items` | 27 990 | Intégrité référentielle validée (0 orpheline) |
| Silver | `orders_status_history` | 404 | Historique SCD Type 2 des statuts |
| Gold | `fact_sales` | 27 990 | Table de faits, Liquid Clustering |
| Gold | `agg_daily_revenue` | 731 | Agrégation quotidienne pour BI |
| Gold | `dim_customer` | 5 000 | Dimension client (churn, segments) |

### Modèle en étoile (Gold)

```
                    dim_customer
                         │
                         │
    dim_product ──── fact_sales ──── dim_date (implicite via order_date)
                         │
                         │
                    dim_seller (implicite via seller_id)
```

---

## ✅ Data Quality

### Règles implémentées sur `silver.orders`

| Règle | Détection |
|---|---|
| `order_id` non NULL | ✅ |
| `customer_id` non NULL | ✅ |
| `order_status` dans la liste des valeurs métier valides | ✅ |
| `order_purchase_timestamp` non postérieur à aujourd'hui | ✅ |

### Pattern de quarantaine

Les lignes invalides ne sont **jamais supprimées silencieusement** — elles sont isolées dans `monitoring.orders_quarantine` avec la raison exacte du rejet, garantissant traçabilité et possibilité de rejouer les règles après correction.

### Logique métier encodée dans la donnée

- `is_revenue_eligible` / `net_revenue` : distinction entre CA brut et CA réel (exclusion des commandes annulées)
- `churn_status` (`ACTIVE` / `AT_RISK` / `NEVER_PURCHASED`)
- `customer_segment` (`HIGH_VALUE` / `MEDIUM_VALUE` / `LOW_VALUE` / `ZERO_REVENUE_ALL_CANCELED` / `NEVER_PURCHASED`)

> 💡 Une incohérence entre `churn_status` et `customer_segment` a été détectée via un sanity check croisé (clients ayant uniquement des commandes annulées), puis corrigée — voir le code pour le détail de cette itération de Data Quality.

---

## ⚙️ Orchestration & Monitoring

### Databricks Workflow : `ecommerce_lakehouse_pipeline`

```
ingest_bronze ──► transform_silver ──► build_gold ──► health_check
```

| Caractéristique | Configuration |
|---|---|
| Compute | Serverless |
| Retries | 3 tentatives automatiques |
| Schedule | Quotidien à 03:00 AM (UTC) |
| Notifications | Email en cas d'échec (`on_failure`) |
| Durée moyenne d'exécution | ~2m30s |

### Health Checks automatisés

Le pipeline se surveille lui-même via 2 contrôles à chaque exécution :
1. **Taux de rejet Data Quality** (seuil d'alerte : 5%)
2. **Fraîcheur des données** (via Delta Transaction Log, seuil : 26h)

Si un seuil est dépassé, la tâche `health_check` lève une exception, faisant échouer intentionnellement le run et déclenchant la notification email — transformant un simple contrôle en véritable **quality gate**.

Les métriques sont historisées dans `monitoring.pipeline_health_checks` pour suivi de tendance.

---

## 📊 Dashboard BI

**"E-commerce Lakehouse - Executive Dashboard"** publié sur Databricks SQL, avec 4 visualisations :

| Visualisation | Type | Insight clé |
|---|---|---|
| Daily Revenue Trends | Line chart | Évolution du CA net sur 731 jours |
| Customer Churn Distribution | Donut chart | 59% des clients à risque de churn |
| Total Lifetime Value by Segment | Bar chart | Concentration de valeur sur le segment HIGH_VALUE (loi de Pareto) |
| Revenue by Product Category | Bar chart | Electronics = catégorie dominante (~9M€) |

---

## 🐛 Incident réel & résolution

Un incident de production a été rencontré et résolu durant le développement, illustrant un cas réel de dette technique :

**Contexte** : Après avoir migré `gold.fact_sales` du partitionnement classique (`PARTITION BY`) vers le **Liquid Clustering** (optimisation Phase 7), le run automatique planifié de la nuit suivante a échoué.

**Diagnostic** : Le notebook source `04_gold_modeling` tentait encore de réécrire la table avec l'ancien schéma de partitionnement (`.partitionBy("order_year_month")`), entrant en conflit avec la nouvelle configuration de clustering :
```
DELTA_CLUSTERING_TO_PARTITIONED_TABLE_WITH_NON_EMPTY_CLUSTERING_COLUMNS
```

**Résolution** : Mise à jour du notebook pour être cohérent avec la stratégie d'organisation physique des données, garantissant l'idempotence du pipeline lors des exécutions futures.

**Leçon retenue** : Toute optimisation manuelle appliquée directement sur une table doit être répercutée dans le code source versionné du pipeline, sous peine de rupture lors de la prochaine exécution automatisée.

---

## 🚀 Installation & Setup

### Prérequis
- Compte [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (gratuit, sans carte bancaire)

### Étapes
1. Créer un catalogue Unity Catalog `ecommerce_lakehouse` avec les schémas `bronze`, `silver`, `gold`, `monitoring` (voir `00_setup_unity_catalog.py`)
2. Exécuter les notebooks dans l'ordre numérique (01 → 10)
3. Créer le Job Workflow `ecommerce_lakehouse_pipeline` avec le DAG décrit ci-dessus
4. Configurer le Schedule et les notifications
5. Publier le Dashboard à partir des requêtes du dossier `dashboard_queries/`

---

## 📈 Résultats clés

- **~55 000 lignes** traitées de bout en bout, tous flux confondus
- **0% de duplication** après double vérification empirique du comportement Auto Loader
- **99.95% de taux de validité** des données en Silver (11 rejets sur 20 001)
- **~2m30s** de temps d'exécution total du pipeline orchestré
- **1 incident de production réel** diagnostiqué et résolu de façon autonome

---

## 🗺️ Roadmap (améliorations futures)

- [ ] Migration vers Lakeflow Declarative Pipelines (ex-DLT) avec `AUTO CDC`
- [ ] Lakehouse Monitoring natif pour la détection de dérive de données
- [ ] Databricks Asset Bundles pour un déploiement CI/CD complet
- [ ] Row-level security via Unity Catalog
- [ ] Intégration MLflow pour un modèle de prédiction de churn

---

## 👤 Auteur

**Ahmed Ala Eddine Baatour**
Data Engineer — Projet réalisé dans le cadre d'une formation pratique approfondie sur Databricks

---

## 📄 License

MIT