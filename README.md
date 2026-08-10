# 🛒 E-Commerce Lakehouse — Databricks End-to-End Data Engineering Project

> Pipeline Data Engineering complet reproduisant l'architecture Lakehouse d'une plateforme e-commerce, implémentant l'architecture Medallion (Bronze/Silver/Gold) sur **Databricks Free Edition**, avec ingestion batch/streaming/CDC, contrôles de qualité de données, orchestration automatisée en Infrastructure as Code, et monitoring en production.

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
- [Infrastructure as Code](#-infrastructure-as-code)
- [Dashboard BI](#-dashboard-bi)
- [Incidents réels & résolutions](#-incidents-réels--résolutions)
- [Installation & Setup](#-installation--setup)
- [Résultats clés](#-résultats-clés)
- [Roadmap](#-roadmap)

---

## 🎯 Vue d'ensemble

Ce projet simule un environnement Data Engineering de production pour une plateforme e-commerce fictive, couvrant l'intégralité du cycle de vie de la donnée :
```
Données brutes → Ingestion (Batch + Streaming + CDC) → Nettoyage & Data Quality 
    → Modélisation métier → Dashboard BI → Orchestration (IaC) → Monitoring
```
**Pourquoi ce projet ?** Développé pour maîtriser Databricks de manière pratique et acquérir une expérience directement transférable en entreprise, avec des cas d'usage réalistes — y compris la résolution de trois incidents de production réels (voir [section dédiée](#-incidents-réels--résolutions)).

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
      Declarative Automation Bundles (Infrastructure as Code, targets dev/prod)
      Databricks Workflows (DAG 4 tâches, retries, schedule quotidien, alerting email)
```

## 🔧 Stack technique

| Catégorie | Technologies |
|---|---|
| Plateforme | Databricks Free Edition (Serverless Compute) |
| Stockage & Format | Delta Lake, Unity Catalog, Volumes |
| Traitement | PySpark, Spark Structured Streaming, Auto Loader |
| Lakeflow | Declarative Pipelines, AUTO CDC |
| Orchestration | Databricks Workflows (Jobs, DAG, Schedules) |
| Infrastructure as Code | Declarative Automation Bundles, Databricks CLI |
| BI | Databricks SQL, Genie AI Dashboards |
| Langages | Python, SQL, YAML |
| Génération de données | Faker (données synthétiques réalistes) |
| Versioning | Git, GitHub |

---

## 📂 Structure du projet
```
ecommerce-lakehouse-databricks/
├── README.md
├── notebooks/
│   ├── 00_setup_unity_catalog.py         # Création catalogue, schémas, volume
│   ├── 01_generate_synthetic_data.py     # Génération customers + orders (Faker)
│   ├── 01b_generate_order_items.py       # Génération + ingestion order_items
│   ├── 02b_bronze_ingestion.py           # Ingestion Bronze (customers, orders)
│   ├── 03_silver_transformation.py       # Dédoublonnage, Data Quality, quarantaine
│   ├── 04_gold_modeling.py               # fact_sales, agg_daily_revenue, dim_customer
│   ├── 05_clickstream_generation.py      # Génération données streaming
│   ├── 06_autoloader_clickstream.py      # Ingestion Auto Loader (incrémentale)
│   ├── 07_cdc_generation.py              # Simulation d'événements CDC
│   ├── 08_cdc_scd2_processing.py         # Implémentation SCD Type 2 (MERGE)
│   ├── 09_pipeline_health_check.py       # Health checks automatisés (quality gate)
│   └── 10_optimization.py                # Liquid Clustering, VACUUM, benchmarks
├── bundle/                                # Infrastructure as Code
│   ├── databricks.yml                     # Configuration principale, targets dev/prod
│   └── resources/
│       └── ecommerce_pipeline.job.yml     # Définition du Job en YAML
└── dashboard_queries/                     # Requêtes SQL du dashboard BI
```

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
| Caractéristique | Configuration |
|---|---|
| Compute | Serverless |
| Retries | 3 tentatives automatiques |
| Schedule | Quotidien à 03:00 AM (UTC), cron `0 0 3 * * ?` |
| Notifications | Email en cas d'échec (`on_failure`) |
| Durée moyenne d'exécution | ~2m30s |

### Health Checks automatisés

Le pipeline se surveille lui-même via 2 contrôles à chaque exécution :
1. **Taux de rejet Data Quality** (seuil d'alerte : 5%)
2. **Fraîcheur des données** (via Delta Transaction Log, seuil : 26h)

Si un seuil est dépassé, la tâche `health_check` lève une exception, faisant échouer intentionnellement le run et déclenchant la notification email — transformant un simple contrôle en véritable **quality gate**.

Les métriques sont historisées dans `monitoring.pipeline_health_checks` pour suivi de tendance.

---

## 🏗️ Infrastructure as Code

Après avoir construit et validé tout le pipeline via l'interface Databricks, le Job a été migré vers **Declarative Automation Bundles** (anciennement Databricks Asset Bundles), pour le rendre déployable en une commande et versionné dans Git plutôt que configuré à la souris.

### Structure du Bundle

```yaml
resources:
  jobs:
    ecommerce_lakehouse_pipeline:
      schedule:
        quartz_cron_expression: "0 0 3 * * ?"
        timezone_id: "UTC"
      tasks:
        - task_key: ingest_bronze
          notebook_task: ...
        - task_key: transform_silver
          depends_on: [ingest_bronze]
          ...
```

### Deux environnements distincts

| Target | Mode | Comportement |
|---|---|---|
| `dev` | `development` | Schedule automatiquement mis en pause par Databricks (sécurité anti-exécution accidentelle) |
| `prod` | `production` | Schedule actif, cron à 3h du matin |

Les deux Jobs coexistent volontairement sur le Workspace — le `dev` permet de tester une modification du pipeline avant de la déployer en `prod`, exactement le pattern utilisé en entreprise.

### Commandes de déploiement

```bash
databricks auth login --host <workspace-url>
databricks bundle validate
databricks bundle deploy --target dev     # environnement de test
databricks bundle deploy --target prod    # environnement de production
```

---

## 🔄 Lakeflow AUTO CDC

Pour comparer avec l'implémentation manuelle du SCD Type 2 (voir `notebooks/08_cdc_scd2_processing.py`), la même logique a été reproduite avec `AUTO CDC` de Lakeflow Declarative Pipelines — l'approche déclarative moderne recommandée par Databricks.

### Comparaison des deux approches

| Aspect | Implémentation manuelle | AUTO CDC |
|---|---|---|
| Lignes de code | ~60 lignes (boucle + MERGE + sanity check) | ~15 lignes déclaratives |
| Gestion de l'ordre des événements | `Window` + boucle manuelle sur `event_sequence` | `sequence_by` natif |
| Résultat final (test sur ORD-0000148) | `delivered`, LSN 1005 | `delivered`, LSN 1005 — identique |

### Code

```python
dp.create_streaming_table(
    name="orders_status_history_autocdc",
    comment="Historique complet des statuts (SCD Type 2)"
)

dp.create_auto_cdc_flow(
    target="orders_status_history_autocdc",
    source="cdc_orders_source",
    keys=["order_id"],
    sequence_by=F.col("change_lsn"),
    stored_as_scd_type=2
)
```

### Observation technique

AUTO CDC utilise directement la valeur du `sequence_by` (ici `change_lsn`) comme borne `__START_AT`/`__END_AT`, plutôt que le timestamp de l'événement source. C'est plus robuste que l'implémentation manuelle : les timestamps sources de ce projet ne sont pas toujours strictement croissants (voir Incident 3 dans la section précédente sur un problème similaire), alors que le LSN, lui, l'est garanti par construction.


## 📊 Dashboard BI

**"E-commerce Lakehouse - Executive Dashboard"** publié sur Databricks SQL, avec 4 visualisations :

| Visualisation | Type | Insight clé |
|---|---|---|
| Daily Revenue Trends | Line chart | Évolution du CA net sur 731 jours |
| Customer Churn Distribution | Donut chart | 59% des clients à risque de churn |
| Total Lifetime Value by Segment | Bar chart | Concentration de valeur sur le segment HIGH_VALUE (loi de Pareto) |
| Revenue by Product Category | Bar chart | Electronics = catégorie dominante (~9M€) |

---

## 🐛 Incidents réels & résolutions

Trois incidents de production ont été rencontrés et résolus durant le développement — chacun a été formateur d'une façon différente.

### Incident 1 : conflit Liquid Clustering / partitioning

**Contexte** : Après avoir migré `gold.fact_sales` du partitionnement classique (`PARTITION BY`) vers le **Liquid Clustering** (optimisation), le run automatique planifié de la nuit suivante a échoué.

**Diagnostic** : Le notebook source `04_gold_modeling` tentait encore de réécrire la table avec l'ancien schéma de partitionnement (`.partitionBy("order_year_month")`), entrant en conflit avec la nouvelle configuration :
```
DELTA_CLUSTERING_TO_PARTITIONED_TABLE_WITH_NON_EMPTY_CLUSTERING_COLUMNS
```

**Résolution** : Mise à jour du notebook pour être cohérent avec la stratégie d'organisation physique des données.

**Leçon** : Toute optimisation manuelle appliquée directement sur une table doit être répercutée dans le code source versionné du pipeline.

### Incident 2 : notebooks introuvables après un déplacement de fichiers

**Contexte** : En nettoyant le Workspace pour préparer l'export vers GitHub, tous les notebooks ont été déplacés dans un sous-dossier `ecommerce_lakehouse_notebooks/`. Le run automatique suivant a échoué en 4 secondes.

**Diagnostic** :
```
Unable to access the notebook "/Workspace/Users/.../02b_bronze_ingestion" in the workspace.
```
Les 4 tâches du Job pointaient encore vers l'ancien chemin, devenu invalide.

**Résolution** : Mise à jour des 4 chemins dans la configuration des tâches.

**Leçon** : Tout déplacement de fichiers doit être immédiatement répercuté dans la configuration du Job qui les référence par chemin absolu.

### Incident 3 : trigger périodique au lieu d'un cron précis

**Contexte** : Lors de la migration du Job vers un Declarative Automation Bundle, la syntaxe `trigger.periodic` (interval de 24h) a été utilisée par erreur au lieu de `schedule.quartz_cron_expression`.

**Diagnostic** : Aucune erreur — le pipeline fonctionnait, mais s'exécutait à un horaire flottant basé sur le moment du déploiement plutôt qu'à 3h du matin précises, comme documenté et voulu.

**Résolution** : Remplacement par `schedule.quartz_cron_expression: "0 0 3 * * ?"`.

**Leçon** : Un incident n'est pas toujours une erreur bloquante — vérifier que le comportement réel correspond bien à l'intention documentée est tout aussi important que de corriger les crashs.

### Incident 4 : dossier de notebooks déplacé à la racine du Workspace

**Contexte** : Trois jours après avoir résolu l'Incident 2, le même symptôme est réapparu : le run automatique a échoué trois nuits de suite avec la même erreur `ResourceNotFound` / notebook introuvable.

**Diagnostic** : Cette fois, la cause était différente. Le dossier `ecommerce_lakehouse_notebooks` s'était retrouvé à la racine du Workspace (`/Workspace/ecommerce_lakehouse_notebooks/`) au lieu de son emplacement habituel dans l'espace utilisateur (`/Workspace/Users/.../ecommerce_lakehouse_notebooks/`), probablement suite à une manipulation lors de la mise en place du Bundle. Le fichier existait bien, mais pas au chemin absolu attendu par le Job — ce qui a d'abord semblé être une répétition de l'Incident 2, avant de vérifier le chemin réel caractère par caractère.

**Résolution** : Déplacement du dossier vers son emplacement correct dans l'espace utilisateur.

**Leçon** : Un même message d'erreur peut avoir des causes racines différentes. Avant de réappliquer une correction déjà connue, il faut vérifier le nouveau contexte plutôt que de supposer que c'est exactement le même problème — ici, comparer le chemin configuré dans le Job avec le chemin réel du fichier, caractère par caractère, a permis d'identifier la vraie cause.

---

## 🚀 Installation & Setup

### Prérequis
- Compte [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (gratuit, sans carte bancaire)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) (pour le déploiement via Bundle)

### Étapes
1. Créer un catalogue Unity Catalog `ecommerce_lakehouse` avec les schémas `bronze`, `silver`, `gold`, `monitoring` (voir `notebooks/00_setup_unity_catalog.py`)
2. Exécuter les notebooks dans l'ordre numérique (01 → 10)
3. Authentifier le CLI : `databricks auth login --host <workspace-url>`
4. Déployer le pipeline : `databricks bundle deploy --target prod` (depuis le dossier `bundle/`)
5. Publier le Dashboard à partir des requêtes du dossier `dashboard_queries/`

---

## 📈 Résultats clés

- **~55 000 lignes** traitées de bout en bout, tous flux confondus
- **0% de duplication** après double vérification empirique du comportement Auto Loader
- **99.95% de taux de validité** des données en Silver (11 rejets sur 20 001)
- **~2m30s** de temps d'exécution total du pipeline orchestré
- **4 incidents de production réels** diagnostiqués et résolus de façon autonome
- **Infrastructure as Code** avec séparation d'environnements dev/prod

---

## 🗺️ Roadmap

- [x] Declarative Automation Bundles (Infrastructure as Code)
- [x] Migration vers Lakeflow Declarative Pipelines (ex-DLT) avec `AUTO CDC`
- [ ] MLflow pour un modèle de prédiction de churn
- [ ] Lakehouse Monitoring natif pour la détection de dérive de données
- [ ] Row-level security via Unity Catalog

---

## 👤 Auteur

**Ahmed Ala Eddine Baatour**
Data Engineer — Projet réalisé dans le cadre d'une formation pratique approfondie sur Databricks

---