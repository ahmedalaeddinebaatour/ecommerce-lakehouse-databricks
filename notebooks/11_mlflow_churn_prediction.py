# Databricks notebook source
CATALOG = "ecommerce_lakehouse"

df_customers = spark.table(f"{CATALOG}.gold.dim_customer")

print(f"Nombre de clients : {df_customers.count()}")
df_customers.printSchema()

print("\n--- Répartition de la target (churn_status) ---")
df_customers.groupBy("churn_status").count().show()

# COMMAND ----------

from pyspark.sql import functions as F

# ============================================================
# Préparation du dataset d'entraînement
# ============================================================

df_ml_ready = (
    df_customers
    .filter(F.col("churn_status") != "NEVER_PURCHASED")  # on exclut les clients sans historique
    .withColumn("is_at_risk", F.when(F.col("churn_status") == "AT_RISK", 1).otherwise(0))
    .select(
        "customer_id",
        "total_orders",
        "lifetime_value",
        "days_since_last_order",
        "customer_segment",
        "is_at_risk"
    )
)

print(f"Nombre de clients dans le dataset ML : {df_ml_ready.count()}")
print("\n--- Répartition de la target binaire ---")
df_ml_ready.groupBy("is_at_risk").count().show()

print("\n--- Vérification des valeurs manquantes ---")
df_ml_ready.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_ml_ready.columns]).show()

df_ml_ready.show(5)

# COMMAND ----------

# MAGIC %pip install scikit-learn mlflow

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import mlflow
import mlflow.sklearn
from pyspark.sql import functions as F
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix

CATALOG = "ecommerce_lakehouse"

# ============================================================
# Reconstruction du dataset (au cas où la session a redémarré)
# ============================================================
df_customers = spark.table(f"{CATALOG}.gold.dim_customer")

df_ml_ready = (
    df_customers
    .filter(F.col("churn_status") != "NEVER_PURCHASED")
    .withColumn("is_at_risk", F.when(F.col("churn_status") == "AT_RISK", 1).otherwise(0))
    .select(
        "customer_id", "total_orders", "lifetime_value",
        "days_since_last_order", "customer_segment", "is_at_risk"
    )
)

# ============================================================
# Conversion Spark -> pandas (scikit-learn ne travaille qu'en pandas)
# ============================================================
pdf = df_ml_ready.toPandas()

# Encodage de la variable catégorielle customer_segment
le = LabelEncoder()
pdf["customer_segment_encoded"] = le.fit_transform(pdf["customer_segment"])

feature_cols = ["total_orders", "lifetime_value", "customer_segment_encoded"]
X = pdf[feature_cols]
y = pdf["is_at_risk"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

print(f"Taille train : {len(X_train)} | Taille test : {len(X_test)}")

# ============================================================
# Entraînement + tracking MLflow
# ============================================================
mlflow.set_experiment("/Users/ahmedalaeddinebaatour@gmail.com/churn_prediction_experiment")

with mlflow.start_run(run_name="logistic_regression_baseline"):

    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    # Log des paramètres
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("features", feature_cols)
    mlflow.log_param("train_size", len(X_train))
    mlflow.log_param("test_size", len(X_test))

    # Log des métriques
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)

    # Log du modèle lui-même
    mlflow.sklearn.log_model(model, "model", input_example=X_train.iloc[:5])

    print(f"✅ Run MLflow terminé")
    print(f"Accuracy  : {accuracy:.3f}")
    print(f"Precision : {precision:.3f}")
    print(f"Recall    : {recall:.3f}")
    print(f"F1 Score  : {f1:.3f}")
    print(f"\nMatrice de confusion:\n{confusion_matrix(y_test, y_pred)}")