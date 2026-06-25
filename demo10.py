from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

TAXI_PATH = "data/datasets/yellow_tripdata_2024-01.parquet"

def main():
    spark = (
        SparkSession.builder
        .appName("J3 - MLlib : prediction du pourboire")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== 1. Charger et nettoyer les donnees ===")
    courses = spark.read.parquet(TAXI_PATH)
    
    # On ne garde que des courses "raisonnables" : montants positifs, distance
    # plausible, pourboire non aberrant. Un modèle n'apprend rien de bon sur du
    # bruit, donc le nettoyage fait partie du travail ML.
    donnees = (
        courses
        .select(
            "trip_distance", "fare_amount", "passenger_count",
            "payment_type", "tip_amount",
        )
        .filter(
            (F.col("fare_amount") > 0) & (F.col("fare_amount") < 200)
            & (F.col("trip_distance") > 0) & (F.col("trip_distance") < 100)
            & (F.col("tip_amount") >= 0) & (F.col("tip_amount") < 100)
            & (F.col("passenger_count") >= 1)
        )
        .na.drop()
    )
    print(f"  Lignes utilisables pour l'apprentissage : {donnees.count():,}\n")
    
    print("=== 2. Decoupage train / test (80 / 20) ===")
    train, test = donnees.randomSplit([0.8, 0.2], seed=42)
    
    print(f"  train : {train.count():,} lignes | test : {test.count():,} lignes\n")
    
    
    print("=== 3. Construire le pipeline (VectorAssembler -> LinearRegression) ===")
    colonnes_entree = ["trip_distance", "fare_amount", "passenger_count", "payment_type"]
    
    assembleur = VectorAssembler(
        inputCols=colonnes_entree,
        outputCol="features",
    )
    
    regression = LinearRegression(
        featuresCol="features",
        labelCol="tip_amount",   # ce qu'on cherche à prédire
        predictionCol="prediction",
    )
    
    
    pipeline = Pipeline(stages=[assembleur, regression])
    print(f"  Features utilisees : {colonnes_entree}")
    print("  Cible : tip_amount\n")
    
    print("=== 4. Entrainement du modele (fit) ===")
    modele = pipeline.fit(train)
    lr_model = modele.stages[-1]  # le modèle de régression est le dernier stage
    print(f"  Coefficients : {lr_model.coefficients}")
    print(f"  Ordonnee a l'origine : {lr_model.intercept:.4f}\n")
    
    print("=== 5. Predictions sur le jeu de test (transform) ===")
    predictions = modele.transform(test)
    predictions.select(
        "trip_distance", "fare_amount", "payment_type",
        F.round("tip_amount", 2).alias("pourboire_reel"),
        F.round("prediction", 2).alias("pourboire_predit"),
    ).show(10, truncate=False)

    print("=== 6. Evaluation du modele ===")
    rmse = RegressionEvaluator(
        labelCol="tip_amount", predictionCol="prediction", metricName="rmse"
    ).evaluate(predictions)
    r2 = RegressionEvaluator(
        labelCol="tip_amount", predictionCol="prediction", metricName="r2"
    ).evaluate(predictions)
    print(f"  RMSE (erreur moyenne en dollars) : {rmse:.3f}")
    print(f"  R2   (part de variance expliquee) : {r2:.3f}")
    
    
    spark.stop()

if __name__ == "__main__":
    main()