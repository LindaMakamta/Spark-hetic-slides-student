import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TAXI_PATH = "data/datasets/yellow_tripdata_2024-01.parquet"
ZONES_PATH = "data/datasets/taxi_zone_lookup.csv"

# Dossiers de travail de la démo.
STREAM_IN = "data/datasets/stream_in"        # source surveillée par le flux
PARTS_DIR = "data/datasets/stream_parts"     # réservoir de fichiers à déposer
CHECKPOINT = "data/datasets/stream_ckpt"     # état du flux (offsets, agrégats)

NB_LOTS = 6  # nombre de fichiers que l'on déposera, un par micro-batch

def preparer_fichiers(spark):
    """Découpe le Parquet taxi en NB_LOTS petits fichiers rangés dans PARTS_DIR.
    On garde le schéma des courses : la source streaming le réutilisera."""
    for d in (STREAM_IN, PARTS_DIR, CHECKPOINT):
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(STREAM_IN, exist_ok=True)
    
    courses = (
        spark.read.parquet(TAXI_PATH)
        .filter(F.col("fare_amount") > 0)
        .select("PULocationID", "total_amount", "tpep_pickup_datetime")
        # On réduit le volume pour une démo rapide et lisible.
        .limit(120000)
        .repartition(NB_LOTS)  # NB_LOTS partitions = NB_LOTS fichiers à déposer
    )
    courses.write.mode("overwrite").parquet(PARTS_DIR)
    print(f"  Réservoir préparé dans {PARTS_DIR} ({NB_LOTS} fichiers).")
    # On retient le schéma pour le déclarer à la source streaming (obligatoire
    # pour une source "fichier" : le flux ne peut pas inférer sur un dossier vide).
    return spark.read.parquet(PARTS_DIR).schema

def main():
    spark = (
        SparkSession.builder
        .appName("J3 - Structured Streaming (dossier)")
        .master("local[*]")
        # Peu de partitions de shuffle : la démo est petite, on évite 200 tâches.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== 1. Préparation des fichiers à streamer ===")
    schema = preparer_fichiers(spark)
    
     # Petite table des zones, lue en batch classique (pas en streaming).
    zones = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(ZONES_PATH)
        .select("LocationID", "Borough")
    )

    print("\n=== 2. Définition du flux (readStream) ===")
    # readStream : même API que read, mais en mode flux. On surveille STREAM_IN.
    # maxFilesPerTrigger=1 : un fichier = un micro-batch (pour bien voir l'effet).
    flux = (
        spark.readStream
        .schema(schema)             # schéma obligatoire pour une source fichier
        .option("maxFilesPerTrigger", 1)
        .parquet(STREAM_IN)
    )
    
    
    # Exactement le même code DataFrame que si on était en mode batch : join + agg
    
    agrege = (
        flux.join(zones, flux["PULocationID"] == zones["LocationID"], "left")
        .groupBy("Borough")
        .agg(
            F.count("*").alias("nb_courses"),
            F.round(F.sum("total_amount"), 2).alias("revenu_cumule"),
        )
    )
    
    print("\n=== 3. Démarrage du flux (writeStream vers la console) ===")
    # outputMode("complete") : on réaffiche tout le tableau agrégé à chaque batch
    # (nécessaire pour une agrégation sans watermark). Sink = console.
    
    requete = (
        agrege.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )
    
    requete.awaitTermination(timeout=3600)
    requete.stop()
    
    spark.stop()
    
    for d in (STREAM_IN, PARTS_DIR, CHECKPOINT):
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    main()