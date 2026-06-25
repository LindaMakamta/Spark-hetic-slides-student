import os
import time
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TAXI_PATH = "data/datasets/yellow_tripdata_2024-01.parquet"
BENCH_DIR = "data/datasets/_bench"

def taille_dossier(path):
    """Taille totale en Mo d'un dossier de sortie Spark."""
    total = 0
    for racine, _, fichiers in os.walk(path):
        for f in fichiers:
            total += os.path.getsize(os.path.join(racine, f))
    return total / (1024 * 1024)


def chrono_lecture_colonne(spark, fmt, path, lecteur):
    """Mesure le temps pour lire UNE colonne et l'agréger."""
    debut = time.time()
    df = lecteur(path)
    # On ne lit qu'une colonne : Parquet ne va lire que celle-ci sur le disque.
    resultat = df.select(F.sum("total_amount").alias("somme")).collect()[0]["somme"]
    duree = time.time() - debut
    print(f"  {fmt:8s} | somme total_amount = {resultat:,.0f} | lu en {duree:5.2f} s")
    return duree

def main():
    spark = (
        SparkSession.builder
        .appName("formats_benchmark")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Repartir d'un dossier propre.
    if os.path.exists(BENCH_DIR):
        shutil.rmtree(BENCH_DIR)
    os.makedirs(BENCH_DIR, exist_ok=True)
    

    print("=== 1. Charger un échantillon des courses taxi ===")
        # On prend un échantillon pour que la demo reste rapide (CSV et JSON sont lents).
    df = spark.read.parquet(TAXI_PATH).sample(fraction=0.2, seed=42)
    # On force une seule partition en sortie pour des tailles de fichier comparables.
    df = df.repartition(1).cache()
    nb = df.count()  # matérialise le cache
    print(f"Lignes de l'échantillon : {nb:,}")
    
    

    csv_path = os.path.join(BENCH_DIR, "csv")
    json_path = os.path.join(BENCH_DIR, "json")
    parquet_path = os.path.join(BENCH_DIR, "parquet")

    print("\n=== 2. Écrire les mêmes données dans trois formats ===")
    df.write.mode("overwrite").option("header", True).csv(csv_path)
    df.write.mode("overwrite").json(json_path)
    # Parquet compressé en snappy par défaut.
    df.write.mode("overwrite").parquet(parquet_path)


    print("\n=== 3. Taille sur disque (compression et stockage colonnaire) ===")
    t_csv = taille_dossier(csv_path)
    t_json = taille_dossier(json_path)
    t_parquet = taille_dossier(parquet_path)
    print(f"  CSV     : {t_csv:7.2f} Mo")
    print(f"  JSON    : {t_json:7.2f} Mo")
    print(f"  Parquet : {t_parquet:7.2f} Mo")
    if t_parquet > 0:
        print(f"  >> Parquet est environ {t_csv / t_parquet:.1f}x plus petit que le CSV.")
        
        
    print("\n=== 4. Temps de lecture d'UNE seule colonne (total_amount) ===")
    print("  Parquet ne lit que la colonne demandée (colonnaire), CSV et JSON lisent tout.")
    chrono_lecture_colonne(
        spark, "CSV", csv_path,
        lambda p: spark.read.option("header", True).option("inferSchema", True).csv(p),
    )
    chrono_lecture_colonne(
        spark, "JSON", json_path,
        lambda p: spark.read.json(p),
    )
    chrono_lecture_colonne(
        spark, "Parquet", parquet_path,
        lambda p: spark.read.parquet(p),
    )

    print("\n=== 5. Predicate pushdown : Parquet filtre dès la lecture ===")
    # explain montre un PushedFilters sur le scan Parquet : le filtre descend au format.
    filtre = spark.read.parquet(parquet_path).filter(F.col("total_amount") > 100)
    print("Plan d'exécution (chercher 'PushedFilters' sur le scan Parquet) :")
    filtre.explain()

    print("\nAstuce : le dossier temporaire data/datasets/_bench/ peut être supprimé.")
    print("Démo formats_benchmark terminée.")
    spark.stop()


if __name__ == "__main__":
    main()
