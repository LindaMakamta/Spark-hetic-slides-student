import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

TAXI_PATH = "data/datasets/yellow_tripdata_2024-01.parquet"

def chrono(label, action):
    """Exécute une action (fonction sans argument) et mesure le temps."""
    debut = time.time()
    resultat = action()
    duree = time.time() - debut
    print(f"  {label} : {duree:.2f} s")
    return resultat, duree

def construire_df(spark):
    """DataFrame "coûteux" : lecture + nettoyage + colonnes dérivées.
    C'est ce calcul que le cache nous évitera de refaire."""
    courses = spark.read.parquet(TAXI_PATH)
    return (
        courses
        .filter((F.col("fare_amount") > 0) & (F.col("trip_distance") > 0))
        .withColumn(
            "duree_min",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
        )
        .withColumn("prix_par_km", F.col("fare_amount") / (F.col("trip_distance") * 1.60934))
        .filter(F.col("duree_min") > 0)
    )
    
def main():
    spark = (
        SparkSession.builder
        .appName("J3 - Cache demo")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Spark UI : http://localhost:4040 (onglet Storage) ===\n")
    
    # -----------------------------------------------------------------------
    # 1. SANS cache : chaque action relit le Parquet et refait tout le calcul.
    #    On enchaîne trois actions et on additionne les temps.
    # -----------------------------------------------------------------------
    print("=== 1. SANS cache : trois actions, tout est recalculé à chaque fois ===")
    
    df = construire_df(spark)
    _, t_a = chrono("count() #1   ", lambda: df.count())
    _, t_b = chrono("agg moyenne  ", lambda: df.agg(F.avg("prix_par_km")).collect())
    _, t_c = chrono("agg duree max", lambda: df.agg(F.max("duree_min")).collect())
    _, t_d = chrono("count() #2  ", lambda: df.count())
    total_sans = t_a + t_b + t_c + t_d
    print(f"  Total sans cache : {total_sans:.2f} s\n")
    
    # -----------------------------------------------------------------------
    # 2. AVEC cache : on demande à Spark de garder le résultat en mémoire.
    #    cache() est paresseux : la matérialisation a lieu à la PREMIÈRE action.
    #    Les suivantes lisent la version en mémoire, sans relire le Parquet.
    # -----------------------------------------------------------------------
    print("=== 2. AVEC cache : le DataFrame est matérialisé une fois ===")
    df2 = construire_df(spark).cache()
    
    # Première action : elle calcule ET remplit le cache (un peu plus lente).
    _, t_d = chrono("count() #1 (remplit le cache)", lambda: df2.count())
    # Actions suivantes : servies depuis la mémoire, beaucoup plus rapides.
    _, t_e = chrono("agg moyenne  (depuis le cache)", lambda: df2.agg(F.avg("prix_par_km")).collect())
    _, t_f = chrono("agg duree max(depuis le cache)", lambda: df2.agg(F.max("duree_min")).collect())
    _, t_g = chrono("count() #2 (depuis le cache) ", lambda: df.count())
    total_avec = t_d + t_e + t_f + t_g
    print(f"  Total avec cache : {total_avec:.2f} s\n")
    
    df2.unpersist()
    
     # -----------------------------------------------------------------------
    # 4. Les niveaux de stockage : persist() permet de choisir où stocker.
    #    cache() = persist(MEMORY_AND_DISK) pour un DataFrame. On peut viser
    #    MEMORY_ONLY (rapide, mais perdu si plus de place) ou DISK_ONLY, etc.
    # -----------------------------------------------------------------------
    print("=== 4. Choisir un niveau avec persist(StorageLevel) ===")
    df3 = construire_df(spark).persist(StorageLevel.MEMORY_AND_DISK)
    df3.count()  # matérialise
    print("  df3 persiste en MEMORY_AND_DISK (déborde sur disque si la RAM manque).")
    print("  Voir l'onglet Storage de la Spark UI : taille en mémoire, fraction cachée.")

    input("\nVa voir l'onglet Storage (http://localhost:4040), puis Entrée pour quitter...")
    df3.unpersist()
    
    spark.stop()
    

if __name__ == "__main__":
    main()