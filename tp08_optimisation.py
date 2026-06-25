import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

spark = (
    SparkSession.builder
    .appName("TP08 - Optimisation")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def chrono(label, fonction):
    """Mesure le temps d'exécution d'une fonction et renvoie son résultat."""
    debut = time.perf_counter()
    resultat = fonction()
    duree = time.perf_counter() - debut
    print(f"[{label}] {duree:.2f} s")
    return resultat

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
)

# Desactiver le broadcast automatique pour forcer un sort-merge join (avec shuffle)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def join_classique():
    return (
        courses
        .join(zones, courses.PULocationID == zones.LocationID, "left")
        .groupBy("Borough")
        .agg(
            F.count("*").alias("nb_courses"),
            F.round(F.sum("total_amount"), 2).alias("revenu_total"),
        )
        .count()  # action : declenche le calcul
    )
    
def join_broadcast():
    return (
        courses
        .join(broadcast(zones), courses.PULocationID == zones.LocationID, "left")
        .groupBy("Borough")
        .agg(
            F.count("*").alias("nb_courses"),
            F.round(F.sum("total_amount"), 2).alias("revenu_total"),
        )
        .count()
    )

chrono("join classique (shuffle)", join_classique)

plan_classique = (
    courses
    .join(zones, courses.PULocationID == zones.LocationID, "left")
    .groupBy("Borough")
    .agg(F.count("*").alias("nb_courses"))
)
plan_classique.explain()


chrono("join broadcast", join_broadcast)

plan_broadcast = (
    courses
    .join(broadcast(zones), courses.PULocationID == zones.LocationID, "left")
    .groupBy("Borough")
    .agg(F.count("*").alias("nb_courses"))
)
plan_broadcast.explain()

travail = (
    courses
    .filter((F.col("trip_distance") > 0) & (F.col("total_amount") > 0))
    .withColumn(
        "duree_min",
        (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long") - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60,
    )
    .withColumn("prix_par_km", F.round(F.col("fare_amount") / (F.col("trip_distance") * 1.60934), 2))
)

# Sans cache : trois actions, donc trois recalculs complets
def trois_actions():
    a = travail.count()
    b = travail.agg(F.avg("duree_min")).collect()
    c = travail.agg(F.avg("prix_par_km")).collect()
    return a

chrono("3 actions SANS cache", trois_actions)

travail.cache()
travail.count()  # premiere action : remplit le cache (a ne PAS chronometrer)

chrono("3 actions AVEC cache", trois_actions)

print("Partitions au depart :", courses.rdd.getNumPartitions())

def via_repartition():
    return courses.repartition(8).count()

def via_coalesce():
    return courses.coalesce(2).count()

chrono("repartition(8)", via_repartition)
chrono("coalesce(2)", via_coalesce)

courses.repartition(8).explain()
courses.coalesce(2).explain()

input()
travail.unpersist()  # liberer la memoire quand on a fini
spark.stop()