from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder
    .appName("tp03_transformations")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
print("Nombre de courses brutes :", courses.count())

courses = courses.withColumn(
    "duree_min",
    (F.col("tpep_dropoff_datetime").cast('timestamp').cast("long") - F.col("tpep_pickup_datetime").cast('timestamp').cast("long")) / 60
)

courses = courses.withColumn(
    "duree_min",
    F.round(F.col("duree_min"), 1)
)

courses.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "duree_min"
).show(5, truncate=False)

courses = courses.withColumn(
    "distance_km",
    F.round(F.col("trip_distance") * 1.60934, 2)
)

courses = courses.withColumn(
    "prix_par_km",
        F.when(F.col("distance_km") > 0, F.round(F.col("fare_amount") / F.col("distance_km"), 2))
     .otherwise(None)   # valeur a renvoyer quand la distance est nulle (None par exemple)
)

courses.select(
    "trip_distance",
    "distance_km",
    "fare_amount",
    "prix_par_km"
).show(5, truncate=False)

courses = courses.withColumn(
    "categorie",
    F.when(F.col("distance_km") < 2, "courte").when(
    F.col("distance_km") < 8, "moyenne").otherwise(
        "longue"
    )
)

courses.groupBy("categorie").count().show()


courses_propres = courses.filter(
    (F.col("duree_min") > 0) &
    (F.col("duree_min") < 180) &
    (F.col("distance_km") > 0) &
    (F.col("fare_amount") >= 0)
)

courses_propres.groupBy("categorie").count().show()


stats_zone = (
    courses_propres
    .groupBy("PULocationID")
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu_total"),
        F.round(F.avg("tip_amount"), 2).alias("pourboire_moyen"),
        F.round(F.avg("duree_min"), 1).alias("duree_moyenne")
    )
)

stats_zone.show()

zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True) # A éviter sur des TRES GROS CSV
    .csv("data/datasets/taxi_zone_lookup.csv")
)

zones.show(5)

resultat = (
    stats_zone
    .join(zones,
          stats_zone.PULocationID == zones.LocationID, "left")
    .select("Borough", "Zone", "nb_courses", "revenu_total", "pourboire_moyen", "duree_moyenne")
)

resultat.show(5, truncate=True)

# Top 10 des zones par revenu total (tri desc)
resultat.orderBy(F.desc("revenu_total")).show(10, truncate=False)

# Top 10 des zones par pourboire moyen, en ne gardant que les zones avec + de 1000 courses
(resultat
 .filter(F.col("nb_courses") >= 1000)
 .orderBy(F.desc("pourboire_moyen"))
 .show(10, truncate=False))

@udf(returnType=StringType())
def toCaps(str):
    return str.upper()

spark.stop()