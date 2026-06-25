from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("TP06 - Window functions")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")

# Nettoyage minimal : montant et distance coherents, janvier 2024 uniquement
courses = courses.filter(
    (F.col("total_amount") > 0) &
    (F.col("trip_distance") > 0) &
    (F.col("tpep_pickup_datetime") >= "2024-01-01") &
    (F.col("tpep_pickup_datetime") < "2024-02-01")
)


# 1. row_number : classer les courses dans chaque zone
fenetre_zone = (
    Window
    .partitionBy("PULocationID")
    .orderBy(
        F.col("total_amount").desc()
    )
)
courses_classes = courses.withColumn("rang", F.row_number().over(fenetre_zone))

top3_par_zone = (
    courses_classes
    .filter(F.col("rang") <= 3)
)
(
    top3_par_zone
    .select(
        "PULocationID",
        "rang",
        "trip_distance",
        "total_amount"
    )
    .orderBy("PULocationID", "rang")
    .show(15, truncate=False)
)

# 2. row_number contre rank contre dense_rank

course_trois_rangs = (  
    courses
    .withColumn("row_number", F.row_number().over(fenetre_zone))
    .withColumn("rank", F.rank().over(fenetre_zone))
    .withColumn("dense_rank", F.dense_rank().over(fenetre_zone))
)

# row_number : numérotation stricte, 1, 2, 3, 4, 5, 6. Deux courses à 70 dollars reçoivent des numéros différents (3, 4, 5). L'ordre entre ex aequo n'est pas déterministe.
# rank : même rang (3) pour les trois courses à 70 dollars, puis saut à 6 (on a "consommé" les rangs 4 et 5).
# dense_rank : même rang (3) pour les ex aequo, mais sans saut, la valeur suivante est 4.

(
    course_trois_rangs
    .filter(F.col("PULocationID") == 132)
    .select("total_amount", "row_number", "rank", "dense_rank")
    .show(20, truncate=False)
 )

revenu_jour = (
    courses
    .withColumn("jour", F.to_date("tpep_pickup_datetime"))
    .groupBy("jour")
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu"),
    )
    .orderBy("jour")
)
revenu_jour.show(31, truncate=False)

fenetre_temps = Window.orderBy("jour")

revenu_variation = (
    revenu_jour
    .withColumn("revenu_veille", F.lag("revenu", 1).over(fenetre_temps))
    .withColumn("variation", F.col("revenu") - F.col("revenu_veille"))
    .withColumn(
        "variation_pct",
        F.round(100 * (F.col("revenu") - F.col("revenu_veille")) / F.col("revenu_veille"), 1)
    )
)
revenu_variation.show(31, truncate=False)

fenetre_7j = Window.orderBy("jour").rowsBetween(-6, 0)

revenu_lisse = revenu_variation.withColumn(
    "moyenne_7j",
    # A completer : moyenne glissante du revenu sur cette fenetre
    F.round(F.avg("revenu").over(fenetre_7j), 2)
)

revenu_lisse.select("jour", "revenu", "moyenne_7j").show(31, truncate=False)

zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
)

# Window d'agregation : somme sur toute la zone (pas de orderBy, donc toute la partition)
fenetre_total_zone = Window.partitionBy("PULocationID")

resultat = (
    courses_classes
    .withColumn("revenu_zone", F.sum("total_amount").over(fenetre_total_zone))
    .withColumn("part_course", F.round(100 * F.col("total_amount") / F.col("revenu_zone"), 3))
    .filter(F.col("rang") <= 3)
    .join(zones, F.col("PULocationID") == zones.LocationID, "left")
    .select("Borough", "Zone", "rang", "total_amount", "part_course")
    .orderBy("Zone", "rang")
)

resultat.explain(True)
resultat.show(20, truncate=False)


input("Spark UI : http://localhost:4040 (Appuyez sur Entrée pour quitter)");

spark.stop()