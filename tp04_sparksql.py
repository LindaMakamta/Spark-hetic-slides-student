from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP04 - Spark SQL")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

courses = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")
zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/datasets/taxi_zone_lookup.csv")
)

courses.createOrReplaceTempView("courses")
zones.createOrReplaceTempView("zones")

spark.sql("SHOW TABLES").show()

# Version API DataFrame
api_filtre = (
    courses
    .filter((F.col("payment_type") == 1) & (F.col("total_amount") > 0))
    .select("tpep_pickup_datetime", "trip_distance", "total_amount")
)

# Version Spark SQL : a completer
sql_filtre = spark.sql("""
    SELECT tpep_pickup_datetime, trip_distance, total_amount
    FROM courses
    WHERE payment_type = 1 AND total_amount > 0
""")

print("API :", api_filtre.count())
print("SQL :", sql_filtre.count())

# Version API DataFrame
api_agg = (
    courses
    .groupBy("PULocationID")
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.sum("total_amount"), 2).alias("revenu_total"),
    )
    .orderBy(F.col("revenu_total").desc())
)

# Version Spark SQL : a completer
sql_agg = spark.sql("""
    SELECT
        PULocationID,
        COUNT(*) AS nb_courses,
        ROUND(SUM(total_amount), 2) AS revenu_total
    FROM courses
    GROUP BY PULocationID
    ORDER BY revenu_total DESC
""")

api_agg.show(5)
sql_agg.show(5)

# Version API DataFrame
api_join = (
    api_agg
    .join(zones, api_agg.PULocationID == zones.LocationID, "left")
    .select("Borough", "Zone", "nb_courses", "revenu_total")
    .orderBy(F.col("revenu_total").desc())
)

# Version Spark SQL : a completer
sql_join = spark.sql("""
    SELECT z.Borough, z.Zone, a.nb_courses, a.revenu_total
    FROM (
        SELECT PULocationID, COUNT(*) AS nb_courses,
               ROUND(SUM(total_amount), 2) AS revenu_total
        FROM courses
        GROUP BY PULocationID
    ) AS a
    LEFT JOIN zones AS z ON a.PULocationID = z.LocationId
    ORDER BY a.revenu_total DESC
""")

api_join.show(10, truncate=False)
sql_join.show(10, truncate=False)

print("===== Plan API =====")
api_agg.explain()

print("===== Plan SQL =====")
sql_agg.explain()

sql_bonus = spark.sql("""
    SELECT
        HOUR(tpep_pickup_datetime) AS heure,
        COUNT(*) AS nb_courses,
        ROUND(AVG(tip_amount), 2) AS pourboire_moyen
    FROM courses
    WHERE payment_type = 1
    GROUP BY HOUR(tpep_pickup_datetime)
    ORDER BY heure
""")
sql_bonus.show(24)

# écrire le sql_bonus en API DataFrame 

# api_bonus

api_bonus = (
    courses
    .filter(F.col("payment_type") == 1)
    .groupBy(F.hour("tpep_pickup_datetime").alias("heure"))
    .agg(
        F.count("*").alias("nb_courses"),
        F.round(F.avg("tip_amount"), 2).alias("pourboire_moyen")
    )
    .orderBy("heure")
)

api_bonus.show(24)