from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TP02 - Premier DataFrame")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

chemin = "data/datasets/yellow_tripdata_2024-01.parquet"

# A completer : lire le fichier Parquet dans un DataFrame nomme df
df = spark.read.parquet(chemin)

print("Type de df :", type(df))

df.printSchema()
df.show(5, truncate=False)

print("Colonnes :", df.columns)

colonnes = ["tpep_pickup_datetime", "trip_distance", "PULocationID", "tip_amount", "total_amount"]
df_court = df.select(colonnes)

df_court.show(5, truncate=False)

df_valides = df.filter(
    (F.col("passenger_count") > 0) & (F.col("trip_distance") > 0) & (F.col("total_amount") > 0)
)

print("Courses valides :", df_valides.count())

df.describe("trip_distance", "fare_amount", "total_amount").show()

# df.summary("trip_distance", "fare_amount", "total_amount").show()

# A. Nombre de courses valides
nb_courses = df_valides.count()
print("Nombre de courses valides :", nb_courses)
 
# B. Distance moyenne des courses valides
df_valides.agg(F.mean("trip_distance")).show()

# C. La course la plus longue et le montant maxium

df_valides.agg(
    F.max("trip_distance").alias("distance_max"),
    F.max("total_amount").alias("total_max")
).show()

spark.stop()