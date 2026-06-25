from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TAXI_PATH = "data/datasets/yellow_tripdata_2024-01.parquet"

def main():
    spark = (
        SparkSession.builder
        .appName("demo_taxi")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("1. Lecture du fichier Parquet :", TAXI_PATH)
    df = spark.read.parquet(TAXI_PATH)
    
    nb_lignes = df.count()
    print("Nombre de lignes :", nb_lignes)
    
    print("2. Affichage du schéma du DataFrame")
    df.printSchema()
    
    print("3. Aperçu de quelques colonnes du DataFrame")
    apercu = df.select(
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "tip_amount",
        "total_amount"
    )
    
    apercu.show(5, truncate=False)
    
    print("4. select avec F.col et expr")
    apercu2 = df.select(
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime"),
        F.col("passenger_count"),
        F.col("trip_distance"),
        F.col("PULocationID").alias("pickup_location"),
        F.col("DOLocationID").alias("dropoff_location"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
        F.expr("tip_amount / total_amount * 100").alias("tip_percentage")
    )
    
    apercu2.show(5, truncate=False)
    
    print("5. Filtrage des courses valides")
    df_filtered = df.select(
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "tip_amount",
        "total_amount"
    ).filter(
        (F.col("passenger_count") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0)
    )
    
    print("Nombre de courses valides :", df_filtered.count())
    
    print("6. withColum pour calculer la durée de la course en minutes et le prix par km")
    # tpep_dropoff_datetime & tpep_pickup_datetime sont des colonnes de type timestamp_ntz, pour pouvoir les convertir en long il faut d'abord les caster en timestamp puis en long
    df_with_duration = df_filtered.withColumn(
        "duration_minutes",
        (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long") - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60
    ).withColumn(
        "price_per_km",
        F.col("fare_amount") / (F.col("trip_distance") * 1.60934)
    )
    
    df_with_duration.show(5, truncate=False)

if __name__ == "__main__":
    main()