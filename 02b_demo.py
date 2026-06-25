from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TAXI_PATH = "data/datasets/yellow_tripdata_2024-01.parquet"
ZONES_PATH = "data/datasets/taxi_zone_lookup.csv"

def main():
    spark = (
        SparkSession.builder
        .appName("demo_agg_join")
        .master("local[*]")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("1. Lecture du fichier Parquet :", TAXI_PATH)
    courses = spark.read.parquet(TAXI_PATH)
    
    print("2. Lecture du fichier CSV :", ZONES_PATH)
    zones = spark.read.csv(ZONES_PATH, header=True, inferSchema=True)
    
    print("3. Affichage du schéma du DataFrame courses")
    courses.printSchema()
    
    print("4. Affichage du schéma du DataFrame zones")
    zones.printSchema()
    
    # On ne garde que les courses valides et colonnes utiles
    
    courses_valides = courses.filter(
        (F.col("passenger_count") > 0) & 
        (F.col("trip_distance") > 0) & 
        (F.col("fare_amount") > 0)
    ).select(
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "fare_amount",
        "tip_amount",
        "total_amount"
    )
    
    
    print("5. Agrégation : revenue et l'activité par zone de départ")
    par_zone = (
        courses_valides.groupBy("PULocationID")
        .agg(
            F.count("*").alias("nb_courses"),
            F.round(F.avg("trip_distance"), 2).alias("distance_moyenne"),
            F.round(F.avg("fare_amount"), 2).alias("tarif_moyen"),
            F.round(F.sum("total_amount"), 2).alias("revenue_total"),
            F.round(F.avg("tip_amount"), 2).alias("pourboire_moyen")
        )
    )
    
    print("Agregat brut par PULocationID")
    par_zone.orderBy(F.desc("nb_courses")).show(5, truncate=False)   
    
    print("6. Jointure avec le DataFrame zones pour obtenir le nom des zones")
    
    par_zone_nom = par_zone.join(
            zones,
            par_zone.PULocationID == zones.LocationID,
            how="inner",
        )
    
    print("Top 10 des zones de départ par revenu total")
    par_zone_nom.select(
        "Borough",
        "Zone",
        "nb_courses",
        "tarif_moyen",
        "pourboire_moyen",
        "revenue_total"
        ).orderBy(
            F.desc("revenue_total")
        ).show(10, truncate=False)
        
    print("=== Demo types de jointures ===")
    
    # Left anti : Identifier les zones de départ qui n'ont pas de correspondance dans le DataFrame zones
    print("Left anti join : zones de départ sans correspondance dans zones")
    par_zone_anti = par_zone.join(
        zones,
        par_zone.PULocationID == zones.LocationID,
        how="left_anti"
    )
    par_zone_anti.show(5, truncate=False)
    
    print("=== Revenues agrégés par Borough ===")
    revenue_par_borough = (
        par_zone_nom.groupBy("Borough")
        .agg(
            F.sum("revenue_total").alias("revenue_total_borough"),
            F.sum("nb_courses").alias("nb_courses_borough")
        )
    )
    revenue_par_borough.orderBy(F.desc("revenue_total_borough")).show(10, truncate=False)
        
    
    spark.stop()
    
if __name__ == "__main__":
    main()