from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TAXI = "data/datasets/yellow_tripdata_2024-01.parquet"

def main():
    spark = (
        SparkSession.builder
        .appName("J2 - Window functions")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    
    courses = (
        spark.read
        .parquet(TAXI)
        .filter((F.col("total_amount") > 0) & (F.col("trip_distance") > 0))
        .select("PULocationID", "trip_distance", "total_amount", "tpep_pickup_datetime")
    )
    
    # 1. Le top 3. des courses les plus chères PAR zone, tout en gardant le détail
    #  ---> un groupBy donnerait une ligne par zone ... mais perte de l'info 
    
    print("=== Top 3 des courses par zone (row_number)")
    
    fenetre_zone = Window.partitionBy("PULocationID").orderBy(F.col("total_amount").desc())
    
    top3 = (
        courses
        .withColumn("rang", F.row_number().over(fenetre_zone))
        .filter(F.col("rang") <= 3)
        .orderBy("PULocationID", "rang")
    )
    
    top3.select("PULocationID", "rang", "trip_distance", "total_amount").show(15, truncate=False)
    
    # 2. Moyenne glissante du revenu sur 7 jours. 
    # le cadre rowsBetween(-6,0) (prendre la ligne courante et les 6 précédentes)
    
    print("=== Moyenne glissante du revenu sur 7 jours ===")
    
    revenu_jour = (
        courses
        .withColumn("jour", F.to_date("tpep_pickup_datetime"))
        .groupBy("jour")
        .agg(F.round(F.sum("total_amount"), 2).alias("revenu"))
    )
    
    fenetre_7j = Window.orderBy("jour").rowsBetween(-6, 0)
    lisse = revenu_jour.withColumn(
        "moyenne_7j",  F.round(F.avg("revenu").over(fenetre_7j), 2)
    ).orderBy("jour")
    
    lisse.show(31, truncate=False)
    
    spark.stop()
    

if __name__ == "__main__":
    main()