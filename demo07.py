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
        .select("PULocationID", "trip_distance", "total_amount", "tpep_pickup_datetime")
    )
    
    courses.show(5, True)
    
    input("Spark UI : http://localhost:4040 (Appuyez sur Entrée pour quitter)");
    spark.stop()
    

if __name__ == "__main__":
    main()