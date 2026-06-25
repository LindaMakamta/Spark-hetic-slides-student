from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TAXI_PATH = "data/datasets/yellow_tripdata_2024-01.parquet"

def main():
    spark = (
        SparkSession.builder
        .appName("demo_api_sql")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    df = spark.read.parquet(TAXI_PATH)
    
    df.groupBy("PULocationID").agg(
        F.count("*").alias("nb_courses"),
        F.round(F.avg("fare_amount"), 2)
            .alias("avg_fare")
    ).orderBy(F.desc("nb_courses")).explain()
    
    df.createOrReplaceTempView("courses")
    
    spark.sql("""
        SELECT PULocationId,
            COUNT(*) AS nb_courses,
            ROUND(AVG(fare_amount),2)
                AS avg_fare
        FROM courses
        GROUP BY PULocationId
        ORDER BY nb_courses DESC
    """).explain()
    
    spark.stop()
    
if __name__ == "__main__":
    main()