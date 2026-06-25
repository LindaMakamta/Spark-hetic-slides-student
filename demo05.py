from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType,
)

DVF_PATH = "data/datasets/dvf_75_2023.csv.gz"
OUT_PATH = "data/datasets/dvf_parquet"

def main():
    spark = (
        SparkSession.builder
        .appName("csv_to_parquet")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    
    # Un schema explicite (plus sur et plus rapide que qu'infer Schema)
    
    # 
    # id_mutation,
    # date_mutation,
    # numero_disposition,
    # nature_mutation,
    # valeur_fonciere,
    # adresse_numero,adresse_suffixe,adresse_nom_voie,adresse_code_voie,code_postal,code_commune,nom_commune,code_departement,ancien_code_commune,ancien_nom_commune,id_parcelle,ancien_id_parcelle,numero_volume,lot1_numero,lot1_surface_carrez,lot2_numero,lot2_surface_carrez,lot3_numero,lot3_surface_carrez,lot4_numero,lot4_surface_carrez,lot5_numero,lot5_surface_carrez,nombre_lots,code_type_local,type_local,surface_reelle_bati,nombre_pieces_principales,code_nature_culture,nature_culture,code_nature_culture_speciale,nature_culture_speciale,surface_terrain,longitude,latitude
    # Le schema doit couvrir toutes les colonnes du CSV dans leur ordre reel.
    # Spark applique un schema explicite par position, pas comme une projection
    # automatique des colonnes nommees.
    schema = StructType([
        StructField("id_mutation", StringType(), True),
        StructField("date_mutation", DateType(), True),
        StructField("numero_disposition", StringType(), True),
        StructField("nature_mutation", StringType(), True),
        StructField("valeur_fonciere", DoubleType(), True),
        StructField("adresse_numero", StringType(), True),
        StructField("adresse_suffixe", StringType(), True),
        StructField("adresse_nom_voie", StringType(), True),
        StructField("adresse_code_voie", StringType(), True),
        StructField("code_postal", StringType(), True),
        StructField("code_commune", StringType(), True),
        StructField("nom_commune", StringType(), True),
        StructField("code_departement", StringType(), True),
        StructField("ancien_code_commune", StringType(), True),
        StructField("ancien_nom_commune", StringType(), True),
        StructField("id_parcelle", StringType(), True),
        StructField("ancien_id_parcelle", StringType(), True),
        StructField("numero_volume", StringType(), True),
        StructField("lot1_numero", StringType(), True),
        StructField("lot1_surface_carrez", DoubleType(), True),
        StructField("lot2_numero", StringType(), True),
        StructField("lot2_surface_carrez", DoubleType(), True),
        StructField("lot3_numero", StringType(), True),
        StructField("lot3_surface_carrez", DoubleType(), True),
        StructField("lot4_numero", StringType(), True),
        StructField("lot4_surface_carrez", DoubleType(), True),
        StructField("lot5_numero", StringType(), True),
        StructField("lot5_surface_carrez", DoubleType(), True),
        StructField("nombre_lots", IntegerType(), True),
        StructField("code_type_local", StringType(), True),
        StructField("type_local", StringType(), True),
        StructField("surface_reelle_bati", DoubleType(), True),
        StructField("nombre_pieces_principales", IntegerType(), True),
        StructField("code_nature_culture", StringType(), True),
        StructField("nature_culture", StringType(), True),
        StructField("code_nature_culture_speciale", StringType(), True),
        StructField("nature_culture_speciale", StringType(), True),
        StructField("surface_terrain", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
    ])
    colonnes_utiles = [
        "date_mutation",
        "valeur_fonciere",
        "code_postal",
        "nom_commune",
        "code_departement",
        "type_local",
        "surface_reelle_bati",
        "nombre_pieces_principales",
        "longitude",
        "latitude",
    ]
    
    # Spark lit le .gz directement
    
    df = (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(DVF_PATH)
        .select(*colonnes_utiles)
    )
    print(f"Lignes lues : {df.count():,}")
    df.show(5, truncate=False)
    
    # Qualité des données nettoyage et valeurs aberrantes
    
    propre = (
        df
        .filter(F.col("type_local").isin("Appartement", "Maison"))
        .filter(F.col("valeur_fonciere").isNotNull() & (F.col("valeur_fonciere") > 0))
        .filter(F.col("surface_reelle_bati") > 9)  # surface plausible
        .dropDuplicates()
    )
    
    propre = propre.na.drop(subset=["valeur_fonciere", "surface_reelle_bati"])
    final = propre.count()
    
    
    print(f"Lignes avant nettoyage : {df.count():,} / apres nettoyage : {final:,}")
    
    # colonne dérivée
    
    enrichi = propre.withColumn(
        "prix_m2",
         F.round(F.col("valeur_fonciere") / F.col("surface_reelle_bati"), 0),
    )
    
    # On filtre les prix au m2 absurdes (erreurs de saisie frequentes dans DVF).
    enrichi = enrichi.filter((F.col("prix_m2") > 500) & (F.col("prix_m2") < 50000))
    
    enrichi.select(
        "nom_commune", "type_local", "surface_reelle_bati", "valeur_fonciere", "prix_m2"
    ).show(10, truncate=False)
    
    # Prix au m2 median par type de local 
    
    (enrichi.groupBy("type_local")
     .agg(
            F.count("*").alias("nb_ventes"),
            F.round(F.expr("percentile_approx(prix_m2, 0.5)"), 0).alias("prix_m2_median"),
        )
     .orderBy(F.desc("nb_ventes"))
     .show()
    )
    
    (
        enrichi.write
        .mode("overwrite")
        .partitionBy("code_departement")
        .parquet(OUT_PATH)
    )
    
    
    print(f"Ecrit dans : {OUT_PATH}/ (partitionne par code_departement)")
    
    relu = spark.read.parquet(OUT_PATH)
    print(f"Lignes relues depuis le Parquet : {relu.count():,}")
    relu.printSchema()
    
    
    spark.stop()

if __name__ == "__main__":
    main()
