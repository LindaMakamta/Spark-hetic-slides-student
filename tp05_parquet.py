from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType,
)

spark = (
    SparkSession.builder
    .appName("TP05 - Ingestion Parquet")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SOURCE = "data/datasets/dvf_75_2023.csv.gz"
CIBLE = "data/datasets/dvf_75_parquet"   # dossier de sortie Parquet

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
    
brut = (
    spark.read
    .option("header", True)
    .option("sep", ",")              # virgule pour DVF ; ";" pour ONISR
    .option("dateFormat", "yyyy-MM-dd")
    .schema(schema)                # passer le schema defini a l'etape 1
    .csv(SOURCE)
    .select(*colonnes_utiles)
)

brut.printSchema()
print("Lignes brutes :", brut.count())
brut.show(5, truncate=False)

essentielles = ["valeur_fonciere", "surface_reelle_bati", "type_local"]

sans_na = brut.na.drop(subset=essentielles) # ne garder que les lignes ou ces 3 colonnes sont non nulles

print("Apres suppression des na :", sans_na.count())

sans_doublons = sans_na.dropDuplicates() # methode qui supprime les lignes entierement identiques

print("Apres suppression des doublons :", sans_doublons.count())

propre = (
    sans_doublons
    .filter(
        (F.col("valeur_fonciere") > 0) &
        (F.col("surface_reelle_bati") > 0) &
        (F.col("type_local").isin("Appartement", "Maison"))
    )
    .withColumn("prix_m2", F.round(F.col("valeur_fonciere") / F.col("surface_reelle_bati"), 0))
    .filter(
        (F.col("prix_m2") >= 1000) &
        (F.col("prix_m2") <= 50000)
    )
    .withColumn("mois", F.month("date_mutation")) # colonne de partionnement
)

print("Apres nettoyage des aberrations :", propre.count())
propre.select("nom_commune", "type_local", "surface_reelle_bati", "valeur_fonciere", "prix_m2").show(5)

(
    propre
    .write
    .mode("overwrite") # écraser la sortie si elle existe déjà
    .partitionBy("mois") # un sous-dossier par moi
    .parquet(CIBLE)
)


print("Ecriture terminee dans", CIBLE)

relu = spark.read.parquet(CIBLE)
relu.printSchema()  
print("Lignes relues :", relu.count()) 
relu.groupBy("mois").count().orderBy("mois").show()

relu.groupBy("type_local").agg(
    F.round(F.avg("prix_m2"), 0).alias("prix_m2_moyen"),
    F.count("*").alias("nb_ventes"),
).show()

spark.stop()