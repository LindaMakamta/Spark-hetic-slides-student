import time

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

from spark_session import get_spark


ANNEE = "2023"

BASE_RAW = f"data/raw/onisr/{ANNEE}"

CARACTERISTIQUES_CSV = f"{BASE_RAW}/caracteristiques-{ANNEE}.csv"
LIEUX_CSV = f"{BASE_RAW}/lieux-{ANNEE}.csv"
USAGERS_CSV = f"{BASE_RAW}/usagers-{ANNEE}.csv"
VEHICULES_CSV = f"{BASE_RAW}/vehicules-{ANNEE}.csv"

SORTIE_SILVER = "data/output/silver/onisr"
SORTIE_GOLD = "data/output/gold/onisr"


schema_caracteristiques = StructType([
    StructField("Num_Acc", StringType(), True),
    StructField("jour", IntegerType(), True),
    StructField("mois", IntegerType(), True),
    StructField("an", IntegerType(), True),
    StructField("hrmn", StringType(), True),
    StructField("lum", IntegerType(), True),
    StructField("dep", StringType(), True),
    StructField("com", StringType(), True),
    StructField("agg", IntegerType(), True),
    StructField("int", IntegerType(), True),
    StructField("atm", IntegerType(), True),
    StructField("col", IntegerType(), True),
    StructField("adr", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
])

schema_lieux = StructType([
    StructField("Num_Acc", StringType(), True),
    StructField("catr", IntegerType(), True),
    StructField("voie", StringType(), True),
    StructField("v1", StringType(), True),
    StructField("v2", StringType(), True),
    StructField("circ", IntegerType(), True),
    StructField("nbv", StringType(), True),
    StructField("vosp", IntegerType(), True),
    StructField("prof", IntegerType(), True),
    StructField("pr", StringType(), True),
    StructField("pr1", StringType(), True),
    StructField("plan", IntegerType(), True),
    StructField("lartpc", StringType(), True),
    StructField("larrout", StringType(), True),
    StructField("surf", IntegerType(), True),
    StructField("infra", IntegerType(), True),
    StructField("situ", IntegerType(), True),
    StructField("vma", IntegerType(), True),
])

schema_usagers = StructType([
    StructField("Num_Acc", StringType(), True),
    StructField("id_usager", StringType(), True),
    StructField("id_vehicule", StringType(), True),
    StructField("num_veh", StringType(), True),
    StructField("place", IntegerType(), True),
    StructField("catu", IntegerType(), True),
    StructField("grav", IntegerType(), True),
    StructField("sexe", IntegerType(), True),
    StructField("an_nais", IntegerType(), True),
    StructField("trajet", IntegerType(), True),
    StructField("secu1", IntegerType(), True),
    StructField("secu2", IntegerType(), True),
    StructField("secu3", IntegerType(), True),
    StructField("locp", IntegerType(), True),
    StructField("actp", StringType(), True),
    StructField("etatp", IntegerType(), True),
])

schema_vehicules = StructType([
    StructField("Num_Acc", StringType(), True),
    StructField("id_vehicule", StringType(), True),
    StructField("num_veh", StringType(), True),
    StructField("senc", IntegerType(), True),
    StructField("catv", IntegerType(), True),
    StructField("obs", IntegerType(), True),
    StructField("obsm", IntegerType(), True),
    StructField("choc", IntegerType(), True),
    StructField("manv", IntegerType(), True),
    StructField("motor", IntegerType(), True),
    StructField("occutc", IntegerType(), True),
])


def lire_csv(spark, chemin, schema):
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(chemin)
    )


def ingestion(spark):
    print("\n===== 1. INGESTION BRONZE =====")

    caracteristiques = lire_csv(spark, CARACTERISTIQUES_CSV, schema_caracteristiques)
    lieux = lire_csv(spark, LIEUX_CSV, schema_lieux)
    usagers = lire_csv(spark, USAGERS_CSV, schema_usagers)
    vehicules = lire_csv(spark, VEHICULES_CSV, schema_vehicules)

    print("\nSchéma caracteristiques :")
    caracteristiques.printSchema()

    print("\nAperçu caracteristiques :")
    caracteristiques.show(5, truncate=False)

    counts_brut = {
        "caracteristiques_brut": caracteristiques.count(),
        "lieux_brut": lieux.count(),
        "usagers_brut": usagers.count(),
        "vehicules_brut": vehicules.count(),
    }

    print("\nNombre de lignes brutes :")
    for table, nb in counts_brut.items():
        print(f"{table} : {nb}")

    return caracteristiques, lieux, usagers, vehicules, counts_brut


def nettoyer_caracteristiques(df):
    return (
        df
        .filter(F.col("Num_Acc").isNotNull())
        .dropDuplicates(["Num_Acc"])
        .filter((F.col("mois") >= 1) & (F.col("mois") <= 12))
        .filter((F.col("jour") >= 1) & (F.col("jour") <= 31))
        .filter(F.col("dep").isNotNull())
        .withColumn("annee", F.lit(int(ANNEE)))
        .withColumn("dep", F.trim(F.col("dep")))
        .withColumn(
            "condition_meteo",
            F.when(F.col("atm") == 1, "Normale")
             .when(F.col("atm") == 2, "Pluie légère")
             .when(F.col("atm") == 3, "Pluie forte")
             .when(F.col("atm") == 4, "Neige / grêle")
             .when(F.col("atm") == 5, "Brouillard / fumée")
             .when(F.col("atm") == 6, "Vent fort / tempête")
             .when(F.col("atm") == 7, "Temps éblouissant")
             .when(F.col("atm") == 8, "Temps couvert")
             .when(F.col("atm") == 9, "Autre")
             .otherwise("Non renseigné")
        )
        .withColumn(
            "luminosite",
            F.when(F.col("lum") == 1, "Plein jour")
             .when(F.col("lum") == 2, "Crépuscule ou aube")
             .when(F.col("lum") == 3, "Nuit sans éclairage public")
             .when(F.col("lum") == 4, "Nuit avec éclairage public non allumé")
             .when(F.col("lum") == 5, "Nuit avec éclairage public allumé")
             .otherwise("Non renseigné")
        )
    )


def nettoyer_lieux(df):
    return (
        df
        .filter(F.col("Num_Acc").isNotNull())
        .dropDuplicates(["Num_Acc"])
        .withColumn(
            "type_route",
            F.when(F.col("catr") == 1, "Autoroute")
             .when(F.col("catr") == 2, "Route nationale")
             .when(F.col("catr") == 3, "Route départementale")
             .when(F.col("catr") == 4, "Voie communale")
             .when(F.col("catr") == 5, "Hors réseau public")
             .when(F.col("catr") == 6, "Parc de stationnement")
             .when(F.col("catr") == 7, "Route de métropole urbaine")
             .otherwise("Autre / non renseigné")
        )
        .withColumn(
            "surface_route",
            F.when(F.col("surf") == 1, "Normale")
             .when(F.col("surf") == 2, "Mouillée")
             .when(F.col("surf") == 3, "Flaques")
             .when(F.col("surf") == 4, "Inondée")
             .when(F.col("surf") == 5, "Enneigée")
             .when(F.col("surf") == 6, "Boue")
             .when(F.col("surf") == 7, "Verglacée")
             .when(F.col("surf") == 8, "Corps gras / huile")
             .when(F.col("surf") == 9, "Autre")
             .otherwise("Non renseigné")
        )
    )


def nettoyer_usagers(df):
    return (
        df
        .filter(F.col("Num_Acc").isNotNull())
        .filter(F.col("grav").isin(1, 2, 3, 4))
        .dropDuplicates(["Num_Acc", "id_usager"])
        .withColumn(
            "gravite",
            F.when(F.col("grav") == 1, "Indemne")
             .when(F.col("grav") == 2, "Tué")
             .when(F.col("grav") == 3, "Blessé hospitalisé")
             .when(F.col("grav") == 4, "Blessé léger")
             .otherwise("Non renseigné")
        )
        .withColumn(
            "categorie_usager",
            F.when(F.col("catu") == 1, "Conducteur")
             .when(F.col("catu") == 2, "Passager")
             .when(F.col("catu") == 3, "Piéton")
             .otherwise("Autre / non renseigné")
        )
        .withColumn(
            "sexe_libelle",
            F.when(F.col("sexe") == 1, "Masculin")
             .when(F.col("sexe") == 2, "Féminin")
             .otherwise("Non renseigné")
        )
        .withColumn(
            "age",
            F.when(
                (F.col("an_nais").isNotNull()) &
                (F.col("an_nais") > 1900) &
                (F.col("an_nais") <= int(ANNEE)),
                F.lit(int(ANNEE)) - F.col("an_nais")
            )
        )
        .withColumn(
            "classe_age",
            F.when(F.col("age").isNull(), "Non renseigné")
             .when(F.col("age") < 18, "Moins de 18 ans")
             .when(F.col("age") < 30, "18-29 ans")
             .when(F.col("age") < 45, "30-44 ans")
             .when(F.col("age") < 60, "45-59 ans")
             .otherwise("60 ans et plus")
        )
    )


def nettoyer_vehicules(df):
    return (
        df
        .filter(F.col("Num_Acc").isNotNull())
        .dropDuplicates(["Num_Acc", "id_vehicule", "num_veh"])
        .withColumn(
            "type_vehicule_simplifie",
            F.when(F.col("catv").isin(1, 2, 30, 31, 32, 33, 34), "Deux-roues")
             .when(F.col("catv").isin(7, 10), "Voiture")
             .when(F.col("catv").isin(13, 14, 15), "Poids lourd / transport")
             .when(F.col("catv").isin(37, 38, 39, 40), "Transport en commun")
             .otherwise("Autre")
        )
    )


def nettoyage(caracteristiques, lieux, usagers, vehicules):
    print("\n===== 2. NETTOYAGE SILVER =====")

    caracteristiques_clean = nettoyer_caracteristiques(caracteristiques)
    lieux_clean = nettoyer_lieux(lieux)
    usagers_clean = nettoyer_usagers(usagers)
    vehicules_clean = nettoyer_vehicules(vehicules)

    counts_clean = {
        "caracteristiques_clean": caracteristiques_clean.count(),
        "lieux_clean": lieux_clean.count(),
        "usagers_clean": usagers_clean.count(),
        "vehicules_clean": vehicules_clean.count(),
    }

    print("\nNombre de lignes après nettoyage :")
    for table, nb in counts_clean.items():
        print(f"{table} : {nb}")

    return caracteristiques_clean, lieux_clean, usagers_clean, vehicules_clean, counts_clean


def ecrire_silver(caracteristiques, lieux, usagers, vehicules):
    print("\n===== 3. ÉCRITURE SILVER PARQUET =====")

    caracteristiques.write.mode("overwrite").partitionBy("dep").parquet(
        f"{SORTIE_SILVER}/caracteristiques"
    )
    lieux.write.mode("overwrite").parquet(f"{SORTIE_SILVER}/lieux")
    usagers.write.mode("overwrite").parquet(f"{SORTIE_SILVER}/usagers")
    vehicules.write.mode("overwrite").parquet(f"{SORTIE_SILVER}/vehicules")

    print(f"Couche silver écrite dans : {SORTIE_SILVER}")


def analyses_gold(spark):
    print("\n===== 4. ANALYSES GOLD =====")

    caracteristiques = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques")
    lieux = spark.read.parquet(f"{SORTIE_SILVER}/lieux")
    usagers = spark.read.parquet(f"{SORTIE_SILVER}/usagers")

    print("\nAnalyse 1 : accidents par département")

    accidents_par_departement = (
        caracteristiques
        .groupBy("dep")
        .agg(F.countDistinct("Num_Acc").alias("nombre_accidents"))
        .orderBy(F.desc("nombre_accidents"))
    )

    accidents_par_departement.show(10, truncate=False)

    print("\nAnalyse 2 : gravité par météo")

    accidents_usagers = caracteristiques.join(usagers, on="Num_Acc", how="inner")

    gravite_par_meteo = (
        accidents_usagers
        .groupBy("condition_meteo", "gravite")
        .agg(F.count("*").alias("nombre_usagers"))
        .orderBy("condition_meteo", F.desc("nombre_usagers"))
    )

    gravite_par_meteo.show(50, truncate=False)

    print("\nAnalyse 3 : top 5 départements par mois")

    accidents_par_mois_dep = (
        caracteristiques
        .groupBy("mois", "dep")
        .agg(F.countDistinct("Num_Acc").alias("nombre_accidents"))
    )

    fenetre_mois = Window.partitionBy("mois").orderBy(F.desc("nombre_accidents"))

    top_departements_par_mois = (
        accidents_par_mois_dep
        .withColumn("rang", F.row_number().over(fenetre_mois))
        .filter(F.col("rang") <= 5)
        .orderBy("mois", "rang")
    )

    top_departements_par_mois.show(60, truncate=False)

    print("\nAnalyse bonus : gravité par type de route")

    accidents_lieux_usagers = (
        caracteristiques
        .join(lieux, on="Num_Acc", how="inner")
        .join(usagers, on="Num_Acc", how="inner")
    )

    gravite_par_type_route = (
        accidents_lieux_usagers
        .groupBy("type_route", "gravite")
        .agg(F.count("*").alias("nombre_usagers"))
        .orderBy("type_route", F.desc("nombre_usagers"))
    )

    gravite_par_type_route.show(50, truncate=False)

    return {
        "analyse_1_accidents_par_departement": accidents_par_departement,
        "analyse_2_gravite_par_meteo": gravite_par_meteo,
        "analyse_3_top_departements_par_mois": top_departements_par_mois,
        "analyse_bonus_gravite_par_type_route": gravite_par_type_route,
    }


def chronometrer(nom, fonction):
    debut = time.time()
    resultat = fonction()
    duree = time.time() - debut
    print(f"{nom} : {duree:.3f} secondes")
    return resultat, duree


def optimisation_cache(spark):
    print("\n===== 5. OPTIMISATION CACHE =====")

    caracteristiques = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques")
    usagers = spark.read.parquet(f"{SORTIE_SILVER}/usagers")

    accidents_usagers = caracteristiques.join(usagers, on="Num_Acc", how="inner")

    def requete_sans_cache():
        return (
            accidents_usagers
            .groupBy("condition_meteo", "gravite")
            .agg(F.count("*").alias("nombre_usagers"))
            .count()
        )

    _, temps_sans_cache = chronometrer("Temps sans cache", requete_sans_cache)

    accidents_usagers_cached = accidents_usagers.cache()
    accidents_usagers_cached.count()

    def requete_avec_cache():
        return (
            accidents_usagers_cached
            .groupBy("condition_meteo", "gravite")
            .agg(F.count("*").alias("nombre_usagers"))
            .count()
        )

    _, temps_avec_cache = chronometrer("Temps avec cache", requete_avec_cache)

    print("\nRésultat optimisation cache :")
    print(f"Sans cache : {temps_sans_cache:.3f} secondes")
    print(f"Avec cache : {temps_avec_cache:.3f} secondes")

    return temps_sans_cache, temps_avec_cache


def exploration_aqe(spark):
    print("\n===== 6. EXPLORATION AQE =====")

    caracteristiques = spark.read.parquet(f"{SORTIE_SILVER}/caracteristiques")

    def requete_test():
        return (
            caracteristiques
            .groupBy("dep")
            .agg(F.countDistinct("Num_Acc").alias("nombre_accidents"))
            .orderBy(F.desc("nombre_accidents"))
            .count()
        )

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    _, temps_aqe_off = chronometrer("AQE désactivé", requete_test)

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    _, temps_aqe_on = chronometrer("AQE activé", requete_test)

    print("\nRésultat exploration AQE :")
    print(f"AQE OFF : {temps_aqe_off:.3f} secondes")
    print(f"AQE ON : {temps_aqe_on:.3f} secondes")

    return temps_aqe_off, temps_aqe_on


def ecrire_gold(resultats):
    print("\n===== 7. ÉCRITURE GOLD =====")

    for nom, df in resultats.items():
        chemin = f"{SORTIE_GOLD}/{nom}"

        (
            df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(chemin)
        )

        print(f"Résultat écrit : {chemin}")


def main():
    spark = get_spark("Projet Jour 4 - ONISR Accidents")

    print("\nSpark UI disponible ici : http://localhost:4040")
    print("Ne ferme pas cette fenêtre pendant les captures Spark UI.")

    caracteristiques, lieux, usagers, vehicules, counts_brut = ingestion(spark)

    caracteristiques_clean, lieux_clean, usagers_clean, vehicules_clean, counts_clean = nettoyage(
        caracteristiques,
        lieux,
        usagers,
        vehicules,
    )

    ecrire_silver(
        caracteristiques_clean,
        lieux_clean,
        usagers_clean,
        vehicules_clean,
    )

    resultats = analyses_gold(spark)

    temps_sans_cache, temps_avec_cache = optimisation_cache(spark)
    temps_aqe_off, temps_aqe_on = exploration_aqe(spark)

    ecrire_gold(resultats)

    print("\n===== CHIFFRES À COPIER DANS LE RAPPORT =====")

    print("\nLignes brutes :")
    for table, nb in counts_brut.items():
        print(f"{table} : {nb}")

    print("\nLignes après nettoyage :")
    for table, nb in counts_clean.items():
        print(f"{table} : {nb}")

    print("\nOptimisation cache :")
    print(f"Sans cache : {temps_sans_cache:.3f} secondes")
    print(f"Avec cache : {temps_avec_cache:.3f} secondes")

    print("\nExploration AQE :")
    print(f"AQE OFF : {temps_aqe_off:.3f} secondes")
    print(f"AQE ON : {temps_aqe_on:.3f} secondes")

    input(
        "\nPipeline terminé. "
        "Garde cette fenêtre ouverte, ouvre http://localhost:4040 pour les captures Spark UI, "
        "puis appuie sur Entrée pour arrêter Spark."
    )

    spark.stop()


if __name__ == "__main__":
    main()