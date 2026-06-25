from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP01 - RDD et paresse")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext # point d'entrée pour les RDD

nombres = sc.parallelize(range(1, 21), numSlices=4) # 4 partitions

print("Partitions :", nombres.getNumPartitions())

print("Contenu :", nombres.collect())

carres = nombres.map(lambda x: x * x)
carres_pairs = carres.filter(lambda x: x % 2 == 0)
print("Carres pairs :", carres_pairs.collect())

def trace(x):
    print(".  -> je calcule", x)
    return x * 10

paresseux = nombres.map(trace)
print(">>> Rien ne s'est exécuté, c'est la paresse.")

resultat = paresseux.collect() # C'est ICI que tout se calcule
print("Résultat :", resultat)

phrases = sc.parallelize([
    "le taxi jaune roule",
    "le taxi jaune attend le client",
    "le client appelle le taxi jaune",
    "la ville dort le taxi roule"
])

par_map = phrases.map(lambda s: s.split(" "))
par_flatmap = phrases.flatMap(lambda s: s.split(" "))
print("Résultat map :", par_map.collect())
print("Résultat flatMap :", par_flatmap.collect())


comtage = (
    phrases
    .flatMap(lambda s: s.split(" ")) # split en mots
    .map(lambda mot: (mot, 1)) # chaque mot devient un couple (mot, 1)
    .reduceByKey(lambda a, b: a + b) # on additionne les 1 pour chaque mot
)

top = comtage.sortBy(
    lambda couple: couple[1],
    ascending=False).take(5)

for mot, count in top:
    print(f"{mot} : {count} occurrences")
    
# DataFrame taxi vers le RDD

df = spark.read.parquet("data/datasets/yellow_tripdata_2024-01.parquet")

comptage_passages = (
    df.rdd
    .filter(lambda row: row["passenger_count"] is not None) # on ignore les lignes sans info sur le nombre de passagers 
    .map(lambda row : (row["passenger_count"], 1)) # on prend la colonne passenger_count
    .reduceByKey(lambda a, b: a + b) # on additionne le nombre de passagers pour chaque valeur  
    .sortByKey() # on trie par nombre de passagers (0, 1, 2, ...)
)

print("Courses par nombre de passagers :")
for nb_passagers, nb_courses in comptage_passages.collect():
    print(f"{nb_passagers} passagers : {nb_courses} courses")
    
    
nb_mots_distincts = comtage.count() # action agrégée, ne ramène pas toutes les données, juste le nombre de lignes du RDD
print("Nombre de mots distincts :", nb_mots_distincts)
    
spark.stop()