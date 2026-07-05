# Projet Spark Jour 4 — Pipeline ONISR 2023

## Présentation du projet

Ce dépôt contient mon projet Spark du jour 4 réalisé avec PySpark.

J’ai choisi le jeu de données ONISR 2023 sur les accidents corporels de la circulation routière. L’objectif du projet est de construire un pipeline complet suivant une architecture :

```text
bronze → silver → gold
```

Le pipeline permet de lire les fichiers CSV bruts ONISR, de nettoyer les données, d’écrire une couche silver au format Parquet, puis de produire plusieurs analyses métier dans une couche gold.

## Jeu de données utilisé

Le jeu de données utilisé est composé de quatre fichiers ONISR 2023 :

```text
data/raw/onisr/2023/caracteristiques-2023.csv
data/raw/onisr/2023/lieux-2023.csv
data/raw/onisr/2023/usagers-2023.csv
data/raw/onisr/2023/vehicules-2023.csv
```

Ces fichiers sont reliés par la clé commune :

```text
Num_Acc
```

## Analyses produites

Les analyses réalisées sont :

1. Nombre d’accidents par département.
2. Gravité des usagers selon les conditions météorologiques.
3. Top 5 des départements les plus accidentogènes par mois avec une window function.
4. Analyse bonus : gravité des usagers selon le type de route.

Les résultats sont disponibles dans :

```text
data/output/gold/onisr
```

## Résultats principaux

### Volumétrie

```text
Lignes brutes :
caracteristiques_brut : 54822
lieux_brut : 70860
usagers_brut : 125789
vehicules_brut : 93585

Lignes après nettoyage :
caracteristiques_clean : 54822
lieux_clean : 54822
usagers_clean : 125671
vehicules_clean : 93585
```

### Optimisation cache

```text
Sans cache : 2.392 secondes
Avec cache : 1.092 secondes
```

### Adaptive Query Execution

```text
AQE OFF : 1.908 secondes
AQE ON : 1.047 secondes
```

## Lancer le pipeline

Depuis la racine du projet :

```bash
python starter-code/pipeline.py
```

Pendant l’exécution, la Spark UI est disponible ici :

```text
http://localhost:4040
```

## Livrables

Les livrables principaux sont :

```text
starter-code/pipeline.py
starter-code/spark_session.py
projects/rapport_spark_onisr_Linda_Makamta.docx
data/output/gold/onisr
```

## Auteure

Linda Makamta

---



# Apache Spark - Ingestion & Calcul distribué

Cours d'introduction à Apache Spark avec PySpark, sur 3 jours de cours et 1 jour de projet. Vous
trouvez ici les slides, les énoncés d'exercices, le sujet du projet, un squelette de code de départ
et les sources de données. Le cours couvre Spark comme outil d'ingestion de données et comme moteur
de calcul distribué.

Langage : Python (PySpark), Apache Spark 4.x, en mode local sur votre machine. Aucune connaissance
préalable de Spark n'est requise (voir `Syllabus.md` pour les prérequis et les objectifs).

Chaque exercice se termine par une section Aide (indices, rappels d'API) pour vous débloquer.

## Contenu

- `Syllabus.md` : notions, objectifs pédagogiques et compétences visées
- `slides/index.html` : les slides du cours (moteur autonome, navigation au clavier)
- `exercises/` : le cahier d'exercices (énoncés, avec une section Aide en fin de chaque énoncé)
- `projects/projet-jour-4.md` : le sujet du projet du jour 4 et sa grille d'évaluation
- `starter-code/` : squelette de pipeline PySpark à compléter pour le projet
- `data/sources-open-data.md` : les sources d'open data, avec `data/download.sh` pour les récupérer

## Mise en route

### 1. Installer PySpark

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark
```

Vérifier que Java 17 ou 21 est installé (`java -version`). Apache Spark 4 requiert Java 17 minimum.

### 2. Télécharger les données du fil rouge

```bash
bash data/download.sh
```

Cela récupère quelques mois de courses de taxi de New York (Parquet), la table des zones, un
département DVF et MovieLens small dans `data/datasets/`. Ces fichiers sont volumineux et ne
doivent pas être committés dans Git.

### 3. Ouvrir les slides en local

Le panneau TP (touche `T`) charge l'énoncé de l'exercice lié à la slide, ce qui nécessite un petit
serveur local.

- Linux ou macOS : `bash scripts/serve.sh`
- Windows : `scripts/serve.bat`

Puis ouvrir `http://localhost:8000/`.

## Navigation des slides

- Flèches gauche et droite : slide précédente et suivante
- `F` : plein écran
- `T` : panneau TP (énoncé de l'exercice lié à la slide)
- `A` : mode assistance (affiche la section Aide de l'exercice)
- `1` `2` `3` `4` : accès direct Jour 1, 2, 3 et projet

## Comment travailler

1. Suivez les slides du jour. Quand une slide propose un exercice, ouvrez le panneau TP avec `T`.
2. Lisez l'énoncé, codez votre solution dans un fichier `.py` (ou un notebook).
3. Bloqué ? La section Aide de chaque énoncé (touche `A`) donne des indices et des rappels d'API.
4. Le jour 4, vous assemblez tout dans un pipeline complet : voir `projects/projet-jour-4.md` et
   le squelette `starter-code/pipeline.py`.

## Pour aller plus loin

- Documentation officielle : https://spark.apache.org/docs/latest/
- Autres jeux d'open data pour pratiquer : `data/sources-open-data.md`

## Licence

Support de cours sous licence [Creative Commons BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/deed.fr).
Vous pouvez le partager et l'adapter en créditant PANDOR MEDIA, mais pas en faire un usage commercial.
Voir [LICENSE](LICENSE). © 2026 PANDOR MEDIA.
