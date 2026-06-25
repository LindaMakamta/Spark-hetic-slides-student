# Projet - Pipeline data de bout en bout avec Spark

Jour 4 de la formation Apache Spark. Une journée, un projet : un pipeline complet, de l'ingestion
des données brutes jusqu'à l'analyse, plus une exploration qui pousse au-delà du cours. Le tout
consigné dans un rapport écrit.

En binôme ou trinôme. Code en PySpark, mode local. Le livrable noté est un **rapport écrit**, pas
une présentation orale. L'objectif n'est pas le pipeline le plus complexe : un pipeline propre, qui
tourne, qui répond à de vraies questions, et un rapport qui l'explique.

---

## 1. Socle attendu

Un pipeline ETL (Extract, Transform, Load), puis une phase d'analyse, puis une exploration, le tout
décrit dans un rapport. Six éléments, attendus de toutes les équipes :

1. **Ingestion propre** : lire le brut (Parquet ou CSV), schéma explicite pour le CSV, typage,
   nettoyage (valeurs manquantes, doublons, valeurs aberrantes), écriture d'une couche
   intermédiaire en Parquet. Si l'ingestion est sale, toute l'analyse est faussée.
2. **Trois analyses** distinctes qui répondent à des questions métier : au moins une agrégation
   (`groupBy` + `agg`), une jointure, une window function. Chaque analyse porte une lecture métier
   écrite : ce que dit le résultat.
3. **Une optimisation mesurée** : broadcast join, cache d'un DataFrame réutilisé, ou
   repartitionnement. Temps avant/après chiffré, ou lecture de plan à l'appui.
4. **Une lecture de la Spark UI** : port 4040, un job avec shuffle, le DAG, les stages et les
   tasks. Capture(s) dans le rapport.
5. **Une exploration au-delà du cours** : choisir une piste du menu (section 6), aller plus loin
   que les TP, mesurer, et écrire ce qu'on en retient. C'est ce qui distingue un projet d'un TP.
6. **Un rapport écrit** : le document qui présente le jeu de données, le pipeline, les analyses et
   leur lecture, l'optimisation, la Spark UI et l'exploration. Gabarit fourni :
   `projects/rapport-modele.md`.

> Tout ce qui dépasse ce socle (deuxième exploration, streaming, MLlib, Delta, multi-mois) est un
> bonus. Mieux vaut un socle solide qu'un bonus bancal posé sur un socle fragile.

---

## 2. Choisir son jeu de données

Quatre options, décrites dans `data/sources-open-data.md` (URLs vérifiées). Le script
`data/download.sh` récupère le taxi, les zones, plusieurs départements DVF et MovieLens. Choisissez
un jeu qui vous parle : un projet sur des données qui vous intéressent se mène mieux.

### Option A : NYC Yellow Taxi multi-mois (recommandé)

Le fil rouge du cours, en plus gros : 3 mois ou plus (`yellow_tripdata_2024-01..03.parquet`) plus la
table des zones (`taxi_zone_lookup.csv`).

- Format : Parquet natif (colonnaire, predicate pushdown, partition pruning).
- Volume : ~3 millions de courses par mois, donc ~9 millions sur 3 mois. De quoi sentir le shuffle.
- Analyses : revenu par heure et par jour de semaine ; top trajets zone à zone (jointure double) ;
  classement des zones par pourboire (window) ; évolution mois après mois.
- Optimisation évidente : broadcast de la table des zones (265 lignes).

### Option B : DVF, valeurs foncières (immobilier France)

Toutes les transactions immobilières d'un département. Excellent pour un public francophone.

- Format : CSV compressé gzip. Bon cas de schéma explicite et de nettoyage sérieux.
- Commencer par un département (`dvf_75_2023.csv.gz`). La couche Parquet petite couronne
  partitionnée par département produite au TP05b est réutilisable comme point de départ.
- Analyses : prix au m2 par commune (protéger la division) ; communes les plus chères (window) ;
  évolution mensuelle ; répartition par `type_local` ; filtrage des transactions à 1 euro.
- Optimisation évidente : cache du DataFrame nettoyé, réutilisé par plusieurs analyses.

### Option C : Accidents corporels ONISR (sécurité routière France)

Tous les accidents corporels d'une année, en quatre fichiers relationnels. Le meilleur choix pour
les jointures multi-tables.

- Format : CSV, séparateur point-virgule, encodage à vérifier (souvent latin1). Schéma explicite.
- Clé de jointure : `Num_Acc` relie les quatre tables (`caracteristiques`, `lieux`, `vehicules`,
  `usagers`).
- Analyses : gravité par météo ; accidents par heure et jour ; classement des départements
  (window) ; profils d'usagers ; tableau croisé des quatre tables.
- Optimisation évidente : broadcast de la plus petite table, ou cache de la table jointe.

### Option D : MovieLens (notes de films)

Le classique des jointures et agrégations. Utile si vous visez le bonus MLlib.

- Format : CSV. Jointure `ratings` + `movies` sur `movieId`. Commencer par `ml-latest-small`.
- Analyses : films les mieux notés (avec un seuil de votes) ; popularité par genre ; notes par
  utilisateur ; classement par genre (window).
- Optimisation évidente : broadcast de `movies` (petit) dans la jointure avec `ratings` (gros).
- Bonus naturel : recommandation par ALS (MLlib).

---

## 3. Étapes de la journée

Architecture vue en cours : couche brute (bronze), nettoyée (silver), agrégée (gold). Pas obligé
d'écrire physiquement les trois couches, mais la logique doit être présente.

```
brut (bronze)  ->  nettoyé (silver, Parquet)  ->  agrégé (gold, résultats)
                                                ->  exploration (mesures)  ->  rapport
```

### Étape 1 : ingestion et nettoyage (bronze -> silver)

- Lire le brut. Pour du CSV, définir un schéma explicite (`StructType`), pas `inferSchema`.
- Inspecter : `printSchema()`, `show(5)`, `count()`, `describe()` sur les colonnes numériques.
- Typer (dates, nombres), dériver les colonnes utiles (`withColumn`).
- Nettoyer : doublons (`dropDuplicates`), manquants (`na.drop`/`na.fill`), aberrants (montants
  négatifs, distances ou surfaces nulles, dates incohérentes).
- Écrire la couche silver en Parquet, partitionnée par une colonne à faible cardinalité (mois,
  département, année).

### Étape 2 : transformation et analyses (silver -> gold)

- Relire la couche Parquet propre (pas le brut).
- Vos trois analyses : une agrégation, une jointure, une window function.
- Votre optimisation : broadcast, cache, ou repartition, en mesurant l'effet.
- Écrire les résultats de synthèse (petits fichiers).

### Étape 3 : exploration au-delà du cours

- Choisir une piste du menu (section 6).
- Isoler une expérience reproductible : un seul réglage qui varie, le reste fixe.
- Mesurer avant/après (temps, octets lus, durée des tasks, ou lecture de plan).
- Noter le résultat, même contre-intuitif ou négatif.

### Étape 4 : rapport

- Remplir le gabarit `projects/rapport-modele.md`.
- Insérer les captures de la Spark UI et les extraits de résultats relevés en cours de route.
- Relire : démarche claire, chiffres présents, limites assumées.

---

## 4. Jalons horaires

Indicatifs. L'important est l'ordre et de ne pas sauter le cadrage ni la rédaction.

| Horaire | Bloc | Ce que vous faites |
|---------|------|--------------------|
| 9h30  | Cadrage (30 min)        | Socle, choix du jeu de données, constitution des équipes |
| 10h00 | Conception (45 min)     | Schéma cible, étapes du pipeline, questions métier, piste d'exploration visée |
| 10h45 | Étape 1 (jusqu'à 13h00) | Ingestion, typage, nettoyage, écriture de la couche silver |
| 13h00 | Pause déjeuner          | - |
| 14h00 | Étape 2 (90 min)        | Agrégations, jointures, window functions, optimisation, lecture de la Spark UI |
| 15h30 | Étape 3 (60 min)        | Exploration au-delà du cours, mesures |
| 16h30 | Étape 4 (60 min)        | Rédaction et relecture du rapport |
| 17h30 | Remise                  | Dépôt du rapport et du code |

> Visez une couche silver écrite et relue avant le déjeuner : c'est le point de bascule de la
> journée. Arrêtez le code vers 16h30 pour garder une heure pleine de rédaction. Le bloc d'ingestion
> (2h15) suppose un CSV à typer et nettoyer ; sur le taxi (Parquet déjà typé) il est plus court,
> alors profitez de l'avance pour une quatrième analyse, un mois de plus, ou un début d'exploration.

---

## 5. Le rapport écrit (livrable principal)

C'est le livrable noté. Pas de présentation orale. Remplir `projects/rapport-modele.md` (ou un
document équivalent). Il contient, dans cet ordre :

- Le jeu de données et le schéma cible.
- Le pipeline : bronze/silver/gold, choix de partitionnement, ce qui a été nettoyé et combien de
  lignes écartées.
- Les trois analyses : pour chacune, la question, le code clé, un extrait de résultat, la lecture
  métier.
- L'optimisation : pourquoi, mesure avant/après ou extrait de plan, ce que ça change.
- La lecture de la Spark UI : où se produit le shuffle, capture(s), commentaire.
- L'exploration au-delà du cours : la piste choisie, le protocole, les mesures, la conclusion.
- Ce qu'on a appris et les limites.

> Le rapport se nourrit du travail au fil de l'eau : capturez la Spark UI et notez les temps pendant
> que ça tourne, pas à 17h. Un chiffre relevé après coup n'est pas reproductible. Visez un document
> dense et lisible : extraits de code, extraits de résultats, captures, phrases courtes.

---

## 6. Menu d'exploration (au-delà du cours)

Choisir au moins une piste. Chacune va plus loin que les TP : à vous de trouver l'API et la méthode.
Le but est un résultat chiffré et une conclusion, pas une démo qui tourne.

- **AQE et nombre de partitions.** Mesurer l'effet de l'Adaptive Query Execution et du nombre de
  partitions de shuffle sur une de vos agrégations. Quel réglage pour votre volume, et que montre le
  plan ?
- **Données mal réparties (skew).** Certaines clés concentrent les lignes (sur le taxi, les zones
  aéroport). Mesurer le déséquilibre des tasks d'un même stage, le corriger, puis remesurer.
- **UDF, pandas_udf et fonction native.** Écrire la même transformation de trois façons et comparer
  les temps. Quand une UDF se justifie-t-elle, et que coûte-t-elle ?
- **Table gérée et upsert.** Remplacer la réécriture complète de la silver par une mise à jour
  incrémentale (catalogue, ou un format transactionnel). Montrer la différence.
- **Déploiement par spark-submit.** Lancer le pipeline en soumission au lieu d'une session
  interactive. Point de départ : `demos/08-deploiement/cluster-standalone.sh`. Qu'est-ce que change
  le passage de `local[*]` à une soumission ?
- **Pushdown mesuré.** Sur une couche Parquet partitionnée, prouver le partition pruning et le
  predicate pushdown, et chiffrer les octets lus avec et sans filtre.
- **Benchmark de formats.** Comparer plusieurs formats et compressions : taille sur disque et temps
  de relecture d'une agrégation. Point de départ : `demos/03-ingestion/formats_benchmark.py`, à
  pousser au-delà (codecs de compression, temps d'une agrégation).
- **(Plus lourd, bonus) Streaming ou MLlib.** Un agrégat continu sur un flux simulé, ou un
  mini-modèle. Bases : `demos/05-streaming`, `demos/06-mllib`, `exercises/09-streaming-mllib.md`.

> Une exploration réussie tient en trois phrases : ce que vous avez testé, ce que vous avez mesuré,
> ce que vous en concluez. Un résultat négatif clairement expliqué vaut un résultat positif.

---

## 7. Livrables

À déposer en fin de journée (un dépôt ou un dossier partagé par équipe) :

- **Le code PySpark** du pipeline, découpé en étapes claires. Partir de `starter-code/pipeline.py`.
- **Les sorties** : la couche silver en Parquet et les fichiers de synthèse des analyses (petits
  fichiers, pas le brut).
- **Le rapport écrit** : `rapport-modele.md` rempli (ou équivalent), captures de la Spark UI
  incluses.

---

## 8. Grille d'évaluation

Notée sur 20 points. Récompense un socle complet et bien expliqué avant la complexité.

| Critère | Points | Attendu |
|---------|--------|---------|
| Ingestion et nettoyage | 4 | Schéma correct (explicite pour le CSV), typage juste, doublons et aberrants traités, couche silver écrite en Parquet. |
| Analyses (3 minimum) | 4 | Une agrégation, une jointure, une window function. Résultats cohérents et lecture métier présente. |
| Optimisation mesurée | 3 | Une optimisation réelle (broadcast, cache, repartition) avec mesure avant/après ou lecture de plan. |
| Lecture de la Spark UI | 2 | Repérer un shuffle, lire le DAG, commenter stages et tasks. |
| Exploration au-delà du cours | 3 | Une piste du menu menée au-delà des TP : protocole, mesure, conclusion chiffrée. |
| Rapport écrit | 3 | Document clair et structuré : démarche, choix, résultats, lectures métier, limites. |
| Qualité du code | 1 | Lisible, découpé, reproductible. Pas de `collect()` inutile sur gros volume. |
| **Total** | **20** | |

> Bonus jusqu'à +2 points (plafonné à 20/20) pour une seconde exploration ou une piste avancée
> réussie et bien expliquée.

---

## 9. Conseils et pièges à éviter

- **Commencez petit.** Un département, un mois, MovieLens small. Le pipeline de bout en bout sur un
  petit volume avant d'ajouter des mois. Ce qui marche sur 100 000 lignes marchera sur 10 millions.
- **Validez le schéma tôt.** Un `printSchema()` dès le début évite de découvrir en fin de journée
  qu'une colonne numérique a été lue comme du texte (fréquent en CSV).
- **Ne collectez pas tout.** `collect()` ou `toPandas()` ramène toutes les données sur le driver et
  peut le faire planter. Utilisez `show()`, `take(n)`, ou écrivez sur disque.
- **Méfiez-vous de la division par zéro.** En Spark, `x / 0` renvoie `Infinity` ou `NaN`, qui
  pollue toutes les moyennes ensuite. Protégez le dénominateur : `F.when(col > 0, ...).otherwise(None)`.
- **Utilisez `&`, `|`, `~`, pas `and`, `or`, `not`** dans les filtres, et parenthésez chaque
  condition.
- **Mesurez avant d'écrire.** Une optimisation ou une exploration sans chiffre ne compte pas.
  Relevez les temps et les captures pendant que le job tourne, pas après.
- **Cadrez l'exploration.** Une piste, une question, une mesure. Mieux vaut une exploration nette
  qu'un chantier inachevé.
- **Gardez l'heure de rédaction.** Arrêtez le code vers 16h30. Un rapport clair sur un pipeline
  simple vaut mieux qu'un code ambitieux non documenté.
