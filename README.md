# TIW6 - TurtlePrediction

> On décrira le ou les programmes implémentés, les algorithmes mis au point pour retrouver les catégories de tortues et les paramètres, ainsi que l’algorithme de prédiction

[TOC]

## Informations groupe

- ICMEN Malik p1704010
- GIL Jérôme p1608911
- TRÉMÉ Guillaume p1507711
- VIGLIANO Léa p1517348

Compte HDFS utilisé : `p1608911`

## Récupération des données

Nous avons effectué cette partie en Python, car il était plus simple de manipuler des outils d'écriture et de requêtes.
Pour récupérer les données, nous avons dans un premier temps écrit en local pour passer plus tard en HDFS avec PySpark.
Cependant, nous avons manqué de temps pour comprendre comment lire et écrire des fichiers CSV.
Nous avons donc opté pour une méthode où nous avons optimisé la mémoire du programme et en utilisant des threads.
Cette méthode nous a posé des difficultés lorsqu'il a fallu récupérer des courses différentes dans un même script, donc
nous avons choisi d'exécuter 4 fois le script avec un paramètre différent dans `analyse.sh`.

## Analyse des comportements

### Script `analyse.sh`

Ce script exécute la récupération des données via [http://tortues.ecoquery.os.univ-lyon1.fr/race]() comme vu
précédemment, puis fait l'analyse pour chaque tortue de chaque course.

Uniquement pour les besoins de debug, nous avons ajouté la possibilité d'exécuter le script d'analyse sans faire la
récupération des données, qui est très longue.
Il n'est cependant pas nécessaire d'ajouter un argument pour faire fonctionner le script.

### Reconnaissance des comportements

#### Régulière

#### Fatiguée

#### Cyclique

#### Lunatique

### Parsing


## Prédiction de la position

### Script `prediction.sh`


