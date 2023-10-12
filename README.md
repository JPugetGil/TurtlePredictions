# TIW6 - TurtlePrediction

[TOC]

## Informations groupe

- ICMEN Malik p1704010
- PUGET GIL Jey p1608911
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
Pour simplifier l'analyse, nous avons choisi de calculer également la "vitesse" de la tortue à ce moment-là,
c'est-à-dire la différence entre la position précédente et la position courante.

Les données sont classées par course puis par tortue (un fichier par tortue et par type de course).

## Analyse des comportements

### Script `analyse.sh`

Ce script exécute la récupération des données via [http://tortues.ecoquery.os.univ-lyon1.fr/race]() comme vu
précédemment, puis fait l'analyse pour chaque tortue de chaque course.

Uniquement pour les besoins du debug, nous avons ajouté la possibilité d'exécuter le script d'analyse sans faire la
récupération des données, qui est très longue.
Il n'est cependant pas nécessaire d'ajouter un argument pour faire fonctionner le script.

### Reconnaissance des comportements

Une fois la récupération réalisée, nous pouvons passer à l'analyse des comportements des tortues pour chaque course.
Pour chaque tortue, nous allons successivement appliquer une batterie de tests, dans cet ordre : si elle est régulière,
si elle est fatiguée, si elle est cyclique. La dernière étape concernant la lunatique permet d'identifier les différents
sous-comportements.

Avant de commencer l'analyse à proprement parler, on filtre les lignes qui ne sont pas complètes (ce qui peut arriver en
cas de problème sur les machines qui interrompent le processus d'écriture).

Pour rappel, la vitesse est calculée à l'acquisition et résulte de la différence entre une position et sa position
précédente *enregistrée*, et ce même si des tops ont été sautés. Certaines vitesses risquent d'être aberrantes.
Nous avons donc dû adapter nos algorithmes pour qu'ils soient résistant à la non-continuité des données.

Pour nos algorithmes, nous avons eu besoin de connaître les index des données que nous traitions pour identifier les
comportements, et nous avons donc dû transformer les types RDD en Array.
Cela impacte donc les performances d'analyse, mais nous avons mesuré que sur nos machines, pour la course large qui
contient environ 28 h de données, l'analyse a mis environ 6-7 min à s'exécuter.

*Note* : Nous avions prévu une récupération des données plus longue, mais elle a été interrompue par la saturation de
stockage du serveur.

Enfin, les données d'analyses sont stockés dans un fichier unique par type de course. Chaque ligne correspond à une
tortue, elle contient son identifiant, son type et les informations concernant son comportement.

#### Analyse d'une régulière

On vérifie que la vitesse est constante sur l'ensemble des tops de la tortue.
En cas de saut de top, la vitesse peut apparaître plus grande que ce qu'elle n'est réellement.
On vérifie donc à chaque top s'il suit directement le précédent, et si c'est le cas, mais que les vitesses varient d'un
top à l'autre ou qu'elle est différente de la première vitesse enregistrée, alors elle n'est pas régulière.

Une amélioration en termes de performance aurait été d'effectuer la vérification uniquement sur 2 ou éventuellement 3
phases de température ou de qualité différentes.
Si elle n'a pas modifié sa vitesse sur plusieurs phases, alors elle on peut considérer qu'elle n'est pas lunatique.
Dans ce cas on ne peut pas être sûr entièrement que la tortue est régulière, si jamais la tortue n'est pas suffisamment
sensible aux modifications entre chaque phase, mais l'expérience nous a montré que ça n'arrivait pas.
On peut alors faire cette hypothèse que les changements de paramètre à chaque phase sont suffisamment significatifs pour
faire changer le comportement des tortues lunatiques.

#### Analyse d'une fatiguée

La tortue fatiguée possède 2 phases dans son comportement : une phase de décélération et une phase d'accélération.
Nous sommes partis de ce principe pour identifier les tortues fatiguées.
On commence par un échantillon de 4 vitesses consécutives, et on calcule l'écart entre chaque (l'accélération).
De cette liste d'écarts, on prend la valeur maximale (dans le cas où l'on arrive à un maximum ou un minimum, cette
valeur est plus petite que les autres), et on fait l'hypothèse pour la suite que c'est le paramètre de la tortue
fatiguée.
Ensuite, on parcourt les autres tops et on compare cette valeur à l'accélération entre deux tops consécutifs.
On calcule par la même occasion la vitesse maximale.
Si elle est identique tout du long (sauf dans les cas où la vitesse courante est à 0 ou au maximum enregistré), alors
on considère qu'elle est cyclique de paramètres de la vitesse maximale et de l'accélération.

De manière analogue à l'analyse de la tortue régulière, on aurait pu se contenter de vérifier sur un nombre limité de
phases pour gagner en performance, mais on partirait d'une hypothèse dont on ne peut pas vérifier la validité.

Par ailleurs, on aurait aussi pu chercher les tops où la vitesse est de 0, calculer l'accélération sur le top suivant,
puis vérifier à chaque top si le pas est le même sur l'un des 2 tops suivants (pour passer les cas des valeurs
maximales et nulles), tout en calculant le maximum.
On peut également ajouter la vérification que l'on tombe de manière régulière en nombre de tops sur la même vitesse
maximale.
Si le comportement est le même sur tous les tops, alors la tortue est fatiguée.

#### Analyse d'une cyclique

On commence par générer un motif, en parcourant le tableau d'avancement de la tortue, jusqu'à arriver à un élément
déjà rencontré.
On vérifie ensuite si ce motif se répète tout au long de notre enregistrement. S’il ne se répète pas, on vérifie
qu’on n'a pas loupé de top. Si oui, on ignore, sinon, elle n'est pas cyclique, et comme on a aussi vérifié qu'elle
n'est ni régulière ni fatiguée, elle est donc lunatique.

On part de l'hypothèse que nous n'avons aucun top manquant dans ce motif, et qu'il est donc complet.
Le motif étant au maximum grand de 800 tops, et que le nombre de tops manqués constaté est très faible, nous avons
considéré cette hypothèse valide. Elle est notamment vérifiée sur les 800 premiers tops de nos données récupérées.
Mais cette hypothèse peut effectivement poser problème dans le cas d'une lunatique qui a un "sous comportement"
cyclique, qui peut survenir plus tard dans l'enregistrement.

#### Analyse de lunatique

On arrive à cette partie d'analyse quand aucun autre comportement ne correspond strictement à la tortue analysée.
On considère donc à ce niveau de l'analyse que la tortue est lunatique.
Dans les tests précédents, nous avons pris soin d'enregistrer l'index où la rupture du comportement analysé se
produit.
On choisit ensuite le comportement dont l'index de rupture est le plus grand, et cela détermine un sous-comportement
de la tortue lunatique et quand il se termine.
On recommence cette analyse (les tests successifs pour le comportement régulier, fatiguée et cyclique) de la tortue
lunatique sur une portion de sa course, celle après la fin de ce sous-comportement, pour déterminer un nouveau
sous-comportement, et ce jusqu'à ce qu'il ne reste plus qu'un élément restant dans la course enregistrée.

À chaque sous comportement trouvé, on enregistre son type, son top de début, la qualité et la température.
Ces données nous servirons pour la prédiction.

### Parsing

Nos données d'analyse sont enregistrées sous forme de fichier CSV à 3 colonnes : (id tortue, type de comportement,
informations de comportement).
Comme les informations nécessaires à la prédiction varie selon le type de comportement, nous avons décidé que le
contenu
de la dernière colonne serait sous la forme d'une chaîne de caractères formatée, que nous pourrons adapter à chaque
comportement.
Nous avons donc implémenté des classes utilitaires spécifiques pour parser les éléments de cette colonne, des classes
modélisant ces données et des méthodes capables de convertir ces classes en chaînes formatées.

- `TurtleBehaviorData`, `TurtleRegularData`, `TurtleTiredData`, `TurtleCyclicData`, `TurtleLunaticData`,
  `TurtleSubBehaviorData` : modélisent les données.
    - `TurtleBehaviorData` est la classe abstraite parente des autres classes de comportement.
    - `TurtleSubBehaviorData` permet de modéliser les différents comportements pris par une tortue lunatique, elle
      contient un attribut de type `TurtleBehaviorData` ainsi que d'autres informations nécessaires à la prédiction.
- `BehaviorFormatter` : s'occupe des conversions entités ↔ chaîne de caractères
- `TurtleDataBuilder` : contient les méthodes de constructions des entités à partir de ligne

## Prédiction de la position

Nous avons considéré pour cette partie qu'il n'était pas demandé de pouvoir déterminer la position de la tortue avec
un `deltatop` négatif, car le terme "prédiction" implique un événement futur, et non passé.
Notre algorithme peut être adapté pour être appliqué avec un `deltatop` négatif, mais nous avons jugé qu'il n'était pas nécessaire de l'implémenter pour répondre au sujet.

Avant d'exécuter la prédiction, nous effectuons une vérification sur les paramètres du script.
Nous vérifions que les positions entrées correspondent au comportement que nous avons déterminé pendant la phase
d'analyse. Si ce n'est pas le cas, nous indiquons l'incohérence et nous ignorons les positions 2 et 3 données pour
effectuer la prédiction.

### Prédiction d'une régulière

Dans le cas d'une tortue régulière, on détermine sa position future en appliquant la formule
$`position = pos_1 + \Delta top \times v`$ où $`v`$ est la vitesse déterminée lors de l’analyse.
Cette prédiction est donc déterministe.

### Prédiction d'une fatiguée

Les 3 positions données en paramètres nous permettent de déterminer si la tortue fatiguée est dans une phase
d’accélération ou de décélération.
Si elle accélère, itérativement, on ajoute à la position la vitesse valant la vitesse précédente + le "pas" déterminé
par l'analyse, jusqu'à ce que la vitesse atteigne le *maximum* dans la phase d'accélération, puis on ajoute à la
position la vitesse précédente - le "pas", jusqu'à atteindre *0* dans la phase de ralentissement, (et inversement si
la tortue décélère).
On effectue chaque avancée un nombre `deltatop` de fois.
Cette prédiction est donc également déterministe.

### Prédiction d'une cyclique

À partir des vitesses déduites des positions données, on détermine à quel moment du cycle en est la tortue.
On récupère donc l'index de la vitesse, et on applique le motif en boucle à partir de cet index sur une durée de
`deltatop`.
Nous obtenons donc une position certaine, dans le cas où le cycle a bien été construit.

### Prédiction d'une lunatique

Comme on part du principe que le comportement ne change pas dans la durée `deltatop`, nous n'avons pas à prédire les
changements de qualité et de température.

On commence par faire une régression linéaire en fonction de la qualité, de la température pour déterminer le
comportement, avec les sous-comportements enregistrés dans l'analyse.
On obtient une fonction de la forme :
$`comportement = qualité \times a + température \times b + c`$, où $`a`$, $`b`$, $`c`$ sont les coefficients propres à
la tortue.
On peut donc identifier le type de comportement pris par la tortue assez précisément.

Afin de trouver les paramètres propres du comportement, on compare la qualité et la température d'entrée avec celles
que nous avons collectées.
On récupère les sous-comportements enregistrés qui ont le même type que celui déterminé par la régression linéaire,
puis on choisit celui dont la température et la qualité sont les plus proches de celles données en entrée.
On applique ensuite ce comportement avec les paramètres associés tels que décrit précédemment.

Cette méthode est donc approximative, mais elle est suffisamment efficace pour un grand nombre de tops enregistrés et
donc un grand nombre de sous-comportements enregistrés.
