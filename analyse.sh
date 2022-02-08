#!/bin/sh

destHDFS="/user/p1608911"
destHDFSdata="$destHDFS/data"
analysisOnly=false

if [ -n "$1" ]
then
  analysisOnly=true
fi

if [ ! $analysisOnly = true ]
then
  echo "Récupération des données de courses..."
  cd ./python || exit
  python3 top_getter_and_saver.py tiny &
  python3 top_getter_and_saver.py small &
  python3 top_getter_and_saver.py medium &
  python3 top_getter_and_saver.py large &
  wait
  echo "Récupération effectuée"

  # Nettoyage des données existantes
  echo "Transfert sur HDFS..."
  hdfs dfs -rm -R $destHDFSdata || true
  hdfs dfs -mkdir $destHDFSdata || true

  hdfs dfs -mkdir "$destHDFSdata/tiny/"
  hdfs dfs -mkdir "$destHDFSdata/small/"
  hdfs dfs -mkdir "$destHDFSdata/medium/"
  hdfs dfs -mkdir "$destHDFSdata/large/"

  hdfs dfs -copyFromLocal -f tiny/* "$destHDFSdata/tiny/"
  hdfs dfs -copyFromLocal -f small/* "$destHDFSdata/small/"
  hdfs dfs -copyFromLocal -f medium/* "$destHDFSdata/medium/"
  hdfs dfs -copyFromLocal -f large/* "$destHDFSdata/large/"

  echo "Transfert sur HDFS effectué."
fi

echo "Analyse des données..."
# TODO Changer chemin du .jar
spark-submit --class TurtlePredictions "$destHDFS/TurtlePredictions-assembly-0.1.jar"
echo "Analyse terminée."
