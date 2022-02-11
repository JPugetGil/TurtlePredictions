#!/bin/sh

if [ -z "$9" ];
then
  echo "Vous n'avez pas tous les arguments requis, exemple de commande :"
  echo "./prediction.sh tiny 1 893127 141559689 141559724 141559764 40 0.3100529516794682 64"
else
  echo "Analyse de la tortue $2 dans la course $1"
  spark-submit --class TurtlePredictions "TurtlePredictions-assembly-0.1.jar" "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9"
fi
