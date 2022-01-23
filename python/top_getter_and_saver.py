#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv

import requests
import time
import threading
import pandas as pd


def get_data(add, r_type):
    print("Récupération des données de la course de type : {}.".format(r_type))
    print("Temps estimé : {} secondes.".format(TOTAL_DURATION))
    race[r_type] = []
    counter = 0
    while counter < TOTAL_DURATION:
        thread = threading.Thread(target=do_request, args=(add, r_type))
        threads.append(thread)
        thread.start()
        counter += PERIOD_BETWEEN_TOPS
        time.sleep(PERIOD_BETWEEN_TOPS)
        print("Temps écoulé : {} secondes.".format(counter))


# Appelle le webservice de la course passée en paramètre
def do_request(add, r_type):
    response = requests.get(add + r_type)
    json_to_dict = response.json()
    if not (json_to_dict in race[r_type]):
        race[r_type].append(json_to_dict)


# Ajoute dans le dictionnaire la vitesse de chaque tortue à chaque top
def compute_tortoises_speed(r_type):
    initialize_tortoises_var(r_type)
    nb_tops = len(race[r_type])
    nb_tortoises = len(race[r_type][0]["tortoises"])
    for i in range(1, nb_tops):
        for j in range(nb_tortoises):
            if "steps" in tortoises_dict[r_type][j]:
                tortoises_dict[r_type][j]["steps"].append({
                    "top": race[r_type][i]["tortoises"][j]["top"],
                    "position": race[r_type][i]["tortoises"][j]["position"],
                    "temperature": race[r_type][i]["temperature"],
                    "qualite": race[r_type][i]["qualite"],
                    "vitesse": (race[r_type][i]["tortoises"][j]["position"] - race[r_type][i - 1]["tortoises"][j][
                        "position"])
                })
            else:
                tortoises_dict[r_type][j] = {
                    "id": race[r_type][i]["tortoises"][j]["id"],
                    "steps": [{
                        "top": race[r_type][i]["tortoises"][j]["top"],
                        "position": race[r_type][i]["tortoises"][j]["position"],
                        "temperature": race[r_type][i]["temperature"],
                        "qualite": race[r_type][i]["qualite"],
                        "vitesse": (race[r_type][i]["tortoises"][j]["position"] - race[r_type][i - 1]["tortoises"][j][
                            "position"])
                    }]
                }


# Initialise le dictionnaire pour chaque tortue
def initialize_tortoises_var(r_type):
    for t in threads:
        t.join()
    nb_tortoises = len(race[r_type][0]["tortoises"])
    tortoises_dict[r_type] = [{} for a in range(nb_tortoises)]


# Ecrit le voyage de chaque tortue dans un fichier csv
def print_tortoise_journey(data_to_write, r_type):
    filename = "tortoises-{}.csv".format(r_type)
    df = pd.json_normalize(data_to_write[r_type])
    df.to_csv(filename, header=False, index=False, quoting=csv.QUOTE_NONE, escapechar=' ')
    print("Récupération des données de la course de type : {} terminée.".format(r_type))
    print("Les données sont disponibles dans le fichier : {}.\n".format(filename))


if __name__ == "__main__":
    PERIOD_BETWEEN_TOPS = 2.75
    TOTAL_DURATION = 300
    threads = []
    # Dictionnaire contenant tous les tops de chaque type de course
    race = {}
    # Dictionnaire contenant les informations + la vitesse à chaque top pour chaque tortue
    tortoises_dict = {}
    address = 'http://tortues.ecoquery.os.univ-lyon1.fr/race/'
    race_types = ["tiny", "small", "medium", "large"]

    print("Récupération des informations..\n"
          "Veuillez patienter environs {} secondes.".format(TOTAL_DURATION * len(race_types)))

    for race_type in race_types:
        get_data(address, race_type)
        compute_tortoises_speed(race_type)
        print_tortoise_journey(tortoises_dict, race_type)