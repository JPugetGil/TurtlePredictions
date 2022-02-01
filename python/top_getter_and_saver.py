#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

import requests
import time
import threading
import pandas as pd


def get_data(add, r_type):
    print("Récupération des données de la course de type : {}.".format(r_type))
    print("Temps estimé : {} secondes.".format(TOTAL_DURATION))
    race[r_type] = []
    time_counter = 0.0
    top_counter = 0
    while time_counter < TOTAL_DURATION:
        thread = threading.Thread(target=do_request, args=(add, r_type))
        threads.append(thread)
        thread.start()
        if top_counter >= TOP_INTERVAL:
            # Calcul des (TOP_INTERVAL - 10) vitesses
            compute_tortoises_speed_interval(r_type)
            # Ecriture des fichiers
            print_tortoise_journey(tortoises_dict, r_type)
            # Vidange de la mémoire
            for i in range(0, len(tortoises_dict[r_type])):
                del tortoises_dict[r_type][i][:TOP_BUFFER]
            # Réinitialise le nombre de tops récupérés
            top_counter = 0
        time_counter += PERIOD_BETWEEN_TOPS
        top_counter += 1
        time.sleep(PERIOD_BETWEEN_TOPS)
        print("Temps écoulé : {} secondes.".format(time_counter))


def do_request(add, r_type):
    """Appelle le webservice de la course passée en paramètre"""
    response = requests.get(add + r_type)
    json_to_dict = response.json()
    if not (json_to_dict in race[r_type]):
        race[r_type].append(json_to_dict)


def compute_tortoises_speed(r_type):
    """Ajoute dans le dictionnaire la vitesse de chaque tortue à chaque top"""
    initialize_tortoises_var(r_type)
    nb_tops = len(race[r_type])
    nb_tortoises = len(race[r_type][0]["tortoises"])
    for i in range(1, nb_tops):
        for j in range(nb_tortoises):
            tortoises_dict[r_type][j].append({
                "top": race[r_type][i]["tortoises"][j]["top"],
                "position": race[r_type][i]["tortoises"][j]["position"],
                "temperature": race[r_type][i]["temperature"],
                "qualite": race[r_type][i]["qualite"],
                "vitesse": (race[r_type][i]["tortoises"][j]["position"] - race[r_type][i - 1]["tortoises"][j][
                    "position"])
            })


def compute_tortoises_speed_interval(r_type):
    """Ajoute dans le dictionnaire la vitesse de chaque tortue à chaque top"""
    initialize_tortoises_var(r_type)
    nb_tortoises = len(race[r_type][0]["tortoises"])
    for i in range(1, TOP_BUFFER):
        for j in range(nb_tortoises):
            tortoises_dict[r_type][j].append({
                "top": race[r_type][i]["tortoises"][j]["top"],
                "position": race[r_type][i]["tortoises"][j]["position"],
                "temperature": race[r_type][i]["temperature"],
                "qualite": race[r_type][i]["qualite"],
                "vitesse": (race[r_type][i]["tortoises"][j]["position"] - race[r_type][i - 1]["tortoises"][j][
                    "position"])
            })

    del race[r_type][:TOP_BUFFER]


def initialize_tortoises_var(r_type):
    """Initialise le dictionnaire pour chaque tortue"""
    for t in threads:
        t.join()
    nb_tortoises = len(race[r_type][0]["tortoises"])
    tortoises_dict[r_type] = [[] for a in range(nb_tortoises)]


def print_tortoise_journey(data_to_write, r_type):
    """Écrit le voyage de chaque tortue dans un fichier csv"""
    if not (os.path.exists(r_type)):
        os.mkdir(r_type)

    for turtleId in range(len(data_to_write[r_type])):
        filename = "{}/tortoises-{}.csv".format(r_type, turtleId)
        if os.path.exists(filename):
            df = pd.DataFrame(data_to_write[r_type][turtleId])
            df.to_csv(filename, mode='a', header=False, index=False)
        else:
            df = pd.DataFrame(data_to_write[r_type][turtleId])
            df.to_csv(filename, index=False)


def get_interval(r_type: str) -> int:
    if 'tiny' == r_type:
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 2)
    elif 'small' == r_type:
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 4)
    elif 'medium' == r_type:
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 8)
    elif 'large' == r_type:
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 16)


if __name__ == "__main__":
    PERIOD_BETWEEN_TOPS = 2.75
    TOTAL_DURATION = 3600 * 24 * 2  # en secondes
    threads = []
    # Dictionnaire contenant tous les tops de chaque type de course
    race = {}
    # Dictionnaire contenant les informations + la vitesse à chaque top pour chaque tortue
    tortoises_dict = {}
    address = 'http://tortues.ecoquery.os.univ-lyon1.fr/race/'
    race_types = ["tiny", "small", "medium", "large"]

    print("Récupération des informations...\n"
          "Veuillez patienter environ {} secondes.".format(TOTAL_DURATION * len(race_types)))

    for race_type in race_types:
        TOP_INTERVAL = get_interval(race_type)
        TOP_BUFFER = TOP_INTERVAL - 5
        get_data(address, race_type)
        compute_tortoises_speed(race_type)
        print_tortoise_journey(tortoises_dict, race_type)
        print(f"Course \"{race_type}\" terminée.")
        del tortoises_dict[race_type]
        del race[race_type]
