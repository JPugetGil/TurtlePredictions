#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys

import requests
import time
import pandas as pd


def get_data(add, r_type):
    print(f"Récupération des données de la course de type : {r_type}.")
    race[r_type] = []
    time_counter = 0.0
    while time_counter < TOTAL_DURATION:
        t_begin = time.time()
        do_request(add, r_type)
        t_stop = time.time()
        delay = (t_stop - t_begin)
        print_information_verbose(f"Durée de récupération : {delay} secondes")
        top_size = len(race[r_type])
        if top_size >= TOP_INTERVAL:
            # Calcul des (TOP_INTERVAL - 5) vitesses
            compute_tortoises_speed_interval(r_type)
            # Écriture des fichiers
            print_tortoise_journey(tortoises_dict, r_type)
            # Vidange de la mémoire
            print_information_verbose(f"Suppression des [:{TOP_BUFFER}] tops de toutes les tortues")
            for i in range(0, len(tortoises_dict[r_type])):
                del tortoises_dict[r_type][i][:TOP_BUFFER - 1]
            # Réinitialise le nombre de tops récupérés
        time_counter += PERIOD_BETWEEN_TOPS
        print_information_verbose(f"Je vais donc attendre : {PERIOD_BETWEEN_TOPS - delay} secondes")
        time.sleep(PERIOD_BETWEEN_TOPS - delay)
        print_information_verbose(f"Taille race[{r_type}] : {top_size}. Temps écoulé : {time_counter} secondes.")


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
            print_information_verbose(f"Écriture sur tortoises_dict[{r_type}][{j}]")
            print_information_verbose(f"Lecture sur les éléments de race[{r_type}][{i}]['tortoises'][{j}]")
            print_information_verbose(f"Lecture sur race[{r_type}][{i - 1}]['tortoises'][{j}]['position']")
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
    print_information_verbose(f"Top buffer : {TOP_BUFFER - 1}. Taille race[r_type] : {len(race[r_type])}")
    for i in range(1, TOP_BUFFER):
        print_information_verbose(f"Écriture sur le tableau tortoises_dict[{r_type}]")
        print_information_verbose(f"Lecture sur les éléments de race[{r_type}][{i}]['tortoises']")
        print_information_verbose(f"Lecture sur race[{r_type}][{i - 1}]['tortoises']")
        for j in range(nb_tortoises):
            if len(tortoises_dict[r_type][j]) == 0 or tortoises_dict[r_type][j][-1]["top"] != \
                    race[r_type][i]["tortoises"][j]["top"]:
                tortoises_dict[r_type][j].append({
                    "top": race[r_type][i]["tortoises"][j]["top"],
                    "position": race[r_type][i]["tortoises"][j]["position"],
                    "temperature": race[r_type][i]["temperature"],
                    "qualite": race[r_type][i]["qualite"],
                    "vitesse": (race[r_type][i]["tortoises"][j]["position"] - race[r_type][i - 1]["tortoises"][j][
                        "position"])
                })

    print_information_verbose(f"Suppression de race[{r_type}][:{TOP_BUFFER - 1}]")
    del race[r_type][:TOP_BUFFER - 1]


def initialize_tortoises_var(r_type):
    """Initialise le dictionnaire pour chaque tortue"""
    nb_tortoises = len(race[r_type][0]["tortoises"])
    tortoises_dict[r_type] = [[] for a in range(nb_tortoises)]


def print_tortoise_journey(data_to_write, r_type):
    """Écrit le voyage de chaque tortue dans un fichier csv"""
    if not (os.path.exists(r_type)):
        print_information_verbose(f"Le dossier {r_type} n'est pas présent, nous le créons et ajoutons les fichiers")
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
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 64)
    elif 'small' == r_type:
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 64)
    elif 'medium' == r_type:
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 64)
    elif 'large' == r_type:
        return TOTAL_DURATION // int(PERIOD_BETWEEN_TOPS * 64)


def print_information_verbose(info: str):
    if VERBOSE_MODE_ENABLED:
        print(info)


if __name__ == "__main__":
    VERBOSE_MODE_ENABLED = len(sys.argv) > 1 and sys.argv[1] == "--verbose"
    PERIOD_BETWEEN_TOPS = 2.25
    TOTAL_DURATION = 3600 * 24 * 2  # en secondes
    threads = []
    # Dictionnaire contenant tous les tops de chaque type de course
    race = {}
    # Dictionnaire contenant les informations + la vitesse à chaque top pour chaque tortue
    tortoises_dict = {}
    address = 'http://tortues.ecoquery.os.univ-lyon1.fr/race/'
    race_type = "small"

    print("Récupération des informations...\n"
          "Veuillez patienter environ {} secondes.".format(TOTAL_DURATION))

    TOP_INTERVAL = get_interval(race_type)
    TOP_BUFFER = TOP_INTERVAL - 5
    print_information_verbose(f"Taille du buffer de la course : {TOP_BUFFER}.")
    get_data(address, race_type)
    compute_tortoises_speed(race_type)
    print_tortoise_journey(tortoises_dict, race_type)
    print_information_verbose(f"Récupération de la course \"{race_type}\" terminée.")
    del tortoises_dict[race_type]
    del race[race_type]