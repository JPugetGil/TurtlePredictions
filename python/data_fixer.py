from os import listdir
from os.path import isfile, join
import sys
import pandas


def check_complete(t):
    n_top = t[0][0]
    for t_info in range(1, len(t)):
        if t[t_info][0] != n_top + 1:
            return False
        else:
            n_top = n_top + 1
    return True


def get_index_of_vitesse(t, vitesse):
    for index, item in enumerate(t):
        if item[4] == vitesse:
            return index
    return -1


def is_tired(t):
    max_s = max(e[4] for e in t)
    max_index = get_index_of_vitesse(t, max_s)
    min_s = min(e[4] for e in t)
    min_index = get_index_of_vitesse(t, min_s)

    if max_index > min_index:
        rhyt = t[min_index + 1][4] - t[min_index][4]
        for i in range(min_index, max_index - 1):
            if t[i + 1][4] - t[i][4] != rhyt:
                return False, None, None
    else:
        rhyt = t[max_index][4] - t[max_index + 1][4]
        for i in range(max_index, min_index - 1):
            if t[i][4] - t[i + 1][4] != rhyt:
                return False, None, None
    return True, max_s, rhyt


def is_cyclic(t):
    meet_elements = []
    for t_info in t:
        if t_info[4] not in meet_elements:
            meet_elements.append(t_info[4])
        else:
            return False, []
    return True, meet_elements


if __name__ == "__main__":
    MY_PATH = "test" if len(sys.argv) <= 1 else sys.argv[1]
    onlyfiles = [f for f in listdir(MY_PATH) if isfile(join(MY_PATH, f))]
    for file in onlyfiles:
        print(file)
        arrayOfTop = []
        df = pandas.read_csv(f"{MY_PATH}/{file}", index_col='top')
        itertuples = df.itertuples()
        for row in itertuples:
            arrayOfTopLen = len(arrayOfTop)
            if arrayOfTopLen == 0 or df.loc[df.index[0], "vitesse"] != row[4]:
                arrayOfTop.append(row)
            else:
                break

        lenArrayOfTop = len(arrayOfTop)

        print(lenArrayOfTop)
        if lenArrayOfTop == 1:
            print("window regular")
            vitesse = arrayOfTop[0][4]
            # TODO : improve regular
        else:
            if check_complete(arrayOfTop):
                print("cycle complet !")
                is_t, max_speed, rhythm = is_tired(arrayOfTop)
                if is_t:
                    print("window tired")
                    print(max_speed)
                    print(rhythm)
                    # TODO : improve tired
                else:
                    is_cyc, cycl = is_cyclic(arrayOfTop)
                    print("window cyclic")
                    print(cycl)
                    # TODO : improve cyclic
            else:
                print("cycle incomplet !")
            for top in arrayOfTop:
                print(top)
