import math
import pandas as pd
import pyodbc
import json
from datetime import datetime, timedelta

from ajax import Ajax

# Connessione a SQL SERVER
SERVER = '172.30.3.92\SQLEXPRESS'
DATABASE = 'fo_dati'
USERNAME = 'fo_user'
PASSWORD = 'ripristino'
CONN_STR = 'DRIVER={SQL Server};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD

# PESO_CM_MED = 21.30  # [Kg/cm] ----> Valido per il formato 1010. Per gli altri formati occorrerà estrarlo da db
# L_PLACCA_TARGET = 7500  # [mm]
ENERGIA_SPEC = 305  # [Kwh/ton]
CARICA_RIF = 15.5  # [ton]
POTENZA_RIF = 1350  # [kW]

QUERY_AJAX_1 = """select top 1 *
            from ajax_fusioni 
            where FORNO = 1
            order by DATA_ORA desc"""

QUERY_AJAX_2 = """select top 1 *
            from ajax_fusioni 
            where FORNO = 2
            order by DATA_ORA desc"""

QUERY_COLATA = """select top 1 *
                from dati_colata_placca
                order by ID_COLATA desc"""

QUERY_PARAM_COLATA = """select top 1000 *
                        from dati_colata_pt
                        order by DATA_ORA desc"""

QUERY_COLATA_SE_INIZIATA = """select top 1 *
                        from dati_colata_pt
                        order by DATA_ORA desc"""


def query_data(query):
    conn = pyodbc.connect(CONN_STR)

    df = pd.read_sql_query(query, conn)

    conn.close()

    return df


def agg_pesocm_placca():
    df_col = query_data(QUERY_COLATA)
    peso_cm_med = df_col["RIC_PESO_CM"][0]
    l_placca_target = df_col["RIC_L_PLACCA"][0]

    return peso_cm_med, l_placca_target


def is_travasato_ajax_1():
    df_actual_ajax_1 = query_data(QUERY_AJAX_1)
    id_fusione_actual_ajax_1 = df_actual_ajax_1["ID_FUSIONE"][0]

    with open("data/data_fusione_old.json", "r") as f:
        data_file = json.load(f)

    id_fusione_old_ajax_1 = int(data_file["id_fusione_old_ajax_1"])

    if id_fusione_actual_ajax_1 > id_fusione_old_ajax_1:
        return True, df_actual_ajax_1["TRAVASATO_PREC"][0], df_actual_ajax_1["ID_FUSIONE"][0]
    else:
        return False, None


def is_travasato_ajax_2():
    df_actual_ajax_2 = query_data(QUERY_AJAX_2)
    id_fusione_actual_ajax_2 = df_actual_ajax_2["ID_FUSIONE"][0]

    with open("data/data_fusione_old.json", "r") as f:
        data_file = json.load(f)

    id_fusione_old_ajax_2 = int(data_file["id_fusione_old_ajax_2"])

    if id_fusione_actual_ajax_2 > id_fusione_old_ajax_2:
        return True, df_actual_ajax_2["TRAVASATO_PREC"][0], df_actual_ajax_2["ID_FUSIONE"][0]
    else:
        return False, None


def update_travasato():
    global travasato, travasato_ajax_1, id_fusione_ajax_1, travasato_ajax_2, id_fusione_ajax_2
    end_travaso = False

    result_ajax_1 = is_travasato_ajax_1()
    if result_ajax_1[0]:
        is_travasato_a1, travasato_ajax_1, id_fusione_ajax_1 = result_ajax_1[0], result_ajax_1[1], result_ajax_1[2]
    else:
        is_travasato_a1 = False

    result_ajax_2 = is_travasato_ajax_2()
    if result_ajax_2[0]:
        is_travasato_a2, travasato_ajax_2, id_fusione_ajax_2 = result_ajax_2[0], result_ajax_2[1], result_ajax_2[2]
    else:
        is_travasato_a2 = False

    # Se l'id fusione attuale è maggiore dell'id fusione old di uno dei due forni, significa che ho appena finito
    # un travaso da un ajax. In questo caso salvo il nuovo id_fusione e metto a 1 il flag end_travaso
    if is_travasato_a1:
        travasato = travasato_ajax_1

        with open("data/data_fusione_old.json", "r") as f:
            data_file = json.load(f)

        data_to_write = {
            "id_fusione_old_ajax_1": str(id_fusione_ajax_1),
            "id_fusione_old_ajax_2": data_file["id_fusione_old_ajax_2"]
        }
        with open("data/data_fusione_old.json", "w") as f:
            json.dump(data_to_write, f, indent=4)

        end_travaso = True

    if is_travasato_a2:
        travasato = travasato_ajax_2

        with open("data/data_fusione_old.json", "r") as f:
            data_file = json.load(f)

        data_to_write = {
            "id_fusione_old_ajax_1": data_file["id_fusione_old_ajax_1"],
            "id_fusione_old_ajax_2": str(id_fusione_ajax_2)
        }
        with open("data/data_fusione_old.json", "w") as f:
            json.dump(data_to_write, f, indent=4)

        end_travaso = True

    # Se è terminato il travaso aggiungo la quantità travasata a quella che c'era inizialmente ed aggiorno il
    # file
    if end_travaso:
        with open("data/fuso_in_junker.json", "r") as f:
            data_file = json.load(f)
        fuso_old = float(data_file["fuso_rimanente"])
        new_fuso_junker = fuso_old + travasato
        data_file["fuso_in_junker_iniziale"] = str(new_fuso_junker)
        data_file["fuso_rimanente"] = str(new_fuso_junker)

        with open("data/fuso_in_junker.json", "w") as f:
            json.dump(data_file, f, indent=4)

        with open('data/stato_junker.json', 'r') as file:
            data = json.load(file)

        data["is_travasato"] = True

        with open("data/stato_junker.json", "w") as f:
            json.dump(data, f, indent=4)

    return end_travaso


def is_forno_in_colata():
    df_colata = query_data(QUERY_COLATA)

    if df_colata["START_SALITA"][0] is None:
        if df_colata["START_DISCESA"][0] is None:
            print("Anomalia Query colata.")
            df_colata_se_iniziata = query_data(QUERY_COLATA_SE_INIZIATA)
            if df_colata["ID_COLATA"][0] != df_colata_se_iniziata["ID_COLATA"][0]:
                print("Colata non iniziata. Esco dalla funzione")
                return
        with open('data/stato_junker.json', 'r') as file:
            data = json.load(file)

        if not data["is_in_colata"]:
            data["is_in_colata"] = True
            data["is_travasato"] = False

            with open("data/stato_junker.json", "w") as f:
                json.dump(data, f, indent=4)

            with open('data/fuso_in_junker.json', 'r') as file:
                data_file = json.load(file)

            data_file["end_colata"] = False
            data_file["is_first_time"] = True
            with open("data/fuso_in_junker.json", "w") as f:
                json.dump(data_file, f, indent=4)

        return True

    else:
        with open('data/stato_junker.json', 'r') as file:
            data = json.load(file)

            with open('data/fuso_in_junker.json', 'r') as file:
                data_fuso = json.load(file)

        if data["is_in_colata"]:
            data["is_in_colata"] = False
            data_fuso["ora_fine_colata"] = "-"
            data_fuso["durata_colata"] = "-"

            with open("data/stato_junker.json", "w") as f:
                json.dump(data, f, indent=4)

            with open("data/fuso_in_junker.json", "w") as f:
                json.dump(data_fuso, f, indent=4)

        return False


def fuso_colato(id_colata):
    query = f"""select VSET, VATT, L_PLACCA, ID_COLATA
                from dati_colata_pt
                where ID_COLATA = {id_colata}
                order by DATA_ORA desc"""

    df_param_colata = query_data(query)
    try:
        l_placca = df_param_colata["L_PLACCA"][0]
    except IndexError:
        print("Esco dalla funzione")
        return

    try:
        data = agg_pesocm_placca()
    except Exception as e:
        print(f"ERRORE QUERY: {e}")
        return

    peso_cm_med = data[0]
    peso_colato = l_placca / 10 * peso_cm_med
    l_placca_target = data[1]

    # Aggiorno il peso rimanente nello Junker
    with open('data/fuso_in_junker.json', 'r') as file:
        data = json.load(file)
    metal_init = float(data["fuso_in_junker_iniziale"])
    metal_actual = metal_init - peso_colato
    data["fuso_rimanente"] = str(round(metal_actual))
    data["lunghezza_placca"] = str(l_placca)
    data["lunghezza_placca_target"] = str(l_placca_target)

    with open("data/fuso_in_junker.json", "w") as f:
        json.dump(data, f, indent=4)


def aggiorna_peso_placca_a_fine_colata():
    QUERY = """select top 1 *
                from dati_colata_pt
                order by DATA_ORA desc"""

    with open('data/fuso_in_junker.json', 'r') as file:
        data = json.load(file)
    old_lung_placca = float(data["lunghezza_placca"])
    is_colata_ended = data["end_colata"]
    is_first_time = data["is_first_time"]

    if not is_first_time:
        if not is_colata_ended:
            df_param_colata = query_data(QUERY)
            lung_placca_actual = df_param_colata["L_PLACCA"][0]

            if old_lung_placca != lung_placca_actual:
                data_placca = agg_pesocm_placca()
                peso_cm_med = data_placca[0]

                peso_colato = lung_placca_actual / 10 * peso_cm_med
                with open('data/fuso_in_junker.json', 'r') as file:
                    data = json.load(file)
                metal_init = float(data["fuso_in_junker_iniziale"])
                metal_actual = metal_init - peso_colato
                data["fuso_rimanente"] = str(round(metal_actual))
                data["lunghezza_placca"] = str(lung_placca_actual)
                data["end_colata"] = False
                with open("data/fuso_in_junker.json", "w") as f:
                    json.dump(data, f, indent=4)
            else:
                with open('data/fuso_in_junker.json', 'r') as file:
                    data = json.load(file)

                # Per evitare la deriva del peso rimanente nello Junker,
                # se il peso che leggo a fine colata è superiore alle 20 ton,
                # lo resetto a 15 ton
                if float(data["fuso_rimanente"]) > 20000:
                    data["fuso_rimanente"] = str(15000)
                elif float(data["fuso_rimanente"]) < 6000:
                    data["fuso_rimanente"] = str(7000)

                data["end_colata"] = True
                with open("data/fuso_in_junker.json", "w") as f:
                    json.dump(data, f, indent=4)
    else:
        data["is_first_time"] = False
        with open("data/fuso_in_junker.json", "w") as f:
            json.dump(data, f, indent=4)


def calcolo_tempo_colata(id_colata):
    global dt_str, data_ini
    query = f"""select VSET, VATT, L_PLACCA, ID_COLATA
                from dati_colata_pt
                where ID_COLATA = {id_colata}
                order by DATA_ORA desc"""

    df_param = query_data(query)

    v_media = df_param[df_param["VATT"] > 0]["VATT"].mean()  # Velocità media [mm/min]
    try:
        l_placca = df_param["L_PLACCA"][0]
    except IndexError:
        print("Colata non iniziata. Esco dalla funzione")
        return
    data_placca = agg_pesocm_placca()
    l_placca_target = data_placca[1]
    l_rimanente = l_placca_target - l_placca  # [mm]
    tempo_rimanente = l_rimanente / v_media

    query_data_ini = f"""select top 1 DATA_ORA
                    from dati_colata_pt
                    where ID_COLATA = {id_colata}
                    order by DATA_ORA desc"""

    df_data_ini = query_data(query_data_ini)
    try:
        data_ini = df_data_ini["DATA_ORA"][0]
    except IndexError:
        print("Indice della data non trovato. Esco dal calcolo della durata colata")

    try:
        data_fine_colata = data_ini + timedelta(minutes=tempo_rimanente)
        dt_str = data_fine_colata.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        print("NAN. Esco dalla funzione")
        return

    with open('data/fuso_in_junker.json', 'r') as file:
        data = json.load(file)

    data["ora_fine_colata"] = dt_str
    data["durata_colata"] = str(round(tempo_rimanente))
    data["v_media"] = str(v_media)

    with open("data/fuso_in_junker.json", "w") as f:
        json.dump(data, f, indent=4)


class Junker:
    def __init__(self):
        self.stato = "vuoto"
        if not is_forno_in_colata():
            update_travasato()
            aggiorna_peso_placca_a_fine_colata()
        else:
            df_colata = query_data(QUERY_COLATA)
            id_colata = df_colata["ID_COLATA"][0]
            fuso_colato(id_colata)
            calcolo_tempo_colata(id_colata)
        self.stato_forno()

    def stato_forno(self):
        with open('data/stato_junker.json', 'r') as file:
            data = json.load(file)

        if data["is_in_colata"]:
            self.stato = "in colata"
        else:
            if data["is_travasato"]:
                self.stato = "pieno"
            else:
                self.stato = "vuoto"

        data["stato_junker"] = self.stato
        with open("data/stato_junker.json", "w") as f:
            json.dump(data, f, indent=4)
