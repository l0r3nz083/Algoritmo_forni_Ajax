import csv

import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import json
from ajax import Ajax

# Connessione a SQL SERVER
SERVER = '172.30.3.92\SQLEXPRESS'
DATABASE = 'fo_dati'
USERNAME = 'fo_user'
PASSWORD = 'ripristino'
CONN_STR = 'DRIVER={SQL Server};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD


def query_data(query):
    conn = pyodbc.connect(CONN_STR)

    df = pd.read_sql_query(query, conn)

    conn.close()

    return df


class Integratore:
    def __init__(self, forno):
        self.ene_ajax = 0
        self.forno = forno
        self.potenza_media = 0
        self.inizio_fusione = None
        self.calcolo_ene()
        self.is_in_superhetaing_1 = None
        self.date_last_thread = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def calcolo_ene(self):
        global data_file, dt, d_ene, d_ene_ajax, id_fusione_old_ajax

        if self.forno == 1:
            with open("data/data_ajax_1.json", "r") as f:
                data_file = json.load(f)
        else:
            with open("data/data_ajax_2.json", "r") as f:
                data_file = json.load(f)

        date_object = datetime.strptime(data_file["DATA_ORA"], "%Y-%d-%m %H:%M:%S")
        id_file = data_file["ID"]
        ene_file_ajax = float(data_file["ENERGIA_AJAX"])

        query_pt = f""" select ID, DATA_ORA, AJA_PT, AJA2_PT
                    from forni_dati_ele
                    where ID >= {id_file}
                    order by DATA_ORA desc
                    """
        query_id_fusione = f"""select top 1 DATA_ORA, ID_FUSIONE
                            from ajax_fusioni
				            where forno = {self.forno}
                            order by ID_FUSIONE desc"""

        id_fusione_old_ajax = int(data_file["ID_FUSIONE"])

        df_nuova_fusione = query_data(query_id_fusione)
        self.inizio_fusione = df_nuova_fusione["DATA_ORA"][0]
        id_fusione_new = df_nuova_fusione["ID_FUSIONE"][0]
        date_nuova_fusione = df_nuova_fusione["DATA_ORA"][0].strftime("%Y-%d-%m %H:%M:%S")

        if id_fusione_old_ajax != id_fusione_new:  # caso di una nuova fusione

            query_pt_nuova_fusione = f""" select ID, DATA_ORA, AJA_PT, AJA2_PT
                    from forni_dati_ele
                    where DATA_ORA >= convert(varchar(20), '{date_nuova_fusione}', 20)
                    order by DATA_ORA desc
                    """
            df_pt_nuova_fusione = query_data(query_pt_nuova_fusione)

            if self.forno == 1:
                try:
                    self.potenza_actual = df_pt_nuova_fusione["AJA_PT"][0]
                except IndexError:
                    print("Il dataframe è vuoto. Esco dalla funzione")
                    return
            else:
                try:
                    self.potenza_actual = df_pt_nuova_fusione["AJA2_PT"][0]
                except IndexError:
                    print("Il dataframe è vuoto. Esco dalla funzione")
                    return

            for i in range(len(df_pt_nuova_fusione) - 1):
                dt = (df_pt_nuova_fusione["DATA_ORA"][i] - df_pt_nuova_fusione["DATA_ORA"][i + 1]).seconds + (
                        df_pt_nuova_fusione["DATA_ORA"][i] -
                        df_pt_nuova_fusione["DATA_ORA"][
                            i + 1]).microseconds / 1000000

                if self.forno == 1:
                    if df_pt_nuova_fusione["AJA_PT"][i] > 500:
                        d_ene_ajax = df_pt_nuova_fusione["AJA_PT"][i] * dt / 3600
                    else:
                        d_ene_ajax = 0
                else:
                    if df_pt_nuova_fusione["AJA2_PT"][i] > 300:
                        d_ene_ajax = df_pt_nuova_fusione["AJA2_PT"][i] * dt / 3600
                    else:
                        d_ene_ajax = 0

                self.ene_ajax += d_ene_ajax

                delta_t_tot = (df_pt_nuova_fusione["DATA_ORA"][0] - df_nuova_fusione["DATA_ORA"][
                    0]).total_seconds() / 3600
                self.potenza_media = self.ene_ajax / delta_t_tot
                if self.potenza_media == 0:
                    self.potenza_media = 100

            data_to_write = {
                "ID": str(df_pt_nuova_fusione["ID"][0]),
                "ID_FUSIONE": str(id_fusione_new),
                "DATA_ORA": df_pt_nuova_fusione["DATA_ORA"][0].strftime("%Y-%d-%m %H:%M:%S"),
                "ENERGIA_AJAX": str(self.ene_ajax),
                "POTENZA_AJAX": str(self.potenza_media)
            }
            if self.forno == 1:
                with open("data/data_ajax_1.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)
            else:
                with open("data/data_ajax_2.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)

        else:  # caso in cui siamo nella stessa fusione

            data_query = query_data(query_pt)
            self.ene_ajax = ene_file_ajax
            print(data_query["AJA_PT"][0])
            if self.forno == 1:
                self.potenza_actual = data_query["AJA_PT"][0]
            else:
                self.potenza_actual = data_query["AJA2_PT"][0]

            for i in range(len(data_query)):
                if i != len(data_query) - 1:
                    dt = (data_query["DATA_ORA"][i] - data_query["DATA_ORA"][i + 1]).seconds + (
                            data_query["DATA_ORA"][i] -
                            data_query["DATA_ORA"][
                                i + 1]).microseconds / 1000000

                    if self.forno == 1:
                        if data_query["AJA_PT"][i] > 500:
                            d_ene_ajax = data_query["AJA_PT"][i] * dt / 3600
                        else:
                            d_ene_ajax = 0
                    else:
                        if data_query["AJA2_PT"][i] > 300:
                            d_ene_ajax = data_query["AJA2_PT"][i] * dt / 3600
                        else:
                            d_ene_ajax = 0
                else:
                    dt = (data_query["DATA_ORA"][i] - date_object).seconds + (
                            data_query["DATA_ORA"][i] -
                            date_object).microseconds / 1000000
                    if self.forno == 1:
                        if data_query["AJA_PT"][i] > 500:
                            d_ene_ajax = data_query["AJA_PT"][i] * dt / 3600
                        else:
                            d_ene_ajax = 0
                    else:
                        if data_query["AJA2_PT"][i] > 300:
                            d_ene_ajax = data_query["AJA2_PT"][i] * dt / 3600
                        else:
                            d_ene_ajax = 0

                self.ene_ajax += d_ene_ajax

                # Calcolo potenza media da energia fornita
                if self.forno == 1:
                    self.potenza_media = (float(data_file["POTENZA_AJAX"]) + data_query["AJA_PT"][0]) / 2
                else:
                    self.potenza_media = (float(data_file["POTENZA_AJAX"]) + data_query["AJA2_PT"][0]) / 2

            data_to_write = {
                "ID": str(data_query["ID"].iloc[0]),
                "ID_FUSIONE": str(id_fusione_new),
                "DATA_ORA": data_query["DATA_ORA"][0].strftime("%Y-%d-%m %H:%M:%S"),
                "ENERGIA_AJAX": str(self.ene_ajax),
                "POTENZA_AJAX": str(self.potenza_media)
            }
            if self.forno == 1:
                with open("data/data_ajax_1.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)
            else:
                with open("data/data_ajax_2.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)

    def save_energies(self):
        ##################################################################
        ##                                                              ##
        ##      ROUTINE PER IL SALVATAGGIO DELLE ENERGIE NEL FILE CSV   ##
        ##                                                              ##
        ##################################################################

        global is_in_superheating_ajax_1, is_in_superheating_ajax_2, is_already_recorded
        query_temp = """ select top 60 *
                        from forni_dati_tc
                        order by DATA_ORA desc
                    """

        query_power = """ select top 18 AJA_PT, AJA2_PT
                        from forni_dati_ele
                        order by DATA_ORA desc
                       """

        data_temp = query_data(query_temp)
        data_power = query_data(query_power)

        t_ajax_1_act = (data_temp["AJA_TCMET_1"][0] + data_temp["AJA_TCMET_2"][0]) / 2
        t_ajax_2_act = (data_temp["AJA2_TCMET_1"][0] + data_temp["AJA2_TCMET_2"][0]) / 2

        t_ajax_1_past = (data_temp["AJA_TCMET_1"][len(data_temp) - 1] + data_temp["AJA_TCMET_2"][
            len(data_temp) - 1]) / 2
        t_ajax_2_past = (data_temp["AJA2_TCMET_1"][len(data_temp) - 1] + data_temp["AJA2_TCMET_2"][
            len(data_temp) - 1]) / 2

        delta_temp_ajax_1 = t_ajax_1_act - t_ajax_1_past
        delta_temp_ajax_2 = t_ajax_2_act - t_ajax_2_past

        t_set_ajax_1 = data_temp["AJA_TH"][0]
        t_set_ajax_2 = data_temp["AJA2_TH"][0]

        ##############################
        ##                          ##
        ##      FORNO AJAX 1        ##
        ##                          ##
        ##############################

        if self.forno == 1:

            with open("data/superheat_ajax_1.json",
                      "r") as f:  # Leggo i file dal json per vedere se il forno è in superheating
                data_sh = json.load(f)

            if int(data_sh["IS_SUPERHEAT"]) == 1:
                is_in_superheating_ajax_1 = True
                self.is_in_superhetaing_1 = True
            else:
                is_in_superheating_ajax_1 = False
                self.is_in_superhetaing_1 = False

            potenza_max = 10

            # Controllo se il forno è in superheating. Se si inizio a salvare nel file temp i dati

            if is_in_superheating_ajax_1:
                ajax_1 = Ajax(1)
                ene_fornita = self.ene_ajax

                # Controllo se la potenza è superiore alla potenza registrata in precedenza
                # se si, aggiorno la variabile nel file temporaneo
                with open("data/temp_ajax_1.json", "r") as f:
                    data = json.load(f)
                potenza_max = float(data["POTENZA_MAX"])

                if data_power["AJA_PT"][0] > potenza_max:
                    potenza_max = data_power["AJA_PT"][0]

                data_to_write = {
                    "ID_FUSIONE": str(ajax_1.id_fusione),
                    "DATA_INIZIO": self.inizio_fusione.strftime("%Y-%d-%m %H:%M:%S"),
                    "DATA_FINE": data_temp["DATA_ORA"][0].strftime("%Y-%d-%m %H:%M:%S"),
                    "ENE_RICHIESTA": str(ajax_1.ene_required),
                    "ENE_FORNITA": str(ene_fornita),
                    "POTENZA_MAX": str(potenza_max),
                    "COUNT_LOW_POWER": data["COUNT_LOW_POWER"],
                    "TON_CARICATE": str(ajax_1.ton_infornate_ajax)
                }
                with open("data/temp_ajax_1.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)

            else:
                df_fusioni = pd.read_csv("data/energia_ajax_1.csv")
                date_last_fusione_str = df_fusioni.tail(1)["DATA_FINE"].values[0]
                date_last_fusione = datetime.strptime(date_last_fusione_str, "%Y-%d-%m %H:%M:%S")
                delta_time = datetime.now() - date_last_fusione

                # Controllo se il forno è in superheat.
                # La condizione per il superheat è che il delta temperatura attuale e quella di 10 minuti prima
                # deve essere > di 7 gradi; la temperatura attuale deve essere > della TH diminuita di
                # 40°C; inoltre devono essere passati 40 minuti dall'ultima fine fusione;
                # nel caso lo scrivo nel file json. Si noti che non setto a 1 la variabile
                # is_in_superheating; questa sarà settata ad 1 alla rilettura del file successiva

                if (delta_temp_ajax_1 > 7) & (t_ajax_1_act >= t_set_ajax_1 - 40) & (delta_time > timedelta(minutes=40)):
                    data_to_write = {
                        "IS_SUPERHEAT": "1"
                    }
                    with open("data/superheat_ajax_1.json", "w") as f:
                        json.dump(data_to_write, f, indent=4)

            # Controllo se siamo a fine fusione. Salvo il file temp in quello definitivo
            # La condizione di fine fusione si verifica quando la differenza tra la potenza attuale e quella max
            # è di -800 Kw per 4 volte (che corrisponde a circa 1 minuto e 20 secondi)
            delta_power_ajax_1 = data_power["AJA_PT"][0] - potenza_max
            counter_low_power = 0
            with open("data/temp_ajax_1.json", "r") as f:  # Leggo i file dal json temporaneo
                data = json.load(f)

            if delta_power_ajax_1 < -800:
                counter_low_power = int(data["COUNT_LOW_POWER"]) + 1

            data["COUNT_LOW_POWER"] = str(counter_low_power)
            with open('data/temp_ajax_1.json', 'w') as file:
                json.dump(data, file, indent=4)

            if is_in_superheating_ajax_1 and counter_low_power == 4:
                with open("data/temp_ajax_1.json", "r") as f:  # Leggo i file dal json temporaneo
                    data = json.load(f)

                data_dict = {"ID_FUSIONE": data["ID_FUSIONE"],
                             "DATA_INIZIO": data["DATA_INIZIO"],
                             "DATA_FINE": data["DATA_FINE"],
                             "ENE_RICHIESTA": data["ENE_RICHIESTA"],
                             "ENE_FORNITA": data["ENE_FORNITA"],
                             "TON_CARICATE": data["TON_CARICATE"]}

                df_data_dict = pd.DataFrame(data_dict, index=[0])

                df_file = pd.read_csv("data/energia_ajax_1.csv")
                ids_fusione = df_file["ID_FUSIONE"]

                is_already_recorded = False  # Controllo se il record non è già stato registrato

                for value in ids_fusione:
                    if int(data["ID_FUSIONE"]) == value:
                        is_already_recorded = True

                is_time_elapsed = False
                data_fine_str = df_file.tail(1)["DATA_FINE"].values[0]

                ora_actual = datetime.now()
                data_fine = datetime.strptime(data_fine_str, "%Y-%d-%m %H:%M:%S")
                differenza = ora_actual - data_fine

                if differenza > timedelta(minutes=20):
                    is_time_elapsed = True

                # Se non è già stato registrato ed è passato un tempo congruo per una fusione lo registro nel file csv
                if (not is_already_recorded) & is_time_elapsed:
                    df_data_dict.to_csv("data/energia_ajax_1.csv", mode="a", header=False, index=False)

                # Dopo aver terminato la fusione risetto a zero la variabile scritta sul file IS_SUPERHEAT
                data_to_write = {
                    "IS_SUPERHEAT": "0"
                }
                with open("data/superheat_ajax_1.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)

        ##############################
        ##                          ##
        ##      FORNO AJAX 2        ##
        ##                          ##
        ##############################
        else:
            with open("data/superheat_ajax_2.json",
                      "r") as f:  # Leggo i file dal json per vedere se il forno è in superheating
                data_sh = json.load(f)

            if int(data_sh["IS_SUPERHEAT"]) == 1:
                is_in_superheating_ajax_2 = True
                self.is_in_superhetaing_2 = True
            else:
                is_in_superheating_ajax_2 = False
                self.is_in_superhetaing_2 = False

            potenza_max = 10
            # Controllo se il forno è in superheating. Se si inizio a salvare nel file temp i dati
            if is_in_superheating_ajax_2:
                ajax_2 = Ajax(2)
                ene_fornita = self.ene_ajax

                # Controllo se la potenza è superiore alla potenza registrata in precedenza
                with open("data/temp_ajax_2.json", "r") as f:
                    data = json.load(f)
                potenza_max = float(data["POTENZA_MAX"])

                if data_power["AJA2_PT"][0] > potenza_max:
                    potenza_max = data_power["AJA2_PT"][0]

                data_to_write = {
                    "ID_FUSIONE": str(ajax_2.id_fusione),
                    "DATA_INIZIO": self.inizio_fusione.strftime("%Y-%d-%m %H:%M:%S"),
                    "DATA_FINE": data_temp["DATA_ORA"][0].strftime("%Y-%d-%m %H:%M:%S"),
                    "ENE_RICHIESTA": str(ajax_2.ene_required),
                    "ENE_FORNITA": str(ene_fornita),
                    "POTENZA_MAX": str(potenza_max),
                    "COUNT_LOW_POWER": data["COUNT_LOW_POWER"],
                    "TON_CARICATE": str(ajax_2.ton_infornate_ajax)
                }
                with open("data/temp_ajax_2.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)

            else:
                df_fusioni = pd.read_csv("data/energia_ajax_2.csv")
                date_last_fusione_str = df_fusioni.tail(1)["DATA_FINE"].values[0]
                date_last_fusione = datetime.strptime(date_last_fusione_str, "%Y-%d-%m %H:%M:%S")
                delta_time = datetime.now() - date_last_fusione

                # print(f"La differenza di temp è: {delta_temp_ajax_2}")
                # print(f"La differenza di temperatura dal set è: {t_ajax_2_act - t_set_ajax_2}")
                # print(delta_time)

                # Controllo se il forno è in superheat.
                # La condizione per il superheat è che il delta temperatura attuale e quella di 10 minuti prima
                # deve essere > di 7 gradi; la temperatura attuale deve essere > della TH diminuita di
                # 40°C; inoltre devono essere passati 40 minuti dall'ultima fine fusione;
                # nel caso lo scrivo nel file json. Si noti che non setto a 1 la variabile
                # is_in_superheating; questa sarà settata ad 1 alla rilettura del file successiva
                if (delta_temp_ajax_2 > 7) & (t_ajax_2_act >= t_set_ajax_2 - 40) & (delta_time > timedelta(minutes=40)):
                    data_to_write = {
                        "IS_SUPERHEAT": "1"
                    }
                    with open("data/superheat_ajax_2.json", "w") as f:
                        json.dump(data_to_write, f, indent=4)

            # Controllo se siamo a fine fusione. Salvo il file temp in quello definitivo
            # La condizione di fine fusione si verifica quando la differenza tra la potenza attuale e quella max è di
            # -500 Kw per 4 volte (che corrisponde a circa 1 minuto e 20 secondi)
            delta_power_ajax_2 = data_power["AJA2_PT"][0] - potenza_max
            counter_low_power = 0
            with open("data/temp_ajax_2.json", "r") as f:  # Leggo i file dal json temporaneo
                data = json.load(f)

            if delta_power_ajax_2 < -500:
                counter_low_power = int(data["COUNT_LOW_POWER"]) + 1

            data["COUNT_LOW_POWER"] = str(counter_low_power)
            with open('data/temp_ajax_2.json', 'w') as file:
                json.dump(data, file, indent=4)

            if is_in_superheating_ajax_2 and counter_low_power == 4:
                with open("data/temp_ajax_2.json", "r") as f:  # Leggo i file dal json temporaneo
                    data = json.load(f)

                data_dict = {"ID_FUSIONE": data["ID_FUSIONE"],
                             "DATA_INIZIO": data["DATA_INIZIO"],
                             "DATA_FINE": data["DATA_FINE"],
                             "ENE_RICHIESTA": data["ENE_RICHIESTA"],
                             "ENE_FORNITA": data["ENE_FORNITA"],
                             "TON_CARICATE": data["TON_CARICATE"]}

                df_data_dict = pd.DataFrame(data_dict, index=[0])

                df_file = pd.read_csv("data/energia_ajax_2.csv")
                ids_fusione = df_file["ID_FUSIONE"]

                is_already_recorded = False  # Controllo se il record non è già stato registrato

                for value in ids_fusione:
                    if int(data["ID_FUSIONE"]) == value:
                        is_already_recorded = True

                is_time_elapsed = False
                data_fine_str = df_file.tail(1)["DATA_FINE"].values[0]

                ora_actual = datetime.now()
                data_fine = datetime.strptime(data_fine_str, "%Y-%d-%m %H:%M:%S")
                differenza = ora_actual - data_fine

                if differenza > timedelta(minutes=20):
                    is_time_elapsed = True

                # Se non è già stato registrato ed è passato un tempo congruo per una fusione lo registro nel file csv
                if (not is_already_recorded) & is_time_elapsed:
                    df_data_dict.to_csv("data/energia_ajax_2.csv", mode="a", header=False, index=False)

                # Dopo aver terminato la fusione risetto a zero la variabile scritta sul file IS_SUPERHEAT
                data_to_write = {
                    "IS_SUPERHEAT": "0"
                }
                with open("data/superheat_ajax_2.json", "w") as f:
                    json.dump(data_to_write, f, indent=4)
        if self.forno == 1:
            return is_in_superheating_ajax_1
        else:
            return is_in_superheating_ajax_2
