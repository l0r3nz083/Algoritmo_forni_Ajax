import pandas as pd

from query import *
from datetime import timedelta
import json
import cx_Oracle


class Ajax(Query):
    def __init__(self, forno):
        super().__init__()
        self.forno = forno
        self.query_fusione = f"""select top 1 *
                            from ajax_fusioni
                            where FORNO = {forno}
                            order by DATA_ORA desc"""

        self.df_ciclo = super().query_ciclo()
        self.df_fusione = super().query_fus()
        self.id_fusione = self.df_fusione["ID_FUSIONE"][0]
        self.date_time_ajax = self.df_fusione["DATA_ORA"][0]
        self.avg_ele_spec_ajax = self.calcolo_ene_spec()
        self.carica_terminata = False
        self.ton_infornate_ajax = self.ton_caricate()
        self.lung_placca_avg = self.df_ciclo["L_PLACCA"].mean()
        self.potenza_media = self.calcolo_potenza_media()
        self.h_prossima_fus_ajax = self.avg_ele_spec_ajax * self.ton_infornate_ajax / self.potenza_media
        self.ene_required = self.ton_infornate_ajax * self.avg_ele_spec_ajax


    def fine_prossima_fusione(self):
        date_fine_fus_ajax = self.date_time_ajax + timedelta(hours=self.h_prossima_fus_ajax)
        date_fine_fus_ajax = date_fine_fus_ajax.strftime('%d-%m-%Y %H:%M:%S')
        return date_fine_fus_ajax

    def tempo_fus(self):
        tempo = self.avg_ele_spec_ajax * self.ton_infornate_ajax / self.potenza_media
        return tempo

    def calcolo_potenza_media(self):
        if self.forno == 1:
            with open("data/data_ajax_1.json", "r") as f:
                data_file = json.load(f)
        else:
            with open("data/data_ajax_2.json", "r") as f:
                data_file = json.load(f)
        pot_media = float(data_file["POTENZA_AJAX"])
        if pot_media < 100:
            pot_media = 100
        return pot_media

    def calcolo_ene_spec(self):
        global file_path
        if self.forno == 1:
            file_path = "data/energia_ajax_1.csv"
        else:
            file_path = "data/energia_ajax_2.csv"

        data_file = pd.read_csv(file_path)

        data_file["ID_FUSIONE"] = pd.to_numeric(data_file["ID_FUSIONE"], errors='coerce')
        data_file["ENE_ACTUAL"] = pd.to_numeric(data_file["ENE_ACTUAL"], errors='coerce')
        data_file["TON_CARICATE"] = pd.to_numeric(data_file["TON_CARICATE"], errors='coerce')

        data_file['ENE_SPEC'] = data_file['ENE_ACTUAL'] / data_file['TON_CARICATE']
        data_file['ENE_SPEC'] = data_file['ENE_SPEC'].replace([float('inf'), float('-inf')],
                                                float('nan'))  # Trattamento di divisione per zero

        df_sorted = data_file.sort_values(by='ID_FUSIONE', ascending=False)
        df_20 = df_sorted.head(20)

        media_ene_spec = df_20['ENE_SPEC'].mean()

        return media_ene_spec

    def ton_caricate(self):
        db_config = {
            'user': 'reparti_ext',
            'password': 'reparti_ext',
            'dsn': 'ORFBG.world',
        }

        connection = cx_Oracle.connect(**db_config)

        cursor = connection.cursor()
        query = "SELECT * FROM reparti_ext.cariche_fusioni WHERE fusione_auto =: fusione"
        fusione = int(self.id_fusione)
        cursor.execute(query, fusione=fusione)
        result_set = cursor.fetchall()

        df = pd.DataFrame(result_set, columns=[col[0] for col in cursor.description])

        cursor.close()
        connection.close()

        try:
            if df.at[0, 'FINE'] is None:
                travasato = self.df_fusione["TRAVASATO_PREC"][0] / 1000
                print(f"Carica da terminare, Ajax {self.forno} previsto {travasato}")
            else:
                travasato = df['KG'].sum() / 1000
                print(f"Carica terminata: Ajax {self.forno}, travasato: {travasato}")
                self.carica_terminata = True
        except Exception as e:
            travasato = self.df_fusione["TRAVASATO_PREC"][0] / 1000
            print(f"Carica da terminare, Ajax {self.forno} previsto {travasato}")
            print(f"ERRORE: {e}")

        return travasato

