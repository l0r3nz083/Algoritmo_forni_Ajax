import math
from ajax import Ajax
from datetime import datetime, timedelta
import json

ENERGIA_SPEC = 305  # [Kwh/ton]
CARICA_RIF = 15.5  # [ton]
POTENZA_RIF = 1350  # [kW]
PESO_PLACCA_RIF = 15.5  # [ton]
#LUNGHEZZA_PLACCA = 7500  # [mm]
TEMPO_CICLO_MIN = 75  # [min]
TEMPO_SURR_RIF = 35  # [min]


def calcolo_sfasamento():
    tempo_tot_ciclo_ore = ENERGIA_SPEC * CARICA_RIF / POTENZA_RIF
    tempo_tot_ciclo_min_rif = math.modf(tempo_tot_ciclo_ore)[0] * 60 + math.modf(tempo_tot_ciclo_ore)[1] * 60

    ajax_1 = Ajax(1)
    ajax_2 = Ajax(2)

    pot_disp_ajax_1 = ajax_1.potenza_media
    pot_disp_ajax_2 = ajax_2.potenza_media

    if pot_disp_ajax_1 < 1000:
        pot_disp_ajax_1 = 1270
    if pot_disp_ajax_2 < 500:
        pot_disp_ajax_2 = 720

    tempo_ciclo_ajax_1_eff = POTENZA_RIF / pot_disp_ajax_1 * tempo_tot_ciclo_min_rif
    tempo_ciclo_ajax_2_eff = POTENZA_RIF / pot_disp_ajax_2 * tempo_tot_ciclo_min_rif
    tempo_surr_ajax_1 = POTENZA_RIF / pot_disp_ajax_1 * TEMPO_SURR_RIF
    tempo_surr_ajax_2 = POTENZA_RIF / pot_disp_ajax_2 * TEMPO_SURR_RIF
    tempo_fusione_ajax_1 = tempo_ciclo_ajax_1_eff - tempo_surr_ajax_1
    tempo_fusione_ajax_2 = tempo_ciclo_ajax_2_eff - tempo_surr_ajax_2

    target_prod_giorn = 1440 / tempo_ciclo_ajax_1_eff * CARICA_RIF + 1440 / tempo_ciclo_ajax_2_eff * CARICA_RIF  # [ton]  # [ton]
    n_col_giorn = target_prod_giorn / PESO_PLACCA_RIF

    t_medio_disp_ciclo_J1 = 1440 / n_col_giorn  # Questo è anche lo sfasamento target ottimale tra i due forni

    fine_fus_1 = datetime.strptime(ajax_1.fine_prossima_fusione(), "%d-%m-%Y %H:%M:%S")
    fine_fus_2 = datetime.strptime(ajax_2.fine_prossima_fusione(), "%d-%m-%Y %H:%M:%S")

    if fine_fus_1 >= fine_fus_2:
        sfasamento_fus_actual = fine_fus_1 - fine_fus_2
        primo_forno = "Ajax 2"
    else:
        sfasamento_fus_actual = fine_fus_2 - fine_fus_1
        primo_forno = "Ajax 1"

    return t_medio_disp_ciclo_J1, sfasamento_fus_actual.total_seconds() / 60, primo_forno, tempo_surr_ajax_1, \
        tempo_surr_ajax_2, tempo_fusione_ajax_1, tempo_fusione_ajax_2, ajax_1.ton_infornate_ajax, ajax_2.ton_infornate_ajax


class Algoritmo:
    # Calcolo sfasamento target ottimale tra i travasi

    def __init__(self):
        data = calcolo_sfasamento()
        self.tempo_medio_disp_ciclo_j1 = data[0]
        self.sfasamento = data[1]
        self.primo_forno = data[2]
        self.tempo_surr_ajax_1 = data[3]
        self.tempo_surr_ajax_2 = data[4]
        self.tempo_fusione_ajax_1 = data[5]
        self.tempo_fusione_ajax_2 = data[6]
        self.ton_caricate_ajax_1 = data[7]
        self.ton_caricate_ajax_2 = data[8]
        self.calcolo()

    def calcolo(self):
        global ritardo
        with open('data/data_ui_ajax_1.json', 'r') as file:
            data = json.load(file)
        data_fine_ajax_1 = datetime.strptime(data["Fine fusione Ajax 1"], "%d-%m-%Y %H:%M")

        with open('data/data_ui_ajax_2.json', 'r') as file:
            data = json.load(file)
        data_fine_ajax_2 = datetime.strptime(data["Fine fusione Ajax 2"], "%d-%m-%Y %H:%M")

        orario_fine_primo_forno = min(data_fine_ajax_1, data_fine_ajax_2)

        with open('data/stato_junker.json', 'r') as file:
            data = json.load(file)

        stato = data["stato_junker"]

        if stato == "pieno":  # se il forno J1 è pieno (calcolo capacità residua forno come per LOMA 2), il valore
            # risulterà pari alla differenza tra durata intera della colata con la ricetta in quel momento attiva sul
            # PLC (come se la colata avesse convenzionalmente inizio nell'istante del calcolo attuale) e tempo
            # residuo fine fusione primo forno
            with open('data/fuso_in_junker.json', 'r') as file:
                data = json.load(file)

            lunghezza_placca = float(data["lunghezza_placca_target"])
            v_media = float(data["v_media"])
            tempo_colata = lunghezza_placca / v_media  # minuti
            orario_fine_colata = datetime.now() + timedelta(minutes=tempo_colata)

            ritardo = int((orario_fine_colata - orario_fine_primo_forno).total_seconds() / 60)
            print(ritardo)

        elif stato == "vuoto":  # Se invece il forno J1 è vuoto (o comunque la capacità residua è inferiore di un x% al
            # peso placca ricetta attiva), il tempo ritardo verrà convenzionalmente assunto pari al tempo minimo
            # assoluto ciclo di svuotamento J1 (perchè il ciclo può avere inizio solo col primo riempimento completo
            # e non può durare meno del tempo minimo).
            ritardo = int(TEMPO_CICLO_MIN - self.sfasamento)
            print(ritardo)

        else:
            with open('data/fuso_in_junker.json', 'r') as file:
                data = json.load(file)
            try:
                orario_fine_colata = datetime.strptime(data["ora_fine_colata"], "%Y-%m-%d %H:%M:%S")
                ritardo = int((orario_fine_colata - orario_fine_primo_forno).total_seconds() / 60)
                print(ritardo)
            except:
                print("Colata non ancora iniziata, esco dalla funzione")
                return

        if self.primo_forno == "Ajax 1":
            t_residuo_se_smezzo = self.tempo_medio_disp_ciclo_j1 - (self.sfasamento - ritardo)
            fraz_trav_ajax_1 = (
                                           PESO_PLACCA_RIF / self.ton_caricate_ajax_2 * self.tempo_fusione_ajax_2 + self.tempo_surr_ajax_2 - self.tempo_surr_ajax_1
                                           - self.tempo_medio_disp_ciclo_j1 + self.sfasamento) / (
                                           PESO_PLACCA_RIF / self.ton_caricate_ajax_1 * self.tempo_fusione_ajax_1
                                           + PESO_PLACCA_RIF / self.ton_caricate_ajax_2 * self.tempo_fusione_ajax_2)
            fraz_trav_ajax_2 = 1 - fraz_trav_ajax_1

            ton_travaso_ajax_1 = PESO_PLACCA_RIF * fraz_trav_ajax_1
            ton_travaso_ajax_2 = PESO_PLACCA_RIF * fraz_trav_ajax_2

            verifica_ciclo_ridotto_ajax_1 = ton_travaso_ajax_1 / self.ton_caricate_ajax_1 * self.tempo_fusione_ajax_1 + self.tempo_surr_ajax_1
            verifica_ciclo_ridotto_ajax_2 = ton_travaso_ajax_2 / self.ton_caricate_ajax_2 * self.tempo_fusione_ajax_2 + self.tempo_surr_ajax_2 + self.sfasamento
            nuovo_sfasamento = verifica_ciclo_ridotto_ajax_2 - verifica_ciclo_ridotto_ajax_1

        else:
            t_residuo_se_smezzo = self.tempo_medio_disp_ciclo_j1 - (self.sfasamento - ritardo)
            fraz_trav_ajax_1 = (
                                           PESO_PLACCA_RIF / self.ton_caricate_ajax_2 * self.tempo_fusione_ajax_2 + self.tempo_surr_ajax_2 - self.tempo_surr_ajax_1
                                           + self.tempo_medio_disp_ciclo_j1 - self.sfasamento) / (
                                       PESO_PLACCA_RIF / self.ton_caricate_ajax_1 * self.tempo_fusione_ajax_1
                                       + PESO_PLACCA_RIF / self.ton_caricate_ajax_2 * self.tempo_fusione_ajax_2)
            fraz_trav_ajax_2 = 1 - fraz_trav_ajax_1

            ton_travaso_ajax_1 = PESO_PLACCA_RIF * fraz_trav_ajax_1
            ton_travaso_ajax_2 = PESO_PLACCA_RIF * fraz_trav_ajax_2

            verifica_ciclo_ridotto_ajax_1 = ton_travaso_ajax_1 / self.ton_caricate_ajax_1 * self.tempo_fusione_ajax_1 + self.tempo_surr_ajax_1 + self.sfasamento
            verifica_ciclo_ridotto_ajax_2 = ton_travaso_ajax_2 / self.ton_caricate_ajax_2 * self.tempo_fusione_ajax_2 + self.tempo_surr_ajax_2
            nuovo_sfasamento = verifica_ciclo_ridotto_ajax_1 - verifica_ciclo_ridotto_ajax_2

        # Fase decisionale
        riscaldo_ajax_1 = 0
        riscaldo_ajax_2 = 0
        if t_residuo_se_smezzo >= TEMPO_CICLO_MIN:
            quant_da_travasare_ajax_1 = ton_travaso_ajax_1
            quant_da_travasare_ajax_2 = ton_travaso_ajax_2

        else:
            quant_da_travasare_ajax_1 = PESO_PLACCA_RIF
            quant_da_travasare_ajax_2 = PESO_PLACCA_RIF

            if TEMPO_CICLO_MIN - (self.sfasamento - ritardo) >= 0 and self.primo_forno == "Ajax 1":
                riscaldo_ajax_2 = TEMPO_CICLO_MIN - (self.sfasamento - ritardo)

            if TEMPO_CICLO_MIN - (self.sfasamento - ritardo) >= 0 and self.primo_forno == "Ajax 2":
                riscaldo_ajax_1 = TEMPO_CICLO_MIN - (self.sfasamento - ritardo)

            if stato == "in colata" and ritardo > 0 and self.primo_forno == "Ajax 1":
                riscaldo_ajax_1 = ritardo

            if stato == "in colata" and ritardo > 0 and self.primo_forno == "Ajax 2":
                riscaldo_ajax_2 = ritardo

        # Salvo i dati nel file
        with open('data/algoritmo.json', 'r') as file:
            data = json.load(file)

        data["da_fine_colata_a_fine_fusione"] = str(ritardo)
        data["sfasamento_forni"] = str(round(self.sfasamento))
        data["fraz_travaso_ajax_1"] = str(fraz_trav_ajax_1)
        data["fraz_travaso_ajax_2"] = str(fraz_trav_ajax_2)
        data["ton_travaso_ajax_1"] = str(ton_travaso_ajax_1)
        data["ton_travaso_ajax_2"] = str(ton_travaso_ajax_2)
        data["quant_da_travasare_ajax_1"] = str(round(quant_da_travasare_ajax_1, 1))
        data["quant_da_travasare_ajax_2"] = str(round(quant_da_travasare_ajax_2, 1))
        data["riscaldo_ajax_1"] = str(round(riscaldo_ajax_1))
        data["riscaldo_ajax_2"] = str(round(riscaldo_ajax_2))
        data["nuovo_sfasamento"] = str(round(nuovo_sfasamento))
        data["sfasamento_ottimale"] = str(round(self.tempo_medio_disp_ciclo_j1))

        with open("data/algoritmo.json", "w") as f:
            json.dump(data, f, indent=4)
