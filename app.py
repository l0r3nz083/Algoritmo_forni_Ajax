from datetime import datetime
from ajax import Ajax
import pandas as pd
import numpy as np
from integratore import Integratore
from flask import Flask, render_template, request
import warnings
import json
import threading
from junker import Junker
from algoritmo import Algoritmo

should_stop = False  # Variabile di controllo per interrompere il thread

warnings.filterwarnings("ignore")
app = Flask(__name__, template_folder='templates')


def save_data(ajax):
    try:
        ajax_1 = Ajax(1)
        ajax_2 = Ajax(2)

        fine_fus_1 = datetime.strptime(ajax_1.fine_prossima_fusione(), "%d-%m-%Y %H:%M:%S")
        fine_fus_2 = datetime.strptime(ajax_2.fine_prossima_fusione(), "%d-%m-%Y %H:%M:%S")

        if ajax == 1:

            integratore_ajax_1 = Integratore(1)

            superheat_ajax_1 = integratore_ajax_1.save_energies()
            date_last_thread = integratore_ajax_1.date_last_thread
            car = "CONFERMATE" if ajax_1.carica_terminata else "PREVISTE"

            vars_ajax_1 = {
                "Integratore Ajax 1": str(round(integratore_ajax_1.ene_ajax)) + " Kwh",
                "Inizio fusione Ajax 1": ajax_1.date_time_ajax.strftime("%d-%m-%Y %H:%M"),
                "Fine fusione Ajax 1": fine_fus_1.strftime("%d-%m-%Y %H:%M"),
                "Pot media Ajax 1": round(integratore_ajax_1.potenza_media),
                f"Ton {car} Ajax 1": str(round(ajax_1.ton_infornate_ajax, 1)) + " ton",
                "Ene richiesta Ajax 1": str(round(ajax_1.ene_required)) + " Kwh",
                "Superheat": superheat_ajax_1,
                "Data ultimo thread": date_last_thread
            }

            with open("data/data_ui_ajax_1.json", "w") as f:
                json.dump(vars_ajax_1, f, indent=4)

        else:

            integratore_ajax_2 = Integratore(2)

            superheat_ajax_2 = integratore_ajax_2.save_energies()
            car = "CONFERMATE" if ajax_2.carica_terminata else "PREVISTE"

            vars_ajax_2 = {
                "Integratore Ajax 2": str(round(integratore_ajax_2.ene_ajax)) + " Kwh",
                "Inizio fusione Ajax 2": ajax_2.date_time_ajax.strftime("%d-%m-%Y %H:%M"),
                "Fine fusione Ajax 2": fine_fus_2.strftime("%d-%m-%Y %H:%M"),
                "Pot media Ajax 2": round(integratore_ajax_2.potenza_media),
                f"Ton {car} Ajax 2": str(round(ajax_2.ton_infornate_ajax, 1)) + " ton",
                "Ene richiesta Ajax 2": str(round(ajax_2.ene_required)) + " Kwh",
                "Superheat": superheat_ajax_2
            }
            with open("data/data_ui_ajax_2.json", "w") as f:
                json.dump(vars_ajax_2, f, indent=4)
    except Exception as e:
        print(f"ERRORE IN SAVE DATA: {e}")


def update_data():
    while not should_stop:
        try:
            # Chiamata alle funzioni get_records() qui
            save_data(1)
            save_data(2)

            junker = Junker()
            algo = Algoritmo()

            threading.Event().wait(20)  # Attendi 20 secondi prima di eseguire nuovamente le funzioni
        except Exception as e:
            print(f"ERRORE NEL THREAD PRINCIPALE: {e}")


@app.route('/')
def index():
    with open("data/data_ui_ajax_1.json", "r") as f:  # Leggo i file dal json dei dati ui per ajax 1
        data_ajax_1 = json.load(f)

    with open("data/data_ui_ajax_2.json", "r") as f:  # Leggo i file dal json ei dati per ui per ajax 2
        data_ajax_2 = json.load(f)

    prog_ajax_1 = progress_ajax_1()
    prog_ajax_2 = progress_ajax_2()
    records_ajax_1 = data_ajax_1
    records_ajax_2 = data_ajax_2
    superheat_ajax_1 = data_ajax_1["Superheat"]
    superheat_ajax_2 = data_ajax_2["Superheat"]
    last_thread = data_ajax_1["Data ultimo thread"]

    return render_template('index.html', records_ajax_1=records_ajax_1,
                           records_ajax_2=records_ajax_2,
                           prog_ajax_1=prog_ajax_1,
                           prog_ajax_2=prog_ajax_2,
                           superheat_ajax_1=superheat_ajax_1,
                           superheat_ajax_2=superheat_ajax_2,
                           last_thread=last_thread)


@app.route('/algoritmo')
def algoritmo():
    with open("data/stato_junker.json", "r") as f:  # Leggo i file dal json ei dati per ui per Junker
        data_junker = json.load(f)

    with open("data/fuso_in_junker.json", "r") as f:  # Leggo i file dal json ei dati per ui per Junker
        data_peso_junker = json.load(f)

    with open("data/algoritmo.json", "r") as f:  # Leggo i file dal json ei dati per ui per Junker
        data_algoritmo = json.load(f)

    stato_junker = data_junker["stato_junker"]

    peso_junker = data_peso_junker["fuso_rimanente"]
    data_fine_colata = data_peso_junker["ora_fine_colata"]
    tempo_rimanente = data_peso_junker["durata_colata"]

    fine_colata_fine_fusione = data_algoritmo["da_fine_colata_a_fine_fusione"]
    sfasamento = data_algoritmo["sfasamento_forni"]
    ton_travaso_ajax_1 = data_algoritmo["ton_travaso_ajax_1"]
    ton_travaso_ajax_2 = data_algoritmo["ton_travaso_ajax_2"]
    effettivo_da_trav_ajax_1 = data_algoritmo["quant_da_travasare_ajax_1"]
    effettivo_da_trav_ajax_2 = data_algoritmo["quant_da_travasare_ajax_2"]
    riscaldo_ajax_1 = data_algoritmo["riscaldo_ajax_1"]
    riscaldo_ajax_2 = data_algoritmo["riscaldo_ajax_2"]
    nuovo_sfasamento = data_algoritmo["nuovo_sfasamento"]
    sfasamento_ottimale = data_algoritmo["sfasamento_ottimale"]

    return render_template('algoritmo.html',
                           stato_junker=stato_junker,
                           peso_junker=peso_junker,
                           fine_colata=data_fine_colata,
                           tempo_rimanente=tempo_rimanente,
                           fine_colata_fine_fusione=fine_colata_fine_fusione,
                           sfasamento=sfasamento,
                           ton_travaso_ajax_1=ton_travaso_ajax_1,
                           ton_travaso_ajax_2=ton_travaso_ajax_2,
                           effettivo_da_trav_ajax_1=effettivo_da_trav_ajax_1,
                           effettivo_da_trav_ajax_2=effettivo_da_trav_ajax_2,
                           riscaldo_ajax_1=riscaldo_ajax_1,
                           riscaldo_ajax_2=riscaldo_ajax_2,
                           nuovo_sfasamento=nuovo_sfasamento,
                           sfasamento_ottimale=sfasamento_ottimale
                           )


@app.route('/data_analysis', methods=['GET', 'POST'])
def data_analysis():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    if request.method == 'POST':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')

    dati_grafico_ajax_1 = leggi_dati_grafico("data/energia_ajax_1.csv", start_date, end_date)
    dati_grafico_ajax_2 = leggi_dati_grafico("data/energia_ajax_2.csv", start_date, end_date)

    x1 = np.array(dati_grafico_ajax_1['DATA_ORA_FINE'])
    x2 = np.array(dati_grafico_ajax_2['DATA_ORA_FINE'])
    y1 = np.array(dati_grafico_ajax_1["DELTA"])
    y2 = np.array(dati_grafico_ajax_2["DELTA"])

    return render_template('data_analysis.html',
                           x1=x1.tolist(),
                           x2=x2.tolist(),
                           y1=y1.tolist(),
                           y2=y2.tolist(),
                           start_date=start_date,
                           end_date=end_date)


def leggi_dati_grafico(file_csv, start_date, end_date):
    dati_grafico = {'DATA_ORA_FINE': [], 'ENE': [], 'DELTA': []}

    df = pd.read_csv(file_csv)
    df['DATA_ORA_FINE'] = pd.to_datetime(df['DATA_FINE'], format="%Y-%d-%m %H:%M:%S")

    # Filtra i dati solo se le date sono state selezionate
    if start_date and end_date:
        start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
        end_date = pd.to_datetime(end_date, format="%Y-%m-%d")

        df_filtered = df[(df['DATA_ORA_FINE'] >= start_date) & (df['DATA_ORA_FINE'] <= end_date)]
    else:
        df_filtered = df

    dati_grafico['DATA_ORA_FINE'] = df_filtered['DATA_ORA_FINE'].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
    # dati_grafico['ENE'] = [df_filtered['ENE_REQUIRED'].astype(float).tolist(),
                           #df_filtered['ENE_ACTUAL'].astype(float).tolist()]
    dati_grafico["DELTA"] = [float(data["ENE_REQUIRED"])-float(data["ENE_ACTUAL"]) for data in df_filtered.to_dict('records')]

    filtered_dati_grafico = {
        key: value
        if key != "DELTA" else [delta for delta in value if -2000 < delta < 2000]
        for key, value in dati_grafico.items()
    }

    return filtered_dati_grafico


def progress_ajax_1():
    ajax_1 = Ajax(1)

    with open("data/data_ajax_1.json", "r") as f:  # Leggo i file dal json temporaneo
        data = json.load(f)

    ene_ajax_1 = float(data["ENERGIA_AJAX"])

    progress_ajax_1 = ene_ajax_1 / ajax_1.ene_required * 100

    return round(progress_ajax_1, 1)


def progress_ajax_2():
    ajax_2 = Ajax(2)

    with open("data/data_ajax_2.json", "r") as f:  # Leggo i file dal json temporaneo
        data = json.load(f)

    ene_ajax_2 = float(data["ENERGIA_AJAX"])

    progress_ajax_2 = ene_ajax_2 / ajax_2.ene_required * 100

    return round(progress_ajax_2, 1)


@app.route('/stop')
def stop_update():
    global should_stop
    should_stop = True
    return 'Aggiornamento interrotto'


if __name__ == '__main__':
    # Avvia il thread per l'aggiornamento periodico dei dati
    update_thread = threading.Thread(target=update_data)
    update_thread.start()

    # Scommentare per avviare su server
    app.run(host="172.30.9.142", port=8507)

    # Avvia l'app Flask
    #app.run()
