import pyodbc
import pandas as pd

# Connessione a SQL SERVER
SERVER = '172.30.3.92\SQLEXPRESS'
DATABASE = 'fo_dati'
USERNAME = 'fo_user'
PASSWORD = 'ripristino'
CONN_STR = 'DRIVER={SQL Server};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD

QUERY_CICLO = ("SELECT top 20 [fo_dati].[dbo].[riep_colate].[ID_COLATA]\n"
               ",convert(varchar(20), [START], 20) as START\n"
               ",convert(varchar(20), [STOP] , 20) as STOP\n"
               ",convert(varchar(8),stop-start, 14) as durata\n"
               ",cast(cast(cast([fo_dati].[dbo].[dati_colata_placca].LIVL_AVG*100 as int) as real)/100 as varchar) as "
               "LIVL_AVG\n"
               ",(ajax_fusioni.ELE_FUS_FINE-ajax_fusioni.ELE_FUS_INI)/nullif(ajax_fusioni.TRAVASATO_PREC,0)*1000 as "
               "ENE_SPEC\n"
               ",(ajax_fusioni.ELE_FUS_FINE-ajax_fusioni.ELE_FUS_INI) / nullif(DATEDIFF(HOUR, ajax_fusioni.DATA_ORA, ajax_fusioni.DATA_ORA_FINE),0) as POTENZA_MED\n"
               ",DATEDIFF(MINUTE, ajax_fusioni.DATA_ORA_FINE,  riep_colate.STOP) as TEMPO_CICLO_J1\n"
               ",DATEDIFF(HOUR, ajax_fusioni.DATA_ORA,  ajax_fusioni.DATA_ORA_FINE) as DURATA_FUSIONE_HOUR\n"
               ",(select MAX(dati_colata_pt.L_PLACCA) from dati_colata_pt\n"
               "	where dati_colata_pt.ID_COLATA = riep_colate.ID_COLATA\n"
               "	group by dati_colata_pt.ID_COLATA) as L_PLACCA\n"
               ",(select top 1 ajax_fusioni.ID from ajax_fusioni \n"
               "   where (DATA_ORA_FINE < START)\n"
               "   order by DATA_ORA_FINE desc) as ID_FUSIONE_TAB\n"
               "  ,ajax_fusioni.ID_FUSIONE\n"
               "  ,ajax_fusioni.FORNO\n"
               "  ,ajax_fusioni.DATA_ORA_FINE\n"
               "  ,DATEDIFF(MINUTE,  ajax_fusioni.DATA_ORA_FINE, START_DISCESA) as PERM_JUNKER\n"
               "FROM [fo_dati].[dbo].[riep_colate]\n"
               "left join [fo_dati].[dbo].[dati_colata_placca] on \n"
               "[fo_dati].[dbo].[riep_colate].ID_COLATA = [fo_dati].[dbo].[dati_colata_placca].ID_COLATA\n"
               "left join ajax_fusioni on (select top 1 ajax_fusioni.ID from ajax_fusioni where (DATA_ORA_FINE < "
               "START) order by DATA_ORA_FINE desc) = ajax_fusioni.ID\n"
               "order by START desc")


class Query:
    def __init__(self, forno=1):
        self.query_ciclo_J1 = QUERY_CICLO
        self.query_fusione = ""
        self.query_ene = ""

    def query_ciclo(self):
        conn = pyodbc.connect(CONN_STR)

        df = pd.read_sql_query(self.query_ciclo_J1, conn)

        conn.close()

        return df

    def query_fus(self):
        conn = pyodbc.connect(CONN_STR)

        df = pd.read_sql_query(self.query_fusione, conn)

        conn.close()

        return df

    def query_ene(self):
        conn = pyodbc.connect(CONN_STR)

        df = pd.read_sql_query(self.query_ene, conn)

        conn.close()

        return df
