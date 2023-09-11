#!/usr/bin/env python
#_*_ codig: utf8 _*_
import psycopg2, json, os
from apps.etltelecom.modules.constants import *
from apps.etltelecom.modules.functions import *

def telecom_main(log_key):
    try:
        dict_summary={}
        response_download_log=download_log(log_key)
        if 'Error:' in response_download_log:
            dict_summary['download_log_error']= response_download_log
            dict_summary_str=json.dumps(dict_summary, sort_keys=True, indent=4)
            print(dict_summary_str)
            print_log(dict_summary_str) #Se registra en el log de eventos el resumen.
            mail_subject='FAIL etltelecom PROD error Download Logs' #Se establece el asunto del correo.
            SendMail(dict_summary_str, mail_subject) #Se envia correo electronico.
            return dict_summary, 404
        else:
            cdndb_connect=psycopg2.connect(data_base_connect_prod)
            cdndb_cur=cdndb_connect.cursor()
            csv_file_path=response_download_log
            with open(csv_file_path, 'r', encoding="latin1") as csv_data:
                consulta_copy = f"COPY telecomdata FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ';');"
                cdndb_cur.copy_expert(consulta_copy, csv_data)
                dict_summary[csv_file_path]={'Sum_csv_Data' : cdndb_cur.rowcount}
                cdndb_connect.commit()
            cdndb_cur.execute("UPDATE telecomdata SET datetime = REPLACE(REPLACE(datetime, 'T', ' '), 'Z', '') WHERE datetime LIKE '%T%';")
            cdndb_connect.commit()
            cdndb_cur.execute("UPDATE telecomdata SET device = 'other'  WHERE device LIKE 'NO DEFINIDO';")
            cdndb_connect.commit()
            cdndb_cur.execute("UPDATE telecomdata SET device = 'WEB' WHERE device LIKE 'STATIONARY' or device LIKE 'CLOUD_CLIENT';")
            cdndb_connect.commit()
            cdndb_cur.execute("UPDATE telecomdata SET country = 'AR' WHERE country LIKE 'error';")
            cdndb_connect.commit()
            cdndb_cur.execute("SELECT DISTINCT telecomdata.contentid FROM telecomdata LEFT JOIN xmldata ON telecomdata.contentid = xmldata.contentid where xmldata.contentid is NULL;")
            contentid_list=cdndb_cur.fetchall()
            if contentid_list != []:
                xml_nofound, dict_xml_extract = extract_xml_data(contentid_list)
                dict_summary[csv_file_path].update({'extract_xml_data': dict_xml_extract})
                for contentid in xml_nofound:
                    cdndb_cur.execute(f"DELETE FROM telecomdata WHERE contentid LIKE '{contentid}';")
                    cdndb_connect.commit()

            else:
                pass
            sql="""INSERT INTO playbacks
            SELECT 
            telecomdata.datetime,
            telecomdata.country,
            'flow',
            telecomdata.device,
            telecomdata.clientid,
            telecomdata.contentid,
            xmldata.contenttype,
            xmldata.channel,
            xmldata.title,
            xmldata.serietitle,
            xmldata.releaseyear,
            xmldata.season,
            xmldata.episode,
            xmldata.genre,
            xmldata.rating,
            xmldata.duration,
            telecomdata.segduration
            FROM telecomdata
            LEFT JOIN xmldata ON telecomdata.contentid = xmldata.contentid
            GROUP BY telecomdata.datetime,
            telecomdata.country,
            telecomdata.device,
            telecomdata.clientid,
            telecomdata.contentid,
            xmldata.contenttype,
            xmldata.channel,
            xmldata.title,
            xmldata.serietitle,
            xmldata.releaseyear,
            xmldata.season,
            xmldata.episode,
            xmldata.genre,
            xmldata.rating,
            xmldata.duration,
            telecomdata.segduration;
            """
            cdndb_cur.execute(sql)
            dict_summary[csv_file_path].update({'sum_Insert_Playbacks': cdndb_cur.rowcount})
            cdndb_connect.commit()
            cdndb_cur.execute('DELETE FROM telecomdata;')
            cdndb_connect.commit()
            os.remove(csv_file_path)
            dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
            print_log(dict_summary_str)
            SendMail(dict_summary_str, 'Summary telecom Data Playbacks')
            cdndb_cur.close()
            cdndb_connect.close()
            return f"{log_key} processed", 200
    except:
        cdndb_cur.close()
        cdndb_connect.close()
        error=sys.exc_info()[2]
        errorinfo=traceback.format_tb(error)[0]
        dict_summary['Error']={
            'Error': str(sys.exc_info()[1]),
            'error_info': errorinfo
        }
        dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
        print_log(dict_summary_str)
        mail_subject='FAIL etltelecom_PROD Execution Error'
        SendMail(dict_summary_str, mail_subject)
        return dict_summary, 404