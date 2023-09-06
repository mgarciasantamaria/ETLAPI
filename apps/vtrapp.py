#!/usr/bin/env python
#_*_ codig: utf8 _*_
import psycopg2, time, json, re, os
import pandas as pd
from modules.constants import *
from modules.functions import *

def mainvtr(log_name):
    try:
        log_path=f"/mnt/ingbox/DATA_VTR/{log_name}"
        dict_summary={}
        cdndb_connect=psycopg2.connect(data_base_connect_prod)
        cdndb_cur=cdndb_connect.cursor()
        df=pd.read_csv(log_path, delimiter=',', low_memory=False)
        quantity=df.shape[0]
        df=df.drop(['TITULO', 'TITULO_lower', 'DURATION'], axis='columns')
        df.rename(columns={'IDEN_VIVIENDA': 'clientid', 'ID_FECH_COMPRA': 'datetime', 'View_Minutos': 'segduration', 'DURATION': 'duration', 'ExternalID': 'contentid'}, inplace=True)
        df['device']='N/A'
        df['country']='CL'
        df['datetime']=df['datetime'].apply(lambda x: re.sub(r"\.[^.]*$", "", x))
        df=df.reindex(columns=['datetime', 'country', 'clientid', 'contentid', 'device', 'segduration'])
        cdndb_cur.executemany("INSERT INTO vtrdata VALUES (%s, %s, %s, %s, %s, %s)", df.values.tolist())
        cdndb_connect.commit()
        time.sleep(2)
        cdndb_cur.execute("SELECT DISTINCT vtrdata.contentid FROM vtrdata LEFT JOIN xmldata ON vtrdata.contentid = xmldata.contentid where xmldata.contentid is NULL;")    
        contentid_list=cdndb_cur.fetchall()
        if contentid_list != []:
            xml_nofound, dict_xml_extract = extract_xml_data(contentid_list)
            dict_summary[log_path]=({'extract_xml_data': dict_xml_extract})
            for contentid in xml_nofound:
                cdndb_cur.execute(f"DELETE FROM vtrdata WHERE contentid LIKE '{contentid}';")
                dict_summary['Delete_Playbacks']=cdndb_cur.rowcount
                cdndb_connect.commit()
        else:
            dict_summary[log_path]=({'extract_xml_data': 0})
            dict_summary['Delete_Playbacks']=0
            pass 
        sql="""INSERT INTO playbackstest
        SELECT 
        vtrdata.datetime,
        vtrdata.country,
        'vtr',
        vtrdata.device,
        vtrdata.clientid,
        vtrdata.contentid,
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
        vtrdata.segduration
        FROM vtrdata
        LEFT JOIN xmldata ON vtrdata.contentid = xmldata.contentid
        GROUP BY vtrdata.datetime,
        vtrdata.country,
        vtrdata.device,
        vtrdata.clientid,
        vtrdata.contentid,
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
        vtrdata.segduration;
        """
        cdndb_cur.execute(sql)
        dict_summary[log_path].update({'sum_Insert_Playbacks': cdndb_cur.rowcount})
        cdndb_connect.commit()
        dict_str=json.dumps(dict_summary[log_path], sort_keys=False, indent=4)
        cdndb_cur.execute('DELETE FROM vtrdata;')
        cdndb_connect.commit()
        #os.remove(log_path)
        SendMail(dict_str, 'Summary VTR Data Playbacks')
        cdndb_cur.close()
        cdndb_connect.close()
        return dict_summary       
        
    except:
        cdndb_cur.close()
        cdndb_connect.close()
        error=sys.exc_info()[2]
        errorinfo=traceback.format_tb(error)[0]
        dict_summary['Error']={
            'Error': str(sys.exc_info()[1]),
            'error_info': errorinfo
        }
        dict_str=json.dumps(dict_summary, sort_keys=False, indent=4)
        print_log(dict_str)
        mail_subject='FAIL VTR_Data_PROD Execution Error'
        SendMail(dict_str, mail_subject)
        return dict_summary