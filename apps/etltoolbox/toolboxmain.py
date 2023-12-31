#!/usr/bin/env python
#_*_ codig: utf8 _*_
from apps.etltoolbox.Modules.functions import *
from apps.etltoolbox.Modules.constants import *
import sys, traceback, gzip, time, datetime, json, psycopg2, os, shutil

def toolbox_main(log_key):
    dict_summary={}
    count_newmanifest=0
    count_VmxSegments=0
    count_newsegments=0
    quantity=0
    date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
    if FlagStatus('r'):    
        try:
            beginning=time.time()
            postgresql=psycopg2.connect(database_Connect)
            curpsql=postgresql.cursor()
            responde_download_log=DownloadLog(log_key)
            if 'Error:' in responde_download_log:
                curpsql.close() #Se cierra la conexion con el cursor de la base de datos.
                postgresql.close() #Se cierra la conexion con la base de datos.
                dict_summary['download_Error'] = responde_download_log
                dict_summary['log'] = log_key
                dict_summary_srt=json.dumps(dict_summary, sort_keys=False, indent=4)
                PrintLog('a', dict_summary_srt, date_log) #Se registra en el log de eventos el resumen.
                mail_subject='API etltoolbox_PROD error Download Logs' #Se establece el asunto del correo.
                SendMail(dict_summary_srt, mail_subject) #Se envia correo electronico.
            else:
                log_path=responde_download_log
                with gzip.open(f'{log_path}', 'rt') as file:
                    for line in file:
                        quantity+=1
                        if '#' in line:
                            pass
                        else:
                            columns=line.split('\t')
                            Uri=columns[7].split('/')
                            Status_Validate='200'==columns[8] or '206'==columns[8]
                            Query_Validate=not('CMCD=' in columns[11]) and not(columns[11]=='-') and len(columns[11].split('&'))==5
                            if len(Uri)==5 and Uri[1]=='prod':
                                Manifest_Validate='master.m3u8'== Uri[4] or 'stream.mpd'==Uri[4] or 'Manifest'==Uri[4]
                                if Manifest_Validate and Status_Validate  and Query_Validate:
                                    Type=columns[7].split('/')[3].split('.')[1] # NEW
                                    SQL="INSERT INTO new_manifests VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                    DATA=(
                                        columns[0]+' '+columns[1],      # datetime
                                        columns[11].split('&')[0],      # clientid
                                        columns[11].split('&')[1],      # contentid
                                        columns[4],                     # ip
                                        columns[14][:15],               # manifestsid
                                        columns[11].split('&')[2][3:],  # country
                                        columns[11].split('&')[3][2:],  # mso
                                        columns[11].split('&')[4][2:],  # device
                                        profile[Type][0],               # Segment Duration
                                        )
                                    curpsql.execute(SQL,DATA)
                                    postgresql.commit()
                                    count_newmanifest+=1
                            elif len(Uri)>5 and Uri[1]=='prod':
                                Video_Validate=Uri[4]=='video' or 'HD' in Uri[4] or 'SD' in Uri[4] or 'video' in Uri[5]
                                Segment_Validate=not('init.mp4' in Uri[len(Uri)-1] or 'iframes.m3u8' in Uri[len(Uri)-1] or 'stream.m3u8' in Uri[len(Uri)-1] or 'video=0' in Uri[len(Uri)-1])
                                Query_Validate_seg=not('CMCD=' in columns[11]) and not(columns[11]=='-') and len(columns[11].split('&'))>=6
                                if Status_Validate and Video_Validate and Segment_Validate and Query_Validate_seg:
                                    if len(columns[11].split('?'))==1:
                                        Type=Uri[3].split('.')[1]
                                        SQL="INSERT INTO new_segmentos VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                                        DATA=(
                                            columns[0]+' '+columns[1],      # datetime
                                            columns[11].split('&')[0][2:],  # clientid
                                            columns[11].split('&')[1][2:],  # contentid
                                            columns[4],                     # ip
                                            columns[11].split('&')[5][3:],  # manifestid
                                            Uri[len(Uri)-1],                # segmento
                                            profile[Type][0],               # Duration
                                            profile[Type][1],               # Type
                                            Uri[3],                         # Mediaid
                                            )    
                                        curpsql.execute(SQL, DATA)
                                        count_newsegments+=1
                                        postgresql.commit()
                                    elif len(columns[11].split('?'))==2:
                                        Type=Uri[3].split('.')[1]
                                        Query=columns[11].split('?')[0]
                                        SQL="INSERT INTO new_segmentos VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                                        DATA=(
                                            columns[0]+' '+columns[1],      # datetime
                                            Query.split('&')[0][2:],        # clientid
                                            Query.split('&')[1][2:],        # contentid
                                            columns[4],                     # ip
                                            Query.split('&')[5][3:],        # manifestid
                                            Uri[len(Uri)-1],                # segmento
                                            profile[Type][0],               # Duration
                                            profile[Type][1],               # Type
                                            Uri[3],                         # Mediaid
                                            )    
                                        curpsql.execute(SQL, DATA)
                                        count_newsegments+=1
                                        postgresql.commit()
                                elif Status_Validate and Video_Validate and Segment_Validate:
                                    Type=Uri[3].split('.')[1]
                                    if Type=='vmxmpd' or Type=='vmxm3u8':
                                        count_VmxSegments+=1
                finish=time.time()
                dict_summary['Log_summary']=log_path.split('/')[-1]
                dict_summary['Lines_processed']=str(quantity)
                dict_summary['New_manifests_Registered']=str(count_newmanifest)
                dict_summary['New_segments_Registered']=str(count_newsegments)
                dict_summary['Vmx_segments']=str(count_VmxSegments)
                dict_summary['Process_duration']=str(round((finish-beginning),3))
                dict_summary_srt=json.dumps(dict_summary, sort_keys=False, indent=8)
                PrintLog("a", dict_summary_srt, date_log)
                file_Name=os.path.basename(log_path)
                shutil.move(log_path, destination_Path+file_Name)
                curpsql.close()
                postgresql.close() #Postgresqlv
        except:
            finish=time.time()
            FlagStatus("w")
            curpsql.close()
            postgresql.close() #Postgresql
            error=sys.exc_info()[2]
            errorinfo=traceback.format_tb(error)[0]
            dict_summary['Log_summary']=log_path.split('/')[-1]
            dict_summary['Lines_processed']=str(quantity)
            dict_summary['New_manifests_Registered']=str(count_newmanifest)
            dict_summary['New_segments_Registered']=str(count_newsegments)
            dict_summary['Vmx_segments']=str(count_VmxSegments)
            dict_summary['Process_duration']=str(round((finish-beginning),3))
            dict_summary['Log_Error']={
                'Error': str(sys.exc_info()[1]),
                'error_info': errorinfo
            }
            dict_summary_srt=json.dumps(dict_summary, sort_keys=False, indent=8)
            PrintLog("a", dict_summary_srt, date_log)
            mail_subject='FAIL etltoolbox_PROD Execution Error'
            SendMail(dict_summary_srt, mail_subject)
    else:
        text_print="etltoolbox_PROD application failure not recognized\n"
        PrintLog("a", text_print, date_log)
    