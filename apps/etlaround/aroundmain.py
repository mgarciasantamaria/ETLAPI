from apps.etlaround.modules.constants import *
from apps.etlaround.modules.functions import *
import gzip, io, time, shutil
#Inicio del codigo principal
def around_main(log_key):
    #Se recoge el dato de fecha y hora en el instante en que se ejecuta el codigo.
    date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
    dict_summary={}
    dict_log={}
    count_segments=0
    quantity=0
    #Se consulta el estado de la bandera "FLAG" en el archivo json.
    if FlagStatus('r'):
        try:
            beginning=time.time()
            #Se recoge la lista de los archivos logs descargados que entrega la funcion Download_Logs.
            response_download_log=DownloadLog(log_key)
            if 'Error:' in response_download_log:
                dict_summary['download_log_error'] = response_download_log
                dict_summary['log'] = log_key
                dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
                PrintLog(dict_summary_str, date_log) #Se registra en el log de eventos el resumen.
                mail_subject='FAIL API around_PROD error Download Logs' #Se establece el asunto del correo.
                SendMail(dict_summary_str, mail_subject) #Se envia correo electronico.
            else:
                log_path=response_download_log
                with gzip.open(log_path, 'rt') as log:
                    #Se lee el contenido del archivo, eliminando la primera lines, y la palabra'#Fields: ' de la segunda linea. el resultado se guarda como una cadena de caracteres en  la variable log_data.
                    log_data=log.read().replace('#Version: 1.0\n#Fields: ', '').replace(' ','\t')
                #Se crea un DataFrame leyendo los datos de la variable log_data como un arcjvo csv, para lo cual se usa el modulo io y su funcion StringIO que trata la cadena de caracteres como un archivo csv delimitado por tabulacion.
                df=pd.read_csv(io.StringIO(log_data), delimiter='\t')
                #Se establece la cantidad de lineas que posee el log
                quantity=df.shape[0]
                #Se crea un Dataframe aplicando los filtros necesarios para separar unicamente los manifests validos. De los mismos se optienen solamente los datos nesesarios.
                df_manifests=df[((df['cs-uri-stem'].str.endswith('index.m3u8')) | (df['cs-uri-stem'].str.endswith('index.mpd')) | (df['cs-uri-stem'].str.endswith('Manifest'))) & ((df['sc-status']==200) | (df['sc-status']==206))][['date', 'time', 'cs-uri-stem', 'cs-uri-query', 'x-edge-request-id', ]]
                if not(df_manifests.empty):
                    #Se aplica la funcion Uri_Transform a todos los datos de la columna 'cs-uri-stem', los cuales se transforman en una lista con los datos mso_name, country, uri_id
                    df_manifests['cs-uri-stem']=df_manifests['cs-uri-stem'].map(UriTransform)
                    #Se dividen los datos de la lista anterior para ser agregados al mismo dataframe como tres columnas
                    df_manifests['mso'], df_manifests['country'], df_manifests['uri_id']=zip(*df_manifests['cs-uri-stem'])
                    #Se aplica la funcion request_id_transform la cual transforma los datos de la columna 'x-edge-request-id', el resultado se guarda en una nueva columna 'manifestid' del mismo dataframe.   
                    df_manifests['manifestid']=df_manifests['x-edge-request-id'].map(RequestIdTransform)
                    #Se aplica la funcion Manifest_Query_Transform a los datos de la columna 'cs-quri-query' y se crea una nueva columna 'client_id' con los datos transformados.
                    df_manifests['client_id']=df_manifests['cs-uri-query'].map(ManifestQueryTransform)
                    #Se crea una nueva columna 'datetime' a partir de la union de las columnas 'date' y 'time'. 
                    df_manifests['datetime']=df_manifests['date'] + " " + df_manifests['time']
                    #Se eliminan las columnas a las cuales se les aplicado la transformacion.
                    df_manifests=df_manifests.drop(['date', 'time', 'cs-uri-query', 'x-edge-request-id', 'cs-uri-stem'], axis='columns')
                    count_manifests=df_manifests.shape[0]
                else:
                    df_manifests['datetime']=None
                    df_manifests['manifestid']=None
                    df_manifests['uri_id']=None
                    df_manifests['mso']=None
                    df_manifests['country']=None
                    df_manifests['client_id']=None
                    #SE establece el contador de manifest a 0
                    count_manifests=0
                #Se crea un DataFrame aplicando los filtros necesarios para encontrar los segmentos de video validos. De los mismo se optienen solamente los datos necesarios.
                df_segments=df[((df['cs-uri-stem'].str.contains(r"index_video_\d+_\d+_\d+.mp4")) | (df['cs-uri-stem'].str.contains(r"Fragments\(v=\d+\)")) | (df['cs-uri-stem'].str.contains(r"index_\d+_\d+.ts"))) & ((df['sc-status']==200) | (df['sc-status']==206))][['date', 'time', 'cs-uri-stem', 'cs-uri-query', 'cs(User-Agent)']]
                if not(df_segments.empty):
                    #Se aplica la funcion Segments_Query_Transform a la columna 'cs-uri-query' el resultado se guarda en una lista.
                    df_segments['cs-uri-query']=df_segments['cs-uri-query'].map(SegmentsQueryTransform)
                    #Se dividen los datos de la lista anterior para ser agregados al mismo dataframe como 2 columnas
                    df_segments['device'], df_segments['manifestid']=zip(*df_segments['cs-uri-query'])
                    condition = ~(df_segments['cs(User-Agent)'].str.contains(r"Mozilla/.*", na=False)) & (df_segments['device'].str.contains('desktop', na=False))
                    df_segments.loc[condition, 'device']='stb'
                    df_segments['datetime']=df_segments['date'] + " " + df_segments['time']
                    df_segments=df_segments.drop(['date', 'time', 'cs-uri-query'], axis='columns')

                    df_group_segments=df_segments.groupby(['manifestid', 'device']).size().reset_index()
                    count=pd.DataFrame({'manifestid':df_group_segments['manifestid'], 'device':df_group_segments['device'], 'segmentos':df_group_segments[0]})
                else:
                    count=pd.DataFrame({'manifestid':[None], 'device':[None], 'segmentos':[None]})
                    pass
                df_merge=pd.merge(df_manifests[['datetime','manifestid','uri_id','mso','country', 'client_id']], count[['manifestid','device','segmentos']], on='manifestid',how='outer')
                count_segments=df_merge['segmentos'].sum()
                df_merge['segmentos']*=10
                df_merge['segmentos']=df_merge['segmentos'].fillna(0)

                df_data=MetadataExtract(df_merge['uri_id'].drop_duplicates().dropna())

                df_merge2=pd.merge(df_merge[['manifestid', 'datetime', 'mso', 'country', 'client_id', 'device', 'segmentos', 'uri_id']], df_data[['uri_id','assetid', 'humanid', 'servicetype', 'contenttype', 'channel', 'title', 'serietitle', 'releaseyear', 'season', 'episode', 'genre', 'rating', 'duration']], on='uri_id', how='left')
                df_merge2=df_merge2.fillna('None')
                summary_dict=df_merge2.to_dict('index')

                for key, value in summary_dict.items():
                    keys_to_remove=[inner_key for inner_key, inner_value in value.items() if inner_value == 'None']
                    for inner_key in keys_to_remove:
                        value.pop(inner_key)
                
                playbacks_task_summary=PlaybacksTask(summary_dict)
                if playbacks_task_summary != '':
                    dict_summary[log_key]={
                        'playbacks_task_Error': playbacks_task_summary,
                        'data_dict': summary_dict 
                    }
                    dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4) #Se transforma el diccionario a formato texto.
                    PrintLog(dict_summary_str, date_log) #Se registra en el log de eventos el resumen.
                    mail_subject='FAIL API etlaround PROD execution error' #Se establece el asunto del correo.
                    SendMail(dict_summary_str, mail_subject) #Se envia correo electronico.
                else:
                    finish=time.time() #Captura del tiempo en el instante que termina de procesar un log.
                    dict_summary[log_key]={
                        'Process_Duration': str(round((finish-beginning), 3)),
                        'Processed_Lines': str(quantity),
                        'Manifests': str(count_manifests),
                        'Segments': str(count_segments),
                        'Summary': summary_dict
                    }
                    dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4) #Se transforma el diccionario a formato texto.
                    PrintLog(dict_summary_str, date_log) #Se registra en el log de eventos el resumen.
                    shutil.move(log_path, destination_Path+log_key)
        except:
            FlagStatus("w") #Se cambia el estado de la bandera "FLAG" a false.
            error=sys.exc_info()[2] #Captura del error generado por el sistema.
            errorinfo=traceback.format_tb(error)[0] #Cartura del detalle del error.
            dict_summary['Log_Error']={
                'Error': str(sys.exc_info()[1]),
                'error_info': errorinfo
            }
            dict_summary_str=json.dumps(dict_log, sort_keys=True, indent=4) #Se transforma el diccionario a formato texto.
            PrintLog(dict_summary_str, date_log) #Se registra en el log de eventos el resumen.
            mail_subject='FAIL etlaround PROD execution error status: FALSE' #Se establece el asunto del correo.
            SendMail(dict_summary_str, mail_subject) #Se envia correo electronico.
            return dict_summary, 404

    else:
        text_print="etlaround_PROD application failure not recognized\n"
        PrintLog(text_print, date_log)

