#!/usr/bin/env python
#_*_ codig: utf8 _*_
json_path="./apps/etltoolbox/json/status.json"
summary_log_Path="./apps/etltoolbox/Logs" # Test
Downloads_Path="./apps/etltoolbox/S3Download"
destination_Path="/mnt/ingbox/LogsCDN/"

data_base_connect="host=10.10.130.152 dbname=toolboxprod user=vodaplications password=V0D-20234pl1c4t10ns" #use (main) (functions:extract_xml_data, Duration_Transform)
profile={   #Use (main)
    'mpd': [4, 'Dash', 'm4s'],
    'vmxmpd': [4, 'Dash', 'm4s'],
    'm3u8' : [6, 'Hls', 'ts'],
    'vmxm3u8' : [6, 'Hls', 'ts'],
    'ism' : [3, 'SmoothStreams']
    }
Bucket_logs='dbeventlogsold' #Use (functions:Dowload_Logs)
Bucket_logs_old='dbeventlogsold' #Use (functions:Dowload_Logs)
aws_profile='pythonapps' #Use (functions:Dowload_Logs, extract_xml_data)
Mail_To=['mgarcia@vcmedios.com.co'] #Use (functions:SendMail)


