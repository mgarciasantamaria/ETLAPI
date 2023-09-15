#!/usr/bin/env python
#_*_ codig: utf8 _*_

#SendMail function constants
msg_From = 'alarmas-aws@vcmedios.com.co'
msg_To = ['ingenieriavcmc@vcmedios.com.co']
smtp_Host = '10.10.130.217'

#DownloadLog function constants
downloads_Path="./apps/etltoolbox/S3Download"
aws_Profile='pythonapps'
bucket='dbeventlogs'

#PrintLog function constants
summary_log_Path="./apps/etltoolbox/Logs" # Test

#FlagStatus function constants
json_path="./apps/etltoolbox/json/status.json"

#main Constants
database_Connect="host=10.10.130.152 dbname=toolboxprod user=vodaplications password=V0D-20234pl1c4t10ns"
profile={   #Use (main)
    'mpd': [4, 'Dash', 'm4s'],
    'vmxmpd': [4, 'Dash', 'm4s'],
    'm3u8' : [6, 'Hls', 'ts'],
    'vmxm3u8' : [6, 'Hls', 'ts'],
    'ism' : [3, 'SmoothStreams']
    }
destination_Path="/mnt/ingbox/LogsCDN/"









