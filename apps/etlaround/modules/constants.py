#!/usr/bin/env python
#_*_ codig: utf8 _*_

#SendMail function constants
msg_From = 'alarmas-aws@vcmedios.com.co'
msg_To = ['ingenieriavcmc@vcmedios.com.co']
smtp_Host = '10.10.130.217'

#DownloadLog function constants
downloads_Path="./apps/etlaround/s3downloads"
bucket='logs-around-prod' 
aws_Profile='pythonapps'

#FlagStatus function constants
json_path="./apps/etlaround/json/status.json"

#PrintLog function constants
log_Path="./apps/etlaround/logs"

#UriTransform fuction constants
dict_mso={
    'mso_montecable': 'mso_montecable_uy',
    'mso_express': 'mso_express_arg',
    'mso_supercanal':'mso_supercanal_arg'
}

#PlaybacksTask function constants
data_base_connect="host=10.10.130.152 dbname=aroundprod user=vodaplications password=V0D-20234pl1c4t10ns" 
db_table='playbacks'

#main functions constants
destination_Path="/mnt/ingbox/LogsCDN/"
