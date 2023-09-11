#!/usr/bin/env python
#_*_ codig: utf8 _*_
json_path="./apps/etlaround/json/vars.json"
log_Path="./apps/etlaround/logs" # Test
Downloads_Path="./apps/etlaround/s3downloads"
destination_Path="/mnt/ingbox/LogsCDN/"

data_base_connect="host=10.10.130.152 dbname=aroundprod user=vodaplications password=V0D-20234pl1c4t10ns" #use (main) (functions:extract_xml_data, Duration_Transform)
db_table='playbacks'
Bucket_logs='logs-around-prod' #Use (functions:Dowload_Logs)
Bucket_logs_old='logs-old-around-prod' #Use (functions:Dowload_Logs)
aws_profile='pythonapps' #Use (functions:Dowload_Logs, extract_xml_data)
Mail_To=['mgarcia@vcmedios.com.co'] #Use (functions:SendMail)

dict_mso={
    'mso_montecable': 'mso_montecable_uy',
    'mso_express': 'mso_express_arg',
    'mso_supercanal':'mso_supercanal_arg'
}

