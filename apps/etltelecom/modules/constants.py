#!/usr/bin/env python
#_*_ codig: utf8 _*_

#SendMail function constants
msg_From = 'alarmas-aws@vcmedios.com.co'
msg_To = ['ingenieriavcmc@vcmedios.com.co', 'cparada@vcmedios.con.co']
smtp_Host = '10.10.130.217'

#ExtractXmlData function constants
buckets={   #Diccionario con keys y values que identifican el canal y el bucket segun contentid.
    "11": ["aenla-in-toolbox", "A&E"],
    "21": ["aenla-in-toolbox", "History"],
    "31": ["aenla-in-toolbox", "Lifetime"],
    "41": ["aenla-in-toolbox", "History2"],
    "51": ["spe-in-toolbox", "AXN"],
    "52": ["spe-in-toolbox", "SONY-MOVIES"],
    "61": ["spe-in-toolbox", "SONY"],
    "62": ["spe-in-toolbox", "SONY-AXN"],
}
channels_Id={
    "HISTORY" : "21",
    "LIFETIME" : "31",
    "HISTORY2" : "41",
    "AXN" : "51",
    "SONYAXN" : "62"
}

#DownloadLog functions constants
Downloads_Path="./apps/etltelecom/s3download"
Bucket_logs='logs-telecom-arg' #Use (functions:Dowload_Logs)

#extractXmlData and DownloadLog functions constants
aws_Profile='pythonapps'

#extractXmlData and main functions constants
database_Connect="host=10.10.130.152 dbname=toolboxprod user=vodaplications password=V0D-20234pl1c4t10ns"

#PrintLog function constants
log_Path="./apps/etltelecom/logs" # Ruta del folder donde se alojan los archivos logs