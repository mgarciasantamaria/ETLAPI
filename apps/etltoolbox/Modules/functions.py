#!/usr/bin/env python
#_*_ codig: utf8 _*_
import datetime, json, boto3, smtplib, datetime
from email.message import EmailMessage
from boto3.s3.transfer import S3Transfer, TransferConfig
from apps.etltoolbox.Modules.constants import *

def SendMail(text, mail_subject):
    msg = EmailMessage()
    msg.set_content(text)
    msg['Subject'] = mail_subject
    msg['From'] = msg_From
    msg['To'] = msg_To
    conexion = smtplib.SMTP(host=smtp_Host, port=25)
    conexion.ehlo()
    conexion.send_message(msg)
    conexion.quit()
    return

def DownloadLog(log_key):
    try:
        log_Path=f'{downloads_Path}/{log_key}'
        aws_session=boto3.Session(profile_name=aws_Profile)
        s3_client=aws_session.client('s3')
        S3Transfer(s3_client, TransferConfig(max_bandwidth=5000000)).download_file(bucket,log_key,log_Path)
        return log_Path
    except Exception as e:
        return f"Error: {e}"


def PrintLog(OPTION, TEXT, DATE_LOG):
    log_file=open(f"{summary_log_Path}/{DATE_LOG}_log.txt", OPTION)
    log_file.write(f"{str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))}\t{TEXT}\n")
    log_file.close()

def FlagStatus(OPTION):
    with open(json_path, "r") as json_file:
            json_data=json.load(json_file)
    if OPTION=="r":
        return json_data["FLAG"]
    elif OPTION=="w":
        json_data["FLAG"]=False
        with open(json_path, "w") as json_file:
            json.dump(json_data, json_file)
    else:
        pass