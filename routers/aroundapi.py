from fastapi import APIRouter, HTTPException
from apps.etlaround.aroundmain import around_main
from multiprocessing import Process
import datetime

aroundapp=APIRouter()

#http://127.0.0.1:8000/toolbox?log_key=logname 
@aroundapp.post("/logs-around-prod")
async def around(log_key: str):
    init_Time = str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"))
    around_Process = Process(target=around_main(log_key))
    around_Process.start()
    print(f"{init_Time} task started for {log_key}")
    return {"log_Key": log_key, "status": "OK"}
