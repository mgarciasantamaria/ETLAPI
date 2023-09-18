from fastapi import APIRouter
from apps.etltoolbox.toolboxmain import toolbox_main
import datetime, multiprocessing

toolboxapp=APIRouter()

#http://127.0.0.1:8000/toolbox?log_key=logname 
@toolboxapp.post("/dbeventlogs")
def toolbox(log_key: str):
    p1 = multiprocessing.Process(target=toolbox_main, args=[log_key])
    p1.start()
    init_Time = str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"))
    print(f"{init_Time} task started for {log_key}")
    return {"log_Key": log_key, "status": "OK"}