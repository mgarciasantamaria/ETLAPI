from fastapi import APIRouter, HTTPException
from apps.etltoolbox.toolboxmain import toolbox_main

toolboxapp=APIRouter()


#http://127.0.0.1:8000/toolbox?log_key=logname 
@toolboxapp.post("/dbeventlogs")
async def toolbox(log_key: str):
    response, status = toolbox_main(log_key)
    if status == 200:
        return response
    elif status == 404:
        raise HTTPException(status_code=status, detail=response)
    
