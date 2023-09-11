from fastapi import APIRouter, HTTPException
from apps.etlaround.aroundmain import around_main

aroundapp=APIRouter()


#http://127.0.0.1:8000/toolbox?log_key=logname 
@aroundapp.post("/logs-around-prod")
async def around(log_key: str):
    response, status = around_main(log_key)
    if status == 200:
        return response
    elif status == 404:
        raise HTTPException(status_code=status, detail=response)
    
