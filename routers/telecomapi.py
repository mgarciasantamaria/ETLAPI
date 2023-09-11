from fastapi import APIRouter, HTTPException
from apps.etltelecom.telecommain import telecom_main

telecomapp=APIRouter()


#http://127.0.0.1:8000/toolbox?log_key=logname 
@telecomapp.post("/logs-telecom-arg")
async def telecom(log_key: str):
    response, status = telecom_main(log_key)
    if status == 200:
        return response
    elif status == 404:
        raise HTTPException(status_code=status, detail=response)
    
