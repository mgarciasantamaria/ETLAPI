from fastapi import APIRouter
from pydantic import BaseModel
from apps.vtrapp import *

vtrapp=APIRouter()


#http://127.0.0.1:8000/vtr?log_name=logname 
@vtrapp.post("/vtr/")
async def vtr(log_name: str):
    return mainvtr(log_name)






