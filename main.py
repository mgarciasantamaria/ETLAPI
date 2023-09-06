from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from routers import vtrapi, testapi

app = FastAPI()

app.include_router(vtrapi.vtrapp)
app.include_router(testapi.apitest)

#$ uvicorn main:app --port 5000 --ssl-keyfile=./key.pem --ssl-certfile=./cert.pem