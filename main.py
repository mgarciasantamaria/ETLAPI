from fastapi import FastAPI
from routers import toolboxapi, testapi, aroundapi, telecomapi

app = FastAPI()

app.include_router(testapi.apitest)
app.include_router(toolboxapi.toolboxapp)
app.include_router(aroundapi.aroundapp)
app.include_router(telecomapi.telecomapp)

#$ uvicorn main:app --port 5000 --ssl-keyfile=./key.pem --ssl-certfile=./cert.pem