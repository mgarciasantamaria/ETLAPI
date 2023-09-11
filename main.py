from fastapi import FastAPI
from routers import toolboxapi, testapi, aroundapi

app = FastAPI()

app.include_router(testapi.apitest)
app.include_router(toolboxapi.toolboxapp)
app.include_router(aroundapi.aroundapp)

#$ uvicorn main:app --port 5000 --ssl-keyfile=./key.pem --ssl-certfile=./cert.pem