from fastapi import FastAPI
from routers import toolboxapi, testapi

app = FastAPI()

app.include_router(testapi.apitest)
app.include_router(toolboxapi.toolboxapp)

#$ uvicorn main:app --port 5000 --ssl-keyfile=./key.pem --ssl-certfile=./cert.pem