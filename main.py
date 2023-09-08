from fastapi import FastAPI
from routers import vtrapi, toolboxapi

app = FastAPI()

app.include_router(vtrapi.vtrapp)
app.include_router(toolboxapi.toolboxapp)

#$ uvicorn main:app --port 5000 --ssl-keyfile=./key.pem --ssl-certfile=./cert.pem