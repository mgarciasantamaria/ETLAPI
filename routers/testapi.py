from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

apitest=APIRouter()

class User(BaseModel):
    id: int
    name: str

users_list=[User(id=1, name="pepito"),
    User(id=2, name="pepita")
]

#http://127.0.0.1:8000/userquery?id=num donde num es un numero entero
@apitest.get("/userquery/")
async def user(id: int):
    return search_user(id)

def search_user(id: int):
    users = filter(lambda user: user.id == id, users_list)
    try:
        return list(users)[0]
    except:
        raise HTTPException(status_code=404, detail="Not found")