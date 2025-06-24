# db/schemas.py

from pydantic import BaseModel

class UserRegisterRequest(BaseModel):
    uid: str
    email: str
    in_game_name: str
