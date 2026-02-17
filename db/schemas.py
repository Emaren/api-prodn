# db/schemas.py

from pydantic import BaseModel

class UserRegisterRequest(BaseModel):
    uid: str | None = None
    email: str | None = None
    in_game_name: str
