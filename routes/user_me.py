# routes/user_me.py

from fastapi import APIRouter, Depends

from db.models import User
from routes.user_routes_async import get_current_user

router = APIRouter(prefix="/api/user", tags=["user"])


@router.get("/me")
async def get_user_me(user: User = Depends(get_current_user)):
    return user.to_dict()
