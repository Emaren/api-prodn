# routes/user_exists.py
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.db import get_db
from db.models import User

router = APIRouter(prefix="/api/user", tags=["user"])

@router.get("/exists")
async def user_exists(name: str, db_gen=Depends(get_db)):
    """
    Lightweight check: does an in-game name already exist?
    Returns {'exists': true|false}
    """
    async with db_gen as db:
        res = await db.execute(select(User).where(User.in_game_name == name))
        return {"exists": res.scalar_one_or_none() is not None}
