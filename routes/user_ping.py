# routes/user_ping.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime, timedelta

from db.db import get_db
from db.models.user import User
from routes.user_me import get_current_user

router = APIRouter(prefix="/api/user", tags=["user"])

@router.get("/ping")
async def ping_anonymous():
    return {"status": "ok"}

@router.get("/online_users")
async def get_online_users(db: AsyncSession = Depends(get_db)):
    two_minutes_ago = datetime.utcnow() - timedelta(minutes=2)

    result = await db.execute(
        select(User).where(User.last_seen > two_minutes_ago)
    )
    users = result.scalars().all()

    return [
        {
            "uid": u.uid,
            "in_game_name": u.in_game_name,
            "verified": u.verified,
        }
        for u in users
    ]

@router.post("/ping")
async def ping_user(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    user.last_seen = datetime.utcnow()
    await db.commit()
    return {"status": "ok"}
