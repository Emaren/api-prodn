# routes/user_register.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from db.models import User
from db.db import get_db
from db.schemas import UserRegisterRequest
from routes.user_me import get_current_user  # ✅ Firebase token required

import logging
logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/api/user/register")
async def register_user(
    payload: UserRegisterRequest,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user)  # ✅ Enforce Firebase
):
    try:
        # ✅ Pull UID/email from Firebase, not client
        uid = current_user["uid"]
        email = current_user["email"]

        if not payload.in_game_name or not payload.in_game_name.strip():
            raise HTTPException(
                status_code=400,
                detail={"field": "in_game_name", "error": "In-game name cannot be blank"}
            )

        # ✅ Block duplicate UID
        existing_user = await db.execute(select(User).where(User.uid == uid))
        user = existing_user.scalar_one_or_none()
        if user:
            return {"message": "User already exists"}

        # 🚫 Duplicate in-game name
        name_check = await db.execute(select(User).where(User.in_game_name == payload.in_game_name))
        name_conflict = name_check.scalar_one_or_none()
        if name_conflict:
            raise HTTPException(
                status_code=400,
                detail={"field": "in_game_name", "error": "In-game name already taken"}
            )

        # ✅ First user is admin
        count_result = await db.execute(select(func.count()).select_from(User))
        user_count = count_result.scalar()
        is_admin = user_count == 0

        new_user = User(
            uid=uid,
            email=email,
            in_game_name=payload.in_game_name,
            is_admin=is_admin,
        )

        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)

        logger.info(f"✅ Registered: {uid} ({email})")
        return {"message": "User registered", "is_admin": is_admin}

    except Exception as e:
        logger.error(f"❌ Registration failed for UID {current_user.get('uid')} - {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to register user")

