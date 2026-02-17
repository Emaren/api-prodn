# routes/user_register.py
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from db.db import get_db
from db.models import User
from db.schemas import UserRegisterRequest
from routes.user_routes_async import bearer_scheme, resolve_request_identity

import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/api/user/register")
async def register_user(
    request: Request,
    payload: UserRegisterRequest,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    identity = resolve_request_identity(
        request,
        credentials=credentials,
        fallback_uid=payload.uid,
        fallback_email=payload.email,
    )
    if not identity or not identity.get("uid"):
        raise HTTPException(status_code=400, detail="Missing uid for registration")

    uid = identity["uid"]
    email = identity.get("email")

    if not payload.in_game_name or not payload.in_game_name.strip():
        raise HTTPException(
            status_code=400,
            detail={"field": "in_game_name", "error": "In-game name cannot be blank"},
        )

    existing_user = await db.execute(select(User).where(User.uid == uid))
    user = existing_user.scalar_one_or_none()
    if user:
        return {"message": "User already exists", "is_admin": user.is_admin}

    name_check = await db.execute(select(User).where(User.in_game_name == payload.in_game_name))
    name_conflict = name_check.scalar_one_or_none()
    if name_conflict:
        raise HTTPException(
            status_code=400,
            detail={"field": "in_game_name", "error": "In-game name already taken"},
        )

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

    logger.info("✅ Registered: %s (%s)", uid, email)
    return {"message": "User registered", "is_admin": is_admin}
