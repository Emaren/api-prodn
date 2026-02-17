# routes/user_routes_async.py

from __future__ import annotations

import base64
import json
import os
from typing import Any

from fastapi import APIRouter, HTTPException, Depends, Body, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from db.db import get_db
from db.models import User, GameStats

router = APIRouter(prefix="/api/user", tags=["user"])

bearer_scheme = HTTPBearer(auto_error=False)
ALLOW_UNVERIFIED_BEARER_IDENTITY = os.getenv(
    "ALLOW_UNVERIFIED_BEARER_IDENTITY",
    "false",
).strip().lower() in {"1", "true", "yes", "on"}

def _token_from_request(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None,
) -> str | None:
    if credentials and credentials.credentials:
        return credentials.credentials

    header = request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        return header.split(" ", 1)[1].strip()
    return None


def _decode_jwt_payload(token: str) -> dict[str, Any] | None:
    parts = token.split(".")
    if len(parts) != 3:
        return None

    try:
        payload_segment = parts[1]
        payload_segment += "=" * (-len(payload_segment) % 4)
        decoded_bytes = base64.urlsafe_b64decode(payload_segment)
        payload = json.loads(decoded_bytes.decode("utf-8"))
        if isinstance(payload, dict):
            return payload
        return None
    except Exception:
        return None


def _identity_from_bearer_token(token: str | None) -> dict[str, str | None] | None:
    if not ALLOW_UNVERIFIED_BEARER_IDENTITY:
        return None

    if not token:
        return None

    # JWT compatibility path: extract claims without provider-specific verification.
    payload = _decode_jwt_payload(token)
    if payload:
        uid = payload.get("uid") or payload.get("sub")
        email = payload.get("email")
        if uid:
            return {"uid": str(uid), "email": str(email) if email else None}

    # Raw-token compatibility path: treat Bearer token as UID directly.
    if token.count(".") == 0:
        return {"uid": token, "email": None}

    return None


def _normalize_uid(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_email(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    return normalized or None


def resolve_request_identity(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = None,
    fallback_uid: str | None = None,
    fallback_email: str | None = None,
) -> dict[str, str | None] | None:
    """
    Resolve identity from (in order):
    1) Bearer token (JWT claim extraction or raw UID token)
    2) x-user-uid/x-user-email headers
    3) explicit fallback values (typically request body)
    """
    token_identity = _identity_from_bearer_token(_token_from_request(request, credentials))
    if token_identity and token_identity.get("uid"):
        return token_identity

    uid = _normalize_uid(request.headers.get("x-user-uid")) or _normalize_uid(fallback_uid)
    email = _normalize_email(request.headers.get("x-user-email")) or _normalize_email(
        fallback_email
    )
    if uid:
        return {"uid": uid, "email": email}
    return None


async def verify_request_identity(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    identity = resolve_request_identity(request, credentials)
    if not identity:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing identity. Provide Bearer token, x-user-uid, or body.uid.",
        )
    return identity

# Get current user from resolved request identity
async def get_current_user(decoded_identity: dict = Depends(verify_request_identity), db: AsyncSession = Depends(get_db)) -> User:
    uid = decoded_identity["uid"]
    user = await db.scalar(select(User).where(User.uid == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user

# /me endpoint (POST for initial signup)
class MeRequest(BaseModel):
    uid: str | None = None
    email: str | None = None
    in_game_name: str | None = Field(None, description="Required on first signup")

@router.post("/me")
async def me(
    request: Request,
    body: MeRequest = Body(...),
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    identity = resolve_request_identity(
        request,
        credentials=credentials,
        fallback_uid=body.uid,
        fallback_email=body.email,
    )
    if not identity or not identity.get("uid"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing UID. Provide token, x-user-uid, or body.uid.",
        )

    uid = identity["uid"]
    email = identity.get("email")

    user = await db.scalar(select(User).where(User.uid == uid))

    if not user:
        if not body.in_game_name:
            # Keep legacy client behavior: 404 triggers /register flow on frontend.
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        first_user = await db.scalar(select(User).limit(1))
        is_admin = first_user is None

        user = User(
            uid=uid,
            email=email,
            in_game_name=body.in_game_name,
            verified=False,
            is_admin=is_admin,
        )

        db.add(user)

        try:
            await db.commit()
            await db.refresh(user)
        except IntegrityError:
            await db.rollback()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="In-game name already taken")

    return user.to_dict()

# /update_name endpoint
class UpdateNameRequest(BaseModel):
    uid: str
    in_game_name: str

@router.post("/update_name")
async def update_name(
    data: UpdateNameRequest,
    db: AsyncSession = Depends(get_db),
):
    user = await db.scalar(select(User).where(User.uid == data.uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    if user.in_game_name:
        active_match = await db.scalar(select(GameStats).filter(
            GameStats.is_final == False,
            GameStats.players.cast(str).ilike(f"%{user.in_game_name}%")
        ))
        if active_match:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot change name during an active match")

    conflict = await db.scalar(select(User).where(User.in_game_name == data.in_game_name))
    if conflict:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="That in-game name is already taken")

    user.in_game_name = data.in_game_name
    await db.commit()
    return {"message": "Name updated", "verified": user.verified}

# /update_wallet endpoint
class UpdateWalletRequest(BaseModel):
    uid: str
    wallet_address: str

@router.post("/update_wallet")
async def update_wallet(
    data: UpdateWalletRequest,
    db: AsyncSession = Depends(get_db),
):
    user = await db.scalar(select(User).where(User.uid == data.uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    user.wallet_address = data.wallet_address
    await db.commit()
    return {"message": "Wallet updated"}

# /online endpoint
@router.get("/online")
async def get_online_users(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(User).where(User.in_game_name.isnot(None)).order_by(desc(User.last_seen))
    )
    users = result.scalars().all()
    return [{"uid": u.uid, "in_game_name": u.in_game_name, "verified": u.verified} for u in users]

# Alias endpoint /online_users → /online
@router.get("/online_users")
async def get_online_users_alias(db: AsyncSession = Depends(get_db)):
    return await get_online_users(db)
