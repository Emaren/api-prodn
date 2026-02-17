# routes/user_routes_async.py

from __future__ import annotations

import json
import os
from typing import Any

from fastapi import APIRouter, HTTPException, Depends, Body, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

try:
    import firebase_admin
    from firebase_admin import auth as fb_auth, credentials as fb_credentials
except Exception:
    firebase_admin = None
    fb_auth = None
    fb_credentials = None

from db.db import get_db
from db.models import User, GameStats

router = APIRouter(prefix="/api/user", tags=["user"])

def _initialize_firebase_if_possible() -> bool:
    """Best-effort Firebase init. Safe no-op when package/creds are absent."""
    if not firebase_admin or not fb_auth or not fb_credentials:
        return False
    if firebase_admin._apps:
        return True

    cert_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "serviceAccountKey.json")
    raw_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")

    try:
        if raw_json:
            cred = fb_credentials.Certificate(json.loads(raw_json))
        else:
            if not os.path.exists(cert_path):
                return False
            cred = fb_credentials.Certificate(cert_path)
        firebase_admin.initialize_app(cred)
        return True
    except Exception:
        return False


FIREBASE_ENABLED = _initialize_firebase_if_possible()

bearer_scheme = HTTPBearer(auto_error=False)

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


def _decode_firebase_token(token: str | None) -> dict[str, Any] | None:
    if not token or not FIREBASE_ENABLED or not fb_auth:
        return None
    try:
        return fb_auth.verify_id_token(token)
    except Exception:
        return None


def resolve_request_identity(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = None,
    fallback_uid: str | None = None,
    fallback_email: str | None = None,
) -> dict[str, str | None] | None:
    """
    Resolve identity from (in order):
    1) Firebase Bearer token (if Firebase is available)
    2) x-user-uid/x-user-email headers
    3) explicit fallback values (typically request body)
    """
    decoded = _decode_firebase_token(_token_from_request(request, credentials))
    if decoded and decoded.get("uid"):
        return {"uid": decoded["uid"], "email": decoded.get("email")}

    uid = request.headers.get("x-user-uid") or fallback_uid
    email = request.headers.get("x-user-email") or fallback_email
    if uid:
        return {"uid": uid, "email": email}
    return None


async def verify_firebase_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    identity = resolve_request_identity(request, credentials)
    if not identity:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing identity. Provide Firebase Bearer token or x-user-uid.",
        )
    return identity

# Get current user from Firebase token
async def get_current_user(decoded_token: dict = Depends(verify_firebase_token), db: AsyncSession = Depends(get_db)) -> User:
    uid = decoded_token["uid"]
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
