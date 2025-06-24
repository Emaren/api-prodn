# routes/user_routes_async.py

from fastapi import APIRouter, HTTPException, Depends, Body, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import firebase_admin
from firebase_admin import auth, credentials

from db.db import get_db
from db.models import User, GameStats

router = APIRouter(prefix="/api/user", tags=["user"])

# Initialize Firebase if not already initialized
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

bearer_scheme = HTTPBearer(auto_error=False)

async def verify_firebase_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header")
    try:
        return auth.verify_id_token(credentials.credentials)
    except Exception:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid or expired token")

# Get current user from Firebase token
async def get_current_user(decoded_token: dict = Depends(verify_firebase_token), db: AsyncSession = Depends(get_db)) -> User:
    uid = decoded_token["uid"]
    user = await db.scalar(select(User).where(User.uid == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user

# /me endpoint (POST for initial signup)
class MeRequest(BaseModel):
    in_game_name: str | None = Field(None, description="Required on first signup")

@router.post("/me")
async def me(
    body: MeRequest = Body(...),
    db: AsyncSession = Depends(get_db),
    decoded_token: dict = Depends(verify_firebase_token),
):
    uid = decoded_token["uid"]
    email = decoded_token.get("email")

    user = await db.scalar(select(User).where(User.uid == uid))

    if not user:
        if not body.in_game_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="First-time users must supply in_game_name")

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

    return user

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

