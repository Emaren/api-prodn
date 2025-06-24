# utils/auth_utils.py
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from db.db import get_db
from db.models.user import User

auth_scheme = HTTPBearer()

async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(auth_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    try:
        token = creds.credentials
        print(f"🔐 Bearer token received: {token[:40]}...")

        decoded = auth.verify_id_token(token)
        uid = decoded["uid"]
        email = decoded.get("email", "unknown")
        print(f"✅ Firebase decoded UID: {uid}, email: {email}")

        result = await db.execute(select(User).where(User.uid == uid))
        user = result.scalar_one_or_none()

        if not user:
            print(f"🆕 No DB user found — creating for UID: {uid}")
            new_user = User(uid=uid, email=email, in_game_name=None, is_admin=False, verified=False)
            db.add(new_user)
            await db.commit()
            await db.refresh(new_user)
            return new_user

        return user

    except Exception as e:
        print(f"❌ Firebase token validation failed: {e}")
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")
