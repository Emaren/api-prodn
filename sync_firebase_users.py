# sync_firebase_users.py
import os
import asyncio
from datetime import datetime

import firebase_admin
from firebase_admin import auth, credentials

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from db.models import User

# 🔑 Load Firebase service account key
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

# 🔌 Postgres connection
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://aoe2user:secretpassword@localhost:5432/aoe2db"
)
engine = create_async_engine(DATABASE_URL)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# 🔁 Sync users
async def sync_users():
    async with Session() as session:
        page = auth.list_users()
        count = 0

        while page:
            for user in page.users:
                uid = user.uid
                email = user.email
                name = user.display_name or ""
                print(f"🔍 Checking {uid} - {email}")

                result = await session.execute(
                    User.__table__.select().where(User.uid == uid)
                )
                existing = result.scalar_one_or_none()

                if existing:
                    print(f"✅ Already exists: {uid}")
                else:
                    print(f"➕ Adding: {uid}")
                    session.add(User(
                        uid=uid,
                        email=email,
                        in_game_name=name,
                        verified=False,
                        created_at=datetime.utcnow()
                    ))
                    count += 1

            page = page.get_next_page()

        await session.commit()
        print(f"✅ Sync complete. {count} new users added.")

# 🚀 Run it
if __name__ == "__main__":
    asyncio.run(sync_users())
