# db/db.py
import os
import ssl
import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import config  # triggers layered .env loading

# ────────────────────────────────────────────────────────────────
# 🛠 Fix DATABASE_URL scheme if Render injects 'postgres://'
# ────────────────────────────────────────────────────────────────
raw_url = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://aoe2hd_user:aoe2hd_pass@localhost:5432/aoe2hd_db"
).strip()

if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql+asyncpg://", 1)

DATABASE_URL = raw_url

# ────────────────────────────────────────────────────────────────
# 🌐 SSL Context for Remote DB
# ────────────────────────────────────────────────────────────────
connect_args = {}
if "localhost" not in DATABASE_URL and "127.0.0.1" not in DATABASE_URL:
    connect_args["ssl"] = ssl.create_default_context()

# ────────────────────────────────────────────────────────────────
# 🚀 SQLAlchemy Async Engine + Session
# ────────────────────────────────────────────────────────────────
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args=connect_args,
    pool_pre_ping=True,            
    pool_recycle=300,            
    pool_timeout=30,                
)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ────────────────────────────────────────────────────────────────
# 📦 DB Init w/ Retry for Render cold-start race condition
# ────────────────────────────────────────────────────────────────
import asyncpg
import asyncio

async def init_db_async():
    from db.base import Base
    auto_create = os.getenv("AUTO_CREATE_TABLES", "false").lower() == "true"
    if not auto_create:
        logging.info("ℹ️ Skipping Base.metadata.create_all (AUTO_CREATE_TABLES != true).")
        return

    retries = 5
    for attempt in range(retries):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logging.info("✅ Async tables created.")
            return
        except asyncpg.exceptions.ConnectionDoesNotExistError as e:
            logging.warning(f"🔁 Retry DB connection ({attempt + 1}/{retries})...")
            await asyncio.sleep(2)
        except Exception as e:
            logging.error(f"❌ DB init failed: {e}")
            raise

# ────────────────────────────────────────────────────────────────
# 🤝 DB Session Dependency
# ────────────────────────────────────────────────────────────────
async def get_db() -> AsyncSession:
    """
    FastAPI dependency: yields an AsyncSession.
    """
    async with async_session() as session:
        yield session

# ────────────────────────────────────────────────────────────────
# (Re-added) AsyncSession provider for routes that need it
# ────────────────────────────────────────────────────────────────
async def get_async_session() -> AsyncSession:
    """
    Alternate FastAPI dependency if you explicitly want an "async_session" name.
    """
    async with async_session() as session:
        yield session

# ────────────────────────────────────────────────────────────────
# 🔍 Example Helper Functions
# ────────────────────────────────────────────────────────────────
async def get_user_by_uid(uid: str):
    from db.models import User
    async with async_session() as session:
        result = await session.execute(
            User.__table__.select().where(User.uid == uid)
        )
        return result.scalar_one_or_none()

async def get_user_by_email(email: str):
    from db.models import User
    async with async_session() as session:
        result = await session.execute(
            User.__table__.select().where(User.email == email)
        )
        return result.scalar_one_or_none()
