# scripts/what_db_am_i_using.py

import dotenv
dotenv.load_dotenv(".env.dev")

import sys
import asyncio
from pathlib import Path
from sqlalchemy import text  # ✅ required to execute raw SQL

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from db.db import engine

async def main():
    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT current_database(), inet_server_addr()"))
        db, host = result.fetchone()
        print(f"📡 Connected to DB: {db} at {host}")

if __name__ == "__main__":
    asyncio.run(main())
