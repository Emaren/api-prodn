# scripts/init_local_db.py

import asyncio
from db.db import init_db_async

if __name__ == "__main__":
    asyncio.run(init_db_async())
