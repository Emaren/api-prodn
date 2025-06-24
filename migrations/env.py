import sys
import os
import asyncio
import json
import pathlib
from logging.config import fileConfig

# Add project root to sys.path so 'db' can be imported
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from alembic import context
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy import create_engine
from dotenv import load_dotenv

from db.models import Base  # now resolvable after sys.path fix

# Load .env if it exists
dotenv_path = pathlib.Path(__file__).parent.parent / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path)

# Resolve database URL from Alembic CLI args, config.json, env, or fallback
cli_args = context.get_x_argument(as_dictionary=True)
DB_URL = (
    context.get_x_argument(as_dictionary=True).get("db_url")
    or os.getenv("DATABASE_URL")
)

if not DB_URL:
    raise RuntimeError("❌ DATABASE_URL is not set. Failing migration.")

# Alembic config object
config = context.config
fileConfig(config.config_file_name)
target_metadata = Base.metadata

def run_migrations_offline():
    context.configure(
        url=DB_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def do_run_migrations(sync_connection):
    context.configure(
        connection=sync_connection,
        target_metadata=target_metadata,
    )
    context.run_migrations()

async def run_migrations_online():
    # Set the URL for Alembic’s config before doing anything
    config.set_main_option("sqlalchemy.url", DB_URL)

    if DB_URL.startswith("postgresql+asyncpg"):
        connectable: AsyncEngine = create_async_engine(DB_URL, future=True)
        async with connectable.begin() as conn:
            await conn.run_sync(do_run_migrations)
    else:
        connectable = create_engine(DB_URL, future=True)
        with connectable.begin() as conn:
            do_run_migrations(conn)


def run_async():
    asyncio.run(run_migrations_online())

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_async()
