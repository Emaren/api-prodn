#!/usr/bin/env python3
"""
Promote or demote a user admin flag in the production DB.

Examples:
  python scripts/set_admin.py --list
  python scripts/set_admin.py --email user@example.com
  python scripts/set_admin.py --uid u_123...
  python scripts/set_admin.py --latest
  python scripts/set_admin.py --email user@example.com --unset
"""

import argparse
import asyncio
import os
from pathlib import Path
from typing import Optional

import asyncpg
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    # Prefer production env, then fallback to generic env.
    load_dotenv(ROOT / ".env.production")
    load_dotenv(ROOT / ".env", override=False)


def normalize_db_url(raw: str) -> str:
    if raw.startswith("postgresql+asyncpg://"):
        return raw.replace("postgresql+asyncpg://", "postgresql://", 1)
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql://", 1)
    return raw


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Set user admin flag")
    parser.add_argument("--uid", help="Target user uid")
    parser.add_argument("--email", help="Target user email")
    parser.add_argument("--name", help="Target in-game name")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Target the most recently created user",
    )
    parser.add_argument(
        "--unset",
        action="store_true",
        help="Demote user instead of promote",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List recent users and exit",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Row limit for --list (default: 20)",
    )
    args = parser.parse_args()

    selectors = [bool(args.uid), bool(args.email), bool(args.name), bool(args.latest)]
    if args.list:
        return args
    if sum(selectors) != 1:
        parser.error("Choose exactly one selector: --uid, --email, --name, or --latest")
    return args


async def fetch_target_uid(conn: asyncpg.Connection, args: argparse.Namespace) -> Optional[str]:
    if args.latest:
        row = await conn.fetchrow(
            """
            select uid
            from users
            order by created_at desc nulls last, id desc
            limit 1
            """
        )
        return row["uid"] if row else None

    if args.uid:
        row = await conn.fetchrow("select uid from users where uid = $1", args.uid.strip())
        return row["uid"] if row else None

    if args.email:
        row = await conn.fetchrow(
            "select uid from users where lower(email) = lower($1)",
            args.email.strip(),
        )
        return row["uid"] if row else None

    row = await conn.fetchrow(
        "select uid from users where lower(in_game_name) = lower($1)",
        args.name.strip(),
    )
    return row["uid"] if row else None


async def list_users(conn: asyncpg.Connection, limit: int) -> None:
    rows = await conn.fetch(
        """
        select uid, email, in_game_name, is_admin, created_at
        from users
        order by created_at desc nulls last, id desc
        limit $1
        """,
        limit,
    )
    if not rows:
        print("No users found.")
        return

    for row in rows:
        print(
            f"uid={row['uid']} "
            f"email={row['email']} "
            f"name={row['in_game_name']} "
            f"is_admin={row['is_admin']} "
            f"created_at={row['created_at']}"
        )


async def run() -> int:
    args = parse_args()
    load_env()

    raw_db_url = os.getenv("DATABASE_URL")
    if not raw_db_url:
        print("DATABASE_URL is not set.")
        return 1

    db_url = normalize_db_url(raw_db_url.strip())
    conn = await asyncpg.connect(db_url)
    try:
        if args.list:
            await list_users(conn, args.limit)
            return 0

        target_uid = await fetch_target_uid(conn, args)
        if not target_uid:
            print("Target user not found.")
            return 1

        desired_admin = not args.unset
        row = await conn.fetchrow(
            """
            update users
            set is_admin = $1
            where uid = $2
            returning uid, email, in_game_name, is_admin
            """,
            desired_admin,
            target_uid,
        )
        if not row:
            print("Update failed.")
            return 1

        print(
            f"Updated user: uid={row['uid']} "
            f"email={row['email']} "
            f"name={row['in_game_name']} "
            f"is_admin={row['is_admin']}"
        )
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
