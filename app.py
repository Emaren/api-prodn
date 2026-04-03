# app.py
from datetime import datetime
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.future import select
import json
import logging
import os
import time

from db.db import init_db_async, get_db
from db.models import GameStats

# Core + user routes are always enabled.
from routes import (
    user_me,
    user_routes_async,
    user_register,
    replay_routes_async,
    debug_routes_async,
    admin_routes_async,
    bets,
    user_ping,
    user_exists,
    chain_id,
    traffic_route,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("aoe2hdbets.api")


def _read_platform_match_id(game: GameStats) -> str | None:
    try:
        key_events = json.loads(game.key_events) if isinstance(game.key_events, str) else (game.key_events or {})
    except Exception:
        key_events = {}

    if not isinstance(key_events, dict):
        return None

    value = key_events.get("platform_match_id")
    if not isinstance(value, str):
        return None

    cleaned = value.strip()
    return cleaned or None


def _game_identity_key(game: GameStats) -> str:
    platform_match_id = _read_platform_match_id(game)
    if platform_match_id:
        return f"platform:{platform_match_id}"
    if getattr(game, "replay_hash", None):
        return f"hash:{game.replay_hash}"
    return f"id:{game.id}"


def _public_match_sort_key(game: GameStats):
    played_at = game.public_played_at() or datetime.min
    parsed_at = game.timestamp or game.created_at or datetime.min
    return (played_at, parsed_at, game.id or 0)


def _parse_allowed_origins() -> list[str]:
    raw = os.getenv("ALLOWED_ORIGINS", "").strip()
    if not raw:
        return [
            "http://localhost:3000",
            "http://localhost:3001",
            "http://localhost:3002",
            "https://aoe2-betting.vercel.app",
            "https://aoe2hdbets.com",
            "https://www.aoe2hdbets.com",
            "https://app-staging.aoe2hdbets.com",
        ]
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


LOG_REQUESTS = _env_bool("LOG_REQUESTS", default=False)
ALLOWED_ORIGINS = _parse_allowed_origins()

class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not LOG_REQUESTS:
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "%s %s -> %s (%.1fms)",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response

app = FastAPI()
app.add_middleware(LogRequestMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    await init_db_async()
    logger.info("API startup complete. Routes=%s", len(app.routes))

app.include_router(user_routes_async.router)
app.include_router(user_register.router)
app.include_router(user_me.router)
app.include_router(replay_routes_async.router)
app.include_router(debug_routes_async.router)
app.include_router(admin_routes_async.router)
app.include_router(bets.router)
app.include_router(user_ping.router)
app.include_router(user_exists.router)
app.include_router(chain_id.router)
app.include_router(traffic_route.router)

@app.get("/")
def root():
    return {"message": "AoE2 Betting Backend api-prodn is running!"}

# ✅ Add /health alias (so monitors that expect /health don't 404)
@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/api/game_stats")
async def get_game_stats(db_gen=Depends(get_db)):
    try:
        async with db_gen as db:
            result = await db.execute(
                select(GameStats)
                .where(GameStats.is_final == True)
                .order_by(GameStats.timestamp.desc())
            )
            games = result.scalars().all()

            unique_games = {}
            for game in games:
                identity_key = _game_identity_key(game)
                if identity_key not in unique_games:
                    unique_games[identity_key] = game

            ordered_games = sorted(
                unique_games.values(),
                key=_public_match_sort_key,
                reverse=True,
            )

            logging.getLogger(__name__).info(
                f"📊 Returning {len(unique_games)} unique games from DB"
            )
            return [g.to_dict() for g in ordered_games]
    except Exception as e:
        logging.error(f"❌ Failed to fetch game stats: {e}", exc_info=True)
        return []
