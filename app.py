# app.py
from datetime import datetime, timezone
from fastapi import FastAPI, Depends, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from sqlalchemy.future import select
import json
import logging
import os
import time

from db.db import init_db_async, get_db
from db.models import GameStats
from utils.game_stats_public_cache import (
    read_game_stats_cache_generation,
    write_game_stats_cache,
)

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

_GAME_STATS_CACHE_TTL_SECONDS = float(os.getenv("AOE2_GAME_STATS_CACHE_TTL_SECONDS", "90"))



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


def _public_sort_datetime(value: datetime | None) -> datetime:
    # Internal ordering only. Legacy source-local values remain naive. Absolute
    # values are normalized to UTC and stripped of tzinfo solely so Python never
    # compares aware and naive datetime objects. Public provenance is unchanged.
    if value is None:
        return datetime.min
    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _public_match_sort_key(game: GameStats):
    played_at = _public_sort_datetime(game.public_played_at())
    parsed_at = _public_sort_datetime(game.timestamp or game.created_at)
    return (played_at, parsed_at, game.id or 0)


def _parse_allowed_origins() -> list[str]:
    raw = os.getenv("ALLOWED_ORIGINS", "").strip()
    if not raw:
        return [
            "http://localhost:3000",
            "http://localhost:3001",
            "http://localhost:3002",
            "https://aoe2-betting.vercel.app",
            "https://aoe2war.com",
            "https://www.aoe2war.com",
            "https://app-staging.aoe2war.com",
        ]
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


LOG_REQUESTS = _env_bool("LOG_REQUESTS", default=False)
ALLOWED_ORIGINS = _parse_allowed_origins()

_REPLAY_UPLOAD_MAX_INFLIGHT = max(
    0,
    int(os.getenv("AOE2_REPLAY_UPLOAD_MAX_INFLIGHT", "0")),
)


class ReplayUploadAdmissionMiddleware:
    """Fail fast before multipart parsing when replay ingestion is saturated."""

    def __init__(self, app):
        self.app = app
        self.inflight = 0

    async def __call__(self, scope, receive, send):
        is_replay_upload = (
            scope.get("type") == "http"
            and scope.get("method") == "POST"
            and scope.get("path") == "/api/replay/upload"
        )

        if not is_replay_upload:
            await self.app(scope, receive, send)
            return

        if (
            _REPLAY_UPLOAD_MAX_INFLIGHT > 0
            and self.inflight >= _REPLAY_UPLOAD_MAX_INFLIGHT
        ):
            response = JSONResponse(
                {
                    "detail": "Replay ingestion busy; retry shortly.",
                    "retryable": True,
                },
                status_code=429,
                headers={
                    "Retry-After": "5",
                    "Connection": "close",
                },
            )
            await response(scope, receive, send)
            return

        self.inflight += 1
        try:
            await self.app(scope, receive, send)
        finally:
            self.inflight -= 1


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
app.add_middleware(ReplayUploadAdmissionMiddleware)

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
async def get_game_stats(
    limit: int | None = Query(default=None, ge=1, le=5000),
    db_gen=Depends(get_db),
):
    cache_key = f"limit:{limit or 'all'}"
    cache_lookup = read_game_stats_cache_generation(cache_key)

    if cache_lookup.payload is not None:
        return cache_lookup.payload

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

            selected_games = ordered_games[:limit] if limit else ordered_games
            payload = [g.to_dict() for g in selected_games]

            cache_written = write_game_stats_cache(
                cache_key,
                payload,
                _GAME_STATS_CACHE_TTL_SECONDS,
                expected_generation=cache_lookup.generation,
            )

            if not cache_written:
                logging.getLogger(__name__).info(
                    "Skipped stale public game-stats cache refill key=%s generation=%s",
                    cache_key,
                    cache_lookup.generation,
                )

            logging.getLogger(__name__).info(
                f"📊 Returning {len(payload)} of {len(unique_games)} unique games from DB"
            )
            return payload
    except Exception as e:
        logging.error(f"❌ Failed to fetch game stats: {e}", exc_info=True)
        return []
