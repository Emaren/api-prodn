# app.py
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.future import select
import logging
import os

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

print(f"DATABASE_URL: {os.getenv('DATABASE_URL')}")

class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        print(f"📩 Incoming Request: {request.method} {request.url}")

        # Prefer the new headers you’re actually using now
        if "x-api-key" in request.headers:
            print(f"🔑 x-api-key present (len={len(request.headers['x-api-key'])})")
        else:
            print("⚠️ No x-api-key header present.")

        if "x-user-uid" in request.headers:
            print(f"👤 x-user-uid: {request.headers['x-user-uid']}")

        # Keep Authorization preview if you still sometimes send it
        if "authorization" in request.headers:
            token_preview = request.headers["authorization"][:40]
            print(f"🔒 Authorization (first 40 chars): {token_preview}...")

        return await call_next(request)

app = FastAPI()
app.add_middleware(LogRequestMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "https://aoe2-betting.vercel.app",
        "https://aoe2hdbets.com",
        "https://www.aoe2hdbets.com",
        "https://app-staging.aoe2hdbets.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    await init_db_async()

    for route in app.routes:
        print(f"✅ {route.path}")

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
                if game.replay_hash not in unique_games:
                    unique_games[game.replay_hash] = game

            logging.getLogger(__name__).info(
                f"📊 Returning {len(unique_games)} unique games from DB"
            )
            return [g.to_dict() for g in unique_games.values()]
    except Exception as e:
        logging.error(f"❌ Failed to fetch game stats: {e}", exc_info=True)
        return []
