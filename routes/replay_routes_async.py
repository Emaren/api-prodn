from fastapi import APIRouter, HTTPException, Depends, Query, Header, UploadFile, File
from pydantic import BaseModel, Field
from sqlalchemy import select
from db.db import get_db
from db.models import GameStats
from datetime import datetime
import json
import logging
import os
from typing import Optional
from pathlib import Path
import tempfile

from utils.replay_parser import parse_replay_full, hash_replay_file

router = APIRouter(prefix="/api", tags=["replay"])

INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")  # set this in env
MAX_REPLAY_UPLOAD_BYTES = int(os.getenv("MAX_REPLAY_UPLOAD_BYTES", str(250 * 1024 * 1024)))


async def require_internal_key(
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key")
):
    # If you set INTERNAL_API_KEY, enforce it. If not set, allow (dev convenience).
    if INTERNAL_API_KEY and x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


class ParseReplayRequest(BaseModel):
    replay_file: str
    replay_hash: str
    parse_iteration: int = 0
    is_final: bool = False
    game_version: str | None = None
    map: dict | None = None
    map_name: str = "Unknown"
    map_size: str = "Unknown"
    game_type: str | None = None
    duration: int = 0
    game_duration: int | None = None
    winner: str = "Unknown"
    players: list = Field(default_factory=list)  # ✅ FIX: no shared mutable default
    played_on: str | None = None
    parse_source: str | None = None
    parse_reason: str | None = None
    original_filename: str | None = None


def _safe_iso_datetime(value: str | None):
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _map_payload(data: ParseReplayRequest):
    map_payload = data.map if isinstance(data.map, dict) else {}
    map_name = data.map_name
    map_size = data.map_size

    if map_name == "Unknown":
        map_name = map_payload.get("name", "Unknown")
    if map_size == "Unknown":
        map_size = map_payload.get("size", "Unknown")

    return {"name": map_name, "size": map_size}


@router.post("/parse_replay")
async def parse_new_replay(
    data: ParseReplayRequest,
    db_gen=Depends(get_db),
    _: bool = Depends(require_internal_key),
    user_uid: str = Header(default="system", alias="x-user-uid"),
    mode: str = Query(default=None),
):
    async with db_gen as db:
        raw_duration = data.duration or data.game_duration or 0
        duration = int(raw_duration) if isinstance(raw_duration, (int, float)) else 0
        map_payload = _map_payload(data)
        played_on = _safe_iso_datetime(data.played_on)

        if mode == "final" and data.is_final:
            existing = await db.execute(
                select(GameStats).where(
                    GameStats.replay_hash == data.replay_hash,
                    GameStats.is_final.is_(True),
                )
            )
            if existing.scalars().first():
                logging.info(f"🛡️ Skipped duplicate final replay: {data.replay_hash}")
                return {"message": "Replay already parsed as final. Skipped."}

        game = GameStats(
            user_uid=user_uid,
            replay_file=data.replay_file,
            replay_hash=data.replay_hash,
            game_version=data.game_version,
            map=json.dumps(map_payload),
            game_type=data.game_type,
            duration=duration,
            game_duration=duration,
            winner=data.winner,
            players=json.dumps(data.players),
            parse_iteration=data.parse_iteration,
            is_final=data.is_final,
            parse_source=data.parse_source or "json_parse",
            parse_reason=data.parse_reason or "json_submission",
            original_filename=data.original_filename,
            played_on=played_on,
        )
        db.add(game)
        await db.commit()

        return {"message": f"Replay stored (iteration {data.parse_iteration})"}


@router.post("/replay/upload")
async def upload_replay_file(
    file: UploadFile = File(...),
    db_gen=Depends(get_db),
    user_uid: str = Header(default="system", alias="x-user-uid"),
):
    original_name = file.filename or "replay.aoe2record"
    suffix = Path(original_name).suffix.lower()
    if suffix not in {".aoe2record", ".aoe2mpgame", ".mgz", ".mgx", ".mgl"}:
        raise HTTPException(status_code=400, detail="Unsupported replay file type")

    fd, temp_path = tempfile.mkstemp(prefix="aoe2-replay-", suffix=suffix)
    os.close(fd)
    written = 0

    try:
        with open(temp_path, "wb") as handle:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_REPLAY_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Replay file too large")
                handle.write(chunk)
    finally:
        await file.close()

    try:
        parsed = await parse_replay_full(temp_path)
        if not parsed:
            raise HTTPException(status_code=422, detail="Failed to parse replay file")

        replay_hash = await hash_replay_file(temp_path)
        if not replay_hash:
            raise HTTPException(status_code=500, detail="Failed to hash replay file")

        map_info = parsed.get("map")
        map_payload = {
            "name": map_info.get("name", "Unknown") if isinstance(map_info, dict) else "Unknown",
            "size": map_info.get("size", "Unknown") if isinstance(map_info, dict) else "Unknown",
        }
        players = parsed.get("players") if isinstance(parsed.get("players"), list) else []
        winner = parsed.get("winner") or "Unknown"
        raw_duration = parsed.get("duration") or parsed.get("game_duration") or 0
        duration = int(raw_duration) if isinstance(raw_duration, (int, float)) else 0
        played_on = _safe_iso_datetime(parsed.get("played_on"))

        async with db_gen as db:
            existing = await db.execute(
                select(GameStats).where(
                    GameStats.replay_hash == replay_hash,
                    GameStats.is_final.is_(True),
                )
            )
            if existing.scalars().first():
                return {
                    "message": "Replay already parsed as final. Skipped.",
                    "replay_hash": replay_hash,
                }

            game = GameStats(
                user_uid=user_uid or "system",
                replay_file=original_name,
                replay_hash=replay_hash,
                game_version=parsed.get("game_version"),
                map=json.dumps(map_payload),
                game_type=parsed.get("game_type"),
                duration=duration,
                game_duration=duration,
                winner=winner,
                players=json.dumps(players),
                parse_iteration=1,
                is_final=True,
                parse_source="file_upload",
                parse_reason="watcher_or_browser",
                original_filename=original_name,
                played_on=played_on,
            )
            db.add(game)
            await db.commit()

        return {
            "message": "Replay parsed and stored",
            "replay_hash": replay_hash,
            "winner": winner,
            "players_count": len(players),
        }
    finally:
        try:
            os.remove(temp_path)
        except FileNotFoundError:
            pass


@router.get("/health")
async def health_check():
    return {"status": "ok"}
