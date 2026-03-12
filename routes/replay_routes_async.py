from fastapi import APIRouter, HTTPException, Depends, Query, Header, UploadFile, File
from pydantic import BaseModel, Field
from sqlalchemy import select
from db.db import get_db
from datetime import datetime
import logging
import os
from typing import Optional, Tuple
from pathlib import Path
import tempfile
import hashlib
import hmac
import base64
import re
from sqlalchemy import update

# Prefer full model set if present (User/ApiKey added in recent migration)
try:
    from db.models import GameStats, User, ApiKey
except Exception:  # pragma: no cover
    from db.models import GameStats  # type: ignore
    User = None  # type: ignore
    ApiKey = None  # type: ignore

from utils.replay_parser import parse_replay_full, hash_replay_file

router = APIRouter(prefix="/api", tags=["replay"])

INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")  # optional; if set, enforces auth for uploads
MAX_REPLAY_UPLOAD_BYTES = int(os.getenv("MAX_REPLAY_UPLOAD_BYTES", str(250 * 1024 * 1024)))
SUPERSEDED_PARSE_REASON = "superseded_by_later_upload"

WATCHER_KEY_RE = re.compile(r"^wolo_([a-f0-9]{12})_(.+)$", re.IGNORECASE)


async def require_internal_key(
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key")
):
    # Internal-only routes still require the internal key if configured.
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
    players: list = Field(default_factory=list)
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


def _norm_name(s: str) -> str:
    return " ".join((s or "").strip().split()).lower()


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def _verify_pbkdf2(secret: str, stored: str) -> bool:
    """
    stored format: pbkdf2_sha256$<iters>$<salt_b64url>$<dk_b64url>
    """
    try:
        algo, iters_s, salt_s, dk_s = stored.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iters = int(iters_s)
        salt = _b64url_decode(salt_s)
        expected = _b64url_decode(dk_s)
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            secret.encode("utf-8"),
            salt,
            iters,
            dklen=len(expected),
        )
        return hmac.compare_digest(derived, expected)
    except Exception:
        return False


def _verify_key_hash(x_api_key: str, stored_hash: str) -> bool:
    """
    Supports two storage formats:
      - pbkdf2_sha256$... (legacy/secure)
      - 64-char sha256 hex of the full key (current simple mode)
    """
    if not stored_hash:
        return False
    if stored_hash.startswith("pbkdf2_sha256$"):
        # For PBKDF2 format, secret = entire key or just the secret?
        # Our PBKDF2 variant (if used) hashes the "secret" portion; to be safe, verify both.
        m = WATCHER_KEY_RE.match(x_api_key)
        if m and _verify_pbkdf2(m.group(2), stored_hash):
            return True
        return _verify_pbkdf2(x_api_key, stored_hash)

    # Otherwise treat as sha256 hex of full api key
    if len(stored_hash) == 64 and all(c in "0123456789abcdef" for c in stored_hash.lower()):
        return hmac.compare_digest(_sha256_hex(x_api_key), stored_hash.lower())

    return False


async def _resolve_upload_identity(db, x_api_key: Optional[str], claimed_uid: str) -> Tuple[str, str]:
    """
    Returns (uploader_uid, mode) where mode is:
      - "internal" when INTERNAL_API_KEY is used
      - "watcher" when a watcher key binds to a user
      - "dev" when INTERNAL_API_KEY is not set and no x-api-key provided
    """
    # 1) Internal trusted key path
    if INTERNAL_API_KEY and x_api_key == INTERNAL_API_KEY:
        return (claimed_uid or "system"), "internal"

    # 2) If internal key configured, require either internal or watcher key
    if INTERNAL_API_KEY and not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API key")

    # 3) Dev convenience: allow missing key if no internal key configured
    if not x_api_key:
        return (claimed_uid or "system"), "dev"

    # 4) Watcher key path: wolo_<prefix>_<secret>
    if ApiKey is None or User is None:
        raise HTTPException(status_code=500, detail="Watcher key support not available (models not loaded)")

    m = WATCHER_KEY_RE.match(x_api_key.strip())
    if not m:
        raise HTTPException(status_code=401, detail="Invalid API key")

    prefix = m.group(1).lower()

    res = await db.execute(
        select(ApiKey).where(
            ApiKey.key_prefix == prefix,
            ApiKey.revoked_at.is_(None),
            ApiKey.kind == "watcher",
        )
    )
    api_key = res.scalars().first()
    if not api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")

    if not _verify_key_hash(x_api_key.strip(), api_key.key_hash):
        raise HTTPException(status_code=401, detail="Invalid API key")

    ures = await db.execute(select(User).where(User.id == api_key.user_id))
    user = ures.scalars().first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Touch last_used_at
    api_key.last_used_at = datetime.utcnow()

    return user.uid, "watcher"


async def _maybe_verify_user_from_replay(db, uploader_uid: str, players: list, claimed_name: Optional[str], method: str):
    """
    If user has a claimed in_game_name (or header provided) and it appears in parsed replay player list,
    mark verified + lock name.
    """
    if User is None:
        return
    if not uploader_uid or uploader_uid == "system":
        return
    if not isinstance(players, list) or not players:
        return

    res = await db.execute(select(User).where(User.uid == uploader_uid))
    user = res.scalars().first()
    if not user:
        return

    claim = (claimed_name or user.in_game_name or "").strip()
    if not claim:
        return

    claim_norm = _norm_name(claim)
    matched = None

    for p in players:
        if not isinstance(p, dict):
            continue
        nm = str(p.get("name", "")).strip()
        if nm and _norm_name(nm) == claim_norm:
            matched = nm
            break

    if not matched:
        return

    # If not locked, normalize stored name to replay spelling
    if not getattr(user, "lock_name", False):
        user.in_game_name = matched

    user.verified = True
    user.lock_name = True
    user.verification_level = 2
    user.verification_method = method
    user.verified_at = datetime.utcnow()


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
            map=map_payload,
            game_type=data.game_type,
            duration=duration,
            game_duration=duration,
            winner=data.winner,
            players=data.players,
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
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
    user_uid: str = Header(default="system", alias="x-user-uid"),
    x_player_name: Optional[str] = Header(default=None, alias="x-player-name"),
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
            raise HTTPException(
                status_code=422,
                detail="Failed to parse replay file. The replay may still be finalizing on disk; retry shortly.",
            )

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
            uploader_uid, mode = await _resolve_upload_identity(db, x_api_key, user_uid)

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
                    "uploader_uid": uploader_uid,
                    "upload_mode": mode,
                }

            previous_versions = []
            if original_name and uploader_uid and uploader_uid != "system":
                prior = await db.execute(
                    select(GameStats.id, GameStats.replay_hash).where(
                        GameStats.user_uid == uploader_uid,
                        GameStats.original_filename == original_name,
                        GameStats.is_final.is_(True),
                        GameStats.parse_source == "file_upload",
                    )
                )
                previous_versions = [
                    row.id
                    for row in prior
                    if row.replay_hash != replay_hash
                ]

            game = GameStats(
                user_uid=uploader_uid or "system",
                replay_file=original_name,
                replay_hash=replay_hash,
                game_version=parsed.get("game_version"),
                map=map_payload,
                game_type=parsed.get("game_type"),
                duration=duration,
                game_duration=duration,
                winner=winner,
                players=players,
                parse_iteration=1,
                is_final=True,
                parse_source="file_upload",
                parse_reason="watcher_or_browser",
                original_filename=original_name,
                played_on=played_on,
            )
            db.add(game)
            await db.flush()

            if previous_versions:
                await db.execute(
                    update(GameStats)
                    .where(GameStats.id.in_(previous_versions))
                    .values(
                        is_final=False,
                        parse_reason=SUPERSEDED_PARSE_REASON,
                    )
                )

            # Auto-verify when upload is proof-tied (watcher) or trusted (internal + x-user-uid)
            if uploader_uid and uploader_uid != "system":
                method = "watcher" if mode == "watcher" else "replay_upload"
                await _maybe_verify_user_from_replay(db, uploader_uid, players, x_player_name, method)

            await db.commit()

        return {
            "message": "Replay parsed and stored",
            "replay_hash": replay_hash,
            "winner": winner,
            "players_count": len(players),
            "uploader_uid": uploader_uid,
            "upload_mode": mode,
        }
    finally:
        try:
            os.remove(temp_path)
        except FileNotFoundError:
            pass


@router.get("/health")
async def health_check():
    return {"status": "ok"}
