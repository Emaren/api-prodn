# utils/replay_parser.py

import os
import io
import logging
import hashlib
import aiofiles
import asyncio
from mgz import header, summary
from utils.extract_datetime import extract_datetime_from_filename

# ───────────────────────────────────────────────
# 🔁 Async-compatible wrapper around sync MGZ logic
# ───────────────────────────────────────────────
async def parse_replay_full(replay_path):
    if not os.path.exists(replay_path):
        logging.error(f"❌ Replay not found: {replay_path}")
        return None

    try:
        async with aiofiles.open(replay_path, "rb") as f:
            file_bytes = await f.read()

        # Use thread to safely run blocking mgz sync logic
        return await asyncio.to_thread(_parse_sync_bytes, replay_path, file_bytes)

    except Exception as e:
        logging.error(f"❌ parse error: {e}")
        return None


def _extract_event_types(summary_obj):
    event_types = []
    seen = set()

    for action in getattr(summary_obj, "_actions", []):
        if len(action) < 2:
            continue
        action_type = action[1]
        name = getattr(action_type, "name", None)
        if not name:
            continue
        label = str(name).lower()
        if label in seen:
            continue
        seen.add(label)
        event_types.append(label)

    return event_types


def _extract_resigned_player_numbers(summary_obj):
    cache = getattr(summary_obj, "_cache", {})
    resigned = cache.get("resigned", set()) if isinstance(cache, dict) else set()
    try:
        return sorted(int(player_number) for player_number in resigned)
    except Exception:
        return []


def _extract_hd_player_ratings(parsed_header):
    hd = getattr(parsed_header, "hd", None)
    players = getattr(hd, "players", None)
    if not players:
        return {}

    ratings = {}

    for player in players:
        try:
            player_number = int(getattr(player, "player_number", -1))
        except Exception:
            continue

        if player_number <= 0:
            continue

        steam_id = getattr(player, "steam_id", None)
        if isinstance(steam_id, int) and steam_id <= 0:
            steam_id = None

        rm_rating = getattr(player, "hd_rm_rating", None)
        dm_rating = getattr(player, "hd_dm_rating", None)

        ratings[player_number] = {
            "steam_id": str(steam_id) if steam_id else None,
            "steam_rm_rating": int(rm_rating) if isinstance(rm_rating, int) else None,
            "steam_dm_rating": int(dm_rating) if isinstance(dm_rating, int) else None,
        }

    return ratings

def _parse_sync_bytes(replay_path, file_bytes):
    try:
        h = header.parse(file_bytes)
        s = summary.Summary(io.BytesIO(file_bytes))
        completed = bool(s.get_completed())
        raw_chat = s.get_chat()
        raw_platform = s.get_platform()
        chat = raw_chat if isinstance(raw_chat, list) else []
        platform = raw_platform if isinstance(raw_platform, dict) else {}
        restored = s.get_restored()
        resigned_player_numbers = _extract_resigned_player_numbers(s)
        hd_player_ratings = _extract_hd_player_ratings(h)

        stats = {
            "game_version": str(h.version),
            "map": {
                "name": s.get_map().get("name", "Unknown"),
                "size": s.get_map().get("size", "Unknown"),
            },
            "game_type": str(s.get_version()),
            "duration": int(s.get_duration() // 1000 if s.get_duration() > 48 * 3600 else s.get_duration()),
        }

        players = []
        winner = None
        for p in s.get_players():
            player_ratings = hd_player_ratings.get(p.get("number")) or {}
            rate_snapshot = p.get("rate_snapshot")
            p_data = {
                "name": p.get("name", "Unknown"),
                "civilization": p.get("civilization", "Unknown"),
                "winner": p.get("winner", False),
                "score": p.get("score", 0),
                "steam_id": player_ratings.get("steam_id") or (str(p.get("user_id")) if p.get("user_id") else None),
                "steam_rm_rating": player_ratings.get("steam_rm_rating"),
                "steam_dm_rating": player_ratings.get("steam_dm_rating"),
            }
            if isinstance(rate_snapshot, (int, float)) and p_data["steam_rm_rating"] is None:
                p_data["steam_rm_rating"] = int(rate_snapshot)
            players.append(p_data)
            if p_data["winner"]:
                winner = p_data["name"]

        stats["players"] = players
        stats["winner"] = winner or "Unknown"
        stats["event_types"] = _extract_event_types(s)
        stats["key_events"] = {
            "completed": completed,
            "has_achievements": bool(s.has_achievements()),
            "postgame_available": s.get_postgame() is not None,
            "owner_player_number": s.get_owner(),
            "resigned_player_numbers": resigned_player_numbers,
            "chat_count": len(chat),
            "platform_id": platform.get("platform_id"),
            "platform_match_id": platform.get("platform_match_id"),
            "rated": platform.get("rated"),
            "lobby_name": platform.get("lobby_name"),
            "restored": bool(restored[0]) if isinstance(restored, tuple) and len(restored) > 0 else False,
        }
        stats["completed"] = completed
        stats["disconnect_detected"] = not completed and len(resigned_player_numbers) == 0

        dt = extract_datetime_from_filename(os.path.basename(replay_path))
        stats["played_on"] = dt.isoformat() if dt else None

        logging.info(f"✅ parse_replay_full => {replay_path}")
        return stats

    except Exception as e:
        logging.error(f"❌ sync parse error: {e}")
        return None

# ───────────────────────────────────────────────
# 🔐 Async SHA256 Hash for replay file
# ───────────────────────────────────────────────
async def hash_replay_file(path):
    try:
        async with aiofiles.open(path, 'rb') as f:
            data = await f.read()
            return hashlib.sha256(data).hexdigest()
    except Exception as e:
        logging.error(f"❌ Failed to hash replay file: {e}")
        return None
