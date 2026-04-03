# utils/replay_parser.py

import os
import io
import logging
import hashlib
import math
import aiofiles
import asyncio
from mgz import header, summary
from utils.extract_datetime import extract_datetime_from_filename

CIVILIZATION_NAMES = {
    1: "Britons",
    2: "Franks",
    3: "Goths",
    4: "Teutons",
    5: "Japanese",
    6: "Chinese",
    7: "Byzantines",
    8: "Persians",
    9: "Saracens",
    10: "Turks",
    11: "Vikings",
    12: "Mongols",
    13: "Celts",
    14: "Spanish",
    15: "Aztecs",
    16: "Mayans",
    17: "Huns",
    18: "Koreans",
    19: "Italians",
    20: "Indians",
    21: "Incas",
    22: "Magyars",
    23: "Slavs",
    24: "Portuguese",
    25: "Ethiopians",
    26: "Malians",
    27: "Berbers",
    28: "Khmer",
    29: "Malay",
    30: "Burmese",
    31: "Vietnamese",
}

# ───────────────────────────────────────────────
# 🔁 Async-compatible wrapper around sync MGZ logic
# ───────────────────────────────────────────────
async def parse_replay_full(replay_path, apply_hd_early_exit_rules=True):
    if not os.path.exists(replay_path):
        logging.error(f"❌ Replay not found: {replay_path}")
        return None

    try:
        async with aiofiles.open(replay_path, "rb") as f:
            file_bytes = await f.read()

        # Use thread to safely run blocking mgz sync logic
        return await asyncio.to_thread(
            _parse_sync_bytes,
            replay_path,
            file_bytes,
            apply_hd_early_exit_rules,
        )

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


def _normalize_steam_id(value):
    if isinstance(value, int) and value > 0:
        return str(value)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _normalize_rating(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _normalize_civilization_name(value):
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, int):
        return CIVILIZATION_NAMES.get(value, f"Unknown ({value})")
    return None


def _normalize_position(value):
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return None

    cleaned = []
    for part in value:
        if isinstance(part, bool) or not isinstance(part, (int, float)):
            return None
        cleaned.append(int(round(part)))

    return cleaned


def _has_meaningful_value(value):
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_has_meaningful_value(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_meaningful_value(item) for item in value)
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _compact_value(value):
    if isinstance(value, dict):
        compacted = {}
        for key, item in value.items():
            if _has_meaningful_value(item):
                compacted[key] = _compact_value(item)
        return compacted

    if isinstance(value, (list, tuple)):
        return [_compact_value(item) for item in value if _has_meaningful_value(item)]

    if isinstance(value, float) and value.is_integer():
        return int(value)

    return value


def _extract_settings_summary(summary_obj):
    raw_settings = summary_obj.get_settings()
    if not isinstance(raw_settings, dict):
        return {}

    settings = {}
    for key, value in raw_settings.items():
        normalized = value
        if isinstance(value, tuple) and len(value) == 2:
            code, label = value
            normalized = label or code
        if _has_meaningful_value(normalized):
            settings[key] = _compact_value(normalized)

    return settings


def _extract_platform_ratings(platform):
    ratings = platform.get("ratings") if isinstance(platform, dict) else None
    if not isinstance(ratings, dict):
        return {}

    platform_ratings = {}
    for name, rating in ratings.items():
        if not isinstance(name, str) or not name.strip():
            continue
        normalized = _normalize_rating(rating)
        if normalized is None:
            continue
        platform_ratings[name.strip()] = normalized

    return platform_ratings


def _normalize_mgz_duration_seconds(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None

    numeric = float(value)
    if numeric <= 0:
        return None

    # mgz full-summary durations/timestamps are accumulated in milliseconds.
    return max(1, int(math.ceil(numeric / 1000.0)))


def _extract_chat_preview(chat):
    if not isinstance(chat, list) or not chat:
        return []

    preview = []
    for raw_entry in chat[-5:]:
        if not isinstance(raw_entry, dict):
            continue

        timestamp = raw_entry.get("timestamp")
        timestamp_seconds = _normalize_mgz_duration_seconds(timestamp)

        message = raw_entry.get("message")
        preview.append(
            {
                "timestamp_seconds": timestamp_seconds,
                "origination": str(raw_entry.get("origination") or "").strip() or None,
                "type": getattr(raw_entry.get("type"), "name", str(raw_entry.get("type") or "")).lower() or None,
                "player_number": _normalize_rating(raw_entry.get("player_number")),
                "message": str(message).strip() if isinstance(message, str) and message.strip() else None,
                "audience": str(raw_entry.get("audience") or "").strip() or None,
            }
        )

    return [entry for entry in preview if _has_meaningful_value(entry)]


def _count_players_with_visible_scores(players):
    if not isinstance(players, list):
        return 0

    count = 0
    for player in players:
        if not isinstance(player, dict):
            continue
        if _normalize_rating(player.get("score")) is not None:
            count += 1

    return count


def _count_players_with_achievements(players):
    if not isinstance(players, list):
        return 0

    count = 0
    for player in players:
        if not isinstance(player, dict):
            continue
        if _has_meaningful_value(player.get("achievements")):
            count += 1

    return count


def _count_players_with_achievement_shells(players):
    if not isinstance(players, list):
        return 0

    count = 0
    for player in players:
        if not isinstance(player, dict):
            continue
        achievements = player.get("achievements")
        if isinstance(achievements, dict) and len(achievements) > 0:
            count += 1

    return count


def _max_game_chat_timestamp_seconds(key_events):
    if not isinstance(key_events, dict):
        return None

    preview = key_events.get("chat_preview")
    if not isinstance(preview, list):
        return None

    max_seconds = 0
    for entry in preview:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("origination") or "").strip().lower() != "game":
            continue
        timestamp = entry.get("timestamp_seconds")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            continue
        numeric = int(timestamp)
        if numeric > max_seconds:
            max_seconds = numeric

    return max_seconds or None


def _apply_completion_metadata(stats):
    key_events = stats.get("key_events") if isinstance(stats.get("key_events"), dict) else {}

    has_scores = bool(key_events.get("has_scores"))
    has_achievements = bool(key_events.get("has_achievements"))
    player_score_count = _count_players_with_visible_scores(stats.get("players"))
    achievement_player_count = _count_players_with_achievements(stats.get("players"))
    achievement_shell_count = max(
        _count_players_with_achievement_shells(stats.get("players")),
        _normalize_rating(key_events.get("achievement_shell_count")) or 0,
    )
    postgame_available = bool(key_events.get("postgame_available"))
    completed = bool(stats.get("completed"))
    resigned_player_numbers = key_events.get("resigned_player_numbers")
    has_resignations = isinstance(resigned_player_numbers, list) and len(resigned_player_numbers) > 0

    if postgame_available:
        completion_source = "postgame"
    elif has_achievements or has_scores or player_score_count > 0 or achievement_player_count > 0:
        completion_source = "scoreboard"
    elif completed and has_resignations:
        completion_source = "resignation"
    elif completed:
        completion_source = "completion_signal"
    else:
        completion_source = None

    key_events["has_scores"] = has_scores or player_score_count > 0
    key_events["has_achievements"] = has_achievements or achievement_player_count > 0
    key_events["player_score_count"] = player_score_count
    key_events["achievement_player_count"] = achievement_player_count
    key_events["achievement_shell_count"] = achievement_shell_count
    key_events["has_achievement_shell"] = achievement_shell_count > 0
    key_events["postgame_available"] = postgame_available
    if completion_source:
        key_events["completion_source"] = completion_source

    stats["has_scores"] = key_events["has_scores"]
    stats["has_achievements"] = key_events["has_achievements"]
    stats["player_score_count"] = player_score_count
    stats["achievement_player_count"] = achievement_player_count
    stats["achievement_shell_count"] = achievement_shell_count
    stats["has_achievement_shell"] = achievement_shell_count > 0
    stats["postgame_available"] = postgame_available
    stats["completion_source"] = completion_source
    stats["key_events"] = key_events

    if (
        completed
        and completion_source == "resignation"
        and not stats.get("parse_reason")
    ):
        stats["parse_reason"] = "recorded_resignation_final"

    return stats


def _apply_hd_early_exit_rules(stats):
    if str(stats.get("game_version") or "").strip() != "Version.HD":
        return stats

    duration_seconds = stats.get("duration")
    if not isinstance(duration_seconds, int) or duration_seconds <= 0 or duration_seconds >= 60:
        return stats

    key_events = stats.get("key_events") if isinstance(stats.get("key_events"), dict) else {}
    max_game_chat_seconds = _max_game_chat_timestamp_seconds(key_events)
    if isinstance(max_game_chat_seconds, int) and max_game_chat_seconds >= 60:
        stats["duration"] = max(duration_seconds, max_game_chat_seconds)
        key_events["duration_source"] = "chat_preview_seconds_override"
        key_events["duration_override_seconds"] = stats["duration"]
        stats["key_events"] = key_events
        return stats

    resigned_player_numbers = key_events.get("resigned_player_numbers")
    has_resign = isinstance(resigned_player_numbers, list) and len(resigned_player_numbers) > 0
    is_rated = bool(key_events.get("rated"))

    if not is_rated or not (has_resign or stats.get("disconnect_detected")):
        return stats

    suppressed_winner = stats.get("winner")
    stats["winner"] = "Unknown"
    stats["completed"] = False
    stats["disconnect_detected"] = True
    stats["parse_reason"] = "hd_early_exit_under_60s"

    players = stats.get("players") if isinstance(stats.get("players"), list) else []
    for player in players:
        if isinstance(player, dict):
            player["winner"] = None

    key_events["completed"] = False
    key_events["early_exit_under_60s"] = True
    key_events["no_rated_result"] = True
    key_events["early_exit_seconds"] = duration_seconds
    if suppressed_winner and suppressed_winner != "Unknown":
        key_events["suppressed_winner"] = suppressed_winner

    stats["key_events"] = key_events
    return stats


def _maybe_apply_hd_early_exit_rules(stats, apply_rules=True):
    if not apply_rules:
        return stats
    return _apply_hd_early_exit_rules(stats)


def _parse_sync_bytes(replay_path, file_bytes, apply_hd_early_exit_rules=True):
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
        platform_ratings = _extract_platform_ratings(platform)
        owner_player_number = s.get_owner()
        raw_duration_ms = s.get_duration()
        normalized_duration_seconds = _normalize_mgz_duration_seconds(raw_duration_ms)

        stats = {
            "game_version": str(h.version),
            "map": {
                "name": s.get_map().get("name", "Unknown"),
                "size": s.get_map().get("size", "Unknown"),
            },
            "game_type": str(s.get_version()),
            "duration": normalized_duration_seconds or 0,
        }

        players = []
        winner = None
        raw_players = list(s.get_players())
        achievement_shell_count = 0
        for p in raw_players:
            player_ratings = hd_player_ratings.get(p.get("number")) or {}
            rate_snapshot = _normalize_rating(p.get("rate_snapshot"))
            steam_id = player_ratings.get("steam_id") or _normalize_steam_id(p.get("user_id"))
            civilization = p.get("civilization", "Unknown")
            raw_achievements = p.get("achievements") or {}
            if isinstance(raw_achievements, dict) and len(raw_achievements) > 0:
                achievement_shell_count += 1
            achievements = _compact_value(raw_achievements)
            p_data = {
                "name": p.get("name", "Unknown"),
                "number": _normalize_rating(p.get("number")),
                "civilization": civilization,
                "civilization_name": _normalize_civilization_name(civilization),
                "winner": p.get("winner", False),
                "score": p.get("score", 0),
                "user_id": steam_id,
                "steam_id": steam_id,
                "steam_rm_rating": player_ratings.get("steam_rm_rating"),
                "steam_dm_rating": player_ratings.get("steam_dm_rating"),
                "rate_snapshot": rate_snapshot,
                "eapm": _normalize_rating(p.get("eapm")),
                "position": _normalize_position(p.get("position")),
                "color_id": _normalize_rating(p.get("color_id")),
                "team_id": _normalize_rating(p.get("team_id")),
                "human": bool(p.get("human")) if p.get("human") is not None else None,
                "prefer_random": bool(p.get("prefer_random")) if p.get("prefer_random") is not None else None,
                "mvp": p.get("mvp"),
                "cheater": bool(p.get("cheater")) if p.get("cheater") is not None else None,
            }
            if achievements:
                p_data["achievements"] = achievements
            if rate_snapshot is not None and p_data["steam_rm_rating"] is None:
                p_data["steam_rm_rating"] = rate_snapshot
            platform_rating = platform_ratings.get(p_data["name"])
            if platform_rating is not None and p_data["rate_snapshot"] is None:
                p_data["rate_snapshot"] = platform_rating
            players.append(p_data)
            if p_data["winner"]:
                winner = p_data["name"]

        owner_player_name = next(
            (
                player.get("name")
                for player in players
                if player.get("number") == owner_player_number
            ),
            None,
        )
        resigned_player_names = [
            player.get("name")
            for player in players
            if player.get("number") in resigned_player_numbers and player.get("name")
        ]

        stats["players"] = players
        stats["winner"] = winner or "Unknown"
        stats["event_types"] = _extract_event_types(s)
        visible_score_count = _count_players_with_visible_scores(players)
        achievement_player_count = _count_players_with_achievements(players)
        has_achievements = bool(s.has_achievements()) or achievement_player_count > 0
        stats["key_events"] = {
            "completed": completed,
            "has_achievements": has_achievements,
            "has_scores": visible_score_count > 0,
            "player_score_count": visible_score_count,
            "achievement_player_count": achievement_player_count,
            "achievement_shell_count": achievement_shell_count,
            "has_achievement_shell": achievement_shell_count > 0,
            "postgame_available": s.get_postgame() is not None,
            "owner_player_number": owner_player_number,
            "owner_player_name": owner_player_name,
            "resigned_player_numbers": resigned_player_numbers,
            "resigned_player_names": resigned_player_names,
            "chat_count": len(chat),
            "platform_id": platform.get("platform_id"),
            "platform_match_id": platform.get("platform_match_id"),
            "rated": platform.get("rated"),
            "lobby_name": platform.get("lobby_name"),
            "restored": bool(restored[0]) if isinstance(restored, tuple) and len(restored) > 0 else False,
            "raw_duration_ms": int(raw_duration_ms) if isinstance(raw_duration_ms, (int, float)) else None,
            "duration_source": "mgz_summary_ms_normalized",
        }
        settings_summary = _extract_settings_summary(s)
        if settings_summary:
            stats["key_events"]["settings"] = settings_summary
        if platform_ratings:
            stats["key_events"]["platform_ratings"] = platform_ratings
        chat_preview = _extract_chat_preview(chat)
        if chat_preview:
            stats["key_events"]["chat_preview"] = chat_preview
        stats["completed"] = completed
        stats["disconnect_detected"] = not completed and len(resigned_player_numbers) == 0
        stats = _apply_completion_metadata(stats)

        dt = extract_datetime_from_filename(replay_path)
        stats["played_on"] = dt.isoformat() if dt else None
        stats = _maybe_apply_hd_early_exit_rules(stats, apply_hd_early_exit_rules)

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
