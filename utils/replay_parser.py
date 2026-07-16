# utils/replay_parser.py

import os
import io
import logging
import hashlib
import math
import struct
import zlib
import aiofiles
import asyncio
import uuid
from construct import GreedyBytes, Int32ul, Struct, Tell
from mgz import compressed_header, const, header, summary
from mgz.fast.header import decompress as decompress_mgz_header
from mgz.model import parse_match
from mgz.util import Version
from utils.extract_datetime import extract_datetime_from_filename
from utils.replay_engine import (
    build_candidate_envelope,
    capture_fragment_body_evidence,
    capture_model_evidence,
    capture_summary_evidence,
    normalize_failure_signature,
)
from utils.replay_team_contract import apply_replay_team_contract


MGZ_HD_TYPE9_GAME_TYPE_LABEL = "TurboRandom9"
_MGZ_HD_TYPE9_PATCHED = False
_HD_FRAGMENT_PREFIX = Struct(
    *compressed_header.subcons[:-2],
    "fragment_offset" / Tell,
    "fragment_tail" / GreedyBytes,
)
_HD_METADATA_PREFIX = Struct(
    *compressed_header.subcons[:8],
    "metadata_fragment_offset" / Tell,
    "metadata_fragment_tail" / GreedyBytes,
)
_HD_TRAILING_HEADER_PREFIX = Struct(
    *compressed_header.subcons[:-1],
    "trailing_header_offset" / Tell,
    "trailing_header_bytes" / GreedyBytes,
)
_HD_SAVED_GAME_SNAPSHOT = Struct(
    *compressed_header.subcons[:11],
    "save_snapshot_word" / Int32ul,
    *compressed_header.subcons[11:-1],
    "saved_game_offset" / Tell,
    "saved_game_tail" / GreedyBytes,
)
_HD_SAVED_GAME_INITIAL_PREFIX = Struct(
    *compressed_header.subcons[:11],
    "save_snapshot_word" / Int32ul,
    *compressed_header.subcons[11:12],
    "saved_game_offset" / Tell,
    "saved_game_tail" / GreedyBytes,
)
_HD_SAVED_GAME_MAP_PREFIX = Struct(
    *compressed_header.subcons[:11],
    "save_snapshot_word" / Int32ul,
    "saved_game_offset" / Tell,
    "saved_game_tail" / GreedyBytes,
)
_HD_SAVED_GAME_MAX_DECOMPRESSED_BYTES = 64 * 1024 * 1024


def _patch_mgz_hd_type9_game_type():
    """
    AoE2 HD can emit lobby game_type_id=9 for Turbo Random-style custom-map games.

    Upstream mgz knows TurboRandom=8 but not this HD-specific value 9. Without
    this compatibility shim,
    header.parse/summary.Summary fail with:
        no decoding mapping for 9 (parsing) -> lobby

    Patch only Construct Enum maps that already look like game-type enums.
    """
    global _MGZ_HD_TYPE9_PATCHED

    if _MGZ_HD_TYPE9_PATCHED:
        return

    patched = 0
    seen = set()

    def looks_like_game_type_enum(decoding, encoding):
        labels = set()

        if isinstance(decoding, dict):
            labels.update(str(value) for value in decoding.values())

        if isinstance(encoding, dict):
            labels.update(str(key) for key in encoding.keys())

        return (
            "TurboRandom" in labels
            and (
                {"RM", "DM"}.issubset(labels)
                or "CaptureTheRelic" in labels
                or "SuddenDeath" in labels
            )
        )

    def visit(obj, depth=0):
        nonlocal patched

        if depth > 18:
            return

        ident = id(obj)
        if ident in seen:
            return
        seen.add(ident)

        for decode_attr, encode_attr in [
            ("decoding", "encoding"),
            ("decmapping", "encmapping"),
            ("_decode_mapping", "_encode_mapping"),
        ]:
            decoding = getattr(obj, decode_attr, None)
            encoding = getattr(obj, encode_attr, None)

            if not looks_like_game_type_enum(decoding, encoding):
                continue

            if isinstance(decoding, dict) and 9 not in decoding:
                decoding[9] = MGZ_HD_TYPE9_GAME_TYPE_LABEL
                patched += 1

            if isinstance(encoding, dict) and MGZ_HD_TYPE9_GAME_TYPE_LABEL not in encoding:
                encoding[MGZ_HD_TYPE9_GAME_TYPE_LABEL] = 9
                patched += 1

        for name in [
            "subcon", "subcons", "cases", "case", "default", "mapping",
            "selector", "thenfield", "elsefield", "fields", "items",
        ]:
            try:
                child = getattr(obj, name)
            except Exception:
                continue

            if isinstance(child, (str, bytes, int, float, bool, type(None))):
                continue

            visit(child, depth + 1)

        if isinstance(obj, dict):
            for child in list(obj.values()):
                if not isinstance(child, (str, bytes, int, float, bool, type(None))):
                    visit(child, depth + 1)

        if isinstance(obj, (list, tuple, set, frozenset)):
            for child in list(obj):
                if not isinstance(child, (str, bytes, int, float, bool, type(None))):
                    visit(child, depth + 1)

    try:
        visit(header)
        if patched:
            logging.info(
                "✅ patched mgz HD game_type_id=9 compatibility maps: %s",
                patched,
            )
    except Exception as error:
        logging.warning("⚠️ failed to patch mgz HD game_type_id=9 compatibility: %s", error)

    _MGZ_HD_TYPE9_PATCHED = True

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

        parsed = await asyncio.to_thread(
            _parse_sync_bytes,
            replay_path,
            file_bytes,
            apply_hd_early_exit_rules,
        )
        return apply_replay_team_contract(parsed)

    except Exception as e:
        logging.error(f"❌ parse error: {e}")
        return None


async def parse_replay_candidate(replay_path, apply_hd_early_exit_rules=True):
    """Return the immutable candidate envelope used by future parser workers."""
    if not os.path.exists(replay_path):
        logging.error(f"❌ Replay not found: {replay_path}")
        return build_candidate_envelope(
            replay_path=replay_path,
            file_bytes=None,
            projection=None,
            evidence=None,
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
            parse_mode="artifact_io_failed",
            failure=normalize_failure_signature(
                FileNotFoundError("replay artifact path does not exist"),
                stage="artifact_read",
            ),
        )

    try:
        async with aiofiles.open(replay_path, "rb") as replay_file:
            file_bytes = await replay_file.read()
    except Exception as error:
        failure = normalize_failure_signature(error, stage="artifact_read")
        logging.error("❌ candidate artifact read error: %s", failure["signature"])
        return build_candidate_envelope(
            replay_path=replay_path,
            file_bytes=None,
            projection=None,
            evidence=None,
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
            parse_mode="artifact_io_failed",
            failure=failure,
        )

    try:
        return await asyncio.to_thread(
            parse_replay_candidate_bytes,
            replay_path,
            file_bytes,
            apply_hd_early_exit_rules,
        )
    except Exception as error:
        failure = normalize_failure_signature(error, stage="candidate_build")
        logging.error("❌ candidate generation error: %s", failure["signature"])
        return build_candidate_envelope(
            replay_path=replay_path,
            file_bytes=file_bytes,
            projection=None,
            evidence=None,
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
            parse_mode="candidate_generation_failed",
            failure=failure,
        )


def parse_replay_candidate_bytes(
    replay_path,
    file_bytes,
    apply_hd_early_exit_rules=True,
):
    """Parse replay bytes into a deterministic, candidate-only run envelope."""
    parsed, parse_diagnostic, parse_mode = _parse_sync_bytes_with_diagnostics(
        replay_path,
        file_bytes,
        apply_hd_early_exit_rules,
        capture_engine_evidence=True,
    )
    if isinstance(parsed, dict):
        evidence = parsed.pop("_engine_evidence", None)
        evidence_failure = parsed.pop("_engine_evidence_failure", None)
        parsed = apply_replay_team_contract(parsed)
        diagnostic = parse_diagnostic or evidence_failure
    else:
        evidence = None
        diagnostic = parse_diagnostic

    return build_candidate_envelope(
        replay_path=replay_path,
        file_bytes=file_bytes,
        projection=parsed,
        evidence=evidence,
        apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        parse_mode=parse_mode,
        failure=diagnostic,
    )


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

        # AoE2 HD parser/library labels are reversed in practice for this replay path:
        #   hd_dm_rating matches the visible HD RM line
        #   hd_rm_rating matches the visible HD DM line
        rm_rating = getattr(player, "hd_dm_rating", None)
        dm_rating = getattr(player, "hd_rm_rating", None)

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


def _safe_get(source, key, default=None):
    if source is None:
        return default
    if isinstance(source, dict):
        return source.get(key, default)
    try:
        return getattr(source, key)
    except Exception:
        pass
    try:
        return source[key]
    except Exception:
        return default


def _safe_int(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _safe_decode_text(value):
    raw = _safe_get(value, "value", value)
    if isinstance(raw, str):
        return raw.strip("\x00").strip() or None
    if isinstance(raw, bytes):
        cleaned = raw.strip(b"\x00")
        for encoding in ("utf-8", "latin-1", "cp1252", "cp437"):
            try:
                decoded = cleaned.decode(encoding).strip()
            except UnicodeDecodeError:
                continue
            if decoded:
                return decoded
    return None


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


MAX_CHAT_TRANSCRIPT_LINES = 500


def _normalize_chat_entry(raw_entry, line_no=None):
    if not isinstance(raw_entry, dict):
        return None

    timestamp = raw_entry.get("timestamp")
    timestamp_seconds = _normalize_mgz_duration_seconds(timestamp)

    message = raw_entry.get("message")
    entry = {
        "timestamp_seconds": timestamp_seconds,
        "origination": str(raw_entry.get("origination") or "").strip() or None,
        "type": getattr(raw_entry.get("type"), "name", str(raw_entry.get("type") or "")).lower() or None,
        "player_number": _normalize_rating(raw_entry.get("player_number")),
        "message": str(message).strip() if isinstance(message, str) and message.strip() else None,
        "audience": str(raw_entry.get("audience") or "").strip() or None,
    }

    if line_no is not None:
        entry["line_no"] = line_no

    return entry if _has_meaningful_value(entry) else None


def _extract_chat_preview(chat):
    if not isinstance(chat, list) or not chat:
        return []

    preview = []
    for raw_entry in chat[-5:]:
        entry = _normalize_chat_entry(raw_entry)
        if entry:
            preview.append(entry)

    return preview


def _extract_chat_transcript(chat, max_lines=MAX_CHAT_TRANSCRIPT_LINES):
    if not isinstance(chat, list) or not chat:
        return []

    transcript = []
    for index, raw_entry in enumerate(chat[:max_lines], start=1):
        entry = _normalize_chat_entry(raw_entry, line_no=index)
        if entry:
            transcript.append(entry)

    return transcript


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


def _header_map_id(parsed_header):
    hd = _safe_get(parsed_header, "hd")
    de = _safe_get(parsed_header, "de")
    scenario = _safe_get(parsed_header, "scenario")
    game_settings = _safe_get(scenario, "game_settings")

    return (
        _safe_int(_safe_get(hd, "selected_map_id"))
        or _safe_int(_safe_get(de, "resolved_map_id"))
        or _safe_int(_safe_get(game_settings, "map_id"))
    )


def _header_map_payload(parsed_header):
    map_info = _safe_get(parsed_header, "map_info")
    dimension = _safe_int(_safe_get(map_info, "size_x"))
    map_id = _header_map_id(parsed_header)
    de_names = getattr(const, "DE_MAP_NAMES", {})
    map_names = getattr(const, "MAP_NAMES", {})
    map_name = de_names.get(map_id) or map_names.get(map_id) or "Unknown"

    return {
        "id": map_id,
        "name": map_name,
        "size": const.MAP_SIZES.get(dimension, "Unknown"),
        "dimension": dimension,
        "header_only": True,
    }


def _header_platform_match_id(parsed_header):
    hd = _safe_get(parsed_header, "hd")
    de = _safe_get(parsed_header, "de")
    guid = _safe_get(hd, "guid") or _safe_get(de, "guid")
    if not isinstance(guid, (bytes, bytearray)) or len(guid) != 16:
        return None
    try:
        return str(uuid.UUID(bytes=bytes(guid)))
    except Exception:
        return None


def _extract_header_player_rows(parsed_header):
    players_by_number = {}
    ratings_by_number = _extract_hd_player_ratings(parsed_header)
    initial = _safe_get(parsed_header, "initial")
    scenario = _safe_get(parsed_header, "scenario")
    game_settings = _safe_get(scenario, "game_settings")
    player_info = list(_safe_get(game_settings, "player_info", []) or [])

    for index, raw_player in enumerate(_safe_get(initial, "players", []) or []):
        attributes = _safe_get(raw_player, "attributes")
        name = _safe_decode_text(_safe_get(attributes, "player_name"))
        if not name or name.upper() == "GAIA":
            continue

        number = (
            _safe_int(_safe_get(raw_player, "number"))
            or _safe_int(_safe_get(raw_player, "player_number"))
            or index
        )
        if number <= 0:
            continue

        civilization = _safe_int(
            _safe_get(raw_player, "civilization")
            or _safe_get(attributes, "civilization")
        )
        player_type = (
            _safe_get(player_info[index], "type")
            if index < len(player_info)
            else None
        )
        player_type_name = str(
            getattr(player_type, "name", player_type) or ""
        ).casefold()
        player = {
            "name": name,
            "number": number,
            "civilization": civilization,
            "civilization_name": _normalize_civilization_name(civilization),
            "winner": None,
            "score": None,
            "user_id": None,
            "steam_id": None,
            "steam_rm_rating": None,
            "steam_dm_rating": None,
            "rate_snapshot": None,
            "eapm": None,
            "position": _normalize_position(
                [
                    _safe_get(attributes, "camera_x"),
                    _safe_get(attributes, "camera_y"),
                ]
            ),
            "color_id": _safe_int(
                _safe_get(raw_player, "player_color")
                or _safe_get(attributes, "player_color")
            ),
            "team_id": None,
            "human": (
                True
                if player_type_name == "human"
                else False if player_type_name == "computer" else None
            ),
            "header_only": True,
        }
        player.update(ratings_by_number.get(number) or {})
        player["user_id"] = player.get("steam_id")
        players_by_number[number] = player

    hd = _safe_get(parsed_header, "hd")
    for raw_player in _safe_get(hd, "players", []) or []:
        number = _safe_int(_safe_get(raw_player, "player_number"))
        if not number or number <= 0:
            continue

        name = _safe_decode_text(_safe_get(raw_player, "name"))
        existing = players_by_number.get(number) or {
            "name": name or f"Player {number}",
            "number": number,
            "civilization": None,
            "civilization_name": None,
            "winner": None,
            "score": None,
            "user_id": None,
            "steam_id": None,
            "steam_rm_rating": None,
            "steam_dm_rating": None,
            "rate_snapshot": None,
            "eapm": None,
            "position": None,
            "color_id": None,
            "team_id": None,
            "human": None,
            "header_only": True,
        }
        if name:
            existing["name"] = name
        steam_id = _normalize_steam_id(_safe_get(raw_player, "steam_id"))
        if steam_id:
            existing["steam_id"] = steam_id
            existing["user_id"] = steam_id
        existing.update({key: value for key, value in (ratings_by_number.get(number) or {}).items() if value is not None})
        players_by_number[number] = existing

    return [
        players_by_number[number]
        for number in sorted(players_by_number)
        if players_by_number[number].get("name")
    ]


def _fragment_relation_name(value):
    return str(getattr(value, "name", value) or "").split(".")[-1].casefold()


def _fragment_diplomacy_groups(parsed_header, players):
    initial = _safe_get(parsed_header, "initial")
    initial_players = list(_safe_get(initial, "players", []) or [])
    real_players = initial_players[1:]
    expected_numbers = set(range(1, len(real_players) + 1))
    actual_numbers = {
        _safe_int(player.get("number"))
        for player in players
        if isinstance(player, dict)
    }
    if actual_numbers != expected_numbers:
        return None

    alliances = {}
    for number, raw_player in enumerate(real_players, start=1):
        attributes = _safe_get(raw_player, "attributes")
        relations = list(_safe_get(attributes, "my_diplomacy", []) or [])
        group = {
            peer_number
            for peer_number in expected_numbers
            if peer_number < len(relations)
            and _fragment_relation_name(relations[peer_number]) in {"ally", "self"}
        }
        group.add(number)
        alliances[number] = frozenset(group)

    for number, group in alliances.items():
        if any(alliances.get(peer) != group for peer in group):
            return None
        if any(number in peer_group for peer, peer_group in alliances.items() if peer not in group):
            return None

    groups = sorted(set(alliances.values()), key=lambda group: min(group))
    if set().union(*groups) != expected_numbers:
        return None

    team_by_number = {
        number: team_id
        for team_id, group in enumerate(groups)
        for number in group
    }
    for player in players:
        number = _safe_int(player.get("number"))
        player["team_id"] = team_by_number.get(number)
        player["team_id_source"] = "header_initial_mutual_diplomacy"

    sizes = [len(group) for group in groups]
    if len(players) == 2 and sizes == [1, 1]:
        diplomacy_type = "1v1"
        team_size = "1v1"
    elif len(groups) == 2 and len(players) > 2:
        diplomacy_type = "TG"
        team_size = "v".join(str(size) for size in sorted(sizes))
    elif len(groups) == len(players):
        diplomacy_type = "FFA"
        team_size = "FFA"
    else:
        diplomacy_type = "Other"
        team_size = "v".join(str(size) for size in sorted(sizes))

    names_by_number = {
        _safe_int(player.get("number")): player.get("name")
        for player in players
        if isinstance(player, dict)
    }
    return {
        "source": "header_initial_mutual_diplomacy",
        "coherent": True,
        "type": diplomacy_type,
        "team_size": team_size,
        "teams": [
            {
                "team_id": team_id,
                "player_numbers": sorted(group),
                "players": [names_by_number.get(number) for number in sorted(group)],
            }
            for team_id, group in enumerate(groups)
        ],
    }


def _fragment_initial_object_summary(parsed_header):
    initial = _safe_get(parsed_header, "initial")
    counts = {}
    total = 0
    for raw_player in _safe_get(initial, "players", []) or []:
        for lane in ("objects", "sleeping_objects", "doppleganger_objects"):
            for raw_object in _safe_get(raw_player, lane, []) or []:
                total += 1
                object_id = (
                    _safe_int(_safe_get(raw_object, "object_id"))
                    or _safe_int(_safe_get(raw_object, "object_type"))
                    or _safe_int(_safe_get(raw_object, "type"))
                )
                key = str(object_id) if object_id is not None else "unknown"
                counts[key] = counts.get(key, 0) + 1
    return {
        "snapshot_scope": "mgz_fragment_initial_header_objects",
        "object_count": total,
        "object_count_semantics": (
            "objects parsed before the unsupported HD lobby boundary; "
            "not objects created during gameplay"
        ),
        "object_type_counts": [
            {"object_id": _safe_int(object_id), "count": counts[object_id]}
            for object_id in sorted(counts, key=lambda value: (_safe_int(value) is None, _safe_int(value) or 0))
        ],
        "objects": [],
    }


def _parse_hd_fragment_header_body_bytes(
    replay_path,
    file_bytes,
    parse_error,
    *,
    capture_engine_evidence=False,
    apply_hd_early_exit_rules=True,
):
    if "-> lobby" not in str(parse_error).casefold():
        return None

    try:
        decompressed = decompress_mgz_header(io.BytesIO(file_bytes)).getvalue()
        parsed_header = _HD_FRAGMENT_PREFIX.parse(decompressed)
    except Exception as fragment_error:
        logging.error("❌ HD fragment header fallback failed: %s", fragment_error)
        return None

    if _safe_get(parsed_header, "version") is not Version.HD:
        return None

    players = _extract_header_player_rows(parsed_header)
    expected_player_count = max(
        0,
        (_safe_int(_safe_get(_safe_get(parsed_header, "replay"), "num_players")) or 0) - 1,
    )
    player_keys = {
        str(player.get("name") or "").strip().casefold()
        for player in players
        if isinstance(player, dict)
    }
    if (
        len(players) < 2
        or len(players) != expected_player_count
        or len(player_keys) != len(players)
        or "" in player_keys
    ):
        logging.error(
            "❌ HD fragment header roster validation failed: expected=%s actual=%s",
            expected_player_count,
            len(players),
        )
        return None

    diplomacy = _fragment_diplomacy_groups(parsed_header, players)
    if not diplomacy:
        logging.error("❌ HD fragment header diplomacy validation failed")
        return None

    map_payload = _header_map_payload(parsed_header)
    map_snapshot = {
        key: value
        for key, value in map_payload.items()
        if key in {"id", "name", "size", "dimension"}
    }
    initial = _safe_get(parsed_header, "initial")
    restore_time_ms = _safe_int(_safe_get(initial, "restore_time")) or 0
    header_length = struct.unpack_from("<I", file_bytes, 0)[0]
    body_failure = None
    try:
        evidence = capture_fragment_body_evidence(
            file_bytes,
            header_length=header_length,
            restore_time_ms=restore_time_ms,
            players=players,
            map_snapshot=map_snapshot,
            diplomacy=diplomacy,
            initial_objects=_fragment_initial_object_summary(parsed_header),
        )
    except Exception as body_error:
        body_failure = normalize_failure_signature(body_error, stage="body_fragment")
        evidence = {
            "dataset": {
                "source": "hd_fragment_header_only",
                "validated_gameplay_truth": False,
            },
            "diplomacy": diplomacy,
            "map_snapshot": map_snapshot,
            "initial_objects": _fragment_initial_object_summary(parsed_header),
            "actions": {
                "available": False,
                "count": None,
                "stream": [],
            },
            "chat": {"available": False, "count": None, "stream": []},
        }

    actions = evidence.get("actions") if isinstance(evidence.get("actions"), dict) else {}
    resignation_timeline = actions.get("resignation_timeline") or []
    resigned_player_numbers = sorted(
        {
            number
            for event in resignation_timeline
            if isinstance(event, dict)
            and (number := _safe_int(event.get("player_number"))) is not None
        }
    )
    names_by_number = {
        _safe_int(player.get("number")): player.get("name")
        for player in players
        if isinstance(player, dict)
    }
    resigned_player_names = [
        names_by_number[number]
        for number in resigned_player_numbers
        if names_by_number.get(number)
    ]
    duration_ms = _safe_int(actions.get("duration_ms"))
    duration_seconds = _normalize_mgz_duration_seconds(duration_ms) or 0
    body_available = actions.get("available") is True
    hd = _safe_get(parsed_header, "hd")
    platform_match_id = _header_platform_match_id(parsed_header)
    original_failure = normalize_failure_signature(parse_error, stage="header")
    key_events = {
        "completed": bool(resigned_player_numbers),
        "header_fragment_recovery": True,
        "header_fragment_boundary": "before_lobby",
        "header_failure_signature": original_failure["signature"],
        "body_stream_recovery": body_available,
        "body_stream_complete": actions.get("body_stream_complete") if body_available else False,
        "body_byte_size": actions.get("body_byte_size") if body_available else max(0, len(file_bytes) - header_length),
        "body_operation_count": actions.get("operation_count") if body_available else None,
        "postgame_available": False,
        "postgame_packet_present": bool((actions.get("type_counts") or {}).get("postgame")),
        "has_scores": False,
        "has_achievements": False,
        "player_score_count": 0,
        "achievement_player_count": 0,
        "achievement_shell_count": 0,
        "has_achievement_shell": False,
        "platform_id": "hd" if _safe_get(hd, "multiplayer") else None,
        "platform_match_id": platform_match_id,
        "rated": bool(_safe_get(hd, "is_ranked")) if hd else None,
        "restored": restore_time_ms > 0,
        "restore_time_ms": restore_time_ms,
        "duration_source": "mgz.fast.body.sync_accumulation" if body_available else None,
        "resigned_player_numbers": resigned_player_numbers,
        "resigned_player_names": resigned_player_names,
        "team_source": diplomacy["source"],
        "diplomacy": diplomacy,
    }
    if body_failure:
        key_events["body_failure_signature"] = body_failure["signature"]

    stats = {
        "game_version": str(_safe_get(parsed_header, "version", "Unknown")),
        "map": map_payload,
        "game_type": "Unknown",
        "duration": duration_seconds,
        "game_duration": duration_seconds,
        "players": players,
        "winner": "Unknown",
        "event_types": sorted((actions.get("type_counts") or {}).keys()),
        "key_events": key_events,
        "completed": bool(resigned_player_numbers),
        "disconnect_detected": body_available and not bool(resigned_player_numbers),
        "parse_reason": (
            "hd_fragment_header_body_recovery"
            if body_available
            else "hd_fragment_header_only_recovery"
        ),
    }
    dt = extract_datetime_from_filename(replay_path)
    stats["played_on"] = dt.isoformat() if dt else None
    stats = _apply_completion_metadata(stats)
    stats = _maybe_apply_hd_early_exit_rules(stats, apply_hd_early_exit_rules)
    if capture_engine_evidence:
        stats["_engine_evidence"] = evidence
        if body_failure:
            stats["_engine_evidence_failure"] = body_failure
    logging.warning(
        "⚠️ HD fragment header/body fallback used for %s after lobby parse failed",
        replay_path,
    )
    return stats


def _decode_trailing_header_chat_records(trailing_bytes):
    records = []
    offset = 0
    while offset < len(trailing_bytes):
        if offset + 4 > len(trailing_bytes):
            return None
        message_length = struct.unpack_from("<I", trailing_bytes, offset)[0]
        offset += 4
        if message_length <= 0 or offset + message_length > len(trailing_bytes):
            return None
        raw_message = trailing_bytes[offset:offset + message_length]
        offset += message_length
        message = _safe_decode_text(raw_message.rstrip(b"\x00"))
        if not message:
            return None
        records.append(
            {
                "ordinal": len(records) + 1,
                "timestamp_ms": 0,
                "origination": "lobby",
                "type": "trailing_length_prefixed_lobby_chat",
                "player_number": None,
                "message": message,
                "audience": None,
                "provenance_class": "direct_header",
                "raw_byte_size": message_length,
            }
        )
    return records


def _parse_hd_trailing_header_body_bytes(
    replay_path,
    file_bytes,
    parse_error,
    *,
    capture_engine_evidence=False,
    apply_hd_early_exit_rules=True,
):
    if "expected end of stream" not in str(parse_error).casefold():
        return None
    try:
        decompressed = decompress_mgz_header(io.BytesIO(file_bytes)).getvalue()
        parsed_header = _HD_TRAILING_HEADER_PREFIX.parse(decompressed)
    except Exception as fragment_error:
        logging.error("❌ HD trailing-header fallback failed: %s", fragment_error)
        return None
    if _safe_get(parsed_header, "version") is not Version.HD:
        return None

    trailing_bytes = bytes(
        _safe_get(parsed_header, "trailing_header_bytes", b"") or b""
    )
    if not trailing_bytes or len(trailing_bytes) > 1024 * 1024:
        return None
    trailing_chat = _decode_trailing_header_chat_records(trailing_bytes)
    if not trailing_chat:
        logging.error("❌ HD trailing-header bytes were not exact lobby chat records")
        return None

    players = _extract_header_player_rows(parsed_header)
    lobby = _safe_get(parsed_header, "lobby")
    raw_teams = list(_safe_get(lobby, "teams", []) or [])
    if len(players) < 2 or len(raw_teams) < len(players):
        return None
    for player in players:
        number = _safe_int(player.get("number"))
        raw_team = raw_teams[number - 1] if number and number <= len(raw_teams) else None
        player["team_id"] = raw_team - 2 if isinstance(raw_team, int) else None
        player["team_id_source"] = "hd_lobby_team_array"
    diplomacy = _hd_metadata_fragment_diplomacy(players)

    map_payload = _header_map_payload(parsed_header)
    hd = _safe_get(parsed_header, "hd")
    custom_map = _safe_decode_text(_safe_get(hd, "custom_random_map_file"))
    if custom_map:
        map_payload["name"] = custom_map.removesuffix(".rms")
        map_payload["custom"] = True
        map_payload["custom_filename"] = custom_map
    map_snapshot = {
        key: value
        for key, value in map_payload.items()
        if key in {"id", "name", "size", "dimension", "custom"}
    }
    initial = _safe_get(parsed_header, "initial")
    restore_time_ms = _safe_int(_safe_get(initial, "restore_time")) or 0
    header_length = struct.unpack_from("<I", file_bytes, 0)[0]
    body_failure = None
    try:
        evidence = capture_fragment_body_evidence(
            file_bytes,
            header_length=header_length,
            restore_time_ms=restore_time_ms,
            players=players,
            map_snapshot=map_snapshot,
            diplomacy=diplomacy,
            initial_objects=_fragment_initial_object_summary(parsed_header),
        )
        evidence["dataset"] = {
            "source": "complete_hd_header_plus_trailing_lobby_chat_plus_direct_body",
            "validated_gameplay_truth": False,
        }
        body_chat = list((evidence.get("chat") or {}).get("stream") or [])
        combined_chat = trailing_chat + body_chat
        for ordinal, entry in enumerate(combined_chat, start=1):
            entry["ordinal"] = ordinal
        evidence["chat"] = {
            "available": True,
            "count": len(combined_chat),
            "stream": combined_chat,
        }
    except Exception as body_error:
        body_failure = normalize_failure_signature(body_error, stage="body_fragment")
        evidence = {
            "dataset": {
                "source": "complete_hd_header_plus_trailing_lobby_chat",
                "validated_gameplay_truth": False,
            },
            "diplomacy": diplomacy,
            "map_snapshot": map_snapshot,
            "initial_objects": _fragment_initial_object_summary(parsed_header),
            "actions": {"available": False, "count": None, "stream": []},
            "chat": {
                "available": True,
                "count": len(trailing_chat),
                "stream": trailing_chat,
            },
        }

    actions = evidence.get("actions") if isinstance(evidence.get("actions"), dict) else {}
    resignation_timeline = actions.get("resignation_timeline") or []
    resigned_player_numbers = sorted(
        {
            number
            for event in resignation_timeline
            if isinstance(event, dict)
            and (number := _safe_int(event.get("player_number"))) is not None
        }
    )
    names_by_number = {
        _safe_int(player.get("number")): player.get("name") for player in players
    }
    resigned_player_names = [
        names_by_number[number]
        for number in resigned_player_numbers
        if names_by_number.get(number)
    ]
    duration_ms = _safe_int(actions.get("duration_ms"))
    duration_seconds = _normalize_mgz_duration_seconds(duration_ms) or 0
    body_available = actions.get("available") is True
    game_type = str(_safe_get(lobby, "game_type", "Unknown"))
    original_failure = normalize_failure_signature(parse_error, stage="header")
    key_events = {
        "completed": bool(resigned_player_numbers),
        "header_trailing_bytes_recovery": True,
        "header_fragment_boundary": "after_complete_lobby_before_termination",
        "header_framing_anomaly": (
            "extra_length_prefixed_lobby_chat_after_declared_count"
        ),
        "header_failure_signature": original_failure["signature"],
        "trailing_header_byte_size": len(trailing_bytes),
        "trailing_header_sha256": hashlib.sha256(trailing_bytes).hexdigest(),
        "recovered_trailing_lobby_chat_count": len(trailing_chat),
        "body_stream_recovery": body_available,
        "body_stream_complete": actions.get("body_stream_complete") if body_available else False,
        "body_byte_size": actions.get("body_byte_size") if body_available else max(0, len(file_bytes) - header_length),
        "body_operation_count": actions.get("operation_count") if body_available else None,
        "postgame_available": False,
        "postgame_packet_present": bool((actions.get("type_counts") or {}).get("postgame")),
        "has_scores": False,
        "has_achievements": False,
        "player_score_count": 0,
        "achievement_player_count": 0,
        "achievement_shell_count": 0,
        "has_achievement_shell": False,
        "platform_id": "hd" if _safe_get(hd, "multiplayer") else None,
        "platform_match_id": _header_platform_match_id(parsed_header),
        "rated": bool(_safe_get(hd, "is_ranked")) if hd else None,
        "restored": restore_time_ms > 0,
        "restore_time_ms": restore_time_ms,
        "duration_source": "mgz.fast.body.sync_accumulation" if body_available else None,
        "resigned_player_numbers": resigned_player_numbers,
        "resigned_player_names": resigned_player_names,
        "team_source": "hd_lobby_team_array",
        "diplomacy": diplomacy,
        "settings": {
            "type": game_type,
            "population_limit": _safe_int(_safe_get(lobby, "population_limit")),
            "lock_teams": bool(_safe_get(lobby, "lock_teams")),
            "treaty_length": _safe_int(_safe_get(_safe_get(lobby, "de"), "treaty_length")),
        },
    }
    if body_failure:
        key_events["body_failure_signature"] = body_failure["signature"]

    stats = {
        "game_version": str(_safe_get(parsed_header, "version", "Unknown")),
        "map": map_payload,
        "game_type": game_type,
        "duration": duration_seconds,
        "game_duration": duration_seconds,
        "players": players,
        "winner": "Unknown",
        "event_types": sorted((actions.get("type_counts") or {}).keys()),
        "key_events": key_events,
        "completed": bool(resigned_player_numbers),
        "disconnect_detected": body_available and not bool(resigned_player_numbers),
        "parse_reason": (
            "hd_trailing_header_body_recovery"
            if body_available
            else "hd_trailing_header_only_recovery"
        ),
    }
    dt = extract_datetime_from_filename(replay_path)
    stats["played_on"] = dt.isoformat() if dt else None
    stats = _apply_completion_metadata(stats)
    stats = _maybe_apply_hd_early_exit_rules(stats, apply_hd_early_exit_rules)
    if capture_engine_evidence:
        stats["_engine_evidence"] = evidence
        if body_failure:
            stats["_engine_evidence_failure"] = body_failure
    logging.warning(
        "⚠️ HD trailing-header/body fallback used for %s after termination check failed",
        replay_path,
    )
    return stats


def _extract_hd_metadata_fragment_players(parsed_header):
    hd = _safe_get(parsed_header, "hd")
    declared_count = _safe_int(_safe_get(hd, "num_players")) or 0
    raw_players = list(_safe_get(hd, "players", []) or [])[:declared_count]
    if declared_count < 2 or declared_count > 8 or len(raw_players) != declared_count:
        return None

    players = []
    for slot_number, raw_player in enumerate(raw_players, start=1):
        player_type = _safe_get(raw_player, "type")
        player_type_name = str(
            getattr(player_type, "name", player_type) or ""
        ).casefold()
        ai_base_name = _safe_decode_text(_safe_get(raw_player, "ai_type"))
        ai_name = _safe_decode_text(_safe_get(raw_player, "ai_name"))
        name = _safe_decode_text(_safe_get(raw_player, "name"))
        if not name and player_type_name == "computer":
            ai_label = ai_name or ai_base_name or "Computer"
            name = f"{ai_label} [AI {slot_number}]"
        if not name:
            return None

        steam_id = _normalize_steam_id(_safe_get(raw_player, "steam_id"))
        raw_player_number = _safe_int(_safe_get(raw_player, "player_number"))
        civilization = _safe_int(_safe_get(raw_player, "civ_id"))
        player = {
            "name": name,
            "number": slot_number,
            "header_player_number": raw_player_number,
            "player_number_source": "hd_metadata_slot_ordinal",
            "player_number_conflict": raw_player_number != slot_number,
            "civilization": civilization,
            "civilization_name": _normalize_civilization_name(civilization),
            "winner": None,
            "score": None,
            "user_id": steam_id,
            "steam_id": steam_id,
            "steam_rm_rating": _normalize_rating(
                _safe_get(raw_player, "hd_dm_rating")
            ),
            "steam_dm_rating": _normalize_rating(
                _safe_get(raw_player, "hd_rm_rating")
            ),
            "rate_snapshot": None,
            "eapm": None,
            "position": None,
            "color_id": _safe_int(_safe_get(raw_player, "color_id")),
            "team_id": _safe_int(_safe_get(raw_player, "team_index")),
            "team_id_source": "hd_platform_metadata",
            "human": (
                True
                if player_type_name == "human"
                else False if player_type_name == "computer" else None
            ),
            "ai_base_name": ai_base_name,
            "ai_name": ai_name,
            "header_only": True,
            "metadata_fragment": True,
        }
        players.append(player)

    if len({player["name"].casefold() for player in players}) != len(players):
        return None
    return players


def _hd_metadata_fragment_diplomacy(players):
    grouped = {}
    for player in players:
        team_id = player.get("team_id")
        if team_id is None:
            return {
                "source": "hd_platform_metadata",
                "coherent": False,
                "type": "Unknown",
                "team_size": "unknown",
                "teams": [],
            }
        grouped.setdefault(team_id, []).append(player)

    team_sizes = sorted(len(team_players) for team_players in grouped.values())
    if len(players) == 2 and team_sizes == [1, 1]:
        diplomacy_type = "1v1"
        team_size = "1v1"
    elif len(grouped) == 2 and len(players) > 2:
        diplomacy_type = "TG"
        team_size = "v".join(str(size) for size in team_sizes)
    elif len(grouped) == len(players):
        diplomacy_type = "FFA"
        team_size = "FFA"
    else:
        diplomacy_type = "Other"
        team_size = "v".join(str(size) for size in team_sizes)
    return {
        "source": "hd_platform_metadata",
        "coherent": True,
        "type": diplomacy_type,
        "team_size": team_size,
        "teams": [
            {
                "team_id": team_id,
                "player_numbers": [player["number"] for player in team_players],
                "players": [player["name"] for player in team_players],
            }
            for team_id, team_players in sorted(grouped.items(), key=lambda item: item[0])
        ],
    }


def _decompress_hd_saved_game_snapshot(file_bytes):
    """Inflate one exact HD saved-game container with a hard output bound."""
    inflater = zlib.decompressobj(wbits=-15)
    decompressed = inflater.decompress(
        file_bytes,
        _HD_SAVED_GAME_MAX_DECOMPRESSED_BYTES + 1,
    )
    if (
        len(decompressed) > _HD_SAVED_GAME_MAX_DECOMPRESSED_BYTES
        or inflater.unconsumed_tail
    ):
        raise ValueError("HD saved-game snapshot exceeds decompression limit")
    decompressed += inflater.flush()
    if len(decompressed) > _HD_SAVED_GAME_MAX_DECOMPRESSED_BYTES:
        raise ValueError("HD saved-game snapshot exceeds decompression limit")
    if not inflater.eof or inflater.unused_data:
        raise ValueError(
            "HD saved-game container is not one exact raw-deflate stream: "
            f"eof={inflater.eof} unused={len(inflater.unused_data)}"
        )
    return decompressed


def _saved_game_metadata_type(parsed_header):
    hd = _safe_get(parsed_header, "hd")
    game_type_id = _safe_int(_safe_get(hd, "game_type"))
    if game_type_id == 9:
        return MGZ_HD_TYPE9_GAME_TYPE_LABEL, game_type_id
    return (
        f"HD game type {game_type_id}" if game_type_id is not None else "Unknown",
        game_type_id,
    )


def _parse_hd_saved_game_snapshot_bytes(
    replay_path,
    file_bytes,
    *,
    capture_engine_evidence=False,
):
    """Decode `.aoe2mpgame` only into a private, non-final candidate snapshot."""
    try:
        decompressed = _decompress_hd_saved_game_snapshot(file_bytes)
    except Exception as container_error:
        return (
            None,
            normalize_failure_signature(container_error, stage="saved_game_container"),
            "mgz_hd_saved_game_container_failed",
        )

    structure_failure = None
    structure_scope = "complete_saved_game_snapshot"
    try:
        parsed_header = _HD_SAVED_GAME_SNAPSHOT.parse(decompressed)
        if bytes(_safe_get(parsed_header, "saved_game_tail", b"") or b""):
            raise ValueError("complete saved-game snapshot parser left trailing bytes")
        parse_mode = "mgz_hd_saved_game_snapshot"
    except Exception as full_error:
        structure_failure = normalize_failure_signature(
            full_error,
            stage="saved_game_snapshot_structure",
        )
        try:
            parsed_header = _HD_SAVED_GAME_INITIAL_PREFIX.parse(decompressed)
            structure_scope = "initial_state_prefix"
            parse_mode = "mgz_hd_saved_game_initial_prefix"
        except Exception:
            try:
                parsed_header = _HD_SAVED_GAME_MAP_PREFIX.parse(decompressed)
                structure_scope = "map_and_platform_prefix"
                parse_mode = "mgz_hd_saved_game_map_prefix"
            except Exception as prefix_error:
                return (
                    None,
                    normalize_failure_signature(
                        prefix_error,
                        stage="saved_game_snapshot_prefix",
                    ),
                    "mgz_hd_saved_game_snapshot_failed",
                )

    if _safe_get(parsed_header, "version") is not Version.HD:
        return (
            None,
            normalize_failure_signature(
                ValueError("saved-game snapshot is not AoE2 HD"),
                stage="saved_game_snapshot_identity",
            ),
            "mgz_hd_saved_game_snapshot_failed",
        )

    initial_available = structure_scope != "map_and_platform_prefix"
    if initial_available:
        players = _extract_header_player_rows(parsed_header)
    else:
        players = _extract_hd_metadata_fragment_players(parsed_header) or []
        # HD platform team indexes are not sufficient truth for this damaged
        # snapshot variant. Preserve roster identity without inventing teams.
        for player in players:
            player["team_id"] = None
            player["team_id_source"] = "unavailable_without_initial_diplomacy"

    player_keys = {
        str(player.get("name") or "").strip().casefold()
        for player in players
        if isinstance(player, dict)
    }
    if (
        not players
        or len(players) > 8
        or len(player_keys) != len(players)
        or "" in player_keys
    ):
        return (
            None,
            normalize_failure_signature(
                ValueError("saved-game snapshot roster is not unique and complete"),
                stage="saved_game_snapshot_roster",
            ),
            "mgz_hd_saved_game_snapshot_failed",
        )

    if initial_available:
        diplomacy = _fragment_diplomacy_groups(parsed_header, players)
        if not diplomacy:
            return (
                None,
                normalize_failure_signature(
                    ValueError("saved-game snapshot diplomacy is not coherent"),
                    stage="saved_game_snapshot_diplomacy",
                ),
                "mgz_hd_saved_game_snapshot_failed",
            )
    else:
        diplomacy = {
            "source": "unavailable_without_initial_diplomacy",
            "coherent": False,
            "type": "Unknown",
            "team_size": "unknown",
            "teams": [],
        }

    map_payload = _header_map_payload(parsed_header)
    hd = _safe_get(parsed_header, "hd")
    custom_map = _safe_decode_text(_safe_get(hd, "custom_random_map_file"))
    if custom_map:
        map_payload["name"] = custom_map.removesuffix(".rms")
        map_payload["custom"] = True
        map_payload["custom_filename"] = custom_map
    map_snapshot = {
        key: value
        for key, value in map_payload.items()
        if key in {"id", "name", "size", "dimension", "custom"}
    }

    initial = _safe_get(parsed_header, "initial")
    restore_time_ms = (
        _safe_int(_safe_get(initial, "restore_time")) if initial_available else None
    )
    lobby = _safe_get(parsed_header, "lobby")
    if structure_scope == "complete_saved_game_snapshot" and lobby is not None:
        game_type = str(_safe_get(lobby, "game_type", "Unknown"))
        game_type_id = _safe_int(_safe_get(lobby, "game_type_id"))
    else:
        game_type, game_type_id = _saved_game_metadata_type(parsed_header)

    undecoded_tail = bytes(
        _safe_get(parsed_header, "saved_game_tail", b"") or b""
    )
    structure_complete = structure_scope == "complete_saved_game_snapshot"
    key_events = {
        "completed": False,
        "saved_game_snapshot": True,
        "artifact_role": "saved_game_snapshot",
        "final_battle_eligible": False,
        "settlement_evidence_eligible": False,
        "result_trusted": False,
        "saved_game_structure_scope": structure_scope,
        "saved_game_structure_complete": structure_complete,
        "saved_game_raw_deflate_complete": True,
        "saved_game_decompressed_byte_size": len(decompressed),
        "saved_game_decompressed_sha256": hashlib.sha256(decompressed).hexdigest(),
        "saved_game_snapshot_word": _safe_int(
            _safe_get(parsed_header, "save_snapshot_word")
        ),
        "saved_game_decoded_prefix_byte_size": _safe_int(
            _safe_get(parsed_header, "saved_game_offset")
        ),
        "saved_game_undecoded_tail_byte_size": len(undecoded_tail),
        "saved_game_undecoded_tail_sha256": (
            hashlib.sha256(undecoded_tail).hexdigest() if undecoded_tail else None
        ),
        "replay_body_available": False,
        "postgame_available": False,
        "postgame_packet_present": False,
        "has_scores": False,
        "has_achievements": False,
        "player_score_count": 0,
        "achievement_player_count": 0,
        "achievement_shell_count": 0,
        "has_achievement_shell": False,
        "platform_id": "hd" if _safe_get(hd, "multiplayer") else None,
        "platform_match_id": _header_platform_match_id(parsed_header),
        "rated": bool(_safe_get(hd, "is_ranked")) if hd else None,
        "restored": restore_time_ms is not None and restore_time_ms > 0,
        "restore_time_ms": restore_time_ms,
        "snapshot_elapsed_seconds": (
            _normalize_mgz_duration_seconds(restore_time_ms)
            if restore_time_ms is not None
            else None
        ),
        "duration_source": None,
        "resigned_player_numbers": [],
        "resigned_player_names": [],
        "team_source": diplomacy["source"],
        "diplomacy": diplomacy,
        "settings": {
            "type": game_type,
            "type_id": game_type_id,
            "difficulty": str(_safe_get(hd, "difficulty") or "Unknown"),
            "population_limit": (
                _safe_int(_safe_get(lobby, "population_limit"))
                if lobby is not None
                else _safe_int(_safe_get(hd, "population_limit"))
            ),
            "lock_teams": (
                bool(_safe_get(lobby, "lock_teams"))
                if lobby is not None
                else bool(_safe_get(hd, "lock_teams"))
            ),
        },
    }
    if structure_failure:
        key_events["saved_game_structure_failure_signature"] = structure_failure[
            "signature"
        ]

    stats = {
        "game_version": str(_safe_get(parsed_header, "version", "Unknown")),
        "map": map_payload,
        "game_type": game_type,
        # A saved snapshot's elapsed clock is not a completed replay duration.
        "duration": 0,
        "game_duration": 0,
        "players": players,
        "winner": "Unknown",
        "event_types": [],
        "key_events": key_events,
        "completed": False,
        "disconnect_detected": False,
        "parse_reason": (
            "hd_saved_game_snapshot"
            if structure_complete
            else f"hd_saved_game_snapshot_{structure_scope}"
        ),
    }
    dt = extract_datetime_from_filename(replay_path)
    stats["played_on"] = dt.isoformat() if dt else None
    stats = _apply_completion_metadata(stats)

    if capture_engine_evidence:
        stats["_engine_evidence"] = {
            "dataset": {
                "source": f"aoe2mpgame_raw_deflate_{structure_scope}",
                "artifact_role": "saved_game_snapshot",
                "validated_gameplay_truth": False,
                "final_battle_eligible": False,
                "settlement_evidence_eligible": False,
                "raw_deflate_complete": True,
                "snapshot_structure_complete": structure_complete,
            },
            "diplomacy": diplomacy,
            "map_snapshot": map_snapshot,
            "initial_objects": (
                _fragment_initial_object_summary(parsed_header)
                if initial_available
                else {
                    "snapshot_scope": "unavailable_without_initial_state",
                    "object_count": None,
                    "objects": [],
                }
            ),
            "actions": {
                "available": False,
                "count": None,
                "stream": [],
            },
            "chat": {
                "available": False,
                "count": None,
                "stream": [],
                "scope": "replay_body_unavailable_in_saved_game_snapshot",
            },
        }

    logging.warning(
        "⚠️ candidate-only HD saved-game snapshot decoder used for %s (%s)",
        replay_path,
        structure_scope,
    )
    return stats, structure_failure, parse_mode


def _parse_hd_metadata_fragment_body_bytes(
    replay_path,
    file_bytes,
    parse_error,
    *,
    capture_engine_evidence=False,
    apply_hd_early_exit_rules=True,
):
    if "-> lobby" in str(parse_error).casefold():
        return None
    try:
        decompressed = decompress_mgz_header(io.BytesIO(file_bytes)).getvalue()
        parsed_header = _HD_METADATA_PREFIX.parse(decompressed)
    except Exception as fragment_error:
        logging.error("❌ HD metadata fragment fallback failed: %s", fragment_error)
        return None
    if _safe_get(parsed_header, "version") is not Version.HD:
        return None

    players = _extract_hd_metadata_fragment_players(parsed_header)
    if not players:
        logging.error("❌ HD metadata fragment roster validation failed")
        return None
    diplomacy = _hd_metadata_fragment_diplomacy(players)
    hd = _safe_get(parsed_header, "hd")
    custom_map = _safe_decode_text(_safe_get(hd, "custom_random_map_file"))
    map_payload = _header_map_payload(parsed_header)
    if custom_map:
        map_payload["name"] = custom_map.removesuffix(".rms")
        map_payload["custom"] = True
        map_payload["custom_filename"] = custom_map
    map_snapshot = {
        key: value
        for key, value in map_payload.items()
        if key in {"id", "name", "size", "dimension", "custom"}
    }
    unavailable_objects = {
        "snapshot_scope": "unavailable_in_hd_metadata_fragment",
        "object_count": None,
        "objects": [],
    }
    header_length = struct.unpack_from("<I", file_bytes, 0)[0]
    body_failure = None
    try:
        evidence = capture_fragment_body_evidence(
            file_bytes,
            header_length=header_length,
            restore_time_ms=0,
            players=players,
            map_snapshot=map_snapshot,
            diplomacy=diplomacy,
            initial_objects=unavailable_objects,
        )
        evidence["dataset"] = {
            "source": "hd_platform_metadata_plus_direct_body",
            "validated_gameplay_truth": False,
        }
    except Exception as body_error:
        body_failure = normalize_failure_signature(body_error, stage="body_fragment")
        evidence = {
            "dataset": {
                "source": "hd_platform_metadata_only",
                "validated_gameplay_truth": False,
            },
            "diplomacy": diplomacy,
            "map_snapshot": map_snapshot,
            "initial_objects": unavailable_objects,
            "actions": {"available": False, "count": None, "stream": []},
            "chat": {"available": False, "count": None, "stream": []},
        }

    actions = evidence.get("actions") if isinstance(evidence.get("actions"), dict) else {}
    resignation_timeline = actions.get("resignation_timeline") or []
    resigned_player_numbers = sorted(
        {
            number
            for event in resignation_timeline
            if isinstance(event, dict)
            and (number := _safe_int(event.get("player_number"))) is not None
        }
    )
    names_by_number = {player["number"]: player["name"] for player in players}
    resigned_player_names = [
        names_by_number[number]
        for number in resigned_player_numbers
        if number in names_by_number
    ]
    duration_ms = _safe_int(actions.get("duration_ms"))
    duration_seconds = _normalize_mgz_duration_seconds(duration_ms) or 0
    body_available = actions.get("available") is True
    game_type_id = _safe_int(_safe_get(hd, "game_type"))
    game_type = (
        MGZ_HD_TYPE9_GAME_TYPE_LABEL
        if game_type_id == 9
        else f"HD game type {game_type_id}" if game_type_id is not None else "Unknown"
    )
    original_failure = normalize_failure_signature(parse_error, stage="header")
    player_number_conflicts = sum(
        bool(player.get("player_number_conflict")) for player in players
    )
    key_events = {
        "completed": bool(resigned_player_numbers),
        "header_metadata_fragment_recovery": True,
        "header_fragment_boundary": "after_hd_platform_metadata",
        "header_failure_signature": original_failure["signature"],
        "body_stream_recovery": body_available,
        "body_stream_complete": actions.get("body_stream_complete") if body_available else False,
        "body_byte_size": actions.get("body_byte_size") if body_available else max(0, len(file_bytes) - header_length),
        "body_operation_count": actions.get("operation_count") if body_available else None,
        "postgame_available": False,
        "postgame_packet_present": bool((actions.get("type_counts") or {}).get("postgame")),
        "has_scores": False,
        "has_achievements": False,
        "player_score_count": 0,
        "achievement_player_count": 0,
        "achievement_shell_count": 0,
        "has_achievement_shell": False,
        "platform_id": "hd" if _safe_get(hd, "multiplayer") else None,
        "platform_match_id": _header_platform_match_id(parsed_header),
        "rated": bool(_safe_get(hd, "is_ranked")) if hd else None,
        "restored": None,
        "duration_source": "mgz.fast.body.sync_accumulation" if body_available else None,
        "resigned_player_numbers": resigned_player_numbers,
        "resigned_player_names": resigned_player_names,
        "team_source": diplomacy["source"],
        "diplomacy": diplomacy,
        "player_number_source": "hd_metadata_slot_ordinal",
        "header_player_number_conflict_count": player_number_conflicts,
        "settings": {
            "type": game_type,
            "type_id": game_type_id,
            "difficulty": str(_safe_get(hd, "difficulty") or "Unknown"),
            "population_limit": _safe_int(_safe_get(hd, "population_limit")),
            "speed": _safe_get(hd, "speed"),
            "lock_teams": bool(_safe_get(hd, "lock_teams")),
            "treaty_length": _safe_int(_safe_get(hd, "treaty_length")),
        },
    }
    if body_failure:
        key_events["body_failure_signature"] = body_failure["signature"]

    stats = {
        "game_version": str(_safe_get(parsed_header, "version", "Unknown")),
        "map": map_payload,
        "game_type": game_type,
        "duration": duration_seconds,
        "game_duration": duration_seconds,
        "players": players,
        "winner": "Unknown",
        "event_types": sorted((actions.get("type_counts") or {}).keys()),
        "key_events": key_events,
        "completed": bool(resigned_player_numbers),
        "disconnect_detected": body_available and not bool(resigned_player_numbers),
        "parse_reason": (
            "hd_metadata_fragment_body_recovery"
            if body_available
            else "hd_metadata_fragment_only_recovery"
        ),
    }
    dt = extract_datetime_from_filename(replay_path)
    stats["played_on"] = dt.isoformat() if dt else None
    stats = _apply_completion_metadata(stats)
    stats = _maybe_apply_hd_early_exit_rules(stats, apply_hd_early_exit_rules)
    if capture_engine_evidence:
        stats["_engine_evidence"] = evidence
        if body_failure:
            stats["_engine_evidence_failure"] = body_failure
    logging.warning(
        "⚠️ HD metadata fragment/body fallback used for %s after header parse failed",
        replay_path,
    )
    return stats


def _parse_header_only_bytes(replay_path, file_bytes, parse_error):
    try:
        parsed_header = header.parse(file_bytes)
    except Exception as header_error:
        logging.error(f"❌ header fallback parse failed: {header_error}")
        return None

    map_payload = _header_map_payload(parsed_header)
    players = _extract_header_player_rows(parsed_header)
    hd = _safe_get(parsed_header, "hd")
    platform_match_id = _header_platform_match_id(parsed_header)
    key_events = {
        "completed": False,
        "header_only_fallback": True,
        "summary_parse_error": str(parse_error)[:160],
        "postgame_available": False,
        "has_scores": False,
        "has_achievements": False,
        "player_score_count": 0,
        "achievement_player_count": 0,
        "achievement_shell_count": 0,
        "has_achievement_shell": False,
        "platform_id": "hd" if _safe_get(hd, "multiplayer") else None,
        "platform_match_id": platform_match_id,
        "rated": bool(_safe_get(hd, "is_ranked")) if hd else None,
    }

    dt = extract_datetime_from_filename(replay_path)
    stats = {
        "game_version": str(_safe_get(parsed_header, "version", "Unknown")),
        "map": map_payload,
        "game_type": str(_safe_get(_safe_get(parsed_header, "lobby"), "game_type", "Unknown")),
        "duration": 0,
        "players": players,
        "winner": "Unknown",
        "event_types": [],
        "key_events": key_events,
        "completed": False,
        "disconnect_detected": False,
        "parse_reason": "header_only_summary_fallback",
        "played_on": dt.isoformat() if dt else None,
    }
    logging.warning(
        "⚠️ header-only replay fallback used for %s after summary parse failed: %s",
        replay_path,
        parse_error,
    )
    return stats


def _safe_match_position(position_obj):
    if position_obj is None:
        return None
    try:
        x = getattr(position_obj, "x", None)
        y = getattr(position_obj, "y", None)
        if x is None or y is None:
            return None
        return [float(x), float(y)]
    except Exception:
        return None


def _safe_match_team_id(team_id):
    if team_id is None:
        return None
    try:
        if isinstance(team_id, (set, frozenset, list, tuple)):
            values = [int(value) for value in team_id]
            return min(values) if values else None
        return int(team_id)
    except Exception:
        return None


def _parse_match_live_fallback_bytes(
    replay_path,
    file_bytes,
    parse_error,
    *,
    capture_engine_evidence=False,
):
    try:
        match = parse_match(io.BytesIO(file_bytes))
    except Exception as fallback_error:
        logging.error(f"❌ parse_match live fallback failed: {fallback_error}")
        return None

    raw_players = list(getattr(match, "players", []) or [])
    players = []

    for raw_player in raw_players:
        civilization_id = _safe_int(getattr(raw_player, "civilization_id", None))
        player = {
            "name": getattr(raw_player, "name", None) or "Unknown",
            "number": _safe_int(getattr(raw_player, "number", None)),
            "civilization": civilization_id,
            "civilization_name": getattr(raw_player, "civilization", None)
            or _normalize_civilization_name(civilization_id),
            "winner": bool(getattr(raw_player, "winner", False)),
            "score": None,
            "user_id": None,
            "steam_id": None,
            "steam_rm_rating": None,
            "steam_dm_rating": None,
            "rate_snapshot": _normalize_rating(getattr(raw_player, "rate_snapshot", None)),
            "eapm": _normalize_rating(getattr(raw_player, "eapm", None)),
            "position": _safe_match_position(getattr(raw_player, "position", None)),
            "color_id": _normalize_rating(getattr(raw_player, "color_id", None)),
            "team_id": _safe_match_team_id(getattr(raw_player, "team_id", None)),
            "human": True,
            "prefer_random": getattr(raw_player, "prefer_random", None),
            "mvp": None,
            "cheater": False,
            "live_fallback": True,
        }
        players.append(player)

    duration_obj = getattr(match, "duration", None)
    try:
        duration_seconds = int(duration_obj.total_seconds()) if duration_obj is not None else 0
    except Exception:
        duration_seconds = 0

    raw_map = getattr(match, "map", None)
    map_name = getattr(raw_map, "name", None) if raw_map is not None else None
    if not map_name and raw_map is not None:
        map_name = str(raw_map)

    completed = bool(getattr(match, "completed", False))
    winner = next((player["name"] for player in players if player.get("winner")), None)

    action_types = []
    seen_action_types = set()
    for action in list(getattr(match, "actions", []) or [])[:5000]:
        action_type = getattr(action, "type", None)
        label = getattr(action_type, "name", None) or str(action_type or "")
        label = label.replace("Action.", "").lower()
        if not label or label in seen_action_types:
            continue
        seen_action_types.add(label)
        action_types.append(label)

    file_info = getattr(match, "file", None)
    dt = extract_datetime_from_filename(replay_path)

    stats = {
        "game_version": str(getattr(match, "version", "Unknown")),
        "map": {
            "name": map_name or "Unknown",
            "size": "Unknown",
        },
        "game_type": str(getattr(match, "type", None) or getattr(match, "game_version", None) or "Unknown"),
        "duration": duration_seconds,
        "game_duration": duration_seconds,
        "players": players,
        "winner": winner or "Unknown",
        "event_types": action_types,
        "key_events": {
            "completed": completed,
            "parse_match_live_fallback": True,
            "summary_parse_error": str(parse_error)[:240],
            "postgame_available": False,
            "has_scores": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "achievement_shell_count": 0,
            "has_achievement_shell": False,
            "platform_id": "hd",
            "dataset": getattr(match, "dataset", None),
            "dataset_id": getattr(match, "dataset_id", None),
            "game_version": getattr(match, "game_version", None),
            "type": getattr(match, "type", None),
            "type_id": getattr(match, "type_id", None),
            "speed": getattr(match, "speed", None),
            "speed_id": getattr(match, "speed_id", None),
            "difficulty": getattr(match, "difficulty", None),
            "difficulty_id": getattr(match, "difficulty_id", None),
            "population_limit": getattr(match, "population", None),
            "map_reveal_choice": getattr(match, "map_reveal", None),
            "lock_teams": getattr(match, "lock_teams", None),
            "cheats": getattr(match, "cheats", None),
            "rated": getattr(match, "rated", None),
            "restored": bool(getattr(match, "restored", False)),
            "duration_source": "mgz_parse_match_timedelta",
            "file_language": getattr(file_info, "language", None) if file_info is not None else None,
            "file_size": getattr(file_info, "size", None) if file_info is not None else None,
            "perspective_player_name": str(getattr(file_info, "perspective", "")) if file_info is not None else None,
        },
        "completed": completed,
        "disconnect_detected": False,
        "parse_reason": "hd_live_parse_match_fallback",
        "played_on": dt.isoformat() if dt else None,
    }

    if capture_engine_evidence:
        try:
            stats["_engine_evidence"] = capture_model_evidence(match)
        except Exception as evidence_error:
            stats["_engine_evidence_failure"] = normalize_failure_signature(
                evidence_error,
                stage="model_evidence",
            )

    logging.warning(
        "⚠️ parse_match live fallback used for %s after summary parse failed: %s",
        replay_path,
        parse_error,
    )
    return stats


def _parse_sync_bytes_with_diagnostics(
    replay_path,
    file_bytes,
    apply_hd_early_exit_rules=True,
    *,
    capture_engine_evidence=False,
):
    _patch_mgz_hd_type9_game_type()
    if (
        capture_engine_evidence
        and os.path.splitext(str(replay_path))[1].casefold() == ".aoe2mpgame"
    ):
        # Saved-game snapshots are a private Engine Room evidence lane. The
        # normal public parser must continue rejecting them as final replays.
        return _parse_hd_saved_game_snapshot_bytes(
            replay_path,
            file_bytes,
            capture_engine_evidence=True,
        )
    stage = "header"
    try:
        h = header.parse(file_bytes)
        stage = "summary"
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
        settings_summary = _extract_settings_summary(s)
        normalized_game_type = settings_summary.get("type")
        enum_game_type_name = getattr(normalized_game_type, "name", None)
        if isinstance(enum_game_type_name, str) and enum_game_type_name.strip():
            normalized_game_type = enum_game_type_name.strip()
        elif isinstance(normalized_game_type, str):
            normalized_game_type = normalized_game_type.strip() or None
        elif normalized_game_type is not None:
            normalized_game_type = str(normalized_game_type).strip() or None

        stats = {
            "game_version": str(h.version),
            "map": {
                "name": s.get_map().get("name", "Unknown"),
                "size": s.get_map().get("size", "Unknown"),
            },
            # Summary.get_version() is a parser/version tuple, not the lobby's
            # gameplay type.  The normalized settings lane is the canonical HD
            # source for RM, DM, TurboRandom9, and related game-type labels.
            "game_type": normalized_game_type or "Unknown",
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
            # HD rate_snapshot follows the visible DM-looking value, not the visible RM line.
            if rate_snapshot is not None and p_data["steam_dm_rating"] is None:
                p_data["steam_dm_rating"] = rate_snapshot
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
        if settings_summary:
            stats["key_events"]["settings"] = settings_summary
        if platform_ratings:
            stats["key_events"]["platform_ratings"] = platform_ratings
        chat_preview = _extract_chat_preview(chat)
        if chat_preview:
            stats["key_events"]["chat_preview"] = chat_preview

        chat_transcript = _extract_chat_transcript(chat)
        if chat_transcript:
            stats["key_events"]["chat_transcript"] = chat_transcript
            stats["key_events"]["chat_transcript_count"] = len(chat_transcript)
            stats["key_events"]["chat_transcript_truncated"] = len(chat) > len(chat_transcript)

        stats["completed"] = completed
        stats["disconnect_detected"] = not completed and len(resigned_player_numbers) == 0
        stats = _apply_completion_metadata(stats)

        dt = extract_datetime_from_filename(replay_path)
        stats["played_on"] = dt.isoformat() if dt else None
        stats = _maybe_apply_hd_early_exit_rules(stats, apply_hd_early_exit_rules)

        if capture_engine_evidence:
            try:
                stats["_engine_evidence"] = capture_summary_evidence(s)
            except Exception as evidence_error:
                logging.warning(
                    "⚠️ parser-engine evidence extraction failed for %s: %s",
                    replay_path,
                    evidence_error,
                )
                stats["_engine_evidence_failure"] = normalize_failure_signature(
                    evidence_error,
                    stage="summary_evidence",
                )

        logging.info(f"✅ parse_replay_full => {replay_path}")
        return stats, None, "mgz_full_summary"

    except Exception as e:
        logging.error(f"❌ sync parse error: {e}")
        diagnostic = normalize_failure_signature(e, stage=stage)
        live_fallback = _parse_match_live_fallback_bytes(
            replay_path,
            file_bytes,
            e,
            capture_engine_evidence=capture_engine_evidence,
        )
        if live_fallback:
            if apply_hd_early_exit_rules:
                live_fallback["parse_reason"] = "hd_final_parse_match_fallback"
                live_fallback.setdefault("key_events", {})["parse_match_final_fallback"] = True
            return live_fallback, diagnostic, "mgz_parse_match_fallback"
        trailing_header_fallback = _parse_hd_trailing_header_body_bytes(
            replay_path,
            file_bytes,
            e,
            capture_engine_evidence=capture_engine_evidence,
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        )
        if trailing_header_fallback:
            return (
                trailing_header_fallback,
                diagnostic,
                "mgz_hd_trailing_header_body_fallback",
            )
        fragment_fallback = _parse_hd_fragment_header_body_bytes(
            replay_path,
            file_bytes,
            e,
            capture_engine_evidence=capture_engine_evidence,
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        )
        if fragment_fallback:
            return (
                fragment_fallback,
                diagnostic,
                "mgz_hd_fragment_header_body_fallback",
            )
        metadata_fragment_fallback = _parse_hd_metadata_fragment_body_bytes(
            replay_path,
            file_bytes,
            e,
            capture_engine_evidence=capture_engine_evidence,
            apply_hd_early_exit_rules=apply_hd_early_exit_rules,
        )
        if metadata_fragment_fallback:
            return (
                metadata_fragment_fallback,
                diagnostic,
                "mgz_hd_metadata_fragment_body_fallback",
            )
        header_fallback = _parse_header_only_bytes(replay_path, file_bytes, e)
        if header_fallback:
            return header_fallback, diagnostic, "mgz_header_only_fallback"
        return None, diagnostic, "mgz_failed"


def _parse_sync_bytes(replay_path, file_bytes, apply_hd_early_exit_rules=True):
    """Backward-compatible internal projection parser."""
    parsed, _diagnostic, _parse_mode = _parse_sync_bytes_with_diagnostics(
        replay_path,
        file_bytes,
        apply_hd_early_exit_rules,
    )
    return parsed

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
