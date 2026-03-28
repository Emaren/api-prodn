import argparse
import json
import os
import sys
from pathlib import Path

import psycopg

SCRIPT_DIR = Path(__file__).resolve().parent
API_ROOT = SCRIPT_DIR.parent
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from utils.replay_parser import _parse_sync_bytes  # noqa: E402
from utils.extract_datetime import extract_datetime_from_filename  # noqa: E402


EARLY_EXIT_PARSE_REASON = "hd_early_exit_under_60s"
DEFAULT_REPLAY_DIR = Path(
    "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame"
)


def iso_or_none(value):
    return value if isinstance(value, str) and value.strip() else None


def derived_parse_reason(row_parse_source, parsed_parse_reason):
    if isinstance(parsed_parse_reason, str) and parsed_parse_reason.strip():
        return parsed_parse_reason.strip()

    source = str(row_parse_source or "").strip()
    if source == "watcher_final":
        return "watcher_final_submission"
    if source == "watcher_live":
        return "watcher_live_iteration"
    if source == "json_parse":
        return "json_submission"
    if source == "file_upload":
        return "watcher_or_browser"
    return "unspecified"


def norm_name(value):
    return str(value or "").strip().lower()


def max_game_chat_timestamp_seconds(key_events):
    if not isinstance(key_events, dict):
        return 0

    preview = key_events.get("chat_preview")
    if not isinstance(preview, list):
        return 0

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

    return max_seconds


def match_uploader_player(players, row_user):
    if not isinstance(players, list) or not row_user:
        return None

    steam_id = str(row_user.get("steam_id") or "").strip()
    if steam_id:
        for player in players:
            if str(player.get("user_id") or "").strip() == steam_id:
                return player

    candidate_names = {
        norm_name(value)
        for value in [
            row_user.get("claimed_name"),
            row_user.get("in_game_name"),
            row_user.get("steam_persona_name"),
        ]
        if value
    }

    for player in players:
        if norm_name(player.get("name")) in candidate_names:
            return player

    return None


def infer_incomplete_uploader_outcome(snapshot, row_user):
    winner = snapshot.get("winner") or "Unknown"
    players = snapshot.get("players") if isinstance(snapshot.get("players"), list) else []
    key_events = snapshot.get("key_events") if isinstance(snapshot.get("key_events"), dict) else {}

    if winner not in {"", None, "Unknown"}:
        return snapshot
    if snapshot.get("parse_reason") == EARLY_EXIT_PARSE_REASON or key_events.get("no_rated_result"):
        return snapshot
    if key_events.get("completed") is not False:
        return snapshot
    if not key_events.get("rated"):
        return snapshot
    if len(players) != 2:
        return snapshot

    uploader_player = match_uploader_player(players, row_user)
    if not uploader_player:
        return snapshot

    uploader_name = str(uploader_player.get("name") or "").strip()
    opponents = [
        dict(player)
        for player in players
        if norm_name(player.get("name")) != norm_name(uploader_name)
    ]
    if len(opponents) != 1:
        return snapshot

    inferred_winner = str(opponents[0].get("name") or "").strip()
    if not inferred_winner:
        return snapshot

    patched_players = []
    for player in players:
        updated = dict(player)
        if norm_name(updated.get("name")) == norm_name(inferred_winner):
            updated["winner"] = True
        elif norm_name(updated.get("name")) == norm_name(uploader_name):
            updated["winner"] = False
        patched_players.append(updated)

    next_key_events = dict(key_events)
    next_key_events["winner_inference"] = {
        "type": "uploader_incomplete_1v1_opponent",
        "uploader_player": uploader_name,
        "inferred_winner": inferred_winner,
    }

    next_snapshot = dict(snapshot)
    next_snapshot["winner"] = inferred_winner
    next_snapshot["players"] = patched_players
    next_snapshot["disconnect_detected"] = True
    next_snapshot["parse_reason"] = "watcher_inferred_opponent_win_on_incomplete_1v1"
    next_snapshot["key_events"] = next_key_events
    return next_snapshot


def repair_inconsistent_early_exit_snapshot(snapshot, original_filename):
    if snapshot.get("parse_reason") != EARLY_EXIT_PARSE_REASON:
        return snapshot

    key_events = snapshot.get("key_events") if isinstance(snapshot.get("key_events"), dict) else {}
    suppressed_winner = str(key_events.get("suppressed_winner") or "").strip()
    max_chat_seconds = max_game_chat_timestamp_seconds(key_events)
    if not suppressed_winner or max_chat_seconds < 60:
        return snapshot

    players = snapshot.get("players") if isinstance(snapshot.get("players"), list) else []
    patched_players = []
    matched_winner = False
    for player in players:
        updated = dict(player)
        player_name = str(updated.get("name") or "").strip()
        if norm_name(player_name) == norm_name(suppressed_winner):
            updated["winner"] = True
            matched_winner = True
        elif player_name:
            updated["winner"] = False
        patched_players.append(updated)

    next_key_events = dict(key_events)
    next_key_events.pop("no_rated_result", None)
    next_key_events.pop("early_exit_under_60s", None)
    next_key_events.pop("early_exit_seconds", None)
    next_key_events["completed"] = False
    next_key_events["disconnect_detected"] = True
    next_key_events["duration_source"] = "chat_preview_seconds_override"
    next_key_events["duration_override_seconds"] = max_chat_seconds
    next_key_events["winner_inference"] = {
        "type": "legacy_in_game_chat_duration_override",
        "suppressed_winner": suppressed_winner,
        "max_game_chat_seconds": max_chat_seconds,
    }

    played_on = snapshot.get("played_on")
    if not played_on and original_filename:
        dt = extract_datetime_from_filename(original_filename)
        played_on = iso_or_none(dt.isoformat() if dt else None)

    return {
        **snapshot,
        "duration": max(int(snapshot.get("duration") or 0), max_chat_seconds),
        "game_duration": max(int(snapshot.get("game_duration") or 0), max_chat_seconds),
        "winner": suppressed_winner,
        "players": patched_players if matched_winner else players,
        "disconnect_detected": True,
        "parse_reason": "watcher_inferred_opponent_win_on_incomplete_1v1"
        if matched_winner and len(players) == 2
        else "watcher_inferred_backfill",
        "key_events": next_key_events,
        "played_on": played_on,
    }


def build_row_snapshot(row):
    (
        _game_id,
        _original_filename,
        game_version,
        map_payload,
        game_type,
        duration,
        game_duration,
        winner,
        players,
        event_types,
        key_events,
        disconnect_detected,
        parse_source,
        parse_reason,
        played_on,
        _user_uid,
        _steam_id,
        _in_game_name,
        _steam_persona_name,
    ) = row

    return {
        "game_version": game_version,
        "map": map_payload,
        "game_type": game_type,
        "duration": duration,
        "game_duration": game_duration,
        "winner": winner,
        "players": players or [],
        "event_types": event_types or [],
        "key_events": key_events or {},
        "disconnect_detected": bool(disconnect_detected),
        "parse_source": parse_source,
        "parse_reason": parse_reason,
        "played_on": iso_or_none(played_on.isoformat() if played_on else None),
    }


def build_row_user(row):
    return {
        "original_filename": row[1],
        "user_uid": row[15],
        "steam_id": row[16],
        "in_game_name": row[17],
        "steam_persona_name": row[18],
        "claimed_name": row[17],
    }


def build_parsed_snapshot(row_parse_source, parsed):
    duration = parsed.get("duration") or parsed.get("game_duration") or 0
    duration = int(duration) if isinstance(duration, (int, float)) else 0

    return {
        "game_version": parsed.get("game_version"),
        "map": parsed.get("map") if isinstance(parsed.get("map"), dict) else {"name": "Unknown", "size": "Unknown"},
        "game_type": parsed.get("game_type"),
        "duration": duration,
        "game_duration": duration,
        "winner": parsed.get("winner") or "Unknown",
        "players": parsed.get("players") if isinstance(parsed.get("players"), list) else [],
        "event_types": parsed.get("event_types") if isinstance(parsed.get("event_types"), list) else [],
        "key_events": parsed.get("key_events") if isinstance(parsed.get("key_events"), dict) else {},
        "disconnect_detected": bool(parsed.get("disconnect_detected")),
        "parse_source": row_parse_source,
        "parse_reason": derived_parse_reason(row_parse_source, parsed.get("parse_reason")),
        "played_on": iso_or_none(parsed.get("played_on")),
    }


def snapshots_differ(left, right):
    return json.dumps(left, sort_keys=True, default=str) != json.dumps(right, sort_keys=True, default=str)


def main():
    parser = argparse.ArgumentParser(
        description="Repair HD duration rows by reparsing local replay files and writing corrected parser-owned fields back to Postgres."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL") or os.getenv("BACKFILL_DATABASE_URL"),
        help="Postgres connection string.",
    )
    parser.add_argument(
        "--replay-dir",
        default=os.getenv("AOE2_REPLAY_DIR") or str(DEFAULT_REPLAY_DIR),
        help="Local replay directory containing .aoe2record files.",
    )
    parser.add_argument(
        "--filename",
        action="append",
        default=[],
        help="Optional original replay filename filter. Repeat for multiple files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing to the database.",
    )
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")

    replay_dir = Path(args.replay_dir).expanduser()
    if not replay_dir.exists():
        raise SystemExit(f"Replay directory not found: {replay_dir}")

    filename_filter = {name for name in args.filename if name}
    checked = 0
    repaired = 0
    changed_files = []

    query = """
        select
            game_stats.id,
            game_stats.original_filename,
            game_stats.game_version,
            game_stats.map,
            game_stats.game_type,
            game_stats.duration,
            game_stats.game_duration,
            game_stats.winner,
            game_stats.players,
            game_stats.event_types,
            game_stats.key_events,
            game_stats.disconnect_detected,
            game_stats.parse_source,
            game_stats.parse_reason,
            game_stats.played_on
            ,
            game_stats.user_uid,
            users.steam_id,
            users.in_game_name,
            users.steam_persona_name
        from game_stats
        left join users on users.uid = game_stats.user_uid
        where game_stats.is_final = true
          and game_stats.original_filename is not null
          and (
            game_stats.parse_reason = %s
            or game_stats.played_on is null
            or (
              game_stats.winner = 'Unknown'
              and coalesce(game_stats.key_events->>'no_rated_result', 'false') <> 'true'
              and coalesce(game_stats.key_events->>'rated', 'false') = 'true'
              and coalesce(game_stats.key_events->>'completed', 'true') = 'false'
              and jsonb_typeof(game_stats.players) = 'array'
              and jsonb_array_length(game_stats.players) = 2
              and coalesce(game_stats.user_uid, '') <> ''
              and game_stats.user_uid <> 'system'
            )
            or exists (
              select 1
              from game_stats later_live
              where later_live.is_final = false
                and later_live.original_filename = game_stats.original_filename
                and later_live.created_at > game_stats.created_at
                and coalesce(later_live.duration, 0) >= coalesce(game_stats.duration, 0) + 60
            )
          )
        order by game_stats.id
    """

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (EARLY_EXIT_PARSE_REASON,))
            rows = cur.fetchall()

            for row in rows:
                game_id = row[0]
                original_filename = row[1]
                if filename_filter and original_filename not in filename_filter:
                    continue

                checked += 1
                current_snapshot = build_row_snapshot(row)
                row_user = build_row_user(row)
                replay_path = replay_dir / original_filename

                if replay_path.exists():
                    parsed = _parse_sync_bytes(str(replay_path), replay_path.read_bytes())
                    if not parsed:
                        continue
                    next_snapshot = build_parsed_snapshot(row[12], parsed)
                    next_snapshot = infer_incomplete_uploader_outcome(next_snapshot, row_user)
                else:
                    next_snapshot = repair_inconsistent_early_exit_snapshot(current_snapshot, original_filename)

                if not snapshots_differ(current_snapshot, next_snapshot):
                    continue

                repaired += 1
                changed_files.append(original_filename)

                if args.dry_run:
                    continue

                cur.execute(
                    """
                    update game_stats
                    set
                        game_version = %s,
                        map = %s::jsonb,
                        game_type = %s,
                        duration = %s,
                        game_duration = %s,
                        winner = %s,
                        players = %s::jsonb,
                        event_types = %s::jsonb,
                        key_events = %s::jsonb,
                        disconnect_detected = %s,
                        parse_reason = %s,
                        played_on = %s
                    where id = %s
                    """,
                    (
                        next_snapshot["game_version"],
                        json.dumps(next_snapshot["map"]),
                        next_snapshot["game_type"],
                        next_snapshot["duration"],
                        next_snapshot["game_duration"],
                        next_snapshot["winner"],
                        json.dumps(next_snapshot["players"]),
                        json.dumps(next_snapshot["event_types"]),
                        json.dumps(next_snapshot["key_events"]),
                        next_snapshot["disconnect_detected"],
                        next_snapshot["parse_reason"],
                        next_snapshot["played_on"],
                        game_id,
                    ),
                )

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(
        json.dumps(
            {
                "checked_files": checked,
                "repaired_rows": repaired,
                "dry_run": args.dry_run,
                "changed_files": changed_files,
            }
        )
    )


if __name__ == "__main__":
    main()
