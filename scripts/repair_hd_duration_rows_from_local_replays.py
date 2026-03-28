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
            id,
            original_filename,
            game_version,
            map,
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
            played_on
        from game_stats
        where is_final = true
          and original_filename is not null
          and parse_reason = %s
        order by id
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

                replay_path = replay_dir / original_filename
                if not replay_path.exists():
                    continue

                checked += 1
                parsed = _parse_sync_bytes(str(replay_path), replay_path.read_bytes())
                if not parsed:
                    continue

                current_snapshot = build_row_snapshot(row)
                next_snapshot = build_parsed_snapshot(row[12], parsed)
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
