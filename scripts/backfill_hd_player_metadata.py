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


DEFAULT_REPLAY_DIR = Path(
    "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame"
)


def normalize_name(value):
    return " ".join((value or "").split()).strip().lower()


def has_meaningful_value(value):
    if value is None:
        return False
    if isinstance(value, dict):
        return any(has_meaningful_value(item) for item in value.values())
    if isinstance(value, list):
        return any(has_meaningful_value(item) for item in value)
    if isinstance(value, str):
        return bool(value.strip())
    return True


def merge_values(existing, parsed):
    if isinstance(existing, dict) and isinstance(parsed, dict):
        merged = dict(existing)
        for key, value in parsed.items():
            if not has_meaningful_value(value):
                continue
            merged[key] = merge_values(merged.get(key), value)
        return merged

    if isinstance(parsed, list):
        return parsed if parsed else existing

    if has_meaningful_value(parsed):
        return parsed

    return existing


def merge_players(existing_players, parsed_players):
    if not isinstance(existing_players, list):
        existing_players = []
    if not isinstance(parsed_players, list):
        return existing_players

    parsed_by_name = {
        normalize_name(player.get("name")): player
        for player in parsed_players
        if isinstance(player, dict) and normalize_name(player.get("name"))
    }

    merged_players = []
    matched_names = set()

    for existing in existing_players:
        if not isinstance(existing, dict):
            merged_players.append(existing)
            continue

        key = normalize_name(existing.get("name"))
        parsed = parsed_by_name.get(key)
        if not parsed:
            merged_players.append(existing)
            continue

        matched_names.add(key)
        merged_players.append(merge_values(existing, parsed))

    for parsed in parsed_players:
        if not isinstance(parsed, dict):
            continue
        key = normalize_name(parsed.get("name"))
        if key and key not in matched_names:
            merged_players.append(parsed)

    return merged_players


def main():
    parser = argparse.ArgumentParser(description="Backfill richer HD player metadata into stored game_stats rows.")
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

    updated = 0
    checked = 0
    filename_filter = {name for name in args.filename if name}

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select id, original_filename, players, key_events from game_stats where is_final = true and original_filename is not null order by id"
            )
            rows = cur.fetchall()

            for game_id, original_filename, players, key_events in rows:
                if filename_filter and original_filename not in filename_filter:
                    continue

                path = replay_dir / original_filename
                if not path.exists():
                    continue

                checked += 1
                parsed = _parse_sync_bytes(str(path), path.read_bytes())
                if not parsed:
                    continue

                next_players = merge_players(players, parsed.get("players"))
                next_key_events = merge_values(key_events or {}, parsed.get("key_events") or {})

                if next_players == players and next_key_events == (key_events or {}):
                    continue

                updated += 1

                if args.dry_run:
                    continue

                cur.execute(
                    "update game_stats set players = %s::jsonb, key_events = %s::jsonb where id = %s",
                    (
                        json.dumps(next_players),
                        json.dumps(next_key_events),
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
                "updated_rows": updated,
                "dry_run": args.dry_run,
                "filename_filter_count": len(filename_filter),
            }
        )
    )


if __name__ == "__main__":
    main()
