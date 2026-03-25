import argparse
import json
import os
from pathlib import Path

from mgz import header
import psycopg


DEFAULT_REPLAY_DIR = Path(
    "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame"
)


def normalize_name(value):
    return " ".join((value or "").split()).strip().lower()


def extract_ratings(path: Path):
    with path.open("rb") as handle:
        parsed = header.parse_stream(handle)

    hd = getattr(parsed, "hd", None)
    players = getattr(hd, "players", None) or []
    ratings = {}

    for player in players:
        player_number = getattr(player, "player_number", -1)
        if not isinstance(player_number, int) or player_number <= 0:
            continue

        name_struct = getattr(player, "name", None)
        raw_name = getattr(name_struct, "value", None)
        if not isinstance(raw_name, (bytes, bytearray)):
            continue

        name = raw_name.decode("utf-8", errors="ignore").replace("\x00", "").strip()
        if not name:
            continue

        steam_id = getattr(player, "steam_id", None)
        ratings[normalize_name(name)] = {
            "steam_id": str(steam_id) if isinstance(steam_id, int) and steam_id > 0 else None,
            "steam_rm_rating": getattr(player, "hd_rm_rating", None),
            "steam_dm_rating": getattr(player, "hd_dm_rating", None),
        }

    return ratings


def main():
    parser = argparse.ArgumentParser(description="Backfill HD player ratings into stored game_stats rows.")
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

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select id, original_filename, players from game_stats where is_final = true and original_filename is not null order by id"
            )
            rows = cur.fetchall()

            for game_id, original_filename, players in rows:
                path = replay_dir / original_filename
                if not path.exists():
                    continue

                checked += 1
                rating_map = extract_ratings(path)
                if not isinstance(players, list) or not rating_map:
                    continue

                changed = False
                merged_players = []

                for player in players:
                    if not isinstance(player, dict):
                        merged_players.append(player)
                        continue

                    merged = dict(player)
                    rating = rating_map.get(normalize_name(str(player.get("name") or "")))

                    if rating:
                        for key in ("steam_id", "steam_rm_rating", "steam_dm_rating"):
                            next_value = rating.get(key)
                            if next_value is not None and merged.get(key) != next_value:
                                merged[key] = next_value
                                changed = True

                    merged_players.append(merged)

                if not changed:
                    continue

                updated += 1

                if args.dry_run:
                    continue

                cur.execute(
                    "update game_stats set players = %s::jsonb where id = %s",
                    (json.dumps(merged_players), game_id),
                )

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(json.dumps({"checked_files": checked, "updated_rows": updated, "dry_run": args.dry_run}))


if __name__ == "__main__":
    main()
