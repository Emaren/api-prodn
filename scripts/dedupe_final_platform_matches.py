import argparse
import json
import os

import psycopg


SUPERSEDED_PARSE_REASON = "superseded_by_later_upload"


def main():
    parser = argparse.ArgumentParser(
        description="Mark duplicate reviewed final matches as non-final using platform_match_id dedupe."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL") or os.getenv("BACKFILL_DATABASE_URL"),
        help="Postgres connection string.",
    )
    parser.add_argument(
        "--keep",
        choices=("oldest", "newest"),
        default="oldest",
        help="Which reviewed row to keep final when duplicates exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing to the database.",
    )
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")

    order = "asc" if args.keep == "oldest" else "desc"
    demoted_ids = []
    duplicate_groups = []

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                    id,
                    replay_hash,
                    created_at,
                    key_events->>'platform_match_id' as platform_match_id
                from game_stats
                where is_final = true
                  and coalesce(key_events->>'platform_match_id', '') <> ''
                order by key_events->>'platform_match_id', created_at {order}, id {order}
                """
            )
            rows = cur.fetchall()

            current_platform_match_id = None
            kept_id = None
            group = []

            for row_id, replay_hash, created_at, platform_match_id in rows:
                if platform_match_id != current_platform_match_id:
                    if len(group) > 1:
                        duplicate_groups.append(group)
                    current_platform_match_id = platform_match_id
                    kept_id = row_id
                    group = [{"id": row_id, "replay_hash": replay_hash, "created_at": created_at.isoformat(), "kept": True}]
                    continue

                group.append({"id": row_id, "replay_hash": replay_hash, "created_at": created_at.isoformat(), "kept": False})
                demoted_ids.append(row_id)

            if len(group) > 1:
                duplicate_groups.append(group)

            if demoted_ids and not args.dry_run:
                cur.execute(
                    """
                    update game_stats
                    set
                        is_final = false,
                        parse_reason = %s
                    where id = any(%s)
                    """,
                    (SUPERSEDED_PARSE_REASON, demoted_ids),
                )

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(
        json.dumps(
            {
                "keep": args.keep,
                "duplicate_group_count": len(duplicate_groups),
                "demoted_row_count": len(demoted_ids),
                "demoted_ids": demoted_ids,
                "dry_run": args.dry_run,
            }
        )
    )


if __name__ == "__main__":
    main()
