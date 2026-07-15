#!/usr/bin/env python3
"""Build a private, read-only reconciliation report for one Engine Room job."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

import psycopg
from psycopg.rows import dict_row


SCRIPT_DIR = Path(__file__).resolve().parent
API_ROOT = SCRIPT_DIR.parent
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from utils.replay_engine_room_reporting import (  # noqa: E402
    aggregate_candidate_summaries,
    summarize_candidate,
)
from utils.replay_engine_room_worker import (  # noqa: E402
    normalize_database_url,
    verify_candidate_object,
)


DEFAULT_REPORT_ROOT = Path(
    "/mnt/HC_Volume_105319120/aoe2-parser-engine/reports"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--job-id", type=int)
    identity.add_argument("--job-identity-hash")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--label")
    return parser.parse_args()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "game_stats_id",
        "original_filename",
        "artifact_sha256",
        "run_status",
        "parse_mode",
        "failure_signature",
        "player_count",
        "team_status",
        "team_format",
        "result_status",
        "result_trusted",
        "result_provenance",
        "winning_player_names",
        "result_bucket",
        "promotion_lane",
        "settlement_evidence_eligible",
        "map_name",
        "duration_seconds",
        "raw_action_count",
        "chat_available",
        "chat_message_count",
        "map_snapshot_available",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            rendered = dict(row)
            rendered["winning_player_names"] = " | ".join(
                str(value) for value in row.get("winning_player_names") or []
            )
            writer.writerow(rendered)
    path.chmod(0o600)


def _load_job(connection: psycopg.Connection, args: argparse.Namespace) -> dict[str, Any]:
    if args.job_id:
        row = connection.execute(
            "SELECT * FROM replay_reprocess_jobs WHERE id = %s", (args.job_id,)
        ).fetchone()
    else:
        row = connection.execute(
            "SELECT * FROM replay_reprocess_jobs WHERE job_identity_hash = %s",
            (args.job_identity_hash,),
        ).fetchone()
    if not row:
        raise SystemExit("Engine Room job was not found")
    return dict(row)


def _load_rows(connection: psycopg.Connection, job_id: int) -> list[dict[str, Any]]:
    return list(
        connection.execute(
            """
            SELECT
              event.sequence AS event_sequence,
              event.detail AS event_detail,
              run.*,
              game.original_filename,
              game.replay_file,
              game.winner AS current_winner,
              game.players AS current_players,
              game.key_events AS current_key_events,
              adjudication.id AS adjudication_id,
              adjudication.decision_status AS adjudication_status,
              adjudication.winning_player_keys AS adjudication_winning_player_keys
            FROM replay_reprocess_job_events event
            JOIN replay_parse_runs run ON run.id = event.parse_run_id
            LEFT JOIN game_stats game ON game.id = run.game_stats_id
            LEFT JOIN LATERAL (
              SELECT id, decision_status, winning_player_keys
              FROM replay_result_adjudications
              WHERE game_stats_id = game.id
                AND decision_status = 'accepted'
                AND affects_stats = TRUE
              ORDER BY created_at DESC, id DESC
              LIMIT 1
            ) adjudication ON TRUE
            WHERE event.job_id = %s
              AND event.event_type = 'artifact_completed'
            ORDER BY event.sequence
            """,
            (job_id,),
        ).fetchall()
    )


def main() -> int:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required and is never printed")
    report_root = args.report_root.expanduser().resolve()
    if not str(report_root).startswith("/mnt/"):
        raise SystemExit("private reports must be written beneath /mnt")
    report_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    report_root.chmod(0o700)

    with psycopg.connect(
        normalize_database_url(args.database_url), row_factory=dict_row
    ) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        job = _load_job(connection, args)
        latest = connection.execute(
            """
            SELECT * FROM replay_reprocess_job_events
            WHERE job_id = %s ORDER BY sequence DESC LIMIT 1
            """,
            (job["id"],),
        ).fetchone()
        source_rows = _load_rows(connection, int(job["id"]))

    summaries: list[dict[str, Any]] = []
    for row in source_rows:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        candidate = verify_candidate_object(
            Path(str(row["candidate_output_storage_key"])).resolve(),
            expected_compressed_sha256=str(row["candidate_output_hash"]),
            expected_compressed_byte_size=int(row["candidate_output_byte_size"]),
            expected_artifact_sha256=str(row["input_hash"]),
            expected_run_idempotency_key=str(row["idempotency_key"]),
            expected_semantic_sha256=metrics.get("candidate_semantic_sha256"),
        )
        current = {
            "game_stats_id": row.get("game_stats_id"),
            "original_filename": row.get("original_filename") or row.get("replay_file"),
            "winner": row.get("current_winner"),
            "players": row.get("current_players"),
            "key_events": row.get("current_key_events"),
            "latest_adjudication": (
                {
                    "id": row.get("adjudication_id"),
                    "decision_status": row.get("adjudication_status"),
                    "winning_player_keys": row.get(
                        "adjudication_winning_player_keys"
                    ),
                }
                if row.get("adjudication_id")
                else None
            ),
        }
        summary = summarize_candidate(candidate, current)
        summary["event_sequence"] = int(row["event_sequence"])
        summaries.append(summary)

    aggregate = aggregate_candidate_summaries(summaries)
    aggregate["job"] = {
        "id": int(job["id"]),
        "job_identity_hash": job["job_identity_hash"],
        "scope_kind": job["scope_kind"],
        "scope": job["scope"],
        "max_artifacts": int(job["max_artifacts"]),
        "latest_event": latest["event_type"] if latest else None,
        "processed": int(latest["processed_count"]) if latest else 0,
        "succeeded": int(latest["succeeded_count"]) if latest else 0,
        "failed": int(latest["failed_count"]) if latest else 0,
        "skipped": int(latest["skipped_count"]) if latest else 0,
    }
    aggregate["generated_at"] = datetime.now(timezone.utc).isoformat()
    aggregate["read_only"] = True
    aggregate["candidate_only"] = True
    aggregate["affects_public_aggregates"] = False

    label = args.label or f"engine-room-job-{job['id']}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    json_path = report_root / f"{label}-summary.json"
    csv_path = report_root / f"{label}-games.csv"
    _write_json(json_path, {"summary": aggregate, "games": summaries})
    _write_csv(csv_path, summaries)
    print(
        json.dumps(
            {
                "summary_path": str(json_path),
                "games_path": str(csv_path),
                "summary": aggregate,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
