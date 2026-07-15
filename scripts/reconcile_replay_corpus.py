#!/usr/bin/env python3
"""Produce a read-only, inspectable replay-corpus reconciliation snapshot."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import psycopg
from psycopg.rows import dict_row

from utils.replay_corpus_reconciliation import (
    archive_profile,
    build_full_artifact_manifest_rows,
    classify_current_result,
    freeze_logical_cohort,
    logical_replay_key,
    normalize_database_url,
    normalize_replay_name,
    safe_hashes,
    scan_archive,
    verify_archive_content_hashes,
)


DEFAULT_JIM_UID = "u_0df73bdbb64646c19e4a9bfd225b3285"
DEFAULT_JIM_ANCHOR_GAME_ID = 16218


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path(
            os.getenv(
                "REPLAY_ARCHIVE_DIR",
                "/mnt/HC_Volume_105319120/aoe2-replay-archive",
            )
        ),
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path(
            os.getenv(
                "REPLAY_ENGINE_REPORT_DIR",
                "/mnt/HC_Volume_105319120/aoe2-parser-engine/reports",
            )
        ),
    )
    parser.add_argument("--jim-user-uid", default=DEFAULT_JIM_UID)
    parser.add_argument("--jim-anchor-game-id", type=int, default=DEFAULT_JIM_ANCHOR_GAME_ID)
    parser.add_argument("--snapshot-label")
    parser.add_argument(
        "--verify-content-hashes",
        action="store_true",
        help="Sequentially hash every archive object and compare it with its locator",
    )
    return parser.parse_args()


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Cannot serialize {type(value)!r}")


def write_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, indent=2, sort_keys=True, default=json_default) + "\n"
    path.write_text(payload, encoding="utf-8")
    path.chmod(0o600)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    path.chmod(0o600)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_database_rows(database_url: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[int]]:
    game_sql = """
        SELECT id, user_uid, replay_file, replay_hash, created_at, played_on,
               timestamp, game_version, map, game_type, duration, game_duration,
               winner, players, event_types, key_events, parse_iteration, is_final,
               disconnect_detected, parse_source, parse_reason, original_filename
        FROM game_stats
        ORDER BY id
    """
    attempt_sql = """
        SELECT id, created_at, user_uid, replay_hash, original_filename,
               parse_source, status, detail, upload_mode, file_size_bytes,
               game_stats_id, played_on
        FROM replay_parse_attempts
        ORDER BY id
    """
    adjudication_sql = """
        SELECT DISTINCT game_stats_id
        FROM replay_result_adjudications
        WHERE decision_status = 'accepted' AND affects_stats = TRUE
    """

    with psycopg.connect(normalize_database_url(database_url), row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        games = list(connection.execute(game_sql).fetchall())
        attempts = list(connection.execute(attempt_sql).fetchall())
        adjudicated_ids = {
            int(row["game_stats_id"])
            for row in connection.execute(adjudication_sql).fetchall()
        }
    return games, attempts, adjudicated_ids


def main() -> int:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required; it is never written to reports or logs")
    if not args.archive_dir.is_dir():
        raise SystemExit(f"Replay archive does not exist: {args.archive_dir}")

    args.report_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    args.report_dir.chmod(0o700)

    games, attempts, adjudicated_ids = load_database_rows(args.database_url)
    archive_objects, invalid_archive_names = scan_archive(args.archive_dir)
    archive_hash_mismatches = (
        verify_archive_content_hashes(args.archive_dir, archive_objects)
        if args.verify_content_hashes
        else []
    )
    archive_by_hash = {obj.sha256: obj for obj in archive_objects}
    archive_hashes = set(archive_by_hash)
    game_hashes = safe_hashes(games)
    attempt_hashes = safe_hashes(attempts)
    known_database_hashes = game_hashes | attempt_hashes

    jim_games, jim_duplicates = freeze_logical_cohort(
        games,
        user_uid=args.jim_user_uid,
        anchor_game_id=args.jim_anchor_game_id,
    )

    attempts_by_hash: dict[str, list[dict[str, Any]]] = {}
    for attempt in attempts:
        replay_hash = normalize_replay_name(attempt.get("replay_hash"))
        attempts_by_hash.setdefault(replay_hash, []).append(attempt)

    jim_manifest = []
    result_buckets: dict[str, int] = {}
    jim_missing_archive = []
    for game in jim_games:
        replay_hash = normalize_replay_name(game.get("replay_hash"))
        result_state = classify_current_result(
            game,
            manually_verified=int(game["id"]) in adjudicated_ids,
        )
        result_buckets[result_state] = result_buckets.get(result_state, 0) + 1
        archive_object = archive_by_hash.get(replay_hash)
        row = {
            "logical_replay_key": logical_replay_key(game),
            "game_stats_id": int(game["id"]),
            "replay_hash": replay_hash,
            "original_filename": game.get("original_filename") or game.get("replay_file") or "",
            "extension": Path(str(game.get("original_filename") or game.get("replay_file") or "")).suffix.casefold(),
            "archive_present": archive_object is not None,
            "archive_relative_path": archive_object.relative_path if archive_object else "",
            "archive_bytes": archive_object.byte_size if archive_object else "",
            "submission_count": len(attempts_by_hash.get(replay_hash, [])),
            "current_result_state": result_state,
            "parse_source": game.get("parse_source") or "",
            "parse_reason": game.get("parse_reason") or "",
            "parse_iteration": int(game.get("parse_iteration") or 0),
            "created_at": game.get("created_at"),
            "played_on": game.get("played_on"),
        }
        jim_manifest.append(row)
        if archive_object is None:
            jim_missing_archive.append(row)

    duplicate_submission_rows = len(attempts) - len(attempt_hashes)
    full_final_rows = [game for game in games if bool(game.get("is_final"))]
    full_final_hashes = safe_hashes(full_final_rows)

    timestamp = datetime.now(timezone.utc)
    label = args.snapshot_label or timestamp.strftime("%Y%m%dT%H%M%SZ")
    prefix = f"replay-corpus-{label}"
    manifest_path = args.report_dir / f"{prefix}-jim-{len(jim_manifest)}-manifest.csv"
    duplicate_path = args.report_dir / f"{prefix}-jim-duplicate-logical-games.csv"
    full_manifest_path = (
        args.report_dir
        / f"{prefix}-full-vault-{len(archive_objects)}-artifact-manifest.csv"
    )
    summary_path = args.report_dir / f"{prefix}-summary.json"

    manifest_fields = [
        "logical_replay_key",
        "game_stats_id",
        "replay_hash",
        "original_filename",
        "extension",
        "archive_present",
        "archive_relative_path",
        "archive_bytes",
        "submission_count",
        "current_result_state",
        "parse_source",
        "parse_reason",
        "parse_iteration",
        "created_at",
        "played_on",
    ]
    write_csv(manifest_path, manifest_fields, jim_manifest)

    duplicate_rows = []
    for key, rows in sorted(jim_duplicates.items()):
        selected_id = max(rows, key=lambda row: (row in jim_games, int(row.get("id") or 0))).get("id")
        for row in rows:
            duplicate_rows.append(
                {
                    "logical_replay_key": key,
                    "game_stats_id": row.get("id"),
                    "selected": row.get("id") == selected_id,
                    "replay_hash": row.get("replay_hash"),
                    "parse_source": row.get("parse_source"),
                    "parse_reason": row.get("parse_reason"),
                    "created_at": row.get("created_at"),
                }
            )
    write_csv(
        duplicate_path,
        [
            "logical_replay_key",
            "game_stats_id",
            "selected",
            "replay_hash",
            "parse_source",
            "parse_reason",
            "created_at",
        ],
        duplicate_rows,
    )

    full_manifest_rows = build_full_artifact_manifest_rows(
        archive_objects,
        games,
        attempts,
    )
    write_csv(
        full_manifest_path,
        [
            "logical_replay_key",
            "game_stats_id",
            "legacy_parse_attempt_id",
            "submitter_uid",
            "replay_hash",
            "original_filename",
            "extension",
            "archive_present",
            "archive_relative_path",
            "archive_bytes",
        ],
        full_manifest_rows,
    )

    summary = {
        "snapshot": {
            "created_at": timestamp,
            "label": label,
            "read_only": True,
            "jim_user_uid": args.jim_user_uid,
            "jim_anchor_game_id": args.jim_anchor_game_id,
            "archive_dir": str(args.archive_dir),
        },
        "grain_definitions": {
            "artifact": "one immutable SHA-256 archive object",
            "submission": "one replay_parse_attempts row",
            "database_replay_row": "one game_stats row",
            "logical_game": "one preferred final replay_file/original_filename key at the frozen anchor",
        },
        "archive": archive_profile(archive_objects, invalid_archive_names),
        "database": {
            "game_stats_rows": len(games),
            "game_stats_unique_hashes": len(game_hashes),
            "final_rows": len(full_final_rows),
            "final_unique_hashes": len(full_final_hashes),
            "parse_attempt_rows": len(attempts),
            "parse_attempt_unique_hashes": len(attempt_hashes),
            "duplicate_submission_rows": duplicate_submission_rows,
            "known_unique_hashes": len(known_database_hashes),
        },
        "integrity": {
            "archive_hashes_not_in_database": len(archive_hashes - known_database_hashes),
            "database_hashes_missing_archive": len(known_database_hashes - archive_hashes),
            "game_stats_hashes_missing_archive": len(game_hashes - archive_hashes),
            "parse_attempt_hashes_missing_archive": len(attempt_hashes - archive_hashes),
        },
        "jim_frozen_cohort": {
            "logical_games": len(jim_manifest),
            "source_final_rows": sum(
                1
                for game in games
                if game.get("user_uid") == args.jim_user_uid
                and bool(game.get("is_final"))
                and int(game.get("id") or 0) <= args.jim_anchor_game_id
            ),
            "duplicate_logical_keys": len(jim_duplicates),
            "duplicate_rows_excluded": sum(len(rows) - 1 for rows in jim_duplicates.values()),
            "archive_present": len(jim_manifest) - len(jim_missing_archive),
            "archive_missing": len(jim_missing_archive),
            "result_state_baseline": dict(sorted(result_buckets.items())),
            "equation_total": sum(result_buckets.values()),
        },
        "artifacts": {
            "jim_manifest": str(manifest_path),
            "jim_manifest_sha256": file_sha256(manifest_path),
            "jim_duplicate_logical_games": str(duplicate_path),
            "jim_duplicate_logical_games_sha256": file_sha256(duplicate_path),
            "full_vault_artifact_manifest": str(full_manifest_path),
            "full_vault_artifact_manifest_sha256": file_sha256(full_manifest_path),
            "full_vault_artifact_manifest_rows": len(full_manifest_rows),
        },
    }
    summary["archive"]["content_hashes_verified"] = (
        len(archive_objects) if args.verify_content_hashes else 0
    )
    summary["archive"]["content_hash_mismatches"] = len(archive_hash_mismatches)
    summary["archive"]["content_hash_mismatch_samples"] = archive_hash_mismatches[:10]
    write_json(summary_path, summary)

    print(json.dumps({"summary": str(summary_path), **summary}, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
