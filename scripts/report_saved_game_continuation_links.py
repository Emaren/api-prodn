#!/usr/bin/env python3
"""Report deterministic candidate-only links between HD saves and recordings."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

import psycopg
from psycopg.rows import dict_row


SCRIPT_DIR = Path(__file__).resolve().parent
API_ROOT = SCRIPT_DIR.parent
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from utils.replay_engine_room_worker import normalize_database_url  # noqa: E402


DEFAULT_REPORT_ROOT = Path(
    "/mnt/HC_Volume_105319120/aoe2-parser-engine/reports"
)
SAVED_PARSE_MODE_PREFIX = "mgz_hd_saved_game_"
SAFE_LABEL_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,119}$")


LINK_SQL = """
WITH latest AS (
  SELECT DISTINCT ON (run.artifact_id)
    run.id,
    run.artifact_id,
    run.game_stats_id,
    run.metrics->>'parse_mode' AS parse_mode,
    artifact.sha256 AS artifact_sha256
  FROM replay_parse_runs run
  JOIN replay_artifacts artifact ON artifact.id = run.artifact_id
  WHERE run.status = 'completed'
  ORDER BY run.artifact_id, run.completed_at DESC, run.id DESC
), facts AS (
  SELECT
    latest.*,
    max(observation.value #>> '{}') FILTER (
      WHERE observation.field_path = 'game.platform_match_id'
    ) AS platform_match_id,
    array_agg(
      DISTINCT lower(observation.value #>> '{}')
      ORDER BY lower(observation.value #>> '{}')
    ) FILTER (
      WHERE observation.field_path = 'player.name'
        AND coalesce(observation.value #>> '{}', '') <> ''
    ) AS player_names,
    array_agg(
      DISTINCT observation.value #>> '{}'
      ORDER BY observation.value #>> '{}'
    ) FILTER (
      WHERE observation.field_path = 'player.steam_id'
        AND coalesce(observation.value #>> '{}', '') NOT IN ('', '0')
    ) AS player_steam_ids
  FROM latest
  JOIN replay_observations observation ON observation.parse_run_id = latest.id
  GROUP BY
    latest.id,
    latest.artifact_id,
    latest.game_stats_id,
    latest.parse_mode,
    latest.artifact_sha256
), saved AS (
  SELECT * FROM facts WHERE parse_mode LIKE %(saved_prefix)s
), recorded AS (
  SELECT * FROM facts WHERE parse_mode NOT LIKE %(saved_prefix)s
)
SELECT
  saved.platform_match_id,
  saved.id AS saved_run_id,
  saved.game_stats_id AS saved_game_stats_id,
  saved.artifact_sha256 AS saved_artifact_sha256,
  saved.parse_mode AS saved_parse_mode,
  saved.player_names AS saved_player_names,
  saved.player_steam_ids AS saved_player_steam_ids,
  recorded.id AS recorded_run_id,
  recorded.game_stats_id AS recorded_game_stats_id,
  recorded.artifact_sha256 AS recorded_artifact_sha256,
  recorded.parse_mode AS recorded_parse_mode,
  recorded.player_names AS recorded_player_names,
  recorded.player_steam_ids AS recorded_player_steam_ids
FROM saved
JOIN recorded USING (platform_match_id)
WHERE coalesce(saved.platform_match_id, '') <> ''
ORDER BY saved.platform_match_id, saved.id, recorded.id
"""


CORPUS_SQL = """
WITH latest AS (
  SELECT DISTINCT ON (artifact_id)
    id,
    artifact_id,
    metrics->>'parse_mode' AS parse_mode
  FROM replay_parse_runs
  WHERE status = 'completed'
  ORDER BY artifact_id, completed_at DESC, id DESC
), platforms AS (
  SELECT
    latest.id,
    latest.parse_mode,
    max(observation.value #>> '{}') FILTER (
      WHERE observation.field_path = 'game.platform_match_id'
    ) AS platform_match_id
  FROM latest
  JOIN replay_observations observation ON observation.parse_run_id = latest.id
  GROUP BY latest.id, latest.parse_mode
)
SELECT
  count(*) AS completed_latest_candidates,
  count(*) FILTER (WHERE parse_mode LIKE %(saved_prefix)s) AS saved_candidates,
  count(*) FILTER (WHERE parse_mode NOT LIKE %(saved_prefix)s) AS recorded_candidates,
  count(*) FILTER (
    WHERE parse_mode LIKE %(saved_prefix)s
      AND coalesce(platform_match_id, '') <> ''
  ) AS saved_candidates_with_platform_match_id,
  count(DISTINCT platform_match_id) FILTER (
    WHERE parse_mode LIKE %(saved_prefix)s
      AND coalesce(platform_match_id, '') <> ''
  ) AS saved_distinct_platform_match_ids
FROM platforms
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--label")
    return parser.parse_args()


def _roster_hash(names: list[str], steam_ids: list[str]) -> str:
    canonical = json.dumps(
        {"names": sorted(names), "steam_ids": sorted(steam_ids)},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_label(value: str | None) -> str:
    label = value or f"saved-game-continuation-links-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    if not SAFE_LABEL_RE.fullmatch(label):
        raise SystemExit("--label must be a safe lowercase filename component")
    return label


def _pair_rows(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in source_rows:
        saved_names = list(source.get("saved_player_names") or [])
        saved_steam_ids = list(source.get("saved_player_steam_ids") or [])
        recorded_names = list(source.get("recorded_player_names") or [])
        recorded_steam_ids = list(source.get("recorded_player_steam_ids") or [])
        names_exact = saved_names == recorded_names and bool(saved_names)
        steam_ids_exact = (
            saved_steam_ids == recorded_steam_ids and bool(saved_steam_ids)
        )
        rows.append(
            {
                "platform_match_id": source["platform_match_id"],
                "saved_run_id": int(source["saved_run_id"]),
                "saved_game_stats_id": source.get("saved_game_stats_id"),
                "saved_artifact_sha256": source["saved_artifact_sha256"],
                "saved_parse_mode": source["saved_parse_mode"],
                "recorded_run_id": int(source["recorded_run_id"]),
                "recorded_game_stats_id": source.get("recorded_game_stats_id"),
                "recorded_artifact_sha256": source["recorded_artifact_sha256"],
                "recorded_parse_mode": source["recorded_parse_mode"],
                "saved_player_count": len(saved_names),
                "recorded_player_count": len(recorded_names),
                "player_names_exact": names_exact,
                "player_steam_ids_exact": steam_ids_exact,
                "saved_roster_sha256": _roster_hash(saved_names, saved_steam_ids),
                "recorded_roster_sha256": _roster_hash(
                    recorded_names, recorded_steam_ids
                ),
                "deterministic_continuation_link": names_exact and steam_ids_exact,
                "candidate_only": True,
                "final_result_evidence": False,
                "settlement_evidence_eligible": False,
            }
        )
    return rows


def _summarize(
    corpus: dict[str, Any], rows: list[dict[str, Any]]
) -> dict[str, Any]:
    by_platform: dict[str, dict[str, set[int]]] = {}
    for row in rows:
        group = by_platform.setdefault(
            str(row["platform_match_id"]), {"saved": set(), "recorded": set()}
        )
        group["saved"].add(int(row["saved_run_id"]))
        group["recorded"].add(int(row["recorded_run_id"]))

    linked_saved = {int(row["saved_run_id"]) for row in rows}
    linked_recorded = {int(row["recorded_run_id"]) for row in rows}
    one_to_one = sum(
        len(group["saved"]) == 1 and len(group["recorded"]) == 1
        for group in by_platform.values()
    )
    multi_checkpoint = sum(
        len(group["saved"]) > 1 for group in by_platform.values()
    )
    exact_pairs = sum(bool(row["deterministic_continuation_link"]) for row in rows)

    saved_candidates = int(corpus["saved_candidates"])
    saved_distinct_ids = int(corpus["saved_distinct_platform_match_ids"])
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
        "candidate_only": True,
        "affects_public_aggregates": False,
        "creates_public_result_truth": False,
        "settlement_evidence_eligible": False,
        "link_rule": (
            "exact HD platform_match_id plus exact normalized player-name and "
            "non-zero Steam-ID rosters"
        ),
        "completed_latest_candidates": int(corpus["completed_latest_candidates"]),
        "saved_candidates": saved_candidates,
        "recorded_candidates": int(corpus["recorded_candidates"]),
        "saved_candidates_with_platform_match_id": int(
            corpus["saved_candidates_with_platform_match_id"]
        ),
        "saved_distinct_platform_match_ids": saved_distinct_ids,
        "shared_platform_match_ids": len(by_platform),
        "linked_saved_checkpoints": len(linked_saved),
        "linked_recorded_candidates": len(linked_recorded),
        "unlinked_saved_checkpoints": saved_candidates - len(linked_saved),
        "unlinked_saved_platform_match_ids": saved_distinct_ids - len(by_platform),
        "one_to_one_platform_match_ids": one_to_one,
        "multi_checkpoint_platform_match_ids": multi_checkpoint,
        "max_saved_checkpoints_per_platform_match_id": max(
            (len(group["saved"]) for group in by_platform.values()), default=0
        ),
        "candidate_pairs": len(rows),
        "deterministic_continuation_pairs": exact_pairs,
        "roster_mismatch_pairs": len(rows) - exact_pairs,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(rows[0]) if rows else [
        "platform_match_id",
        "saved_run_id",
        "recorded_run_id",
        "deterministic_continuation_link",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    path.chmod(0o600)


def main() -> int:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required and is never printed")
    report_root = args.report_root.expanduser().resolve()
    if not str(report_root).startswith("/mnt/"):
        raise SystemExit("private reports must be written beneath /mnt")
    report_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    report_root.chmod(0o700)

    params = {"saved_prefix": f"{SAVED_PARSE_MODE_PREFIX}%"}
    with psycopg.connect(
        normalize_database_url(args.database_url), row_factory=dict_row
    ) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        corpus = dict(connection.execute(CORPUS_SQL, params).fetchone())
        source_rows = [
            dict(row) for row in connection.execute(LINK_SQL, params).fetchall()
        ]

    rows = _pair_rows(source_rows)
    summary = _summarize(corpus, rows)
    label = _safe_label(args.label)
    csv_path = report_root / f"{label}.csv"
    json_path = report_root / f"{label}.json"
    _write_csv(csv_path, rows)
    json_path.write_text(
        json.dumps({"summary": summary, "links": rows}, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    json_path.chmod(0o600)

    output = {
        "summary": summary,
        "csv": {
            "path": str(csv_path),
            "bytes": csv_path.stat().st_size,
            "sha256": _file_sha256(csv_path),
        },
        "json": {
            "path": str(json_path),
            "bytes": json_path.stat().st_size,
            "sha256": _file_sha256(json_path),
        },
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if summary["roster_mismatch_pairs"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
