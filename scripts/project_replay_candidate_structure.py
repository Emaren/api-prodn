#!/usr/bin/env python3
"""Plan or apply strictly gated Engine Room structural projections to GameStats."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

SCRIPT_DIR = Path(__file__).resolve().parent
API_ROOT = SCRIPT_DIR.parent
if str(API_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(API_ROOT))

from utils.replay_engine_room_worker import (  # noqa: E402
    normalize_database_url,
    verify_candidate_object,
)

POLICY_VERSION = "engine-room-structural-display/v1"
SAFE_CLASSIFICATION = "safe_structural_display_no_result_authority"
DEFAULT_RECEIPT_ROOT = Path(
    "/mnt/HC_Volume_105319120/aoe2-parser-engine/structural-promotions"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--all-unparsed", action="store_true")
    scope.add_argument("--game-id", type=int, action="append")
    parser.add_argument("--mode", choices=("plan", "apply"), default="plan")
    parser.add_argument("--receipt-root", type=Path, default=DEFAULT_RECEIPT_ROOT)
    parser.add_argument("--authorization-label", default="operator-reviewed")
    parser.add_argument("--expect-total", type=int)
    parser.add_argument("--expect-safe", type=int)
    parser.add_argument("--expect-blocked", type=int)
    parser.add_argument("--expect-missing", type=int)
    parser.add_argument("--include-projections", action="store_true")
    return parser.parse_args()


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _known_text(value: Any) -> bool:
    return _clean(value).casefold() not in {
        "",
        "unknown",
        "none",
        "null",
        "pending",
        "unresolved",
    }


def player_key(player: Mapping[str, Any]) -> str | None:
    stable = _clean(player.get("stable_player_key") or player.get("player_key"))
    if stable:
        return stable.casefold()
    steam_id = _clean(player.get("steam_id") or player.get("user_id"))
    if steam_id:
        return f"steam:{steam_id}".casefold()
    name = _clean(player.get("name"))
    return f"name:{name.casefold()}" if name else None


def team_value(player: Mapping[str, Any]) -> Any:
    for key in ("team_id", "team", "team_index"):
        if key in player and player.get(key) is not None:
            return player.get(key)
    return None


def candidate_team(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    projection = _mapping(candidate.get("projection"))
    events = _mapping(projection.get("key_events"))
    return _mapping(events.get("team_resolution"))


def candidate_artifact_role(candidate: Mapping[str, Any]) -> str | None:
    projection = _mapping(candidate.get("projection"))
    events = _mapping(projection.get("key_events"))
    value = events.get("artifact_role")
    if isinstance(value, Mapping):
        return _clean(value.get("role")) or None
    return _clean(value) or None


def observation_conflict_count(candidate: Mapping[str, Any]) -> int:
    return sum(
        _clean(observation.get("conflict_state")).casefold() not in {"", "none"}
        for observation in _list(candidate.get("observations"))
        if isinstance(observation, Mapping)
    )


def require_current_artifact_binding(row: Mapping[str, Any]) -> None:
    current_hash = _clean(row.get("replay_hash")).casefold()
    candidate_hash = _clean(row.get("input_hash")).casefold()
    if (
        len(current_hash) != 64
        or len(candidate_hash) != 64
        or current_hash != candidate_hash
    ):
        raise RuntimeError(
            "candidate parse run is stale: input_hash does not match "
            "current game_stats.replay_hash"
        )


def classify_structure(
    candidate: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    accepted_adjudications: int,
    linked_markets: int,
    linked_claims: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    projection = _mapping(candidate.get("projection"))
    candidate_map = _mapping(projection.get("map"))
    candidate_players = [
        player
        for player in _list(projection.get("players"))
        if isinstance(player, Mapping)
    ]
    team = candidate_team(candidate)
    run = _mapping(candidate.get("run"))

    if current.get("is_final") is not True:
        reasons.append("game_stats_not_final")
    if _clean(current.get("parse_reason")).casefold() != "watcher_final_unparsed":
        reasons.append("game_stats_not_unparsed_cohort")
    if accepted_adjudications:
        reasons.append("accepted_adjudication_present")
    if linked_markets:
        reasons.append(f"linked_markets={linked_markets}")
    if linked_claims:
        reasons.append(f"linked_claims={linked_claims}")
    if _known_text(current.get("winner")):
        reasons.append("effective_winner_already_known")
    if run.get("status") == "failed":
        reasons.append("candidate_run_failed")
    if not _known_text(candidate_map.get("name")):
        reasons.append("candidate_map_unknown")
    if len(candidate_players) < 2 or len(candidate_players) > 8:
        reasons.append("candidate_roster_out_of_bounds")

    names = [_clean(player.get("name")) for player in candidate_players]
    keys = [player_key(player) for player in candidate_players]
    teams = [team_value(player) for player in candidate_players]
    if candidate_players and not all(names):
        reasons.append("candidate_roster_name_missing")
    if candidate_players and not all(keys):
        reasons.append("candidate_roster_identity_missing")
    if len({key for key in keys if key}) != len(candidate_players):
        reasons.append("candidate_roster_identity_not_unique")
    if team.get("status") != "resolved":
        reasons.append("candidate_teams_not_resolved")
    if candidate_players and any(value is None for value in teams):
        reasons.append("candidate_player_team_missing")
    conflicts = observation_conflict_count(candidate)
    if conflicts:
        reasons.append(f"candidate_observation_conflicts={conflicts}")

    if reasons:
        return "private_review", reasons
    return SAFE_CLASSIFICATION, ["all_structural_projection_gates_passed"]


def projection_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "game_version": row.get("game_version"),
        "map": row.get("map"),
        "game_type": row.get("game_type"),
        "winner": row.get("winner"),
        "players": row.get("players"),
        "key_events": row.get("key_events"),
        "parse_source": row.get("parse_source"),
        "parse_reason": row.get("parse_reason"),
    }


def build_after_structure(
    current: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    parse_run_id: int,
) -> dict[str, Any]:
    projection = deepcopy(dict(_mapping(candidate.get("projection"))))
    projected_players = []
    for raw_player in _list(projection.get("players")):
        if not isinstance(raw_player, Mapping):
            continue
        player = deepcopy(dict(raw_player))
        player["winner"] = None
        projected_players.append(player)

    current_events = deepcopy(dict(_mapping(current.get("key_events"))))
    previous_failure = {
        key: current_events.get(key)
        for key in ("parse_failed", "parse_failure_detail", "watcher_final_unparsed")
        if key in current_events
    }
    for stale_key in previous_failure:
        current_events.pop(stale_key, None)

    team_resolution = deepcopy(dict(candidate_team(candidate)))
    marker = {
        "policy_version": POLICY_VERSION,
        "source_parse_run_id": parse_run_id,
        "source_artifact_sha256": _mapping(candidate.get("artifact")).get("sha256"),
        "source_candidate_semantic_sha256": _mapping(candidate.get("candidate")).get(
            "semantic_sha256"
        ),
        "artifact_role": candidate_artifact_role(candidate),
        "map_name": _mapping(projection.get("map")).get("name"),
        "player_count": len(projected_players),
        "team_status": team_resolution.get("status"),
        "previous_parse_reason": current.get("parse_reason"),
        "previous_failure": previous_failure,
        "result_authority": False,
        "affects_results": False,
        "affects_bets": False,
        "settlement_authority": False,
    }
    current_events["team_resolution"] = team_resolution
    current_events["engine_room_structural_projection"] = marker

    return {
        "game_version": projection.get("game_version") or current.get("game_version"),
        "map": projection.get("map"),
        "game_type": projection.get("game_type") or current.get("game_type"),
        "winner": None,
        "players": projected_players,
        "key_events": current_events,
        "parse_source": current.get("parse_source") or "watcher_final",
        "parse_reason": "engine_room_structural_projection",
    }


def load_candidate(run: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(run.get("metrics"))
    return verify_candidate_object(
        Path(str(run["candidate_output_storage_key"])).resolve(),
        expected_compressed_sha256=str(run["candidate_output_hash"]),
        expected_compressed_byte_size=int(run["candidate_output_byte_size"]),
        expected_artifact_sha256=str(run["input_hash"]),
        expected_run_idempotency_key=str(run["run_idempotency_key"]),
        expected_semantic_sha256=metrics.get("candidate_semantic_sha256"),
    )


def load_rows(
    connection: psycopg.Connection,
    *,
    all_unparsed: bool,
    game_ids: list[int] | None,
) -> list[dict[str, Any]]:
    where = "game.is_final IS TRUE AND game.parse_reason = 'watcher_final_unparsed'"
    params: tuple[Any, ...] = ()
    if not all_unparsed:
        where = "game.id = ANY(%s)"
        params = (sorted(set(game_ids or [])),)

    return [
        dict(row)
        for row in connection.execute(
            f"""
            SELECT
              game.*,
              run.id AS source_parse_run_id,
              run.idempotency_key AS run_idempotency_key,
              run.input_hash,
              run.parser_name,
              run.parser_version,
              run.pass_name,
              run.pass_version,
              run.schema_version,
              run.candidate_output_hash,
              run.candidate_output_storage_key,
              run.candidate_output_byte_size,
              run.metrics,
              (SELECT count(*) FROM replay_result_adjudications adjudication
                WHERE adjudication.game_stats_id = game.id
                  AND adjudication.decision_status = 'accepted') AS accepted_adjudications,
              (SELECT count(*) FROM bet_markets market
                WHERE market.linked_game_stats_id = game.id
                   OR market.late_final_game_stats_id = game.id) AS linked_markets,
              (SELECT count(*) FROM pending_wolo_claims claim
                WHERE claim.source_game_stats_id = game.id) AS linked_claims
            FROM game_stats game
            LEFT JOIN LATERAL (
              SELECT * FROM replay_parse_runs candidate_run
              WHERE candidate_run.game_stats_id = game.id
                AND candidate_run.status = 'completed'
                AND lower(candidate_run.input_hash) = lower(game.replay_hash)
              ORDER BY candidate_run.completed_at DESC, candidate_run.id DESC
              LIMIT 1
            ) run ON TRUE
            WHERE {where}
            ORDER BY game.id
            """,
            params,
        ).fetchall()
    ]


def build_plans(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    plans = []
    for row in rows:
        before = projection_snapshot(row)
        if row.get("source_parse_run_id") is None:
            plans.append(
                {
                    "game_stats_id": int(row["id"]),
                    "classification": "missing_current_candidate",
                    "reasons": ["no_completed_exact_current_hash_candidate"],
                    "before": before,
                    "after": before,
                    "source_parse_run_id": None,
                    "source_artifact_sha256": row.get("replay_hash"),
                    "source_candidate_semantic_sha256": None,
                }
            )
            continue

        require_current_artifact_binding(row)
        candidate = load_candidate(row)
        classification, reasons = classify_structure(
            candidate,
            row,
            accepted_adjudications=int(row["accepted_adjudications"]),
            linked_markets=int(row["linked_markets"]),
            linked_claims=int(row["linked_claims"]),
        )
        after = (
            build_after_structure(
                row,
                candidate,
                parse_run_id=int(row["source_parse_run_id"]),
            )
            if classification == SAFE_CLASSIFICATION
            else before
        )
        plans.append(
            {
                "game_stats_id": int(row["id"]),
                "classification": classification,
                "reasons": reasons,
                "before": before,
                "after": after,
                "source_parse_run_id": int(row["source_parse_run_id"]),
                "source_artifact_sha256": _clean(row.get("replay_hash")).casefold(),
                "source_candidate_semantic_sha256": _mapping(
                    candidate.get("candidate")
                ).get("semantic_sha256"),
            }
        )
    return plans


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as handle:
        json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2, default=str)
        handle.write("\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def assert_expectation(name: str, actual: int, expected: int | None) -> None:
    if expected is not None and actual != expected:
        raise RuntimeError(f"{name} drifted: expected {expected}, found {actual}")


def main() -> int:
    args = parse_args()
    if not args.database_url:
        raise RuntimeError("DATABASE_URL is required")

    database_url = normalize_database_url(args.database_url)
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        rows = load_rows(
            connection,
            all_unparsed=bool(args.all_unparsed),
            game_ids=args.game_id,
        )
        plans = build_plans(rows)

        safe = [p for p in plans if p["classification"] == SAFE_CLASSIFICATION]
        missing = [p for p in plans if p["classification"] == "missing_current_candidate"]
        blocked = [
            p
            for p in plans
            if p["classification"] not in {SAFE_CLASSIFICATION, "missing_current_candidate"}
        ]

        assert_expectation("total", len(plans), args.expect_total)
        assert_expectation("safe", len(safe), args.expect_safe)
        assert_expectation("blocked", len(blocked), args.expect_blocked)
        assert_expectation("missing", len(missing), args.expect_missing)

        now = datetime.now(timezone.utc)
        receipt_id = now.strftime("%Y%m%dT%H%M%SZ") + "-" + stable_hash(
            {
                "policy": POLICY_VERSION,
                "mode": args.mode,
                "game_ids": [p["game_stats_id"] for p in plans],
                "authorization": args.authorization_label,
            }
        )[:12]
        receipt_dir = args.receipt_root.resolve() / receipt_id
        receipt_dir.mkdir(parents=True, exist_ok=False)

        before_hash = stable_hash([p["before"] for p in plans])
        summary = {
            "ok": True,
            "mode": args.mode,
            "policy_version": POLICY_VERSION,
            "authorization_label": args.authorization_label,
            "total": len(plans),
            "safe": len(safe),
            "blocked": len(blocked),
            "missing_current_candidate": len(missing),
            "safe_game_ids": [p["game_stats_id"] for p in safe],
            "blocked_game_ids": [p["game_stats_id"] for p in blocked],
            "missing_game_ids": [p["game_stats_id"] for p in missing],
            "before_projection_sha256": before_hash,
            "writes_performed": False,
            "receipt_dir": str(receipt_dir),
        }
        write_json_atomic(receipt_dir / "plan.json", plans)
        write_json_atomic(receipt_dir / "summary.before.json", summary)

        if args.mode == "apply" and safe:
            with connection.transaction():
                for plan in safe:
                    after = plan["after"]
                    result = connection.execute(
                        """
                        UPDATE game_stats
                        SET
                          game_version = %s,
                          map = %s,
                          game_type = %s,
                          winner = NULL,
                          players = %s,
                          key_events = %s,
                          parse_source = %s,
                          parse_reason = %s
                        WHERE id = %s
                          AND is_final IS TRUE
                          AND parse_reason = 'watcher_final_unparsed'
                          AND lower(replay_hash) = lower(%s)
                        """,
                        (
                            after.get("game_version"),
                            Jsonb(after.get("map")),
                            after.get("game_type"),
                            Jsonb(after.get("players")),
                            Jsonb(after.get("key_events")),
                            after.get("parse_source"),
                            after.get("parse_reason"),
                            plan["game_stats_id"],
                            plan["source_artifact_sha256"],
                        ),
                    )
                    if result.rowcount != 1:
                        raise RuntimeError(
                            f"guarded update failed for game {plan['game_stats_id']}"
                        )

            after_rows = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT id, game_version, map, game_type, winner, players,
                           key_events, parse_source, parse_reason
                    FROM game_stats
                    WHERE id = ANY(%s)
                    ORDER BY id
                    """,
                    ([p["game_stats_id"] for p in safe],),
                ).fetchall()
            ]
            expected_by_id = {p["game_stats_id"]: p["after"] for p in safe}
            errors = []
            for row in after_rows:
                expected = expected_by_id[int(row["id"])]
                actual = projection_snapshot(row)
                if stable_hash(actual) != stable_hash(expected):
                    errors.append(int(row["id"]))
            if errors:
                raise RuntimeError(f"post-apply projection mismatch: {errors}")

            summary["writes_performed"] = True
            summary["after_projection_sha256"] = stable_hash(
                [projection_snapshot(row) for row in after_rows]
            )

        summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        write_json_atomic(receipt_dir / "summary.json", summary)

        public_summary = dict(summary)
        if not args.include_projections:
            public_summary.pop("safe_game_ids", None)
        print(json.dumps(public_summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
