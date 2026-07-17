#!/usr/bin/env python3
"""Plan or apply strictly gated Engine Room result projections to GameStats."""

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


POLICY_VERSION = "engine-room-effective-result/v1"
DEFAULT_RECEIPT_ROOT = Path(
    "/mnt/HC_Volume_105319120/aoe2-parser-engine/promotions"
)
SAFE_CLASSIFICATION = "safe_result_correction_no_financial_history"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--game-id", type=int, action="append", required=True)
    parser.add_argument("--mode", choices=("plan", "apply"), default="plan")
    parser.add_argument("--receipt-root", type=Path, default=DEFAULT_RECEIPT_ROOT)
    parser.add_argument("--authorization-label", default="operator-reviewed")
    parser.add_argument(
        "--include-projections",
        action="store_true",
        help="Include full before/after payloads in stdout (private receipts always retain them).",
    )
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
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def player_key(player: Mapping[str, Any]) -> str | None:
    steam_id = _clean(player.get("steam_id") or player.get("user_id"))
    if steam_id and steam_id.isdigit():
        return f"steam:{steam_id}"
    name = _clean(player.get("name"))
    return f"name:{name.casefold()}" if name else None


def current_winner_keys(current: Mapping[str, Any]) -> set[str]:
    events = _mapping(current.get("key_events"))
    result = _mapping(events.get("result_resolution"))
    keys = {
        _clean(value).casefold()
        for value in _list(result.get("winning_player_keys"))
        if _clean(value)
    }
    if keys:
        return keys
    return {
        key
        for player in _list(current.get("players"))
        if isinstance(player, Mapping) and player.get("winner") is True
        and (key := player_key(player))
    }


def candidate_result(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    projection = _mapping(candidate.get("projection"))
    events = _mapping(projection.get("key_events"))
    return _mapping(events.get("result_resolution"))


def candidate_team(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    projection = _mapping(candidate.get("projection"))
    events = _mapping(projection.get("key_events"))
    return _mapping(events.get("team_resolution"))


def candidate_winner_keys(candidate: Mapping[str, Any]) -> set[str]:
    return {
        _clean(value).casefold()
        for value in _list(candidate_result(candidate).get("winning_player_keys"))
        if _clean(value)
    }


def classify_projection(
    candidate: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    accepted_adjudications: int,
    linked_markets: int,
    linked_claims: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    projection = _mapping(candidate.get("projection"))
    events = _mapping(projection.get("key_events"))
    run = _mapping(candidate.get("run"))
    result = candidate_result(candidate)
    team = candidate_team(candidate)
    candidate_keys = candidate_winner_keys(candidate)
    current_keys = current_winner_keys(current)
    players = [
        player
        for player in _list(projection.get("players"))
        if isinstance(player, Mapping)
    ]

    if events.get("artifact_role") == "saved_game_snapshot":
        return "saved_checkpoint_not_final", ["saved_game_snapshot"]
    if run.get("status") not in {"succeeded", "recovered"}:
        return "candidate_parse_failed", ["candidate_run_not_successful"]
    if accepted_adjudications:
        return "manual_adjudication_preserved", ["accepted_adjudication_present"]
    if current_keys:
        if current_keys == candidate_keys:
            return "matches_effective_truth", ["winner_keys_already_match"]
        if not candidate_keys or result.get("result_trusted") is not True:
            return "candidate_lower_confidence", ["effective_result_already_known"]
        return "candidate_conflicts_with_effective_truth", ["winner_keys_conflict"]
    if linked_markets or linked_claims:
        return "financially_linked_missing_result", [
            f"linked_markets={linked_markets}",
            f"linked_claims={linked_claims}",
        ]
    if current.get("is_final") is not True:
        reasons.append("game_stats_not_final")
    if len(_list(current.get("players"))) >= 2:
        reasons.append("effective_roster_not_placeholder")
    if len(players) < 2 or len(players) > 8:
        reasons.append("candidate_roster_out_of_bounds")
    roster_key_values = [player_key(player) for player in players]
    if any(key is None for key in roster_key_values):
        reasons.append("candidate_roster_identity_missing")
    if len(set(roster_key_values)) != len(players):
        reasons.append("candidate_roster_identity_not_unique")
    if team.get("status") != "resolved":
        reasons.append("candidate_teams_not_resolved")
    if result.get("result_status") != "resolved" or result.get("result_trusted") is not True:
        reasons.append("candidate_result_not_trusted")
    if result.get("result_provenance") not in {
        "complete_losing_team_resignation",
        "postgame_winner_flags",
        "scoreboard_winner_flags",
    }:
        reasons.append("candidate_result_provenance_not_allowlisted")
    if not candidate_keys:
        reasons.append("candidate_winner_keys_missing")
    roster_keys = {key for key in roster_key_values if key is not None}
    if not candidate_keys.issubset(roster_keys):
        reasons.append("candidate_winners_outside_roster")
    if projection.get("completed") is not True:
        reasons.append("candidate_not_completed")
    if reasons:
        return "private_review", reasons
    return SAFE_CLASSIFICATION, ["all_strict_projection_gates_passed"]


def projection_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "game_version": row.get("game_version"),
        "map": row.get("map"),
        "game_type": row.get("game_type"),
        "duration": row.get("duration"),
        "game_duration": row.get("game_duration"),
        "winner": row.get("winner"),
        "players": row.get("players"),
        "event_types": row.get("event_types"),
        "key_events": row.get("key_events"),
        "disconnect_detected": row.get("disconnect_detected"),
        "parse_source": row.get("parse_source"),
        "parse_reason": row.get("parse_reason"),
    }


def build_after_projection(
    current: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    parse_run_id: int,
) -> dict[str, Any]:
    projection = deepcopy(dict(_mapping(candidate.get("projection"))))
    winner_keys = candidate_winner_keys(candidate)
    players = []
    for raw_player in _list(projection.get("players")):
        if not isinstance(raw_player, Mapping):
            continue
        player = deepcopy(dict(raw_player))
        player["winner"] = player_key(player) in winner_keys
        players.append(player)

    current_events = deepcopy(dict(_mapping(current.get("key_events"))))
    candidate_events = deepcopy(dict(_mapping(projection.get("key_events"))))
    # The candidate's parser_engine node is a large private audit envelope. The
    # effective row only needs the compact, content-addressed projection marker.
    candidate_events.pop("parser_engine", None)
    for stale_key in (
        "parse_failed",
        "parse_failure_detail",
        "watcher_final_unparsed",
    ):
        current_events.pop(stale_key, None)
    current_events.update(candidate_events)
    current_events["engine_room_effective_projection"] = {
        "policy_version": POLICY_VERSION,
        "source_parse_run_id": parse_run_id,
        "source_artifact_sha256": _mapping(candidate.get("artifact")).get("sha256"),
        "source_candidate_semantic_sha256": _mapping(candidate.get("candidate")).get(
            "semantic_sha256"
        ),
        "financial_impact_classification": "no_linked_financial_history",
    }
    return {
        "game_version": projection.get("game_version"),
        "map": projection.get("map"),
        "game_type": projection.get("game_type"),
        "duration": int(projection.get("duration") or 0),
        "game_duration": int(
            projection.get("game_duration") or projection.get("duration") or 0
        ),
        "winner": None,
        "players": players,
        "event_types": projection.get("event_types") or [],
        "key_events": current_events,
        "disconnect_detected": bool(projection.get("disconnect_detected")),
        # Preserve where the replay entered the system while replacing the stale
        # watcher_final_unparsed reason with the adjudicated Engine Room outcome.
        "parse_source": current.get("parse_source") or "watcher_final",
        "parse_reason": "engine_room_trusted_result",
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


def load_plan_rows(
    connection: psycopg.Connection,
    game_ids: list[int],
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
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
        JOIN LATERAL (
          SELECT * FROM replay_parse_runs candidate_run
          WHERE candidate_run.game_stats_id = game.id
            AND candidate_run.status = 'completed'
          ORDER BY candidate_run.completed_at DESC, candidate_run.id DESC
          LIMIT 1
        ) run ON TRUE
        WHERE game.id = ANY(%s)
        ORDER BY game.id
        """,
        (game_ids,),
    ).fetchall()
    found = {int(row["id"]) for row in rows}
    missing = sorted(set(game_ids) - found)
    if missing:
        raise RuntimeError(f"games have no completed candidate run: {missing}")

    plans = []
    for raw_row in rows:
        row = dict(raw_row)
        candidate = load_candidate(row)
        classification, reasons = classify_projection(
            candidate,
            row,
            accepted_adjudications=int(row["accepted_adjudications"]),
            linked_markets=int(row["linked_markets"]),
            linked_claims=int(row["linked_claims"]),
        )
        before = projection_snapshot(row)
        after = (
            build_after_projection(
                row,
                candidate,
                parse_run_id=int(row["source_parse_run_id"]),
            )
            if classification == SAFE_CLASSIFICATION
            else before
        )
        parser_contract = {
            "parser": row["parser_name"],
            "implementation_version": row["parser_version"],
            "schema_version": row["schema_version"],
            "pass_name": row["pass_name"],
            "pass_version": row["pass_version"],
        }
        decision_material = {
            "policy_version": POLICY_VERSION,
            "game_stats_id": int(row["id"]),
            "replay_hash": row["replay_hash"],
            "source_parse_run_id": int(row["source_parse_run_id"]),
            "source_candidate_output_hash": row["candidate_output_hash"],
            "source_candidate_semantic_sha256": _mapping(candidate.get("candidate")).get(
                "semantic_sha256"
            ),
            "parser_contract": parser_contract,
            "classification": classification,
            "before_projection": before,
            "after_projection": after,
            "financial_impact_classification": (
                "no_linked_financial_history"
                if not row["linked_markets"] and not row["linked_claims"]
                else "linked_financial_history"
            ),
        }
        plans.append(
            {
                **decision_material,
                "decision_hash": stable_hash(decision_material),
                "reasons": reasons,
                "accepted_adjudications": int(row["accepted_adjudications"]),
                "linked_markets": int(row["linked_markets"]),
                "linked_claims": int(row["linked_claims"]),
                "candidate_run_status": _mapping(candidate.get("run")).get("status"),
                "candidate_parse_mode": _mapping(candidate.get("run")).get(
                    "parse_mode"
                ),
                "candidate_winning_player_keys": sorted(
                    candidate_winner_keys(candidate)
                ),
            }
        )
    return plans


def write_private_receipt(
    root: Path,
    receipt: Mapping[str, Any],
) -> tuple[Path, bytes, bool]:
    root = root.expanduser().resolve()
    if not str(root).startswith("/mnt/"):
        raise RuntimeError("promotion receipts must live beneath /mnt")
    payload = canonical_json_bytes(receipt) + b"\n"
    digest = hashlib.sha256(payload).hexdigest()
    destination = root / digest[:2] / digest[2:4] / f"{digest}.json"
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination.parent.chmod(0o700)
    if destination.exists():
        if destination.read_bytes() != payload or destination.stat().st_mode & 0o777 != 0o600:
            raise RuntimeError("existing promotion receipt failed integrity validation")
        return destination, payload, False
    with tempfile.NamedTemporaryFile(
        mode="wb",
        prefix=".projection-receipt-",
        suffix=".tmp",
        dir=destination.parent,
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        os.chmod(temporary, 0o600)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination, payload, True


def apply_plans(
    connection: psycopg.Connection,
    plans: list[dict[str, Any]],
    *,
    receipt_root: Path,
    authorization_label: str,
) -> list[dict[str, Any]]:
    allowed = {SAFE_CLASSIFICATION, "matches_effective_truth"}
    unsafe = [plan for plan in plans if plan["classification"] not in allowed]
    if unsafe:
        raise RuntimeError(
            "apply requires every row to pass strict safe-result gates: "
            + ", ".join(
                f"{row['game_stats_id']}={row['classification']}" for row in unsafe
            )
        )

    prepared = []
    reused = []
    applied_at = datetime.now(timezone.utc).isoformat()
    for plan in plans:
        if plan["classification"] == "matches_effective_truth":
            existing = connection.execute(
                """
                SELECT id, sha256, storage_key
                FROM replay_evidence_artifacts
                WHERE evidence_kind = 'effective_projection_receipt'
                  AND source_parse_run_id = %s
                  AND metadata ->> 'game_stats_id' = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    plan["source_parse_run_id"],
                    str(plan["game_stats_id"]),
                ),
            ).fetchone()
            if not existing:
                raise RuntimeError(
                    f"GameStats {plan['game_stats_id']} matches candidate truth but has "
                    "no Engine Room projection receipt"
                )
            reused.append(
                {
                    "game_stats_id": plan["game_stats_id"],
                    "receipt_sha256": existing["sha256"],
                    "receipt_path": existing["storage_key"],
                    "reused": True,
                }
            )
            continue
        receipt = {
            "contract_version": "1",
            "receipt_kind": "effective_replay_result_projection",
            "database_evidence_row_is_commit_proof": True,
            "authorized_by": authorization_label,
            "applied_at": applied_at,
            **plan,
        }
        path, payload, receipt_created = write_private_receipt(receipt_root, receipt)
        prepared.append(
            {
                "plan": plan,
                "receipt": receipt,
                "receipt_path": path,
                "receipt_bytes": payload,
                "receipt_sha256": hashlib.sha256(payload).hexdigest(),
                "receipt_created": receipt_created,
            }
        )

    try:
        with connection.transaction():
            for item in prepared:
                _apply_prepared_projection(connection, item)
    except Exception:
        for item in prepared:
            if item["receipt_created"]:
                item["receipt_path"].unlink(missing_ok=True)
        raise

    return reused + [
        {
            "game_stats_id": item["plan"]["game_stats_id"],
            "decision_hash": item["plan"]["decision_hash"],
            "receipt_sha256": item["receipt_sha256"],
            "receipt_path": str(item["receipt_path"]),
            "reused": False,
        }
        for item in prepared
    ]


def _apply_prepared_projection(
    connection: psycopg.Connection,
    item: Mapping[str, Any],
) -> None:
    plan = item["plan"]
    game_id = int(plan["game_stats_id"])
    current = connection.execute(
        "SELECT * FROM game_stats WHERE id = %s FOR UPDATE",
        (game_id,),
    ).fetchone()
    if not current:
        raise RuntimeError(f"GameStats {game_id} disappeared before apply")
    if stable_hash(projection_snapshot(current)) != stable_hash(
        plan["before_projection"]
    ):
        raise RuntimeError(f"GameStats {game_id} changed after the plan was built")

    after = plan["after_projection"]
    connection.execute(
        """
        UPDATE game_stats SET
          game_version = %s,
          map = %s,
          game_type = %s,
          duration = %s,
          game_duration = %s,
          winner = %s,
          players = %s,
          event_types = %s,
          key_events = %s,
          disconnect_detected = %s,
          parse_source = %s,
          parse_reason = %s
        WHERE id = %s
        """,
        (
            after["game_version"],
            Jsonb(after["map"]),
            after["game_type"],
            after["duration"],
            after["game_duration"],
            after["winner"],
            Jsonb(after["players"]),
            Jsonb(after["event_types"]),
            Jsonb(after["key_events"]),
            after["disconnect_detected"],
            after["parse_source"],
            after["parse_reason"],
            game_id,
        ),
    )

    evidence = connection.execute(
        """
        INSERT INTO replay_evidence_artifacts (
          sha256, byte_size, storage_provider, storage_key,
          evidence_kind, media_type, source_parse_run_id,
          source_candidate_output_hash, captured_at, metadata
        ) VALUES (%s, %s, 'filesystem', %s,
          'effective_projection_receipt', 'application/json', %s,
          %s, %s, %s)
        RETURNING id
        """,
        (
            item["receipt_sha256"],
            len(item["receipt_bytes"]),
            str(item["receipt_path"]),
            plan["source_parse_run_id"],
            plan["source_candidate_output_hash"],
            item["receipt"]["applied_at"],
            Jsonb(
                {
                    "game_stats_id": game_id,
                    "decision_hash": plan["decision_hash"],
                    "policy_version": POLICY_VERSION,
                    "classification": plan["classification"],
                    "financial_impact_classification": plan[
                        "financial_impact_classification"
                    ],
                    "affects_public_aggregates": True,
                    "affects_financial_history": False,
                }
            ),
        ),
    ).fetchone()
    evidence_id = int(evidence["id"])
    connection.execute(
        """
        INSERT INTO replay_evidence_links (
          evidence_artifact_id, parse_run_id, idempotency_key,
          purpose, metadata
        ) VALUES (%s, %s, %s, 'effective_projection_receipt', %s)
        """,
        (
            evidence_id,
            plan["source_parse_run_id"],
            f"effective-projection-receipt:{item['receipt_sha256']}",
            Jsonb({"game_stats_id": game_id}),
        ),
    )

    observations = connection.execute(
        """
        SELECT id, field_path FROM replay_observations
        WHERE parse_run_id = %s
          AND field_path IN ('result.winning_player_keys', 'teams.resolution')
        ORDER BY field_path
        """,
        (plan["source_parse_run_id"],),
    ).fetchall()
    if {row["field_path"] for row in observations} != {
        "result.winning_player_keys",
        "teams.resolution",
    }:
        raise RuntimeError(f"GameStats {game_id} lacks promotion observations")
    for observation in observations:
        promotion_key = f"game:{game_id}:{observation['field_path']}:effective-v1"
        promotion_material = {
            "decision_hash": plan["decision_hash"],
            "observation_id": int(observation["id"]),
            "promotion_key": promotion_key,
            "receipt_sha256": item["receipt_sha256"],
        }
        promotion_hash = stable_hash(promotion_material)
        connection.execute(
            """
            INSERT INTO replay_observation_promotions (
              observation_id, game_stats_id, idempotency_key,
              promotion_key, decision_hash, policy_version,
              reason, affects_public_aggregates
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE)
            """,
            (
                int(observation["id"]),
                game_id,
                f"effective-promotion:{promotion_hash}",
                promotion_key,
                promotion_hash,
                POLICY_VERSION,
                json.dumps(
                    {
                        "reason": "Strict candidate result projection passed all gates.",
                        "receipt_sha256": item["receipt_sha256"],
                        "financial_impact_classification": plan[
                            "financial_impact_classification"
                        ],
                    },
                    sort_keys=True,
                ),
            ),
        )


def main() -> int:
    args = parse_args()
    game_ids = sorted(set(args.game_id))
    if len(game_ids) != len(args.game_id):
        raise SystemExit("duplicate --game-id values are not allowed")
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required and is never printed")

    with psycopg.connect(
        normalize_database_url(args.database_url),
        row_factory=dict_row,
    ) as connection:
        if args.mode == "plan":
            connection.execute("SET TRANSACTION READ ONLY")
        plans = load_plan_rows(connection, game_ids)
        applied = (
            apply_plans(
                connection,
                plans,
                receipt_root=args.receipt_root,
                authorization_label=args.authorization_label,
            )
            if args.mode == "apply"
            else []
        )

    classifications: dict[str, int] = {}
    for plan in plans:
        label = str(plan["classification"])
        classifications[label] = classifications.get(label, 0) + 1
    output = {
        "mode": args.mode,
        "policy_version": POLICY_VERSION,
        "game_count": len(plans),
        "classifications": dict(sorted(classifications.items())),
        "financial_mutations": 0,
        "chain_transactions": 0,
        "plans": (
            plans
            if args.include_projections
            else [
                {
                    key: value
                    for key, value in plan.items()
                    if key not in {"before_projection", "after_projection"}
                }
                for plan in plans
            ]
        ),
        "applied": applied,
    }
    print(json.dumps(output, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
