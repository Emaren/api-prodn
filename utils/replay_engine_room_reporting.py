"""Read-only reconciliation summaries for completed Engine Room candidates."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable, Mapping


NON_RESULTS = {"", "draw", "none", "null", "pending", "unknown", "unresolved"}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalized_set(value: Any) -> set[str]:
    return {_clean(item).casefold() for item in _list(value) if _clean(item)}


def _current_winner_keys(current: Mapping[str, Any]) -> set[str]:
    adjudication = _mapping(current.get("latest_adjudication"))
    if adjudication and adjudication.get("decision_status") == "accepted":
        keys = _normalized_set(adjudication.get("winning_player_keys"))
        if keys:
            return keys

    events = _mapping(current.get("key_events"))
    result = _mapping(events.get("result_resolution"))
    keys = _normalized_set(result.get("winning_player_keys"))
    if keys:
        return keys

    players = [item for item in _list(current.get("players")) if isinstance(item, Mapping)]
    winners = []
    for player in players:
        if not player.get("winner"):
            continue
        stable = (
            player.get("stable_player_key")
            or player.get("player_key")
            or (f"steam:{player.get('steam_id')}" if player.get("steam_id") else None)
            or player.get("name")
        )
        if stable:
            winners.append(stable)
    return _normalized_set(winners)


def _candidate_result(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    projection = _mapping(candidate.get("projection"))
    events = _mapping(projection.get("key_events"))
    return _mapping(events.get("result_resolution"))


def _candidate_team(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    projection = _mapping(candidate.get("projection"))
    events = _mapping(projection.get("key_events"))
    return _mapping(events.get("team_resolution"))


def candidate_result_bucket(candidate: Mapping[str, Any]) -> str:
    run = _mapping(candidate.get("run"))
    if run.get("status") == "failed":
        return "unsupported_or_parse_failed"
    projection = _mapping(candidate.get("projection"))
    players = [item for item in _list(projection.get("players")) if isinstance(item, Mapping)]
    if len(players) < 2:
        return "private_review_no_roster"

    result = _candidate_result(candidate)
    if result.get("result_status") == "resolved":
        winning_keys = _normalized_set(result.get("winning_player_keys"))
        winning_names = _normalized_set(result.get("winning_player_names"))
        if winning_keys or winning_names:
            if result.get("result_trusted") is True:
                return "resolved_trusted_direct_evidence"
            return "resolved_coherent_parser_evidence"

    team = _candidate_team(candidate)
    if team.get("status") == "resolved":
        return "private_review_result_not_proven"
    return "private_review_team_or_result_incomplete"


def candidate_promotion_lane(
    candidate: Mapping[str, Any], current: Mapping[str, Any]
) -> str:
    adjudication = _mapping(current.get("latest_adjudication"))
    if adjudication and adjudication.get("decision_status") == "accepted":
        return "human_verdict_preserved"

    result = _candidate_result(candidate)
    candidate_keys = _normalized_set(result.get("winning_player_keys"))
    if not candidate_keys or result.get("result_status") != "resolved":
        return "private_review"

    current_keys = _current_winner_keys(current)
    if current_keys == candidate_keys:
        return "matches_current_truth"
    if not current_keys:
        return "candidate_improves_missing_result"
    return "candidate_conflicts_with_current_truth"


def _observation_profile(candidate: Mapping[str, Any]) -> dict[str, Any]:
    provenance = Counter()
    field_roots = Counter()
    conflict_count = 0
    exact_count = 0
    material_count = 0
    for item in _list(candidate.get("observations")):
        if not isinstance(item, Mapping):
            continue
        provenance_class = _clean(item.get("provenance_class")) or "unspecified"
        provenance[provenance_class] += 1
        field = _clean(item.get("field")) or "unspecified"
        field_roots[field.split(".", 1)[0]] += 1
        conflict_count += str(item.get("conflict_state") or "none") != "none"
        exact_count += item.get("exact") is True
        material_count += provenance_class != "absent"
    return {
        "total": sum(provenance.values()),
        "material": material_count,
        "exact": exact_count,
        "conflicts": conflict_count,
        "provenance": dict(sorted(provenance.items())),
        "field_roots": dict(sorted(field_roots.items())),
    }


def summarize_candidate(
    candidate: Mapping[str, Any], current: Mapping[str, Any]
) -> dict[str, Any]:
    artifact = _mapping(candidate.get("artifact"))
    parser = _mapping(candidate.get("parser"))
    run = _mapping(candidate.get("run"))
    projection = _mapping(candidate.get("projection"))
    result = _candidate_result(candidate)
    team = _candidate_team(candidate)
    actions = _mapping(candidate.get("actions"))
    evidence = _mapping(candidate.get("evidence"))
    chat = _mapping(evidence.get("chat"))
    map_snapshot = _mapping(evidence.get("map_snapshot"))
    players = [item for item in _list(projection.get("players")) if isinstance(item, Mapping)]
    failure = _mapping(run.get("failure"))
    result_bucket = candidate_result_bucket(candidate)
    promotion_lane = candidate_promotion_lane(candidate, current)
    return {
        "game_stats_id": current.get("game_stats_id"),
        "original_filename": current.get("original_filename") or "",
        "artifact_sha256": artifact.get("sha256"),
        "parser_version": parser.get("implementation_version"),
        "schema_version": parser.get("schema_version"),
        "pass_name": parser.get("pass_name"),
        "pass_version": parser.get("pass_version"),
        "run_status": run.get("status"),
        "parse_mode": run.get("parse_mode"),
        "failure_signature": failure.get("signature"),
        "player_count": len(players),
        "team_status": team.get("status"),
        "team_format": team.get("format"),
        "result_status": result.get("result_status"),
        "result_trusted": result.get("result_trusted") is True,
        "result_provenance": result.get("result_provenance"),
        "winning_player_keys": sorted(_normalized_set(result.get("winning_player_keys"))),
        "winning_player_names": list(result.get("winning_player_names") or []),
        "current_winning_player_keys": sorted(_current_winner_keys(current)),
        "result_bucket": result_bucket,
        "promotion_lane": promotion_lane,
        "settlement_evidence_eligible": (
            result_bucket == "resolved_trusted_direct_evidence"
            and (len(players) == 2 or team.get("status") == "resolved")
        ),
        "map_name": _mapping(projection.get("map")).get("name"),
        "duration_seconds": projection.get("duration") or projection.get("game_duration"),
        "raw_action_count": actions.get("count"),
        "chat_available": chat.get("available") is True,
        "chat_message_count": chat.get("count") if chat.get("available") is True else None,
        "map_snapshot_available": map_snapshot.get("available") is True,
        "observation_profile": _observation_profile(candidate),
    }


def aggregate_candidate_summaries(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    source = list(rows)
    result_buckets = Counter(_clean(row.get("result_bucket")) for row in source)
    promotion_lanes = Counter(_clean(row.get("promotion_lane")) for row in source)
    parse_modes = Counter(_clean(row.get("parse_mode")) for row in source)
    failures = Counter(
        _clean(row.get("failure_signature"))
        for row in source
        if _clean(row.get("failure_signature"))
    )
    parser_contracts = Counter(
        "|".join(
            _clean(row.get(key))
            for key in ("parser_version", "schema_version", "pass_name", "pass_version")
        )
        for row in source
    )
    observation_provenance = Counter()
    field_roots = Counter()
    emitted = material = exact = conflicts = actions = 0
    for row in source:
        profile = _mapping(row.get("observation_profile"))
        emitted += int(profile.get("total") or 0)
        material += int(profile.get("material") or 0)
        exact += int(profile.get("exact") or 0)
        conflicts += int(profile.get("conflicts") or 0)
        actions += int(row.get("raw_action_count") or 0)
        observation_provenance.update(_mapping(profile.get("provenance")))
        field_roots.update(_mapping(profile.get("field_roots")))

    manually_verified = sum(
        row.get("promotion_lane") == "human_verdict_preserved" for row in source
    )
    non_manual = [
        row for row in source if row.get("promotion_lane") != "human_verdict_preserved"
    ]
    private_review = sum(
        str(row.get("result_bucket") or "").startswith("private_review")
        for row in non_manual
    )
    unsupported = sum(
        row.get("result_bucket") == "unsupported_or_parse_failed"
        for row in non_manual
    )
    resolved = len(non_manual) - private_review - unsupported
    return {
        "candidate_rows": len(source),
        "result_buckets": dict(sorted(result_buckets.items())),
        "promotion_lanes": dict(sorted(promotion_lanes.items())),
        "parse_modes": dict(sorted(parse_modes.items())),
        "failure_buckets": dict(failures.most_common()),
        "parser_contracts": dict(sorted(parser_contracts.items())),
        "equation": {
            "resolved_by_parser": resolved,
            "resolved_by_promoted_historical_evidence": 0,
            "manually_verified": manually_verified,
            "private_review_candidates": private_review,
            "unsupported_or_corrupt": unsupported,
            "total": len(source),
            "balanced": (
                resolved + manually_verified + private_review + unsupported
                == len(source)
            ),
        },
        "advanced_stat_coverage": {
            "raw_actions": actions,
            "observations_emitted": emitted,
            "observations_material": material,
            "observations_exact": exact,
            "observation_conflicts": conflicts,
            "provenance": dict(sorted(observation_provenance.items())),
            "field_roots": dict(sorted(field_roots.items())),
            "chat_available_games": sum(row.get("chat_available") is True for row in source),
            "map_snapshot_available_games": sum(
                row.get("map_snapshot_available") is True for row in source
            ),
        },
        "settlement_evidence_eligible": sum(
            row.get("settlement_evidence_eligible") is True for row in source
        ),
    }
