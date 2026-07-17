from scripts.project_replay_candidate_results import (
    SAFE_CLASSIFICATION,
    build_after_projection,
    classify_projection,
    current_winner_keys,
    stable_hash,
)


def candidate(*, trusted=True, artifact_role=None):
    players = [
        {"name": "Alpha", "steam_id": "1", "team_id": 0, "winner": None},
        {"name": "Bravo", "steam_id": "2", "team_id": 0, "winner": None},
        {"name": "Charlie", "steam_id": "3", "team_id": 1, "winner": None},
        {"name": "Delta", "steam_id": "4", "team_id": 1, "winner": None},
    ]
    result = {
        "result_status": "resolved" if trusted else "review_required",
        "result_trusted": trusted,
        "result_provenance": "complete_losing_team_resignation",
        "winning_player_keys": ["steam:1", "steam:2"] if trusted else [],
    }
    events = {
        "completed": True,
        "team_resolution": {"status": "resolved", "format": "2v2"},
        "result_resolution": result,
    }
    if artifact_role:
        events["artifact_role"] = artifact_role
    return {
        "artifact": {"sha256": "a" * 64},
        "parser": {"schema_version": "fixture"},
        "run": {"status": "recovered", "parse_mode": "fixture"},
        "candidate": {"semantic_sha256": "b" * 64},
        "projection": {
            "game_version": "Version.HD",
            "map": {"name": "Arabia"},
            "game_type": "RM",
            "duration": 900,
            "players": players,
            "event_types": ["resign"],
            "key_events": events,
            "completed": True,
            "disconnect_detected": False,
        },
    }


def current(**overrides):
    value = {
        "id": 42,
        "is_final": True,
        "game_version": None,
        "map": None,
        "game_type": None,
        "duration": 0,
        "game_duration": 0,
        "winner": "Unknown",
        "players": [{"name": "Uploader", "winner": None}],
        "event_types": [],
        "key_events": {"watcher_upload": True},
        "disconnect_detected": False,
        "parse_source": "watcher_final",
        "parse_reason": "watcher_final_unparsed",
    }
    value.update(overrides)
    return value


def classify(value, effective=None, **gates):
    return classify_projection(
        value,
        effective or current(),
        accepted_adjudications=gates.get("accepted_adjudications", 0),
        linked_markets=gates.get("linked_markets", 0),
        linked_claims=gates.get("linked_claims", 0),
    )


def test_strict_missing_result_without_financial_history_is_safe():
    classification, reasons = classify(candidate())
    assert classification == SAFE_CLASSIFICATION
    assert reasons == ["all_strict_projection_gates_passed"]


def test_adjudication_financial_and_saved_snapshot_gates_are_absolute():
    assert classify(candidate(), accepted_adjudications=1)[0] == (
        "manual_adjudication_preserved"
    )
    assert classify(candidate(), linked_markets=1)[0] == (
        "financially_linked_missing_result"
    )
    assert classify(
        candidate(artifact_role="saved_game_snapshot")
    )[0] == "saved_checkpoint_not_final"


def test_known_effective_result_matches_or_blocks_candidate():
    matching = current(
        players=[
            {"name": "Alpha", "steam_id": "1", "winner": True},
            {"name": "Bravo", "steam_id": "2", "winner": True},
        ]
    )
    assert current_winner_keys(matching) == {"steam:1", "steam:2"}
    assert classify(candidate(), matching)[0] == "matches_effective_truth"

    conflict = current(
        players=[
            {"name": "Charlie", "steam_id": "3", "winner": True},
            {"name": "Delta", "steam_id": "4", "winner": True},
        ]
    )
    assert classify(candidate(), conflict)[0] == (
        "candidate_conflicts_with_effective_truth"
    )


def test_projection_marks_every_winning_teammate_and_preserves_watcher_context():
    effective = current(
        key_events={
            "watcher_upload": True,
            "parse_failed": True,
            "parse_failure_detail": "old parser failed",
            "watcher_final_unparsed": True,
        }
    )
    fixture = candidate()
    fixture["projection"]["key_events"]["parser_engine"] = {"large": "private"}
    after = build_after_projection(effective, fixture, parse_run_id=99)
    winners = [player["name"] for player in after["players"] if player["winner"]]
    assert winners == ["Alpha", "Bravo"]
    assert after["winner"] is None
    assert after["key_events"]["watcher_upload"] is True
    assert "parse_failed" not in after["key_events"]
    assert "parse_failure_detail" not in after["key_events"]
    assert "watcher_final_unparsed" not in after["key_events"]
    assert "parser_engine" not in after["key_events"]
    assert after["parse_source"] == "watcher_final"
    assert after["parse_reason"] == "engine_room_trusted_result"
    receipt = after["key_events"]["engine_room_effective_projection"]
    assert receipt["source_parse_run_id"] == 99
    assert receipt["financial_impact_classification"] == (
        "no_linked_financial_history"
    )
    assert stable_hash(after) == stable_hash(after)
