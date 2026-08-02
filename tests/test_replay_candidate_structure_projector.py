from scripts.project_replay_candidate_structure import (
    POLICY_VERSION,
    SAFE_CLASSIFICATION,
    build_after_structure,
    classify_structure,
    stable_hash,
)


def candidate(*, team_status="resolved", conflicts=0, run_status="recovered"):
    observations = [
        {"conflict_state": "conflict"}
        for _ in range(conflicts)
    ]
    return {
        "artifact": {"sha256": "a" * 64},
        "run": {"status": run_status, "parse_mode": "fixture"},
        "candidate": {"semantic_sha256": "b" * 64},
        "observations": observations,
        "projection": {
            "game_version": "Version.HD",
            "map": {"name": "Forest Nothing Feitoria", "size": "6 player"},
            "game_type": "RM",
            "players": [
                {"name": "Alpha", "steam_id": "1", "team_id": 0, "winner": True},
                {"name": "Bravo", "steam_id": "2", "team_id": 0, "winner": True},
                {"name": "Charlie", "steam_id": "3", "team_id": 1, "winner": False},
                {"name": "Delta", "steam_id": "4", "team_id": 1, "winner": False},
            ],
            "key_events": {
                "artifact_role": "saved_game_snapshot",
                "team_resolution": {
                    "status": team_status,
                    "format": "2v2",
                    "teams": [
                        {"team_id": 0, "players": ["Alpha", "Bravo"]},
                        {"team_id": 1, "players": ["Charlie", "Delta"]},
                    ],
                },
                "result_resolution": {
                    "result_status": "review_required",
                    "result_trusted": False,
                },
            },
        },
    }


def current(**overrides):
    value = {
        "id": 42,
        "is_final": True,
        "game_version": None,
        "map": {"name": "Unknown", "size": "Unknown"},
        "game_type": None,
        "winner": "Unknown",
        "players": [{"name": "Uploader", "winner": None}],
        "key_events": {
            "watcher_final_unparsed": True,
            "parse_failed": True,
        },
        "parse_source": "watcher_final",
        "parse_reason": "watcher_final_unparsed",
    }
    value.update(overrides)
    return value


def classify(value, effective=None, **gates):
    return classify_structure(
        value,
        effective or current(),
        accepted_adjudications=gates.get("accepted_adjudications", 0),
        linked_markets=gates.get("linked_markets", 0),
        linked_claims=gates.get("linked_claims", 0),
    )


def test_saved_checkpoint_with_resolved_structure_is_display_safe_without_result():
    classification, reasons = classify(candidate())
    assert classification == SAFE_CLASSIFICATION
    assert reasons == ["all_structural_projection_gates_passed"]


def test_financial_manual_conflict_and_team_gates_block_projection():
    assert classify(candidate(), accepted_adjudications=1)[0] == "private_review"
    assert classify(candidate(), linked_markets=1)[0] == "private_review"
    assert classify(candidate(), linked_claims=1)[0] == "private_review"
    assert classify(candidate(team_status="conflicting"))[0] == "private_review"
    assert classify(candidate(conflicts=1))[0] == "private_review"


def test_projection_publishes_structure_but_strips_all_result_authority():
    fixture = candidate()
    after = build_after_structure(current(), fixture, parse_run_id=99)

    assert after["map"]["name"] == "Forest Nothing Feitoria"
    assert [player["name"] for player in after["players"]] == [
        "Alpha",
        "Bravo",
        "Charlie",
        "Delta",
    ]
    assert all(player["winner"] is None for player in after["players"])
    assert after["winner"] is None
    assert after["parse_reason"] == "engine_room_structural_projection"
    assert "watcher_final_unparsed" not in after["key_events"]
    assert "parse_failed" not in after["key_events"]
    assert after["key_events"]["team_resolution"]["status"] == "resolved"

    marker = after["key_events"]["engine_room_structural_projection"]
    assert marker["policy_version"] == POLICY_VERSION
    assert marker["source_parse_run_id"] == 99
    assert marker["previous_failure"] == {
        "parse_failed": True,
        "watcher_final_unparsed": True,
    }
    assert marker["result_authority"] is False
    assert marker["affects_results"] is False
    assert marker["affects_bets"] is False
    assert marker["settlement_authority"] is False
    assert stable_hash(after) == stable_hash(after)
