from utils.replay_engine_room_reporting import (
    aggregate_candidate_summaries,
    candidate_promotion_lane,
    candidate_result_bucket,
    summarize_candidate,
)


def candidate(*, trusted=True, winner_keys=None, status="succeeded"):
    winner_keys = winner_keys if winner_keys is not None else ["steam:1", "steam:2"]
    result_status = "resolved" if winner_keys else "review_required"
    return {
        "artifact": {"sha256": "a" * 64},
        "parser": {
            "implementation_version": "1.8.51",
            "schema_version": "2026-07-14.1",
            "pass_name": "hd_deterministic_evidence",
            "pass_version": "1",
        },
        "run": {"status": status, "parse_mode": "mgz_full_summary", "failure": None},
        "projection": {
            "players": [
                {"name": "Jim", "steam_id": "1", "winner": bool(winner_keys)},
                {"name": "Rick", "steam_id": "2", "winner": bool(winner_keys)},
                {"name": "MTR", "steam_id": "3", "winner": False},
                {"name": "Jake", "steam_id": "4", "winner": False},
            ],
            "map": {"name": "Forest Nothing"},
            "duration": 1800,
            "key_events": {
                "team_resolution": {"status": "resolved", "format": "2v2"},
                "result_resolution": {
                    "result_status": result_status,
                    "result_trusted": trusted,
                    "result_provenance": "complete_losing_team_resignation",
                    "winning_player_keys": winner_keys,
                    "winning_player_names": ["Jim", "Rick"] if winner_keys else [],
                },
            },
        },
        "actions": {"count": 1200},
        "evidence": {
            "chat": {"available": True, "count": 5},
            "map_snapshot": {"available": True},
        },
        "observations": [
            {
                "field": "result.winning_player_keys",
                "provenance_class": "derived_coherent",
                "exact": False,
                "conflict_state": "none",
            },
            {
                "field": "postgame.military.score",
                "provenance_class": "absent",
                "exact": False,
                "conflict_state": "none",
            },
        ],
    }


def test_trusted_complete_team_result_is_resolved_and_settlement_evidence_eligible():
    current = {"game_stats_id": 42, "players": [], "key_events": {}}
    summary = summarize_candidate(candidate(), current)
    assert summary["result_bucket"] == "resolved_trusted_direct_evidence"
    assert summary["promotion_lane"] == "candidate_improves_missing_result"
    assert summary["settlement_evidence_eligible"] is True
    assert summary["observation_profile"]["material"] == 1


def test_human_verdict_always_stays_authoritative_in_report_lane():
    current = {
        "latest_adjudication": {
            "decision_status": "accepted",
            "winning_player_keys": ["steam:3", "steam:4"],
        }
    }
    assert candidate_promotion_lane(candidate(), current) == "human_verdict_preserved"


def test_missing_result_goes_to_private_review_without_inventing_a_winner():
    no_result = candidate(winner_keys=[])
    assert candidate_result_bucket(no_result) == "private_review_result_not_proven"
    assert candidate_promotion_lane(no_result, {}) == "private_review"


def test_aggregate_equation_and_advanced_counts_balance():
    resolved = summarize_candidate(candidate(), {})
    review = summarize_candidate(candidate(winner_keys=[]), {})
    failed_candidate = candidate(status="failed", winner_keys=[])
    failed_candidate["projection"] = None
    failed_candidate["run"]["failure"] = {"signature": "summary:invalid_container:x"}
    failed = summarize_candidate(failed_candidate, {})
    aggregate = aggregate_candidate_summaries([resolved, review, failed])
    assert aggregate["equation"] == {
        "resolved_by_parser": 1,
        "resolved_by_promoted_historical_evidence": 0,
        "manually_verified": 0,
        "private_review_candidates": 1,
        "unsupported_or_corrupt": 1,
        "total": 3,
        "balanced": True,
    }
    assert aggregate["advanced_stat_coverage"]["raw_actions"] == 3600
    assert aggregate["advanced_stat_coverage"]["observations_emitted"] == 6
