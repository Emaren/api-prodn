import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.append(str(Path(__file__).resolve().parents[1]))

from routes.replay_routes_async import (
    _finality_response,
    UNPARSED_FINAL_PARSE_REASON,
    _build_unparsed_watcher_final_payload,
    _derive_upload_parse_metadata,
    _extract_platform_match_id,
    _has_reliable_final_signal,
    _infer_incomplete_uploader_outcome,
    _normalize_live_disconnect_detected,
    _parse_bool_header,
    _parse_positive_int_header,
    _should_upgrade_duplicate_final,
    _should_refresh_reviewed_match,
    _split_previous_version_supersession,
)


def test_finality_response_exposes_retry_and_completeness_contract():
    live = _finality_response(
        {"is_final": False, "players_count": 8, "winner": "Unknown"},
        finality_status="live",
    )
    assert live["parse_completeness"] == "live_roster"
    assert live["should_continue_monitoring"] is True
    assert live["betting_eligible"] is False

    unresolved_team_final = _finality_response(
        {
            "is_final": True,
            "players_count": 8,
            "winner": "Jim",
            "team_resolution": {
                "status": "incomplete",
                "confidence": "low",
                "team_count": 0,
                "winning_team_id": None,
            },
        },
        finality_status="trusted_final",
        should_settle=True,
    )
    assert unresolved_team_final["has_reliable_teams"] is False
    assert unresolved_team_final["betting_eligible"] is False

    resolved_team_final = _finality_response(
        {
            "is_final": True,
            "players_count": 8,
            "winner": "Jim",
            "team_resolution": {
                "status": "resolved",
                "confidence": "high",
                "team_count": 2,
                "winning_team_id": 0,
                "winning_player_keys": ["name:jim", "name:tekki", "name:rick", "name:scavanger_ab"],
                "result_status": "resolved",
                "result_trusted": True,
                "result_provenance": "complete_losing_team_resignation",
                "result_evidence": {
                    "sources": ["complete_losing_team_resignation"],
                },
            },
        },
        finality_status="trusted_final",
        should_settle=True,
        raw_replay_archived=True,
    )
    assert resolved_team_final["has_coherent_winning_team"] is True
    assert resolved_team_final["result_resolved"] is True
    assert resolved_team_final["result_trusted"] is True
    assert resolved_team_final["betting_eligible"] is True
    assert resolved_team_final["stats_eligible"] is True
    assert resolved_team_final["raw_replay_archived"] is True
    assert resolved_team_final["artifact_accepted"] is True
    assert resolved_team_final["final_artifact_accepted"] is True
    assert resolved_team_final["winning_team_id"] == 0

    final = _finality_response(
        {"is_final": True, "players_count": 2, "winner": "Player One"},
        finality_status="trusted_final",
        should_settle=True,
    )
    assert final["final_accepted"] is True
    assert final["should_continue_monitoring"] is False
    assert final["betting_eligible"] is True


def test_final_recorded_separates_archive_stats_and_betting_readiness():
    response = _finality_response(
        {
            "is_final": True,
            "players_count": 4,
            # Legacy scalar remains for display compatibility but cannot settle a
            # team game without structured trusted result evidence.
            "winner": "Merik",
            "team_resolution": {
                "status": "resolved",
                "confidence": "high",
                "team_count": 2,
                "winning_team_id": 1,
                "winning_player_keys": ["name:emaren", "name:merik"],
                "result_status": "resolved",
                "result_trusted": False,
                "result_provenance": "coherent_player_winner_flags",
                "result_evidence": {"sources": ["coherent_player_winner_flags"]},
            },
        },
        finality_status="final_recorded",
        should_settle=False,
        raw_replay_archived=True,
    )

    assert response["finality_status"] == "final_recorded"
    assert response["raw_replay_archived"] is True
    assert response["parse_completed"] is True
    assert response["final_submission_received"] is True
    assert response["result_resolved"] is True
    assert response["stats_eligible"] is True
    assert response["result_trusted"] is False
    assert response["betting_eligible"] is False
    assert response["should_settle"] is False
    assert response["final_accepted"] is False
    assert response["should_continue_monitoring"] is False


def test_team_scalar_winner_never_bypasses_structured_result_contract():
    response = _finality_response(
        {
            "is_final": True,
            "players_count": 8,
            "winner": "Jim",
            "team_resolution": {
                "status": "resolved",
                "confidence": "high",
                "team_count": 2,
                "winning_team_id": None,
                "winning_player_keys": [],
                "result_status": "unresolved",
                "result_trusted": False,
            },
        },
        finality_status="final_recorded",
        should_settle=True,
        raw_replay_archived=True,
    )

    assert response["result_resolved"] is False
    assert response["has_reliable_winner"] is False
    assert response["stats_eligible"] is False
    assert response["betting_eligible"] is False


def test_parse_bool_header_understands_live_and_final_flags():
    assert _parse_bool_header("true", False) is True
    assert _parse_bool_header("final", False) is True
    assert _parse_bool_header("false", True) is False
    assert _parse_bool_header("live", True) is False
    assert _parse_bool_header(None, True) is True


def test_parse_positive_int_header_uses_positive_values_only():
    assert _parse_positive_int_header("3", 1) == 3
    assert _parse_positive_int_header("0", 1) == 1
    assert _parse_positive_int_header("-7", 2) == 2
    assert _parse_positive_int_header("abc", 4) == 4


def test_derive_upload_parse_metadata_prefers_watcher_live_defaults():
    parse_source, parse_reason = _derive_upload_parse_metadata(
        upload_mode="watcher",
        is_final=False,
        requested_source=None,
        requested_reason=None,
        parsed_reason="watcher_or_browser",
    )

    assert parse_source == "watcher_live"
    assert parse_reason == "watcher_live_iteration"


def test_derive_upload_parse_metadata_preserves_parser_reason_when_specific():
    parse_source, parse_reason = _derive_upload_parse_metadata(
        upload_mode="watcher",
        is_final=True,
        requested_source=None,
        requested_reason=None,
        parsed_reason="hd_early_exit_under_60s",
    )

    assert parse_source == "watcher_final"
    assert parse_reason == "hd_early_exit_under_60s"


def test_derive_upload_parse_metadata_overrides_generic_watcher_reason_with_parser_truth():
    parse_source, parse_reason = _derive_upload_parse_metadata(
        upload_mode="watcher",
        is_final=True,
        requested_source="watcher_final",
        requested_reason="watcher_final_submission",
        parsed_reason="recorded_resignation_final",
    )

    assert parse_source == "watcher_final"
    assert parse_reason == "recorded_resignation_final"


def test_extract_platform_match_id_trims_valid_values():
    assert _extract_platform_match_id({"platform_match_id": "  abc-123  "}) == "abc-123"
    assert _extract_platform_match_id({"platform_match_id": ""}) is None
    assert _extract_platform_match_id({"platform_match_id": None}) is None
    assert _extract_platform_match_id([]) is None


def test_infer_incomplete_uploader_outcome_never_manufactures_authoritative_result():
    user = SimpleNamespace(
        steam_id="76561198065420384",
        in_game_name="Emaren",
        steam_persona_name="Emaren",
    )
    parsed = {
        "winner": "Unknown",
        "completed": False,
        "players": [
            {"name": "Emaren", "user_id": "76561198065420384", "winner": None},
            {"name": "Sniper", "user_id": "76561198041444664", "winner": None},
        ],
        "key_events": {
            "rated": True,
            "completed": False,
            "platform_match_id": "abc-123",
        },
    }

    inferred = _infer_incomplete_uploader_outcome(parsed, user, None)

    assert inferred is None
    assert parsed["winner"] == "Unknown"
    assert all(player["winner"] is None for player in parsed["players"])
    assert "winner_inference" not in parsed["key_events"]


def test_infer_incomplete_uploader_outcome_skips_under_60_no_result():
    user = SimpleNamespace(
        steam_id="76561198065420384",
        in_game_name="Emaren",
        steam_persona_name="Emaren",
    )
    parsed = {
        "winner": "Unknown",
        "completed": False,
        "parse_reason": "hd_early_exit_under_60s",
        "players": [
            {"name": "Emaren", "user_id": "76561198065420384", "winner": None},
            {"name": "kaoritec", "user_id": "76561198904976282", "winner": None},
        ],
        "key_events": {
            "rated": True,
            "completed": False,
            "no_rated_result": True,
        },
    }

    assert _infer_incomplete_uploader_outcome(parsed, user, None) is None


def test_has_reliable_final_signal_rejects_completion_without_result_proof():
    assert not _has_reliable_final_signal(
        {
            "winner": "Unknown",
            "key_events": {
                "completed": True,
                "postgame_available": False,
            },
        }
    )


def test_has_reliable_final_signal_rejects_deprecated_inferred_disconnect_outcome():
    inferred = {
        "winner": "Sniper",
    }

    assert not _has_reliable_final_signal(
        {
            "winner": "Unknown",
            "key_events": {
                "completed": False,
                "postgame_available": False,
            },
        },
        inferred,
    )


def test_has_reliable_final_signal_accepts_structured_complete_team_result():
    assert _has_reliable_final_signal(
        {
            "winner": "Merik",
            "players": [
                {"name": "Emaren", "number": 1, "team_id": 1, "winner": True},
                {"name": "Merik", "number": 2, "team_id": 1, "winner": True},
                {"name": "javier_sv1907", "number": 3, "team_id": 0, "winner": False},
                {"name": "Matzar117", "number": 4, "team_id": 0, "winner": False},
            ],
            "key_events": {
                "completed": True,
                "resigned_player_numbers": [3, 4],
                "postgame_available": False,
            },
        }
    )


def test_has_reliable_final_signal_rejects_paused_unknown_replay():
    assert not _has_reliable_final_signal(
        {
            "winner": "Unknown",
            "key_events": {
                "completed": False,
                "postgame_available": False,
            },
        }
    )


def test_has_reliable_final_signal_rejects_header_only_fallback_with_players():
    assert not _has_reliable_final_signal(
        {
            "winner": "Unknown",
            "players": [
                {"name": "Julio Alvarez", "header_only": True},
                {"name": "gauthier.massicot", "header_only": True},
            ],
            "key_events": {
                "completed": False,
                "header_only_fallback": True,
            },
        }
    )


def test_build_unparsed_watcher_final_payload_preserves_uploader_identity_and_time():
    user = SimpleNamespace(
        uid="u_julio",
        steam_id="76561198190973517",
        in_game_name="Julio Alvarez",
        steam_persona_name="Julio",
    )

    payload = _build_unparsed_watcher_final_payload(
        original_name="MP Replay v5.8 @2026.06.05 093848 (2).aoe2record",
        uploader_uid="u_julio",
        uploader_user=user,
        claimed_name=None,
        parse_failure_detail="Failed to parse replay file.",
        file_size_bytes=1147959,
    )

    assert payload["parse_reason"] == UNPARSED_FINAL_PARSE_REASON
    assert payload["winner"] == "Unknown"
    assert payload["played_on"].year == 2026
    assert payload["played_on"].hour == 9
    assert payload["players"] == [
        {
            "name": "Julio Alvarez",
            "civilization": "Unknown",
            "team": None,
            "score": None,
            "winner": None,
            "position": None,
            "user_id": "76561198190973517",
            "steam_rm_rating": None,
            "steam_dm_rating": None,
            "eapm": None,
            "watcher_uploader_fallback": True,
        }
    ]
    assert payload["key_events"]["watcher_final_unparsed"] is True
    assert payload["key_events"]["uploader_player_name"] == "Julio Alvarez"


def test_normalize_live_disconnect_detected_clears_active_live_false_positive():
    assert not _normalize_live_disconnect_detected(
        False,
        True,
        {
            "completed": False,
            "postgame_available": False,
        },
    )


def test_normalize_live_disconnect_detected_preserves_final_disconnect_signal():
    assert _normalize_live_disconnect_detected(
        True,
        True,
        {
            "completed": False,
        },
    )


def test_should_upgrade_duplicate_final_when_resignation_truth_is_clearer():
    existing_game = SimpleNamespace(
        parse_reason="watcher_final_submission",
        disconnect_detected=True,
        key_events={
            "completed": True,
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
        },
    )

    assert _should_upgrade_duplicate_final(
        existing_game,
        "recorded_resignation_final",
        False,
        {
            "completed": True,
            "completion_source": "resignation",
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "achievement_shell_count": 2,
        },
    )


def test_should_upgrade_duplicate_final_when_existing_row_is_unparsed_proof():
    existing_game = SimpleNamespace(
        parse_reason=UNPARSED_FINAL_PARSE_REASON,
        disconnect_detected=False,
        key_events={"watcher_final_unparsed": True},
    )

    assert _should_upgrade_duplicate_final(
        existing_game,
        "header_only_summary_fallback",
        False,
        {"header_only_fallback": True},
    )


def test_should_not_upgrade_duplicate_final_without_better_truth():
    existing_game = SimpleNamespace(
        parse_reason="recorded_resignation_final",
        disconnect_detected=False,
        key_events={
            "completed": True,
            "completion_source": "resignation",
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
        },
    )

    assert not _should_upgrade_duplicate_final(
        existing_game,
        "recorded_resignation_final",
        False,
        {
            "completed": True,
            "completion_source": "resignation",
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "achievement_shell_count": 0,
        },
    )


def test_should_upgrade_duplicate_final_when_achievement_shell_count_improves():
    existing_game = SimpleNamespace(
        parse_reason="recorded_resignation_final",
        disconnect_detected=False,
        key_events={
            "completed": True,
            "completion_source": "resignation",
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "achievement_shell_count": 0,
        },
    )

    assert _should_upgrade_duplicate_final(
        existing_game,
        "recorded_resignation_final",
        False,
        {
            "completed": True,
            "completion_source": "resignation",
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "achievement_shell_count": 2,
        },
    )


def test_split_previous_version_supersession_keeps_final_when_live_shell_exists():
    rows = [
        SimpleNamespace(id=10, replay_hash="old-live-hash"),
        SimpleNamespace(id=11, replay_hash="old-final-only-hash"),
        SimpleNamespace(id=12, replay_hash=None),
    ]

    demote_ids, mark_only_ids = _split_previous_version_supersession(
        rows,
        {"old-live-hash"},
    )

    assert demote_ids == [11, 12]
    assert mark_only_ids == [10]


def test_should_refresh_reviewed_match_when_later_final_is_much_longer():
    existing_game = SimpleNamespace(
        duration=256,
        key_events={"chat_count": 2},
        event_types=["build", "order"],
    )

    assert _should_refresh_reviewed_match(
        existing_game,
        3288,
        {"chat_count": 2},
        [],
        ["build", "order"],
    )


def test_should_not_refresh_reviewed_match_for_small_progress_bump():
    existing_game = SimpleNamespace(
        duration=1200,
        key_events={"chat_count": 6},
        event_types=["build", "order", "move"],
    )

    assert not _should_refresh_reviewed_match(
        existing_game,
        1220,
        {"chat_count": 6},
        [],
        ["build", "order", "move"],
    )


def test_should_refresh_reviewed_match_when_postgame_truth_arrives():
    existing_game = SimpleNamespace(
        duration=61,
        key_events={
            "completed": True,
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "chat_count": 1,
        },
        event_types=["order", "move", "build"],
    )

    assert _should_refresh_reviewed_match(
        existing_game,
        61,
        {
            "completed": True,
            "postgame_available": True,
            "has_achievements": True,
            "player_score_count": 2,
            "achievement_player_count": 2,
            "chat_count": 1,
        },
        [],
        ["order", "move", "build"],
    )


def test_should_refresh_reviewed_match_when_scores_arrive_without_duration_gain():
    existing_game = SimpleNamespace(
        duration=300,
        key_events={
            "completed": True,
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "chat_count": 4,
        },
        event_types=["order", "move"],
    )

    assert _should_refresh_reviewed_match(
        existing_game,
        300,
        {
            "completed": True,
            "postgame_available": False,
            "has_achievements": False,
            "player_score_count": 2,
            "achievement_player_count": 0,
            "chat_count": 4,
        },
        [],
        ["order", "move"],
    )
