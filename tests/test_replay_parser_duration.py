import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.replay_parser import (
    _apply_completion_metadata,
    _apply_hd_early_exit_rules,
    _maybe_apply_hd_early_exit_rules,
    _normalize_mgz_duration_seconds,
)


def test_normalize_mgz_duration_seconds_converts_millis_to_seconds():
    assert _normalize_mgz_duration_seconds(480) == 1
    assert _normalize_mgz_duration_seconds(6007) == 7
    assert _normalize_mgz_duration_seconds(26460) == 27


def test_apply_hd_early_exit_rules_suppresses_under_60s_rated_result():
    stats = {
        "game_version": "Version.HD",
        "duration": 27,
        "winner": "Emaren",
        "completed": True,
        "disconnect_detected": False,
        "players": [
            {"name": "Emaren", "winner": True},
            {"name": "kaoritec", "winner": False},
        ],
        "key_events": {
            "rated": True,
            "completed": True,
            "resigned_player_numbers": [2],
        },
    }

    patched = _apply_hd_early_exit_rules(stats)

    assert patched["winner"] == "Unknown"
    assert patched["completed"] is False
    assert patched["disconnect_detected"] is True
    assert patched["parse_reason"] == "hd_early_exit_under_60s"
    assert patched["key_events"]["no_rated_result"] is True
    assert patched["key_events"]["suppressed_winner"] == "Emaren"
    assert all(player["winner"] is None for player in patched["players"])


def test_apply_hd_early_exit_rules_skips_false_under_60_when_game_chat_runs_long():
    stats = {
        "game_version": "Version.HD",
        "duration": 8,
        "winner": "Julio Alvarez",
        "completed": True,
        "disconnect_detected": False,
        "players": [
            {"name": "Roger", "winner": False},
            {"name": "Julio Alvarez", "winner": True},
        ],
        "key_events": {
            "rated": True,
            "completed": True,
            "resigned_player_numbers": [1],
            "chat_preview": [
                {
                    "origination": "game",
                    "timestamp_seconds": 1249,
                    "message": "9",
                },
                {
                    "origination": "game",
                    "timestamp_seconds": 6781,
                    "message": "gg",
                },
            ],
        },
    }

    patched = _apply_hd_early_exit_rules(stats)

    assert patched["winner"] == "Julio Alvarez"
    assert patched["duration"] == 6781
    assert patched["key_events"]["duration_source"] == "chat_preview_seconds_override"
    assert "parse_reason" not in patched


def test_apply_hd_early_exit_rules_keeps_real_short_final_over_60_seconds():
    stats = {
        "game_version": "Version.HD",
        "duration": 83,
        "winner": "Emaren",
        "completed": True,
        "disconnect_detected": False,
        "parse_reason": "recorded_resignation_final",
        "players": [
            {"name": "Emaren", "winner": True},
            {"name": "Latin_k", "winner": False},
        ],
        "key_events": {
            "rated": True,
            "completed": True,
            "resigned_player_numbers": [2],
        },
    }

    patched = _apply_hd_early_exit_rules(stats)

    assert patched["winner"] == "Emaren"
    assert patched["completed"] is True
    assert patched["disconnect_detected"] is False
    assert patched["parse_reason"] == "recorded_resignation_final"


def test_maybe_apply_hd_early_exit_rules_skips_live_pulse_override_when_disabled():
    stats = {
        "game_version": "Version.HD",
        "duration": 27,
        "winner": "Emaren",
        "completed": True,
        "disconnect_detected": False,
        "players": [
            {"name": "Emaren", "winner": True},
            {"name": "kaoritec", "winner": False},
        ],
        "key_events": {
            "rated": True,
            "completed": True,
            "resigned_player_numbers": [2],
        },
    }

    patched = _maybe_apply_hd_early_exit_rules(stats, False)

    assert patched["winner"] == "Emaren"
    assert patched["completed"] is True
    assert patched["disconnect_detected"] is False
    assert "parse_reason" not in patched


def test_apply_completion_metadata_marks_resignation_final_without_scoreboard():
    stats = {
        "completed": True,
        "disconnect_detected": False,
        "players": [
            {"name": "Emaren", "winner": True, "score": None},
            {"name": "Bo", "winner": False, "score": None},
        ],
        "key_events": {
            "completed": True,
            "resigned_player_numbers": [2],
            "has_scores": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "achievement_shell_count": 2,
            "postgame_available": False,
        },
    }

    patched = _apply_completion_metadata(stats)

    assert patched["parse_reason"] == "recorded_resignation_final"
    assert patched["completion_source"] == "resignation"
    assert patched["postgame_available"] is False
    assert patched["player_score_count"] == 0
    assert patched["achievement_player_count"] == 0
    assert patched["achievement_shell_count"] == 2
    assert patched["has_achievement_shell"] is True
    assert patched["key_events"]["completion_source"] == "resignation"


def test_apply_completion_metadata_prefers_scoreboard_when_scores_exist():
    stats = {
        "completed": True,
        "disconnect_detected": False,
        "players": [
            {"name": "Emaren", "winner": True, "score": 2012},
            {"name": "Bo", "winner": False, "score": 1833},
        ],
        "key_events": {
            "completed": True,
            "resigned_player_numbers": [2],
            "has_scores": False,
            "has_achievements": False,
            "player_score_count": 0,
            "achievement_player_count": 0,
            "postgame_available": False,
        },
    }

    patched = _apply_completion_metadata(stats)

    assert patched["completion_source"] == "scoreboard"
    assert patched["has_scores"] is True
    assert patched["player_score_count"] == 2
    assert patched["key_events"]["completion_source"] == "scoreboard"
