import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.replay_parser import _apply_hd_early_exit_rules, _normalize_mgz_duration_seconds


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
