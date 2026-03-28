import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.append(str(Path(__file__).resolve().parents[1]))

from routes.replay_routes_async import (
    _derive_upload_parse_metadata,
    _extract_platform_match_id,
    _infer_incomplete_uploader_outcome,
    _parse_bool_header,
    _parse_positive_int_header,
)


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


def test_extract_platform_match_id_trims_valid_values():
    assert _extract_platform_match_id({"platform_match_id": "  abc-123  "}) == "abc-123"
    assert _extract_platform_match_id({"platform_match_id": ""}) is None
    assert _extract_platform_match_id({"platform_match_id": None}) is None
    assert _extract_platform_match_id([]) is None


def test_infer_incomplete_uploader_outcome_promotes_opponent_for_long_rated_1v1():
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

    assert inferred is not None
    assert inferred["winner"] == "Sniper"
    assert inferred["disconnect_detected"] is True
    assert inferred["parse_reason"] == "watcher_inferred_opponent_win_on_incomplete_1v1"
    assert inferred["key_events"]["winner_inference"]["uploader_player"] == "Emaren"


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
