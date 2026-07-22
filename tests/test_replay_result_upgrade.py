from types import SimpleNamespace

from routes.replay_routes_async import (
    _has_trusted_resolved_result,
    _should_refresh_reviewed_match,
    _should_upgrade_duplicate_final,
)


def unresolved_events():
    return {
        "completed": False,
        "result_resolution": {
            "result_status": "review_required",
            "result_trusted": False,
            "winning_team_id": None,
            "winning_player_keys": [],
            "winning_player_names": [],
        },
    }


def trusted_events():
    return {
        "completed": True,
        "result_resolution": {
            "result_status": "resolved",
            "result_trusted": True,
            "winning_team_id": 1,
            "winning_player_keys": ["uid:winner"],
            "winning_player_names": ["Winner"],
            "result_provenance": "complete_losing_team_resignation",
        },
    }


def existing_game(key_events):
    return SimpleNamespace(
        key_events=key_events,
        parse_reason="watcher_final_unparsed",
        disconnect_detected=False,
        duration=2400,
        event_types=["move", "order"],
    )


def test_trusted_result_signal_requires_resolved_trusted_winner():
    assert _has_trusted_resolved_result(trusted_events()) is True
    assert _has_trusted_resolved_result(unresolved_events()) is False


def test_duplicate_final_upgrades_when_result_truth_becomes_trusted():
    existing = existing_game(unresolved_events())

    assert (
        _should_upgrade_duplicate_final(
            existing,
            "watcher_final_unparsed",
            False,
            trusted_events(),
        )
        is True
    )


def test_reviewed_match_refreshes_on_trusted_result_even_without_more_duration():
    existing = existing_game(unresolved_events())

    assert (
        _should_refresh_reviewed_match(
            existing,
            incoming_duration=2400,
            incoming_key_events=trusted_events(),
            incoming_players=[],
            incoming_event_types=["move", "order"],
        )
        is True
    )


def test_untrusted_result_does_not_force_duplicate_upgrade():
    existing = existing_game(unresolved_events())

    assert (
        _should_upgrade_duplicate_final(
            existing,
            "watcher_final_unparsed",
            False,
            unresolved_events(),
        )
        is False
    )


def test_untrusted_result_does_not_replace_existing_trusted_result():
    existing = existing_game(trusted_events())

    assert (
        _should_refresh_reviewed_match(
            existing,
            incoming_duration=2400,
            incoming_key_events=unresolved_events(),
            incoming_players=[],
            incoming_event_types=["move", "order"],
        )
        is False
    )
