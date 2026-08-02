from types import SimpleNamespace

from routes.replay_routes_async import (
    _has_trusted_resolved_result,
    _should_advance_unresolved_reviewed_match_artifact,
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


def existing_game(
    key_events,
    *,
    replay_hash="a" * 64,
    artifact_size=620_711,
):
    enriched_key_events = dict(key_events)
    enriched_key_events["watcher_upload"] = {
        "file_size_bytes": artifact_size,
        "server_sha256": replay_hash,
        "final_candidate": True,
    }

    return SimpleNamespace(
        replay_hash=replay_hash,
        key_events=enriched_key_events,
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

def test_larger_unresolved_final_artifact_advances_identity():
    existing = existing_game(
        unresolved_events(),
        replay_hash="a" * 64,
        artifact_size=620_711,
    )

    assert (
        _should_advance_unresolved_reviewed_match_artifact(
            existing,
            "b" * 64,
            621_770,
        )
        is True
    )


def test_artifact_identity_does_not_advance_for_same_or_smaller_bytes():
    existing = existing_game(
        unresolved_events(),
        replay_hash="a" * 64,
        artifact_size=744_996,
    )

    assert not _should_advance_unresolved_reviewed_match_artifact(
        existing,
        "a" * 64,
        745_100,
    )
    assert not _should_advance_unresolved_reviewed_match_artifact(
        existing,
        "b" * 64,
        744_996,
    )
    assert not _should_advance_unresolved_reviewed_match_artifact(
        existing,
        "b" * 64,
        743_856,
    )


def test_artifact_identity_requires_known_existing_final_size():
    existing = existing_game(
        unresolved_events(),
        replay_hash="a" * 64,
        artifact_size=0,
    )

    assert not _should_advance_unresolved_reviewed_match_artifact(
        existing,
        "b" * 64,
        621_770,
    )


def test_unresolved_artifact_never_replaces_trusted_result_even_when_larger():
    existing = existing_game(
        trusted_events(),
        replay_hash="a" * 64,
        artifact_size=620_711,
    )

    assert not _should_advance_unresolved_reviewed_match_artifact(
        existing,
        "b" * 64,
        621_770,
    )
    assert (
        _should_refresh_reviewed_match(
            existing,
            incoming_duration=3600,
            incoming_key_events=unresolved_events(),
            incoming_players=[],
            incoming_event_types=[
                "move",
                "order",
                "build",
                "research",
                "tribute",
            ],
        )
        is False
    )


def test_small_duration_growth_without_result_uses_artifact_identity_lane():
    existing = existing_game(
        unresolved_events(),
        replay_hash="a" * 64,
        artifact_size=620_711,
    )

    assert (
        _should_refresh_reviewed_match(
            existing,
            incoming_duration=2402,
            incoming_key_events=unresolved_events(),
            incoming_players=[],
            incoming_event_types=["move", "order"],
        )
        is False
    )
    assert (
        _should_advance_unresolved_reviewed_match_artifact(
            existing,
            "b" * 64,
            621_770,
        )
        is True
    )
