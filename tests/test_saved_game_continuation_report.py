from scripts.report_saved_game_continuation_links import _pair_rows, _summarize


def _source_row(**overrides):
    row = {
        "platform_match_id": "match-1",
        "saved_run_id": 11,
        "saved_game_stats_id": 101,
        "saved_artifact_sha256": "a" * 64,
        "saved_parse_mode": "mgz_hd_saved_game_snapshot",
        "saved_player_names": ["jim", "tony"],
        "saved_player_steam_ids": ["1", "2"],
        "recorded_run_id": 21,
        "recorded_game_stats_id": 201,
        "recorded_artifact_sha256": "b" * 64,
        "recorded_parse_mode": "mgz_full_summary",
        "recorded_player_names": ["jim", "tony"],
        "recorded_player_steam_ids": ["1", "2"],
    }
    row.update(overrides)
    return row


def test_pair_requires_exact_name_and_steam_rosters():
    exact, mismatch = _pair_rows(
        [
            _source_row(),
            _source_row(
                platform_match_id="match-2",
                saved_run_id=12,
                recorded_run_id=22,
                recorded_player_steam_ids=["1", "3"],
            ),
        ]
    )

    assert exact["deterministic_continuation_link"] is True
    assert exact["saved_roster_sha256"] == exact["recorded_roster_sha256"]
    assert mismatch["player_names_exact"] is True
    assert mismatch["player_steam_ids_exact"] is False
    assert mismatch["deterministic_continuation_link"] is False
    assert "saved_player_names" not in exact
    assert "saved_player_steam_ids" not in exact


def test_summary_counts_multi_checkpoint_links_without_promoting_truth():
    rows = _pair_rows(
        [
            _source_row(),
            _source_row(saved_run_id=12),
            _source_row(
                platform_match_id="match-2",
                saved_run_id=13,
                recorded_run_id=22,
            ),
        ]
    )
    summary = _summarize(
        {
            "completed_latest_candidates": 6,
            "saved_candidates": 4,
            "recorded_candidates": 2,
            "saved_candidates_with_platform_match_id": 4,
            "saved_distinct_platform_match_ids": 3,
        },
        rows,
    )

    assert summary["shared_platform_match_ids"] == 2
    assert summary["linked_saved_checkpoints"] == 3
    assert summary["linked_recorded_candidates"] == 2
    assert summary["unlinked_saved_checkpoints"] == 1
    assert summary["unlinked_saved_platform_match_ids"] == 1
    assert summary["one_to_one_platform_match_ids"] == 1
    assert summary["multi_checkpoint_platform_match_ids"] == 1
    assert summary["max_saved_checkpoints_per_platform_match_id"] == 2
    assert summary["creates_public_result_truth"] is False
    assert summary["settlement_evidence_eligible"] is False
