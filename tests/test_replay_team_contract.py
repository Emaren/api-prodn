import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.replay_team_contract import (
    apply_replay_team_contract,
    canonicalize_replay_players,
    resolve_replay_teams,
)


def team_players(size):
    return [
        {
            "name": f"Player {index + 1}",
            "steamId": f"7656119800000000{index}",
            "teamNumber": 0 if index % 2 == 0 else 1,
            "playerNumber": index + 1,
            "winner": index % 2 == 0,
        }
        for index in range(size * 2)
    ]


def test_canonical_player_contract_preserves_hd_team_zero_and_aliases():
    [player] = canonicalize_replay_players(
        [{"name": "Jim", "teamId": 0, "steamId": "76561198000000001", "totalScore": 12}]
    )
    assert player["team_id"] == 0
    assert player["steam_id"] == "76561198000000001"
    assert player["score"] == 12


def test_explicit_2v2_3v3_and_4v4_resolve_independent_of_player_order():
    for size in (2, 3, 4):
        players = team_players(size)
        normal = resolve_replay_teams(players, final=True)
        reversed_result = resolve_replay_teams(list(reversed(players)), final=True)
        assert normal["status"] == "resolved"
        assert normal["format"] == f"{size}v{size}"
        assert normal["teams"] == reversed_result["teams"]
        assert normal["winning_team_id"] is None
        assert normal["winning_player_keys"] == []
        assert normal["result_status"] == "review_required"
        assert normal["result_trusted"] is False
        assert normal["result_evidence"]["winner_flag_team_id"] == 0


def test_team_games_fail_closed_without_two_complete_equal_explicit_teams():
    missing = team_players(2)
    missing[0].pop("teamNumber")
    assert resolve_replay_teams(missing)["status"] == "incomplete"

    three_teams = team_players(2)
    three_teams[0]["teamNumber"] = 2
    result = resolve_replay_teams(three_teams)
    assert result["status"] == "conflicting"
    assert "expected_exactly_two_teams" in result["reason_codes"]


def test_winning_team_requires_every_winner_and_every_loser_flag():
    players = team_players(2)
    players[1]["winner"] = True
    result = resolve_replay_teams(players, final=True)
    assert result["status"] == "resolved"
    assert result["winning_team_id"] is None


def test_contract_embeds_resolution_in_key_events():
    stats = apply_replay_team_contract(
        {
            "players": team_players(2),
            "completed": True,
            "completion_source": "resignation",
            "key_events": {
                "completed": True,
                "completion_source": "resignation",
                "resigned_player_numbers": [2, 4],
            },
        },
        final=True,
    )
    assert stats["team_resolution"]["format"] == "2v2"
    assert stats["key_events"]["team_resolution"] == stats["team_resolution"]
    assert stats["winning_team_id"] == 0
    assert stats["winning_player_keys"] == stats["team_resolution"]["winning_player_keys"]
    assert stats["key_events"]["result_resolution"] == stats["result_resolution"]


def test_golden_hd_2v2_requires_full_losing_team_resignation_for_trusted_result():
    # Mirrors the supplied 2026-07-06 18:28:42 HD replay: Emaren and Merik
    # won as one complete team and both opponents resigned.
    players = [
        {"name": "Emaren", "number": 1, "team_id": 1, "winner": True},
        {"name": "Merik", "number": 2, "team_id": 1, "winner": True},
        {"name": "javier_sv1907", "number": 3, "team_id": 0, "winner": False},
        {"name": "Matzar117", "number": 4, "team_id": 0, "winner": False},
    ]
    result = resolve_replay_teams(
        players,
        final=True,
        key_events={
            "completed": True,
            "resigned_player_numbers": [3, 4],
            "resigned_player_names": ["javier_sv1907", "Matzar117"],
            "postgame_available": False,
            "has_scores": False,
            "has_achievements": False,
        },
    )

    assert result["winning_team_id"] == 1
    assert result["winning_player_names"] == ["Emaren", "Merik"]
    assert result["result_status"] == "resolved"
    assert result["result_confidence"] == "high"
    assert result["result_trusted"] is True
    assert result["result_provenance"] == "complete_losing_team_resignation"


def test_first_team_resignation_is_display_evidence_not_settlement_proof():
    players = [
        {"name": "Alpha", "number": 1, "team_id": 0, "winner": True},
        {"name": "Bravo", "number": 2, "team_id": 0, "winner": True},
        {"name": "Charlie", "number": 3, "team_id": 1, "winner": False},
        {"name": "Delta", "number": 4, "team_id": 1, "winner": False},
    ]
    result = resolve_replay_teams(
        players,
        final=True,
        key_events={
            "completed": True,
            "resigned_player_numbers": [3],
            "postgame_available": False,
        },
    )

    assert result["winning_team_id"] is None
    assert result["winning_player_names"] == []
    assert result["result_status"] == "review_required"
    assert result["result_confidence"] == "review"
    assert result["result_trusted"] is False
    assert result["result_evidence"]["complete_losing_team_resignation"] is False
    assert result["result_evidence"]["partially_resigned_team_ids"] == [1]

    stats = apply_replay_team_contract(
        {
            "players": players,
            "completed": True,
            "completion_source": "resignation",
            "key_events": {
                "completed": True,
                "completion_source": "resignation",
                "resigned_player_numbers": [3],
            },
        },
        final=True,
    )
    assert stats["completed"] is False
    assert stats["key_events"]["raw_mgz_completed_signal"] is True
    assert stats["key_events"]["resignation_proves_team_completion"] is False
    assert stats["completion_source"] == "team_resignation_review_required"


def test_exactly_one_fully_resigned_team_derives_opponent_without_winner_flags():
    players = [
        {"name": "Alpha", "number": 1, "team_id": 0, "winner": None},
        {"name": "Bravo", "number": 2, "team_id": 0, "winner": None},
        {"name": "Charlie", "number": 3, "team_id": 1, "winner": None},
        {"name": "Delta", "number": 4, "team_id": 1, "winner": None},
    ]
    result = resolve_replay_teams(
        players,
        final=True,
        key_events={"resigned_player_numbers": [3, 4]},
    )

    assert result["winning_team_id"] == 0
    assert result["winning_player_names"] == ["Alpha", "Bravo"]
    assert result["result_trusted"] is True
    assert result["result_provenance"] == "complete_losing_team_resignation"
    assert result["result_evidence"]["winner_flags_coherent"] is False
    assert result["result_evidence"]["fully_resigned_team_ids"] == [1]


def test_both_fully_resigned_teams_and_conflicting_evidence_stay_review_only():
    players = [
        {"name": "Alpha", "number": 1, "team_id": 0, "winner": True},
        {"name": "Bravo", "number": 2, "team_id": 0, "winner": True},
        {"name": "Charlie", "number": 3, "team_id": 1, "winner": False},
        {"name": "Delta", "number": 4, "team_id": 1, "winner": False},
    ]
    both = resolve_replay_teams(
        players,
        final=True,
        key_events={"resigned_player_numbers": [1, 2, 3, 4]},
    )
    assert both["winning_team_id"] is None
    assert both["result_status"] == "review_required"
    assert both["result_provenance"] == "conflicting_result_evidence"
    assert both["result_evidence"]["resignation_state"] == "multiple_complete_teams"

    conflict = resolve_replay_teams(
        players,
        final=True,
        key_events={"resigned_player_numbers": [1, 2]},
    )
    assert conflict["winning_team_id"] is None
    assert conflict["result_status"] == "review_required"
    assert conflict["result_evidence"]["resignation_result_conflict"] is True


def test_golden_hd_no_resignation_keeps_result_unresolved():
    result = resolve_replay_teams(
        [
            {"name": "Emaren", "number": 1, "team_id": 1, "winner": None},
            {"name": "lucas T", "number": 2, "team_id": 0, "winner": None},
        ],
        final=True,
        key_events={
            "completed": False,
            "resigned_player_numbers": [],
            "postgame_available": False,
            "has_scores": False,
            "has_achievements": False,
        },
    )

    assert result["winning_team_id"] is None
    assert result["winning_player_keys"] == []
    assert result["result_status"] == "review_required"
    assert result["result_trusted"] is False



def test_postgame_can_trust_winner_flags_concentrated_on_one_explicit_team():
    players = [
        {"name": "Alpha", "number": 1, "team_id": 0, "winner": True},
        {"name": "Bravo", "number": 2, "team_id": 0, "winner": None},
        {"name": "Charlie", "number": 3, "team_id": 1, "winner": False},
        {"name": "Delta", "number": 4, "team_id": 1, "winner": False},
    ]

    result = resolve_replay_teams(
        players,
        final=True,
        key_events={"postgame_available": True},
    )

    assert result["winning_team_id"] == 0
    assert result["winning_player_names"] == ["Alpha", "Bravo"]
    assert result["result_trusted"] is True
    assert (
        result["result_provenance"]
        == "postgame_single_team_winner_flags"
    )


def test_single_team_partial_winner_flags_without_decisive_completion_stay_review_only():
    players = [
        {"name": "Alpha", "number": 1, "team_id": 0, "winner": True},
        {"name": "Bravo", "number": 2, "team_id": 0, "winner": None},
        {"name": "Charlie", "number": 3, "team_id": 1, "winner": False},
        {"name": "Delta", "number": 4, "team_id": 1, "winner": False},
    ]

    result = resolve_replay_teams(
        players,
        final=True,
        key_events={},
    )

    assert result["winning_team_id"] is None
    assert result["result_trusted"] is False
    assert (
        result["result_provenance"]
        == "single_team_player_winner_flags_review"
    )


def test_winner_flags_on_both_teams_remain_review_only_even_with_postgame():
    players = [
        {"name": "Alpha", "number": 1, "team_id": 0, "winner": True},
        {"name": "Bravo", "number": 2, "team_id": 0, "winner": None},
        {"name": "Charlie", "number": 3, "team_id": 1, "winner": True},
        {"name": "Delta", "number": 4, "team_id": 1, "winner": False},
    ]

    result = resolve_replay_teams(
        players,
        final=True,
        key_events={"postgame_available": True},
    )

    assert result["winning_team_id"] is None
    assert result["result_trusted"] is False
