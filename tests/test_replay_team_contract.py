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
        assert normal["winning_team_id"] == 0


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
    stats = apply_replay_team_contract({"players": team_players(2), "key_events": {}}, final=True)
    assert stats["team_resolution"]["format"] == "2v2"
    assert stats["key_events"]["team_resolution"] == stats["team_resolution"]
