"""Canonical replay-player and team-resolution contract for AoE2HD data.

Player order is presentation data, never team membership. Team games resolve only
from two complete, equally sized sets of explicit replay team IDs. HD team ID 0 is
valid and must not be treated as missing.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def _integer(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _number(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return int(parsed) if parsed.is_integer() else parsed


def _boolean(value: Any) -> bool | None:
    if value in (True, 1, "1", "true", "True"):
        return True
    if value in (False, 0, "0", "false", "False"):
        return False
    return None


def _first(record: dict, *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] is not None:
            return record[key]
    return None


def _team_id(value: Any) -> int | str | None:
    if value is None or value == "":
        return None
    parsed = _integer(value)
    if parsed is not None:
        return None if parsed == -1 else parsed
    cleaned = _clean_text(value)
    if not cleaned or cleaned.lower() in {"none", "unknown", "null", "-1"}:
        return None
    return cleaned


def _steam_id(value: Any) -> str | None:
    cleaned = _clean_text(str(value)) if value is not None else None
    return cleaned if cleaned and cleaned.isdigit() and 15 <= len(cleaned) <= 20 else None


def canonicalize_replay_player(value: Any) -> dict | None:
    if not isinstance(value, dict):
        return None
    name = _clean_text(_first(value, "name", "player", "player_name", "displayName"))
    if not name:
        return None

    player = deepcopy(value)
    steam_id = _steam_id(_first(value, "steam_id", "steamId", "user_id"))
    civilization = _first(value, "civilization", "civilization_id", "civilizationId")
    civilization_id = _integer(civilization)
    civilization_name = _clean_text(
        _first(value, "civilization_name", "civilizationName")
    )
    if civilization_name is None and isinstance(civilization, str) and civilization_id is None:
        civilization_name = _clean_text(civilization)

    player.update(
        {
            "name": name,
            "steam_id": steam_id,
            "user_id": steam_id,
            "civilization": civilization_id if civilization_id is not None else civilization,
            "civilization_name": civilization_name,
            "color_id": _integer(_first(value, "color_id", "colorId", "color")),
            "position": _first(value, "position"),
            "team_id": _team_id(
                _first(value, "team_id", "teamId", "team_number", "teamNumber", "team")
            ),
            "number": _integer(_first(value, "number", "player_number", "playerNumber")),
            "winner": _boolean(_first(value, "winner", "isWinner", "won")),
            "score": _integer(_first(value, "score", "total_score", "totalScore")),
            "rate_snapshot": _integer(
                _first(value, "rate_snapshot", "rating_snapshot", "ratingSnapshot")
            ),
            "eapm": _number(_first(value, "eapm")),
            "achievements": _first(value, "achievements"),
        }
    )
    return player


def canonicalize_replay_players(values: Any) -> list[dict]:
    if not isinstance(values, list):
        return []
    players = [canonicalize_replay_player(value) for value in values]
    return [player for player in players if player is not None]


def _stable_player_key(player: dict) -> str:
    steam_id = player.get("steam_id")
    if steam_id:
        return f"steam:{steam_id}"
    return f"name:{str(player.get('name') or '').casefold()}"


def resolve_replay_teams(values: Any, *, final: bool = False) -> dict:
    players = canonicalize_replay_players(values)
    reason_codes: list[str] = []
    player_count = len(players)
    stable_keys = [_stable_player_key(player) for player in players]
    if len(set(stable_keys)) != player_count:
        reason_codes.append("duplicate_player_identity")

    if player_count == 2 and not reason_codes:
        teams = [
            {"team_id": stable_key, "players": [player["name"]], "player_keys": [stable_key]}
            for stable_key, player in sorted(zip(stable_keys, players), key=lambda item: item[0])
        ]
        status = "resolved"
        game_format = "1v1"
        provenance = "one_vs_one_roster"
    elif player_count not in {4, 6, 8}:
        reason_codes.append("roster_incomplete" if player_count < 2 else "unsupported_team_format")
        teams = []
        status = "incomplete" if player_count < 2 else "unsupported"
        game_format = "unknown"
        provenance = "unresolved"
    elif any(player.get("team_id") is None for player in players):
        reason_codes.append("team_id_missing")
        teams = []
        status = "incomplete"
        game_format = "unknown"
        provenance = "unresolved"
    else:
        grouped: dict[str, list[dict]] = {}
        original_team_ids: dict[str, Any] = {}
        for player in players:
            team_key = str(player["team_id"])
            grouped.setdefault(team_key, []).append(player)
            original_team_ids.setdefault(team_key, player["team_id"])
        if len(grouped) != 2:
            reason_codes.append("expected_exactly_two_teams")
        expected_size = player_count // 2
        if any(len(team_players) != expected_size for team_players in grouped.values()):
            reason_codes.append("unequal_team_sizes")

        if reason_codes:
            teams = []
            status = "conflicting"
            game_format = "unknown"
            provenance = "unresolved"
        else:
            def team_sort(item: tuple[str, list[dict]]) -> tuple[int, Any]:
                try:
                    return (0, int(item[0]))
                except ValueError:
                    return (1, item[0])

            teams = []
            for team_key, team_players in sorted(grouped.items(), key=team_sort):
                ordered = sorted(team_players, key=_stable_player_key)
                teams.append(
                    {
                        "team_id": original_team_ids[team_key],
                        "players": [player["name"] for player in ordered],
                        "player_keys": [_stable_player_key(player) for player in ordered],
                    }
                )
            status = "resolved"
            game_format = f"{expected_size}v{expected_size}"
            provenance = "explicit_final_team_ids" if final else "explicit_replay_team_ids"

    winning_team_id = None
    if status == "resolved":
        winning_indexes = []
        losing_indexes = []
        player_by_key = {_stable_player_key(player): player for player in players}
        for index, team in enumerate(teams):
            flags = [player_by_key[key].get("winner") for key in team["player_keys"]]
            if flags and all(flag is True for flag in flags):
                winning_indexes.append(index)
            if flags and all(flag is False for flag in flags):
                losing_indexes.append(index)
        if (
            len(winning_indexes) == 1
            and len(losing_indexes) == 1
            and winning_indexes[0] != losing_indexes[0]
        ):
            winning_team_id = teams[winning_indexes[0]]["team_id"]

    return {
        "status": status,
        "format": game_format,
        "confidence": "high" if status == "resolved" else "low",
        "provenance": provenance,
        "reason_codes": sorted(set(reason_codes)),
        "player_count": player_count,
        "team_count": len(teams),
        "teams": teams,
        "winning_team_id": winning_team_id,
    }


def apply_replay_team_contract(stats: Any, *, final: bool | None = None) -> Any:
    if not isinstance(stats, dict):
        return stats
    players = canonicalize_replay_players(stats.get("players"))
    stats["players"] = players
    is_final = bool(stats.get("completed")) if final is None else bool(final)
    resolution = resolve_replay_teams(players, final=is_final)
    key_events = stats.get("key_events") if isinstance(stats.get("key_events"), dict) else {}
    key_events = dict(key_events)
    key_events["team_resolution"] = resolution
    stats["key_events"] = key_events
    stats["team_resolution"] = resolution
    return stats
