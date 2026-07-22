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


def _result_resolution(
    players: list[dict],
    teams: list[dict],
    winner_flag_team_id: Any,
    single_team_winner_flag_team_id: Any,
    key_events: Any,
) -> dict:
    """Describe result truth separately from lobby/team truth.

    ``mgz`` winner flags are useful evidence, but in an HD team game they can be
    inferred after the first resignation.  Resignation proof therefore resolves
    a result only when exactly one complete explicit team resigned.  The other
    explicit team is then the derived winner.  No complete team, both complete
    teams, or a conflict with coherent winner flags remains review-only.
    """
    events = key_events if isinstance(key_events, dict) else {}
    player_by_key = {_stable_player_key(player): player for player in players}
    resigned_numbers = {
        _integer(value)
        for value in events.get("resigned_player_numbers", [])
        if _integer(value) is not None
    }
    resigned_names = {
        str(value).casefold()
        for value in events.get("resigned_player_names", [])
        if isinstance(value, str) and value.strip()
    }

    def player_resigned(player: dict) -> bool:
        number = _integer(player.get("number"))
        name = str(player.get("name") or "").casefold()
        return (number is not None and number in resigned_numbers) or (
            bool(name) and name in resigned_names
        )

    fully_resigned_team_ids = []
    partially_resigned_team_ids = []
    resignation_counts_by_team = []
    for team in teams:
        team_players = [
            player_by_key[player_key]
            for player_key in team.get("player_keys") or []
            if player_key in player_by_key
        ]
        resigned_count = sum(player_resigned(player) for player in team_players)
        player_count = len(team_players)
        resignation_counts_by_team.append(
            {
                "team_id": team.get("team_id"),
                "player_count": player_count,
                "resigned_player_count": resigned_count,
            }
        )
        if player_count and resigned_count == player_count:
            fully_resigned_team_ids.append(team.get("team_id"))
        elif resigned_count > 0:
            partially_resigned_team_ids.append(team.get("team_id"))

    resignation_derived_winning_team_id = None
    if len(teams) == 2 and len(fully_resigned_team_ids) == 1:
        resigned_team_id = fully_resigned_team_ids[0]
        resignation_derived_winning_team_id = next(
            (
                team.get("team_id")
                for team in teams
                if team.get("team_id") != resigned_team_id
            ),
            None,
        )

    winner_evidence_team_id = (
        winner_flag_team_id
        if winner_flag_team_id is not None
        else single_team_winner_flag_team_id
    )
    resignation_result_conflict = bool(
        resignation_derived_winning_team_id is not None
        and winner_evidence_team_id is not None
        and resignation_derived_winning_team_id != winner_evidence_team_id
    )
    complete_losing_team_resignation = bool(
        resignation_derived_winning_team_id is not None
        and not resignation_result_conflict
    )
    postgame_available = bool(events.get("postgame_available"))
    scoreboard_available = bool(
        events.get("has_scores")
        or events.get("has_achievements")
        or _integer(events.get("player_score_count"))
        or _integer(events.get("achievement_player_count"))
    )
    winner_flags_coherent = winner_flag_team_id is not None
    winner_flags_single_team = single_team_winner_flag_team_id is not None

    sources: list[str] = []
    if postgame_available:
        sources.append("postgame")
    if scoreboard_available:
        sources.append("scoreboard")
    if complete_losing_team_resignation:
        sources.append("complete_losing_team_resignation")
    if winner_flags_coherent:
        sources.append("coherent_player_winner_flags")
    elif winner_flags_single_team:
        sources.append("single_team_player_winner_flags")

    winning_team_id = None
    if resignation_result_conflict or len(fully_resigned_team_ids) > 1:
        provenance = "conflicting_result_evidence"
    elif complete_losing_team_resignation:
        winning_team_id = resignation_derived_winning_team_id
        provenance = "complete_losing_team_resignation"
    elif postgame_available and winner_evidence_team_id is not None:
        winning_team_id = winner_evidence_team_id
        provenance = (
            "postgame_winner_flags"
            if winner_flags_coherent
            else "postgame_single_team_winner_flags"
        )
    elif scoreboard_available and winner_evidence_team_id is not None:
        winning_team_id = winner_evidence_team_id
        provenance = (
            "scoreboard_winner_flags"
            if winner_flags_coherent
            else "scoreboard_single_team_winner_flags"
        )
    elif winner_evidence_team_id is not None:
        provenance = (
            "coherent_player_winner_flags_review"
            if winner_flags_coherent
            else "single_team_player_winner_flags_review"
        )
    else:
        provenance = "insufficient_result_evidence"

    trusted = winning_team_id is not None
    winning_team = next(
        (team for team in teams if team.get("team_id") == winning_team_id),
        None,
    )
    winning_player_keys = list(winning_team.get("player_keys") or []) if winning_team else []
    winning_player_names = list(winning_team.get("players") or []) if winning_team else []
    if len(fully_resigned_team_ids) == 1:
        resignation_state = "exactly_one_complete_team"
    elif len(fully_resigned_team_ids) > 1:
        resignation_state = "multiple_complete_teams"
    else:
        resignation_state = "no_complete_team"

    return {
        "winning_team_id": winning_team_id,
        "result_status": "resolved" if trusted else "review_required",
        "result_confidence": "high" if trusted else "review",
        "result_provenance": provenance,
        "result_trusted": trusted,
        "winning_player_keys": winning_player_keys,
        "winning_player_names": winning_player_names,
        "result_evidence": {
            "sources": sources,
            "winner_flags_coherent": winner_flags_coherent,
            "winner_flags_single_team": winner_flags_single_team,
            "winner_flag_team_id": winner_flag_team_id,
            "single_team_winner_flag_team_id": single_team_winner_flag_team_id,
            "postgame_available": postgame_available,
            "scoreboard_available": scoreboard_available,
            "complete_losing_team_resignation": complete_losing_team_resignation,
            "resignation_state": resignation_state,
            "fully_resigned_team_ids": fully_resigned_team_ids,
            "partially_resigned_team_ids": partially_resigned_team_ids,
            "resignation_counts_by_team": resignation_counts_by_team,
            "resignation_derived_winning_team_id": (
                resignation_derived_winning_team_id
            ),
            "resignation_result_conflict": resignation_result_conflict,
            "team_completion_from_resignations": complete_losing_team_resignation,
        },
    }


def resolve_replay_teams(
    values: Any,
    *,
    final: bool = False,
    key_events: Any = None,
) -> dict:
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

    winner_flag_team_id = None
    single_team_winner_flag_team_id = None
    if status == "resolved":
        winning_indexes = []
        losing_indexes = []
        true_flag_indexes = []
        player_by_key = {_stable_player_key(player): player for player in players}

        for index, team in enumerate(teams):
            flags = [
                player_by_key[key].get("winner")
                for key in team["player_keys"]
            ]

            if flags and all(flag is True for flag in flags):
                winning_indexes.append(index)

            if flags and all(flag is False for flag in flags):
                losing_indexes.append(index)

            if any(flag is True for flag in flags):
                true_flag_indexes.append(index)

        if (
            len(winning_indexes) == 1
            and len(losing_indexes) == 1
            and winning_indexes[0] != losing_indexes[0]
        ):
            winner_flag_team_id = teams[winning_indexes[0]]["team_id"]

        elif len(true_flag_indexes) == 1:
            candidate_index = true_flag_indexes[0]
            candidate_flags = [
                player_by_key[key].get("winner")
                for key in teams[candidate_index]["player_keys"]
            ]

            # Never override an explicit loser flag on the candidate team.
            # This remains weaker evidence and is trusted only alongside
            # decisive postgame/scoreboard proof.
            if all(flag is not False for flag in candidate_flags):
                single_team_winner_flag_team_id = (
                    teams[candidate_index]["team_id"]
                )

    result = {
        "status": status,
        "format": game_format,
        "confidence": "high" if status == "resolved" else "low",
        "provenance": provenance,
        "reason_codes": sorted(set(reason_codes)),
        "player_count": player_count,
        "team_count": len(teams),
        "teams": teams,
        "winning_team_id": None,
    }
    result.update(
        _result_resolution(
            players,
            teams,
            winner_flag_team_id,
            single_team_winner_flag_team_id,
            key_events,
        )
    )
    return result


def apply_replay_team_contract(stats: Any, *, final: bool | None = None) -> Any:
    if not isinstance(stats, dict):
        return stats
    players = canonicalize_replay_players(stats.get("players"))
    stats["players"] = players
    is_final = bool(stats.get("completed")) if final is None else bool(final)
    key_events = stats.get("key_events") if isinstance(stats.get("key_events"), dict) else {}
    key_events = dict(key_events)
    resolution = resolve_replay_teams(
        players,
        final=is_final,
        key_events=key_events,
    )
    completion_source = str(
        stats.get("completion_source") or key_events.get("completion_source") or ""
    )
    if completion_source == "resignation":
        resignation_proves_team_completion = bool(
            resolution.get("result_evidence", {}).get(
                "team_completion_from_resignations"
            )
        )
        key_events["raw_mgz_completed_signal"] = bool(
            stats.get("completed") or key_events.get("completed")
        )
        key_events["resignation_proves_team_completion"] = (
            resignation_proves_team_completion
        )
        if resignation_proves_team_completion:
            stats["completion_source"] = "complete_team_resignation"
            key_events["completion_source"] = "complete_team_resignation"
        else:
            # HD's completion/winner flags can flip on the first teammate
            # resignation.  Preserve that raw signal above, but do not project it
            # as a completed team game without a single fully resigned team.
            stats["completed"] = False
            key_events["completed"] = False
            stats["completion_source"] = "team_resignation_review_required"
            key_events["completion_source"] = "team_resignation_review_required"
            stats["parse_reason"] = "team_resignation_not_complete"
    key_events["team_resolution"] = resolution
    key_events["result_resolution"] = {
        key: resolution[key]
        for key in (
            "result_status",
            "result_confidence",
            "result_provenance",
            "result_trusted",
            "winning_team_id",
            "winning_player_keys",
            "winning_player_names",
            "result_evidence",
        )
    }
    stats["key_events"] = key_events
    stats["team_resolution"] = resolution
    stats["winning_team_id"] = resolution["winning_team_id"]
    stats["winning_player_keys"] = resolution["winning_player_keys"]
    stats["winning_player_names"] = resolution["winning_player_names"]
    stats["result_resolution"] = key_events["result_resolution"]
    return stats
