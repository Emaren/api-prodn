"""Read-only corpus reconciliation helpers for the AoE2HD Parser Engine Room.

This module deliberately separates storage grains:

* artifact: one immutable SHA-256 replay object;
* submission: one watcher/upload parse-attempt row;
* database replay row: one historical ``game_stats`` projection;
* logical game: one preferred final projection after deterministic de-duplication.

Nothing here promotes parser output or mutates production truth.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence


SHA256_RE = re.compile(r"^(?P<sha>[0-9a-f]{64})(?P<suffix>\.[^.]+)?$")
NON_RESULTS = {
    "",
    "draw",
    "none",
    "null",
    "pending",
    "unknown",
    "unresolved",
}


@dataclass(frozen=True)
class ArchiveObject:
    sha256: str
    suffix: str
    relative_path: str
    byte_size: int
    layout_valid: bool


def normalize_database_url(value: str) -> str:
    """Return a psycopg-compatible URL without exposing or otherwise altering it."""

    return value.replace("postgresql+asyncpg://", "postgresql://", 1)


def normalize_replay_name(value: Any) -> str:
    return str(value or "").strip().casefold()


def logical_replay_key(row: Mapping[str, Any]) -> str:
    """Match the player-profile replay grain used for the historical Jim count."""

    replay_name = normalize_replay_name(row.get("replay_file"))
    if not replay_name:
        replay_name = normalize_replay_name(row.get("original_filename"))
    if replay_name:
        return f"replay:{replay_name}"
    return f"row:{row.get('id')}"


def _date_score(value: Any) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
    return 0.0


def has_reliable_scalar_winner(value: Any) -> bool:
    return normalize_replay_name(value) not in NON_RESULTS


def preferred_game_rank(row: Mapping[str, Any]) -> tuple[int, float, int]:
    """Mirror the existing profile preference policy without changing public truth."""

    parse_source = normalize_replay_name(row.get("parse_source"))
    parse_reason = normalize_replay_name(row.get("parse_reason"))
    score = 0
    if parse_reason == "superseded_by_later_upload":
        score -= 100_000
    if bool(row.get("is_final")):
        score += 10_000
    if parse_source == "watcher_final":
        score += 2_000
    if has_reliable_scalar_winner(row.get("winner")):
        score += 1_000
    if "final" in parse_reason:
        score += 250
    if "resignation" in parse_reason:
        score += 150

    recency = max(
        _date_score(row.get("played_on")),
        _date_score(row.get("timestamp")),
        _date_score(row.get("created_at")),
    )
    return score, recency, int(row.get("id") or 0)


def freeze_logical_cohort(
    rows: Iterable[Mapping[str, Any]],
    *,
    user_uid: str,
    anchor_game_id: int,
) -> tuple[list[Mapping[str, Any]], dict[str, list[Mapping[str, Any]]]]:
    """Freeze a deterministic final-game cohort at an immutable game-id anchor."""

    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("user_uid") != user_uid:
            continue
        if not bool(row.get("is_final")):
            continue
        if int(row.get("id") or 0) > anchor_game_id:
            continue
        grouped[logical_replay_key(row)].append(row)

    preferred: list[Mapping[str, Any]] = []
    duplicates: dict[str, list[Mapping[str, Any]]] = {}
    for key, candidates in grouped.items():
        ordered = sorted(candidates, key=preferred_game_rank, reverse=True)
        preferred.append(ordered[0])
        if len(ordered) > 1:
            duplicates[key] = ordered

    preferred.sort(key=lambda row: int(row.get("id") or 0))
    return preferred, duplicates


def parse_archive_object(root: Path, path: Path) -> ArchiveObject | None:
    match = SHA256_RE.fullmatch(path.name.casefold())
    if not match:
        return None
    sha256 = match.group("sha")
    relative = path.relative_to(root)
    parts = relative.parts
    layout_valid = (
        len(parts) == 3 and parts[0] == sha256[:2] and parts[1] == sha256[2:4]
    )
    return ArchiveObject(
        sha256=sha256,
        suffix=(match.group("suffix") or "").casefold(),
        relative_path=str(relative),
        byte_size=path.stat().st_size,
        layout_valid=layout_valid,
    )


def scan_archive(root: Path) -> tuple[list[ArchiveObject], list[str]]:
    objects: list[ArchiveObject] = []
    invalid: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        parsed = parse_archive_object(root, path)
        if parsed is None:
            invalid.append(str(path.relative_to(root)))
        else:
            objects.append(parsed)
    return objects, invalid


def verify_archive_content_hashes(
    root: Path, objects: Sequence[ArchiveObject]
) -> list[dict[str, str]]:
    """Read every archived byte and report objects whose filename hash is false."""

    mismatches: list[dict[str, str]] = []
    for obj in objects:
        digest = hashlib.sha256()
        with (root / obj.relative_path).open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        actual = digest.hexdigest()
        if actual != obj.sha256:
            mismatches.append(
                {
                    "relative_path": obj.relative_path,
                    "expected_sha256": obj.sha256,
                    "actual_sha256": actual,
                }
            )
    return mismatches


def _player_rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _key_events(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def classify_current_result(row: Mapping[str, Any], *, manually_verified: bool) -> str:
    """Classify existing evidence for audit only; never promote from this label."""

    if manually_verified:
        return "manually_verified"

    parse_reason = normalize_replay_name(row.get("parse_reason"))
    players = _player_rows(row.get("players"))
    if "unparsed" in parse_reason or len(players) < 2:
        return "private_review_candidate"

    events = _key_events(row.get("key_events"))
    team_resolution = events.get("team_resolution")
    if isinstance(team_resolution, Mapping):
        teams = team_resolution.get("teams")
        winning_team_id = team_resolution.get("winning_team_id")
        if (
            team_resolution.get("status") == "resolved"
            and isinstance(teams, list)
            and len(teams) == 2
            and winning_team_id is not None
        ):
            return "resolved_coherent_team"

    if len(players) == 2 and has_reliable_scalar_winner(row.get("winner")):
        player_names = {normalize_replay_name(player.get("name")) for player in players}
        if normalize_replay_name(row.get("winner")) in player_names:
            return "resolved_coherent_1v1"

    if len(players) > 2:
        return "team_result_revalidation_required"
    return "private_review_candidate"


def archive_profile(objects: Sequence[ArchiveObject], invalid: Sequence[str]) -> dict[str, Any]:
    hash_counts = Counter(obj.sha256 for obj in objects)
    return {
        "files": len(objects) + len(invalid),
        "valid_objects": len(objects),
        "unique_hashes": len(hash_counts),
        "bytes": sum(obj.byte_size for obj in objects),
        "extensions": dict(sorted(Counter(obj.suffix or "<none>" for obj in objects).items())),
        "duplicate_hashes": sum(1 for count in hash_counts.values() if count > 1),
        "duplicate_objects": sum(max(0, count - 1) for count in hash_counts.values()),
        "invalid_filenames": len(invalid),
        "layout_mismatches": sum(1 for obj in objects if not obj.layout_valid),
    }


def safe_hashes(rows: Iterable[Mapping[str, Any]]) -> set[str]:
    hashes = set()
    for row in rows:
        value = normalize_replay_name(row.get("replay_hash"))
        if re.fullmatch(r"[0-9a-f]{64}", value):
            hashes.add(value)
    return hashes


def build_full_artifact_manifest_rows(
    objects: Sequence[ArchiveObject],
    games: Iterable[Mapping[str, Any]],
    attempts: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build one deterministic worker row per immutable archive artifact.

    Historical game and submission rows are provenance links. They never create
    duplicate parser work for identical bytes.
    """

    games_by_hash: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    attempts_by_hash: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for game in games:
        replay_hash = normalize_replay_name(game.get("replay_hash"))
        if re.fullmatch(r"[0-9a-f]{64}", replay_hash):
            games_by_hash[replay_hash].append(game)
    for attempt in attempts:
        replay_hash = normalize_replay_name(attempt.get("replay_hash"))
        if re.fullmatch(r"[0-9a-f]{64}", replay_hash):
            attempts_by_hash[replay_hash].append(attempt)

    rows: list[dict[str, Any]] = []
    for artifact in sorted(objects, key=lambda item: (item.sha256, item.relative_path)):
        matching_games = games_by_hash.get(artifact.sha256, [])
        game = max(
            matching_games,
            key=lambda row: (bool(row.get("is_final")), preferred_game_rank(row)),
            default=None,
        )
        matching_attempts = attempts_by_hash.get(artifact.sha256, [])
        attempt = max(
            matching_attempts,
            key=lambda row: (
                game is not None and row.get("game_stats_id") == game.get("id"),
                game is not None and row.get("user_uid") == game.get("user_uid"),
                _date_score(row.get("created_at")),
                int(row.get("id") or 0),
            ),
            default=None,
        )
        original_filename = (
            (game or {}).get("original_filename")
            or (game or {}).get("replay_file")
            or (attempt or {}).get("original_filename")
            or Path(artifact.relative_path).name
        )
        submitter_uid = (game or {}).get("user_uid") or (attempt or {}).get("user_uid")
        rows.append(
            {
                "logical_replay_key": f"artifact:{artifact.sha256}",
                "game_stats_id": int((game or {}).get("id") or 0) or "",
                "legacy_parse_attempt_id": int((attempt or {}).get("id") or 0) or "",
                "submitter_uid": submitter_uid or "",
                "replay_hash": artifact.sha256,
                "original_filename": str(original_filename),
                "extension": artifact.suffix,
                "archive_present": True,
                "archive_relative_path": artifact.relative_path,
                "archive_bytes": artifact.byte_size,
            }
        )
    return rows
