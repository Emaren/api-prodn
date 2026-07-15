"""Deterministic, candidate-only evidence contract for replay parsing.

This module deliberately does not promote a parse or mutate public truth.  It
turns one immutable replay artifact into a reproducible candidate envelope that
the Parser Engine Room can persist as a parse run plus normalized observations.
The legacy ``GameStats`` projection remains separate and backward compatible.

Raw action and initial-map evidence is returned in the candidate envelope so a
worker can store it on the replay volume.  Only a compact run receipt should be
embedded in the current hot ``game_stats.key_events`` JSONB column.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from enum import Enum
import hashlib
from importlib.metadata import PackageNotFoundError, version as package_version
import json
import math
from pathlib import Path
import re
from typing import Any

from mgz.reference import get_consts


PARSER_CONTRACT_VERSION = "1.1"
PARSER_SCHEMA_VERSION = "2026-07-15.2"
PARSER_IMPLEMENTATION = "aoe2war.mgz_hd"
PARSER_PASS_NAME = "hd_deterministic_evidence"
PARSER_PASS_VERSION = "2"
MAX_COMPACT_RECEIPT_JSON_BYTES = 32 * 1024

PROVENANCE_DIRECT_HEADER = "direct_header"
PROVENANCE_DIRECT_ACTION = "direct_action"
PROVENANCE_DIRECT_POSTGAME = "direct_postgame"
PROVENANCE_DERIVED = "derived_coherent"
PROVENANCE_INFERRED_REVIEW_ONLY = "inferred_review_only"
PROVENANCE_ABSENT = "absent"

_AGE_TECHNOLOGIES = {
    101: "Feudal Age",
    102: "Castle Age",
    103: "Imperial Age",
}
_KNOWN_SETTINGS = {
    "all_technologies",
    "cheats",
    "difficulty",
    "ending_age",
    "hidden_civs",
    "lock_speed",
    "lock_teams",
    "map_reveal_choice",
    "multiqueue",
    "population_limit",
    "speed",
    "starting_age",
    "starting_resources",
    "team_together",
    "treaty_length",
    "type",
    "victory_condition",
}
_KNOWN_POSTGAME_FIELDS = (
    "military.score",
    "military.units_killed",
    "military.hit_points_killed",
    "military.units_lost",
    "military.buildings_razed",
    "military.hit_points_razed",
    "military.buildings_lost",
    "military.units_converted",
    "economy.score",
    "economy.food_collected",
    "economy.wood_collected",
    "economy.stone_collected",
    "economy.gold_collected",
    "economy.tribute_sent",
    "economy.tribute_received",
    "economy.trade_gold",
    "economy.relic_gold",
    "technology.score",
    "technology.feudal_time",
    "technology.castle_time",
    "technology.imperial_time",
    "technology.explored_percent",
    "technology.research_count",
    "technology.research_percent",
    "society.score",
    "society.total_wonders",
    "society.total_castles",
    "society.total_relics",
    "society.villager_high",
)
_ACTION_FAMILIES = {
    "build": "construction_command",
    "wall": "construction_command",
    "queue": "production_queue_command",
    "de_queue": "production_queue_command",
    "research": "research_command",
    "buy": "market_command",
    "sell": "market_command",
    "tribute": "tribute_command",
    "resign": "resignation",
    "flare": "team_signal",
}
_ACTION_PACKET_IDENTITY_FIELDS = (
    "timestamp_ms",
    "type",
    "player_number",
    "payload",
)
_URL_RE = re.compile(r"\b[a-z][a-z0-9+.-]{2,}://[^\s]+", re.IGNORECASE)
_QUOTED_PATH_RE = re.compile(
    r'''(?P<quote>["'])(?:[A-Za-z]:[\\/]|/)[^"'\r\n]+(?P=quote)''',
    re.IGNORECASE,
)
_PATH_WITH_SPACES_RE = re.compile(
    r"""
    (?:[A-Za-z]:[\\/]|/(?=[^\s]))
    (?:(?!\s+(?:at|near|offset|line|column|while|because|after|before|with|token|signature)\b)[^\r\n])+?
    (?=
        \s+(?:at|near|offset|line|column|while|because|after|before|with|token|signature)\b
        |:\s+(?:permission|denied|not|invalid|failed|error)\b
        |$
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)
_PATH_RE = re.compile(
    r"(?:[A-Za-z]:[\\/][^\s]+|/(?:[^\s/:]+/)+[^\s:]+)",
    re.IGNORECASE,
)
_BEARER_RE = re.compile(r"\bbearer\s+[a-z0-9._~+/=-]+", re.IGNORECASE)
_JWT_RE = re.compile(
    r"\b[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}\b",
    re.IGNORECASE,
)
_KNOWN_TOKEN_RE = re.compile(
    r"\b(?:sk-[a-z0-9_-]{12,}|gh[pousr]_[a-z0-9]{20,}|akia[a-z0-9]{16})\b",
    re.IGNORECASE,
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"""
    \b(?P<key>
        x[_-]?api[_-]?key|api[_-]?key|access[_-]?key|auth(?:orization)?|bearer|cookie|
        credential|password|passwd|secret|session(?:[_-]?id)?|(?:access|refresh|id)[_-]?token|
        token|private[_-]?key|client[_-]?secret
    )\b
    \s*(?:=|:)\s*
    (?:"[^"]*"|'[^']*'|[^\s,;]+)
    """,
    re.IGNORECASE | re.VERBOSE,
)
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_HEX_RE = re.compile(r"\b0x[0-9a-f]+\b", re.IGNORECASE)
_NUMBER_RE = re.compile(r"\b\d+\b")


def _mgz_version() -> str:
    try:
        return package_version("mgz")
    except PackageNotFoundError:
        return "unavailable"


def parser_identity(*, apply_hd_early_exit_rules: bool) -> dict[str, Any]:
    """Return the complete identity of a deterministic parser pass."""
    return {
        "implementation": PARSER_IMPLEMENTATION,
        "implementation_version": _mgz_version(),
        "schema_version": PARSER_SCHEMA_VERSION,
        "pass_name": PARSER_PASS_NAME,
        "pass_version": PARSER_PASS_VERSION,
        "options": {
            "apply_hd_early_exit_rules": bool(apply_hd_early_exit_rules),
        },
    }


def _json_primitive(value: Any) -> Any:
    """Convert parser values into stable JSON without lossy string reprs."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return int(value) if value.is_integer() else round(value, 6)
    if isinstance(value, Enum):
        return str(value.name).lower()
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return {
            "byte_size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    if isinstance(value, dict):
        return {
            str(key): _json_primitive(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_primitive(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_json_primitive(item) for item in value]
        return sorted(normalized, key=_canonical_json)
    return str(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_primitive(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_candidate_json(candidate: dict[str, Any]) -> str:
    """Serialize one candidate envelope for durable, byte-stable worker output."""
    return _canonical_json(candidate)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def _integer(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or not parsed.is_integer():
        return None
    return int(parsed)


def _format_from_projection(projection: dict[str, Any] | None) -> str | None:
    if not isinstance(projection, dict):
        return None
    game_version = str(projection.get("game_version") or "").casefold()
    if "version.hd" in game_version or game_version == "hd":
        return "aoe2_hd"
    if "version.de" in game_version or game_version == "de":
        return "aoe2_de"
    if game_version:
        return "legacy_aoc"
    return None


def artifact_descriptor(
    replay_path: str,
    file_bytes: bytes | None,
    projection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suffix = Path(replay_path).suffix.casefold()
    return {
        "sha256": hashlib.sha256(file_bytes).hexdigest() if file_bytes is not None else None,
        "byte_size": len(file_bytes) if file_bytes is not None else None,
        "original_extension": suffix or None,
        "container_hint": suffix.removeprefix(".") or None,
        "format": _format_from_projection(projection),
    }


def pass_idempotency_key(
    artifact_sha256: str,
    identity: dict[str, Any],
) -> str:
    """Key one immutable artifact/parser/schema/pass/options combination."""
    return _stable_hash(
        {
            "artifact_sha256": artifact_sha256,
            "parser": identity,
        }
    )


def _snake_case(value: str) -> str:
    normalized = re.sub(r"(?<!^)(?=[A-Z])", "_", value)
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", normalized)
    return normalized.strip("_").casefold() or "exception"


def normalize_failure_signature(error: BaseException, *, stage: str) -> dict[str, Any]:
    """Group parser errors without leaking paths or unstable byte offsets."""
    raw_message = _clean_text(str(error)) or error.__class__.__name__
    message = _URL_RE.sub("<url>", raw_message)
    message = _QUOTED_PATH_RE.sub("<path>", message)
    message = _PATH_WITH_SPACES_RE.sub("<path>", message)
    message = _PATH_RE.sub("<path>", message)
    message = _BEARER_RE.sub("<redacted>", message)
    message = _JWT_RE.sub("<redacted>", message)
    message = _KNOWN_TOKEN_RE.sub("<redacted>", message)
    message = _SECRET_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group('key')}=<redacted>",
        message,
    )
    message = _UUID_RE.sub("<uuid>", message)
    message = _HEX_RE.sub("<hex>", message)
    message = _NUMBER_RE.sub("<n>", message).casefold()
    message = message[:240]
    exception_class = _snake_case(error.__class__.__name__)
    lowered = raw_message.casefold()

    if any(
        marker in lowered
        for marker in (
            "unpack requires a buffer",
            "unexpected end",
            "end of stream",
            "stream read less",
            "truncated",
            "eof",
            "not enough data",
            "could not read enough bytes",
            "packer '<l' error",
        )
    ):
        category = "truncated_or_incomplete"
        retryable = True
    elif "sha256" in lowered and (
        "mismatch" in lowered or "does not match" in lowered
    ):
        category = "artifact_integrity"
        retryable = False
    elif "no decoding mapping" in lowered or "unsupported" in lowered:
        category = "unsupported_encoding_or_enum"
        retryable = False
    elif "invalid mgz" in lowered or "could not parse" in lowered:
        category = "invalid_container"
        retryable = False
    elif isinstance(error, (OSError, IOError)):
        category = "artifact_io"
        retryable = True
    else:
        category = "parser_exception"
        retryable = False

    fingerprint = _stable_hash(
        {
            "stage": stage,
            "category": category,
            "exception_class": exception_class,
            "normalized_message": message,
        }
    )
    return {
        "signature": f"{stage}:{category}:{exception_class}:{fingerprint[:16]}",
        "fingerprint": fingerprint,
        "stage": stage,
        "category": category,
        "exception_class": exception_class,
        "normalized_message": message,
        "retryable": retryable,
    }


def _dataset_parts(summary_obj: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        dataset = summary_obj.get_dataset()
    except Exception:
        dataset = {}
    cache = getattr(summary_obj, "_cache", {})
    cached = cache.get("dataset") if isinstance(cache, dict) else None
    reference = cached[1] if isinstance(cached, tuple) and len(cached) > 1 else {}
    return (
        _json_primitive(dataset) if isinstance(dataset, dict) else {},
        reference if isinstance(reference, dict) else {},
    )


def _reference_name(reference: dict[str, Any], lane: str, object_id: Any) -> str | None:
    normalized_id = _integer(object_id)
    if normalized_id is None:
        return None
    values = reference.get(lane)
    if not isinstance(values, dict):
        return None
    raw = values.get(str(normalized_id))
    if isinstance(raw, dict):
        return _clean_text(raw.get("name"))
    return _clean_text(raw)


def _normalize_action_payload(
    payload: Any,
    reference: dict[str, Any],
    constants: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    normalized = {
        str(key): _json_primitive(value)
        for key, value in sorted(payload.items(), key=lambda pair: str(pair[0]))
        if value is not None
    }
    labels: dict[str, str] = {}
    lookup_lanes = {
        "technology_id": ("technologies", "technology"),
        "building_id": ("objects", "building"),
        "unit_id": ("objects", "unit"),
    }
    for field, (lane, label_field) in lookup_lanes.items():
        name = _reference_name(reference, lane, payload.get(field))
        if name:
            labels[label_field] = name

    constant_lanes = {
        "formation_id": ("formations", "formation"),
        "stance_id": ("stances", "stance"),
        "command_id": ("commands", "command"),
        "order_id": ("orders", "order"),
        "resource_id": ("resources", "resource"),
    }
    for field, (lane, label_field) in constant_lanes.items():
        field_id = _integer(payload.get(field))
        values = constants.get(lane)
        name = values.get(str(field_id)) if isinstance(values, dict) and field_id is not None else None
        cleaned = _clean_text(name)
        if cleaned:
            labels[label_field] = cleaned

    if labels:
        normalized["labels"] = labels
    return normalized


def _player_number_from_model(player: Any) -> int | None:
    if player is None:
        return None
    return _integer(getattr(player, "number", None))


def _action_packet_identity_material(action: dict[str, Any]) -> dict[str, Any]:
    """Return the normalized, ordinal-free identity of one parsed action packet."""
    return {
        field: action.get(field)
        for field in _ACTION_PACKET_IDENTITY_FIELDS
    }


def _annotate_action_packet_identities(
    actions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Attach stable packet hashes and summarize exact parsed-packet duplicates.

    This is deliberately an identity-normalization lane, not an assertion that
    repeated packets are invalid gameplay actions.  The raw stream remains in
    original order and every packet remains present.
    """
    identity_ordinals: dict[str, list[int]] = defaultdict(list)
    first_by_identity: dict[str, dict[str, Any]] = {}
    normalized_actions: list[dict[str, Any]] = []

    for packet_index, action in enumerate(actions, start=1):
        identity = _stable_hash(_action_packet_identity_material(action))
        action["packet_identity_sha256"] = identity
        ordinal = _integer(action.get("ordinal")) or packet_index
        identity_ordinals[identity].append(ordinal)
        if identity not in first_by_identity:
            first_by_identity[identity] = action
            normalized_actions.append(action)

    multiplicities = Counter(
        len(identity_ordinals[identity])
        for identity in first_by_identity
    )
    duplicate_identities = [
        {
            "packet_identity_sha256": identity,
            "multiplicity": len(identity_ordinals[identity]),
            "first_ordinal": min(identity_ordinals[identity]),
            "last_ordinal": max(identity_ordinals[identity]),
        }
        for identity in sorted(first_by_identity)
        if len(identity_ordinals[identity]) > 1
    ]
    unique_count = len(first_by_identity)
    summary = {
        "identity_algorithm": "sha256_canonical_json_v1",
        "identity_fields": list(_ACTION_PACKET_IDENTITY_FIELDS),
        "normalization_scope": "exact_parsed_packet_identity_only",
        "validated_gameplay_truth": False,
        "unique_action_identity_count": unique_count,
        "exact_duplicate_packet_excess": max(0, len(actions) - unique_count),
        "identity_multiplicity_summary": [
            {
                "multiplicity": multiplicity,
                "unique_identity_count": multiplicities[multiplicity],
                "raw_packet_count": multiplicity * multiplicities[multiplicity],
            }
            for multiplicity in sorted(multiplicities)
        ],
        "duplicate_packet_identities": duplicate_identities,
    }
    return normalized_actions, summary


def _action_activity_summary(
    actions: list[dict[str, Any]],
    duration_ms: int | None,
    players_by_number: dict[int, str],
    *,
    metric_lane: str,
) -> list[dict[str, Any]]:
    timestamps_by_player: dict[int, list[int]] = defaultdict(list)
    types_by_player: dict[int, Counter[str]] = defaultdict(Counter)
    families_by_player: dict[int, Counter[str]] = defaultdict(Counter)
    for action in actions:
        number = _integer(action.get("player_number"))
        timestamp_ms = _integer(action.get("timestamp_ms"))
        if number is None or timestamp_ms is None or action.get("type") == "ai_order":
            continue
        timestamps_by_player[number].append(timestamp_ms)
        types_by_player[number][str(action.get("type") or "unclassified")] += 1
        families_by_player[number][str(action.get("command_family") or "other_command")] += 1

    duration_minutes = (duration_ms / 60_000) if duration_ms and duration_ms > 0 else None
    result = []
    for number in sorted(timestamps_by_player):
        timestamps = sorted(timestamps_by_player[number])
        minute_buckets = Counter(timestamp // 60_000 for timestamp in timestamps)
        gaps = [
            later - earlier
            for earlier, later in zip(timestamps, timestamps[1:])
        ]
        action_count = len(timestamps)
        recorded_rate = (
            int(round(action_count / duration_minutes))
            if duration_minutes
            else None
        )
        result.append(
            {
                "player_number": number,
                "player_name": players_by_number.get(number),
                "action_packet_count": action_count,
                "action_type_counts": {
                    action_type: types_by_player[number][action_type]
                    for action_type in sorted(types_by_player[number])
                },
                "command_family_counts": {
                    family: families_by_player[number][family]
                    for family in sorted(families_by_player[number])
                },
                "first_action_ms": timestamps[0],
                "last_action_ms": timestamps[-1],
                "active_minute_count": len(minute_buckets),
                "peak_actions_in_one_minute": max(minute_buckets.values()),
                "largest_recorded_action_gap_ms": max(gaps) if gaps else None,
                "eapm": recorded_rate,
                "eapm_formula": (
                    "round(non-AI actions carrying player_id / replay duration minutes)"
                ),
                "metric_lane": metric_lane,
                "normalization": (
                    "exact_packet_identity_first_occurrence"
                    if metric_lane == "experimental_exact_packet_identity_normalized"
                    else "none_raw_packet_lane"
                ),
                "metric_interpretation": (
                    "parsed packet-rate diagnostic; not validated effective gameplay APM"
                ),
                "validated_gameplay_truth": False,
                "provenance_class": PROVENANCE_DERIVED,
            }
        )
    return result


def _resignation_lanes(
    actions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return raw resignation packets and one earliest semantic event per player."""
    raw_timeline = [
        {
            "ordinal": action.get("ordinal"),
            "timestamp_ms": action.get("timestamp_ms"),
            "player_number": action.get("player_number"),
            "player_name": action.get("player_name"),
            "packet_identity_sha256": action.get("packet_identity_sha256"),
        }
        for action in actions
        if action.get("type") == "resign"
    ]
    by_player: dict[tuple[str, Any], list[dict[str, Any]]] = defaultdict(list)
    for event in raw_timeline:
        player_number = _integer(event.get("player_number"))
        player_name = _clean_text(event.get("player_name"))
        if player_number is not None:
            key = ("number", player_number)
        elif player_name:
            key = ("name", player_name.casefold())
        else:
            # A packet with no player identity remains in the raw evidence lane;
            # it cannot safely become a player-level semantic resignation.
            continue
        by_player[key].append(event)

    semantic_timeline = []
    for events in by_player.values():
        earliest = min(
            events,
            key=lambda event: (
                event.get("timestamp_ms") is None,
                event.get("timestamp_ms") or 0,
                event.get("ordinal") or 0,
            ),
        )
        semantic_timeline.append(
            {
                "timestamp_ms": earliest.get("timestamp_ms"),
                "player_number": earliest.get("player_number"),
                "player_name": earliest.get("player_name"),
                "earliest_raw_ordinal": earliest.get("ordinal"),
                "raw_packet_count_for_player": len(events),
                "provenance_class": PROVENANCE_DERIVED,
            }
        )
    semantic_timeline.sort(
        key=lambda event: (
            event.get("timestamp_ms") is None,
            event.get("timestamp_ms") or 0,
            event.get("player_number") is None,
            event.get("player_number") or 0,
            str(event.get("player_name") or "").casefold(),
        )
    )
    return raw_timeline, semantic_timeline


def capture_summary_evidence(summary_obj: Any) -> dict[str, Any]:
    """Capture byte-backed evidence while the already-parsed Summary is alive."""
    dataset, reference = _dataset_parts(summary_obj)
    try:
        constants = get_consts()
    except Exception:
        constants = {}

    try:
        players = list(summary_obj.get_players() or [])
    except Exception:
        players = []
    players_by_number = {
        number: str(player.get("name") or "")
        for player in players
        if isinstance(player, dict)
        and (number := _integer(player.get("number"))) is not None
    }

    action_stream: list[dict[str, Any]] = []
    for ordinal, raw_action in enumerate(getattr(summary_obj, "_actions", []) or [], start=1):
        if not isinstance(raw_action, tuple) or len(raw_action) < 3:
            continue
        timestamp, action_type, payload = raw_action[:3]
        label = _clean_text(getattr(action_type, "name", None)) or _clean_text(str(action_type))
        action_name = (label or "unclassified").replace("Action.", "").casefold()
        normalized_payload = _normalize_action_payload(payload, reference, constants)
        player_number = _integer(normalized_payload.get("player_id"))
        action_stream.append(
            {
                "ordinal": ordinal,
                "timestamp_ms": _integer(timestamp),
                "type": action_name,
                "command_family": _ACTION_FAMILIES.get(action_name, "other_command"),
                "player_number": player_number,
                "player_name": players_by_number.get(player_number),
                "payload": normalized_payload,
                "provenance_class": PROVENANCE_DIRECT_ACTION,
            }
        )

    try:
        raw_map = summary_obj.get_map()
    except Exception:
        raw_map = {}
    tiles = raw_map.get("tiles") if isinstance(raw_map, dict) else []
    tiles = tiles if isinstance(tiles, list) else []
    terrain_counts: Counter[int] = Counter()
    elevation_counts: Counter[int] = Counter()
    tile_fingerprint_rows = []
    for tile in tiles:
        if not isinstance(tile, dict):
            continue
        x = _integer(tile.get("x"))
        y = _integer(tile.get("y"))
        terrain_id = _integer(tile.get("terrain_id"))
        elevation = _integer(tile.get("elevation"))
        if terrain_id is not None:
            terrain_counts[terrain_id] += 1
        if elevation is not None:
            elevation_counts[elevation] += 1
        tile_fingerprint_rows.append([x, y, terrain_id, elevation])

    terrain_histogram = []
    for terrain_id in sorted(terrain_counts):
        terrain_histogram.append(
            {
                "terrain_id": terrain_id,
                "terrain_name": _reference_name(reference, "terrain", terrain_id),
                "tile_count": terrain_counts[terrain_id],
            }
        )

    try:
        raw_objects = summary_obj.get_objects()
    except Exception:
        raw_objects = {}
    objects = raw_objects.get("objects") if isinstance(raw_objects, dict) else []
    initial_objects = []
    for raw_object in objects if isinstance(objects, list) else []:
        if not isinstance(raw_object, dict):
            continue
        object_id = _integer(raw_object.get("object_id"))
        initial_objects.append(
            {
                "instance_id": _integer(raw_object.get("instance_id")),
                "object_id": object_id,
                "object_name": _reference_name(reference, "objects", object_id),
                "class_id": _integer(raw_object.get("class_id")),
                "player_number": _integer(raw_object.get("player_number")),
                "x": _json_primitive(raw_object.get("x")),
                "y": _json_primitive(raw_object.get("y")),
            }
        )
    initial_objects.sort(
        key=lambda item: (
            item.get("instance_id") is None,
            item.get("instance_id") or 0,
            item.get("object_id") or 0,
        )
    )
    initial_object_counts: Counter[tuple[int | None, str | None]] = Counter(
        (item.get("object_id"), item.get("object_name"))
        for item in initial_objects
    )

    identity_normalized_actions, packet_identity = _annotate_action_packet_identities(
        action_stream
    )
    duration_ms = _integer(getattr(summary_obj, "get_duration")())
    type_counts = Counter(action.get("type") for action in action_stream)
    raw_resignations, semantic_resignations = _resignation_lanes(action_stream)
    age_up_commands = []
    market_commands = []
    tribute_commands = []
    for action in action_stream:
        payload = action.get("payload") if isinstance(action.get("payload"), dict) else {}
        if action.get("type") == "research":
            technology_id = _integer(payload.get("technology_id"))
            if technology_id in _AGE_TECHNOLOGIES:
                age_up_commands.append(
                    {
                        "timestamp_ms": action.get("timestamp_ms"),
                        "player_number": action.get("player_number"),
                        "player_name": action.get("player_name"),
                        "technology_id": technology_id,
                        "age": _AGE_TECHNOLOGIES[technology_id],
                        "meaning": "research_command_recorded_not_completion_proof",
                    }
                )
        if action.get("type") in {"buy", "sell"}:
            market_commands.append(
                {
                    "timestamp_ms": action.get("timestamp_ms"),
                    "type": action.get("type"),
                    "player_number": action.get("player_number"),
                    "player_name": action.get("player_name"),
                    "resource_id": payload.get("resource_id"),
                    "resource": (payload.get("labels") or {}).get("resource"),
                    "amount": payload.get("amount"),
                }
            )
        if action.get("type") == "tribute":
            tribute_commands.append(
                {
                    "timestamp_ms": action.get("timestamp_ms"),
                    "player_number": action.get("player_number"),
                    "player_name": action.get("player_name"),
                    "recipient_player_number": _integer(payload.get("player_id_to")),
                    "resource_id": payload.get("resource_id"),
                    "resource": (payload.get("labels") or {}).get("resource"),
                    "amount": payload.get("amount"),
                }
            )

    try:
        diplomacy = summary_obj.get_diplomacy()
    except Exception:
        diplomacy = {}

    try:
        raw_chat = summary_obj.get_chat()
    except Exception:
        raw_chat = []
    chat_stream = []
    for ordinal, raw_entry in enumerate(
        raw_chat if isinstance(raw_chat, list) else [],
        start=1,
    ):
        if not isinstance(raw_entry, dict):
            continue
        entry_type = raw_entry.get("type")
        type_name = _clean_text(getattr(entry_type, "name", None)) or _clean_text(
            str(entry_type or "")
        )
        chat_stream.append(
            {
                "ordinal": ordinal,
                "timestamp_ms": _integer(raw_entry.get("timestamp")),
                "origination": _clean_text(raw_entry.get("origination")),
                "type": type_name.casefold() if type_name else None,
                "player_number": _integer(raw_entry.get("player_number")),
                "message": _clean_text(raw_entry.get("message")),
                "audience": _clean_text(raw_entry.get("audience")),
                "provenance_class": PROVENANCE_DIRECT_ACTION,
            }
        )

    return {
        "dataset": dataset,
        "diplomacy": _json_primitive(diplomacy),
        "map_snapshot": {
            "id": _integer(raw_map.get("id")) if isinstance(raw_map, dict) else None,
            "name": raw_map.get("name") if isinstance(raw_map, dict) else None,
            "size": raw_map.get("size") if isinstance(raw_map, dict) else None,
            "dimension": _integer(raw_map.get("dimension")) if isinstance(raw_map, dict) else None,
            "seed": _integer(raw_map.get("seed")) if isinstance(raw_map, dict) else None,
            "custom": raw_map.get("custom") if isinstance(raw_map, dict) else None,
            "zr": raw_map.get("zr") if isinstance(raw_map, dict) else None,
            "tile_count": len(tile_fingerprint_rows),
            "tile_sha256": _stable_hash(tile_fingerprint_rows),
            "terrain_histogram": terrain_histogram,
            "elevation_histogram": [
                {"elevation": elevation, "tile_count": elevation_counts[elevation]}
                for elevation in sorted(elevation_counts)
            ],
        },
        "initial_objects": {
            "snapshot_scope": "mgz_initial_header_objects_only",
            "object_count": len(initial_objects),
            "object_count_semantics": (
                "mgz-filtered initial header object records after annex de-duplication; "
                "not units or buildings created during gameplay"
            ),
            "object_type_counts": [
                {
                    "object_id": object_id,
                    "object_name": object_name,
                    "count": initial_object_counts[(object_id, object_name)],
                }
                for object_id, object_name in sorted(
                    initial_object_counts,
                    key=lambda item: (
                        item[0] is None,
                        item[0] or 0,
                        item[1] or "",
                    ),
                )
            ],
            "objects": initial_objects,
            "max_starting_town_centers_per_player": (
                _integer(raw_objects.get("tcs"))
                if isinstance(raw_objects, dict)
                else None
            ),
            "town_center_metric_semantics": (
                "maximum initial Town Center count assigned to any one player; "
                "not a total Town Center count"
            ),
            "starting_stone_wall_presence": (
                raw_objects.get("stone_walls")
                if isinstance(raw_objects, dict)
                else None
            ),
            "starting_palisade_wall_presence": (
                raw_objects.get("palisade_walls")
                if isinstance(raw_objects, dict)
                else None
            ),
            "wall_metric_semantics": (
                "mgz initial-header wall presence hints; not completed map classification"
            ),
        },
        "actions": {
            "available": True,
            "count": len(action_stream),
            "stream_semantics": (
                "immutable parsed packet order including repeated exact packets"
            ),
            "type_counts": {
                str(action_type): type_counts[action_type]
                for action_type in sorted(type_counts)
            },
            **packet_identity,
            "raw_activity_by_player": _action_activity_summary(
                action_stream,
                duration_ms,
                players_by_number,
                metric_lane="raw_parsed_action_packets",
            ),
            "identity_normalized_activity_by_player": _action_activity_summary(
                identity_normalized_actions,
                duration_ms,
                players_by_number,
                metric_lane="experimental_exact_packet_identity_normalized",
            ),
            "raw_resignation_timeline": raw_resignations,
            "resignation_timeline": semantic_resignations,
            "age_up_research_commands": age_up_commands,
            "market_commands": market_commands,
            "tribute_commands": tribute_commands,
            "stream": action_stream,
        },
        "chat": {
            "available": True,
            "count": len(chat_stream),
            "stream": chat_stream,
        },
    }


def capture_model_evidence(match: Any) -> dict[str, Any]:
    """Best-effort direct action lane for the existing parse_match fallback."""
    actions = []
    players_by_number = {
        number: str(getattr(player, "name", "") or "")
        for player in list(getattr(match, "players", []) or [])
        if (number := _player_number_from_model(player)) is not None
    }
    for ordinal, action in enumerate(list(getattr(match, "actions", []) or []), start=1):
        timestamp = getattr(action, "timestamp", None)
        try:
            timestamp_ms = int(round(timestamp.total_seconds() * 1000))
        except Exception:
            timestamp_ms = None
        action_type = getattr(action, "type", None)
        label = _clean_text(getattr(action_type, "name", None)) or _clean_text(str(action_type))
        player_number = _player_number_from_model(getattr(action, "player", None))
        payload = _json_primitive(getattr(action, "payload", {}) or {})
        position = getattr(action, "position", None)
        if position is not None and isinstance(payload, dict):
            payload = dict(payload)
            payload["x"] = _json_primitive(getattr(position, "x", None))
            payload["y"] = _json_primitive(getattr(position, "y", None))
        actions.append(
            {
                "ordinal": ordinal,
                "timestamp_ms": timestamp_ms,
                "type": (label or "unclassified").replace("Action.", "").casefold(),
                "command_family": _ACTION_FAMILIES.get(
                    (label or "unclassified").replace("Action.", "").casefold(),
                    "other_command",
                ),
                "player_number": player_number,
                "player_name": players_by_number.get(player_number),
                "payload": payload,
                "provenance_class": PROVENANCE_DIRECT_ACTION,
            }
        )
    duration = getattr(match, "duration", None)
    try:
        duration_ms = int(round(duration.total_seconds() * 1000))
    except Exception:
        duration_ms = None
    identity_normalized_actions, packet_identity = _annotate_action_packet_identities(
        actions
    )
    raw_resignations, semantic_resignations = _resignation_lanes(actions)
    counts = Counter(action["type"] for action in actions)
    return {
        "dataset": {},
        "diplomacy": {},
        "map_snapshot": {},
        "initial_objects": {
            "snapshot_scope": "unavailable_in_model_fallback",
            "object_count": None,
            "objects": [],
        },
        "actions": {
            "available": True,
            "count": len(actions),
            "stream_semantics": (
                "immutable parsed packet order including repeated exact packets"
            ),
            "type_counts": {key: counts[key] for key in sorted(counts)},
            **packet_identity,
            "raw_activity_by_player": _action_activity_summary(
                actions,
                duration_ms,
                players_by_number,
                metric_lane="raw_parsed_action_packets",
            ),
            "identity_normalized_activity_by_player": _action_activity_summary(
                identity_normalized_actions,
                duration_ms,
                players_by_number,
                metric_lane="experimental_exact_packet_identity_normalized",
            ),
            "raw_resignation_timeline": raw_resignations,
            "resignation_timeline": semantic_resignations,
            "age_up_research_commands": [],
            "market_commands": [],
            "tribute_commands": [],
            "stream": actions,
        },
        "chat": {"available": False, "count": None, "stream": []},
    }


def _player_subject(player: dict[str, Any]) -> dict[str, Any]:
    steam_id = _clean_text(str(player.get("steam_id"))) if player.get("steam_id") else None
    name = _clean_text(player.get("name"))
    player_key = f"steam:{steam_id}" if steam_id else f"name:{(name or '').casefold()}"
    return {
        "type": "player",
        "player_key": player_key,
        "player_number": _integer(player.get("number")),
        "player_name": name,
    }


def _observation(
    field: str,
    value: Any,
    provenance_class: str,
    evidence_source: str,
    *,
    subject: dict[str, Any] | None = None,
    exact: bool = True,
) -> dict[str, Any]:
    return {
        "field": field,
        "value": _json_primitive(value),
        "provenance_class": provenance_class,
        "evidence_source": evidence_source,
        "subject": subject or {"type": "game"},
        "exact": bool(exact),
        "conflict_state": "none",
    }


def build_observations(
    projection: dict[str, Any],
    evidence: dict[str, Any],
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    key_events = projection.get("key_events") if isinstance(projection.get("key_events"), dict) else {}
    map_snapshot = evidence.get("map_snapshot") if isinstance(evidence.get("map_snapshot"), dict) else {}
    postgame_available = bool(key_events.get("postgame_available"))

    def add(field: str, value: Any, provenance: str, source: str, **kwargs: Any) -> None:
        observations.append(_observation(field, value, provenance, source, **kwargs))

    add("game.version", projection.get("game_version"), PROVENANCE_DIRECT_HEADER, "mgz.header.version")
    add(
        "game.type",
        projection.get("game_type"),
        PROVENANCE_DIRECT_HEADER,
        "mgz.summary.settings.type",
    )
    add("game.duration_seconds", projection.get("duration"), PROVENANCE_DERIVED, "mgz.summary.duration_ms_normalized")
    completion_provenance = PROVENANCE_DIRECT_POSTGAME if postgame_available else PROVENANCE_DERIVED
    add("game.completed", bool(projection.get("completed")), completion_provenance, "mgz.summary.get_completed")
    for field in ("platform_id", "platform_match_id", "rated", "lobby_name", "restored"):
        value = key_events.get(field)
        add(f"game.{field}", value, PROVENANCE_DIRECT_HEADER if value is not None else PROVENANCE_ABSENT, f"mgz.header.{field}")

    for field in ("id", "name", "size", "dimension", "seed", "custom", "zr"):
        value = map_snapshot.get(field)
        add(f"map.{field}", value, PROVENANCE_DIRECT_HEADER if value is not None else PROVENANCE_ABSENT, f"mgz.summary.map.{field}")
    for field in ("tile_count", "tile_sha256", "terrain_histogram", "elevation_histogram"):
        value = map_snapshot.get(field)
        add(f"map.{field}", value, PROVENANCE_DIRECT_HEADER if value is not None else PROVENANCE_ABSENT, f"mgz.header.map_tiles.{field}")

    settings = key_events.get("settings") if isinstance(key_events.get("settings"), dict) else {}
    for field in sorted(_KNOWN_SETTINGS | set(settings)):
        value = settings.get(field)
        add(
            f"settings.{field}",
            value,
            PROVENANCE_DIRECT_HEADER if value is not None else PROVENANCE_ABSENT,
            f"mgz.summary.settings.{field}",
        )

    diplomacy = evidence.get("diplomacy")
    if diplomacy:
        add("game.diplomacy", diplomacy, PROVENANCE_DERIVED, "mgz.summary.get_diplomacy")

    for player in projection.get("players") if isinstance(projection.get("players"), list) else []:
        if not isinstance(player, dict):
            continue
        subject = _player_subject(player)
        direct_fields = (
            "name",
            "number",
            "steam_id",
            "civilization",
            "civilization_name",
            "color_id",
            "team_id",
            "position",
            "human",
            "steam_rm_rating",
            "steam_dm_rating",
            "rate_snapshot",
        )
        for field in direct_fields:
            value = player.get(field)
            add(
                f"player.{field}",
                value,
                PROVENANCE_DIRECT_HEADER if value is not None else PROVENANCE_ABSENT,
                f"mgz.header.player.{field}",
                subject=subject,
            )
        eapm = player.get("eapm")
        add(
            "player.recorded_eapm",
            eapm,
            PROVENANCE_DERIVED if eapm is not None else PROVENANCE_ABSENT,
            "mgz.summary.non_ai_player_actions_per_duration_minute",
            subject=subject,
            exact=False,
        )

        winner = player.get("winner")
        if winner is None:
            winner_provenance = PROVENANCE_ABSENT
        elif postgame_available:
            winner_provenance = PROVENANCE_DIRECT_POSTGAME
        elif (projection.get("result_resolution") or {}).get("result_trusted"):
            winner_provenance = PROVENANCE_DERIVED
        else:
            winner_provenance = PROVENANCE_INFERRED_REVIEW_ONLY
        add(
            "player.winner",
            winner,
            winner_provenance,
            "mgz.summary.player_winner_with_team_contract",
            subject=subject,
            exact=winner_provenance in {PROVENANCE_DIRECT_POSTGAME, PROVENANCE_DERIVED},
        )

        achievements = player.get("achievements")
        achievements = achievements if isinstance(achievements, dict) else {}
        for postgame_field in _KNOWN_POSTGAME_FIELDS:
            value: Any = achievements
            for part in postgame_field.split("."):
                value = value.get(part) if isinstance(value, dict) else None
            add(
                f"player.postgame.{postgame_field}",
                value,
                PROVENANCE_DIRECT_POSTGAME if value is not None else PROVENANCE_ABSENT,
                "mgz.postgame.achievements",
                subject=subject,
                exact=value is not None,
            )
        score = player.get("score")
        add(
            "player.postgame.score",
            score,
            PROVENANCE_DIRECT_POSTGAME if score is not None else PROVENANCE_ABSENT,
            "mgz.postgame.score",
            subject=subject,
            exact=score is not None,
        )

    team_resolution = projection.get("team_resolution") if isinstance(projection.get("team_resolution"), dict) else {}
    add("teams.resolution", team_resolution, PROVENANCE_DERIVED, "aoe2war.replay_team_contract")
    trusted = bool(team_resolution.get("result_trusted"))
    add("result.trusted", trusted, PROVENANCE_DERIVED, "aoe2war.replay_team_contract")
    add(
        "result.winning_team_id",
        team_resolution.get("winning_team_id"),
        PROVENANCE_DERIVED if trusted else PROVENANCE_INFERRED_REVIEW_ONLY if team_resolution.get("winning_team_id") is not None else PROVENANCE_ABSENT,
        "aoe2war.replay_team_contract",
        exact=trusted,
    )
    add(
        "result.winning_player_keys",
        team_resolution.get("winning_player_keys") or [],
        PROVENANCE_DERIVED if trusted else PROVENANCE_INFERRED_REVIEW_ONLY if team_resolution.get("winning_player_keys") else PROVENANCE_ABSENT,
        "aoe2war.replay_team_contract",
        exact=trusted,
    )

    actions = evidence.get("actions") if isinstance(evidence.get("actions"), dict) else {}
    actions_available = bool(actions) and actions.get("available") is not False
    action_provenance = PROVENANCE_DIRECT_ACTION if actions_available else PROVENANCE_ABSENT
    add("actions.raw_count", actions.get("count") if actions_available else None, action_provenance, "mgz.summary.actions", exact=actions_available)
    add(
        "actions.unique_packet_identity_count",
        actions.get("unique_action_identity_count") if actions_available else None,
        PROVENANCE_DERIVED if actions_available else PROVENANCE_ABSENT,
        "aoe2war.canonical_action_packet_identity_v1",
        exact=actions_available,
    )
    add(
        "actions.exact_duplicate_packet_excess",
        actions.get("exact_duplicate_packet_excess") if actions_available else None,
        PROVENANCE_DERIVED if actions_available else PROVENANCE_ABSENT,
        "aoe2war.canonical_action_packet_identity_v1",
        exact=actions_available,
    )
    add("actions.type_counts", actions.get("type_counts", {}) if actions_available else None, action_provenance, "mgz.summary.actions", exact=actions_available)
    add(
        "actions.raw_activity_by_player",
        actions.get("raw_activity_by_player", []) if actions_available else None,
        PROVENANCE_DERIVED if actions_available else PROVENANCE_ABSENT,
        "aoe2war.raw_recorded_action_activity",
        exact=False,
    )
    add(
        "actions.identity_normalized_activity_by_player",
        actions.get("identity_normalized_activity_by_player", []) if actions_available else None,
        PROVENANCE_INFERRED_REVIEW_ONLY if actions_available else PROVENANCE_ABSENT,
        "aoe2war.experimental_exact_packet_identity_normalization",
        exact=False,
    )
    add(
        "actions.raw_resignation_timeline",
        actions.get("raw_resignation_timeline", []) if actions_available else None,
        action_provenance,
        "mgz.action.resign.raw_packets",
        exact=actions_available,
    )
    add(
        "actions.resignation_timeline",
        actions.get("resignation_timeline", []) if actions_available else None,
        PROVENANCE_DERIVED if actions_available else PROVENANCE_ABSENT,
        "aoe2war.earliest_resignation_per_player",
        exact=False,
    )
    add("actions.age_up_research_commands", actions.get("age_up_research_commands", []) if actions_available else None, action_provenance, "mgz.action.research", exact=False)
    add("actions.market_commands", actions.get("market_commands", []) if actions_available else None, action_provenance, "mgz.action.buy_sell", exact=actions_available)
    add("actions.tribute_commands", actions.get("tribute_commands", []) if actions_available else None, action_provenance, "mgz.action.tribute", exact=actions_available)
    chat = evidence.get("chat") if isinstance(evidence.get("chat"), dict) else {}
    chat_available = bool(chat) and chat.get("available") is not False
    chat_provenance = PROVENANCE_DIRECT_ACTION if chat_available else PROVENANCE_ABSENT
    add("chat.message_count", chat.get("count") if chat_available else None, chat_provenance, "mgz.chat", exact=chat_available)
    add("chat.timeline", chat.get("stream", []) if chat_available else None, chat_provenance, "mgz.chat", exact=chat_available)

    return sorted(
        observations,
        key=lambda observation: (
            observation["field"],
            _canonical_json(observation.get("subject")),
            observation["provenance_class"],
            _canonical_json(observation.get("value")),
        ),
    )


def build_candidate_envelope(
    *,
    replay_path: str,
    file_bytes: bytes | None,
    projection: dict[str, Any] | None,
    evidence: dict[str, Any] | None,
    apply_hd_early_exit_rules: bool,
    parse_mode: str,
    failure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    identity = parser_identity(apply_hd_early_exit_rules=apply_hd_early_exit_rules)
    artifact = artifact_descriptor(replay_path, file_bytes, projection)
    run_key = (
        pass_idempotency_key(artifact["sha256"], identity)
        if artifact.get("sha256")
        else None
    )
    evidence = evidence if isinstance(evidence, dict) else {}
    observations = build_observations(projection, evidence) if projection else []
    actions = evidence.get("actions") if isinstance(evidence.get("actions"), dict) else {
        "available": False,
        "count": None,
        "type_counts": {},
        "unique_action_identity_count": None,
        "exact_duplicate_packet_excess": None,
        "identity_multiplicity_summary": [],
        "duplicate_packet_identities": [],
        "raw_activity_by_player": [],
        "identity_normalized_activity_by_player": [],
        "raw_resignation_timeline": [],
        "resignation_timeline": [],
        "age_up_research_commands": [],
        "market_commands": [],
        "tribute_commands": [],
        "stream": [],
    }
    status = "failed" if projection is None else "recovered" if failure else "succeeded"
    candidate_state = "failed" if projection is None else "candidate"
    hash_material = {
        "artifact_sha256": artifact["sha256"],
        "artifact_byte_size": artifact["byte_size"],
        "parser": identity,
        "parse_mode": parse_mode,
        "observations": observations,
        "dataset": evidence.get("dataset") or {},
        "diplomacy": evidence.get("diplomacy") or {},
        "map_snapshot": evidence.get("map_snapshot") or {},
        "initial_objects": evidence.get("initial_objects") or {},
        "chat": evidence.get("chat") or {},
        "actions": actions,
        "failure": failure,
    }
    semantic_hash = _stable_hash(hash_material) if projection is not None else None
    envelope = {
        "contract_version": PARSER_CONTRACT_VERSION,
        "artifact": artifact,
        "parser": identity,
        "run": {
            "idempotency_key": run_key,
            "status": status,
            "parse_mode": parse_mode,
            "failure": failure,
        },
        "candidate": {
            "state": candidate_state,
            "semantic_sha256": semantic_hash,
            "promotion_status": "candidate_only",
            "changes_effective_truth": False,
        },
        "observations": observations,
        "evidence": {
            "dataset": evidence.get("dataset") or {},
            "diplomacy": evidence.get("diplomacy") or {},
            "map_snapshot": evidence.get("map_snapshot") or {},
            "initial_objects": evidence.get("initial_objects") or {},
            "chat": evidence.get("chat") or {},
        },
        "actions": actions,
        "projection": projection,
    }
    if projection is not None:
        key_events = projection.get("key_events") if isinstance(projection.get("key_events"), dict) else {}
        key_events = dict(key_events)
        key_events["parser_engine"] = compact_candidate_receipt(envelope)
        projection["key_events"] = key_events
    return envelope


def compact_candidate_receipt(candidate: dict[str, Any]) -> dict[str, Any]:
    """Return a bounded receipt that is safe to reference outside candidate storage."""
    artifact = candidate.get("artifact") if isinstance(candidate.get("artifact"), dict) else {}
    parser = candidate.get("parser") if isinstance(candidate.get("parser"), dict) else {}
    run = candidate.get("run") if isinstance(candidate.get("run"), dict) else {}
    candidate_state = candidate.get("candidate") if isinstance(candidate.get("candidate"), dict) else {}
    actions = candidate.get("actions") if isinstance(candidate.get("actions"), dict) else {}
    evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), dict) else {}
    initial_objects = (
        evidence.get("initial_objects")
        if isinstance(evidence.get("initial_objects"), dict)
        else {}
    )
    failure = run.get("failure") if isinstance(run.get("failure"), dict) else None
    actions_available = actions.get("available") is not False and actions.get("count") is not None
    market_commands = actions.get("market_commands") or []
    market_type_counts = Counter(
        str(command.get("type") or "unclassified")
        for command in market_commands
        if isinstance(command, dict)
    )
    receipt = {
        "contract_version": candidate.get("contract_version"),
        "artifact_sha256": artifact.get("sha256"),
        "artifact_byte_size": artifact.get("byte_size"),
        "format": artifact.get("format"),
        "parser_implementation": parser.get("implementation"),
        "parser_version": parser.get("implementation_version"),
        "parser_schema_version": parser.get("schema_version"),
        "pass_name": parser.get("pass_name"),
        "pass_version": parser.get("pass_version"),
        "pass_options": parser.get("options"),
        "idempotency_key": run.get("idempotency_key"),
        "run_status": run.get("status"),
        "parse_mode": run.get("parse_mode"),
        "candidate_semantic_sha256": candidate_state.get("semantic_sha256"),
        "promotion_status": candidate_state.get("promotion_status"),
        "changes_effective_truth": False,
        "observation_count": len(candidate.get("observations") or []),
        "raw_action_count": actions.get("count"),
        "unique_action_identity_count": actions.get("unique_action_identity_count"),
        "exact_duplicate_packet_excess": actions.get("exact_duplicate_packet_excess"),
        "identity_multiplicity_summary": actions.get("identity_multiplicity_summary") or [],
        "action_type_counts": actions.get("type_counts") or {},
        "recorded_evidence": {
            "dataset": evidence.get("dataset") or {},
            "diplomacy": evidence.get("diplomacy") or {},
            "map_snapshot": evidence.get("map_snapshot") or {},
            "initial_objects": {
                "snapshot_scope": initial_objects.get("snapshot_scope"),
                "object_count": initial_objects.get("object_count"),
                "object_count_semantics": initial_objects.get("object_count_semantics"),
                "object_type_counts": initial_objects.get("object_type_counts") or [],
                "max_starting_town_centers_per_player": initial_objects.get(
                    "max_starting_town_centers_per_player"
                ),
                "town_center_metric_semantics": initial_objects.get(
                    "town_center_metric_semantics"
                ),
                "starting_stone_wall_presence": initial_objects.get(
                    "starting_stone_wall_presence"
                ),
                "starting_palisade_wall_presence": initial_objects.get(
                    "starting_palisade_wall_presence"
                ),
                "wall_metric_semantics": initial_objects.get("wall_metric_semantics"),
            },
            "raw_activity_by_player": actions.get("raw_activity_by_player") or [],
            "identity_normalized_activity_by_player": (
                actions.get("identity_normalized_activity_by_player") or []
            ),
            "resignation_count": (
                len(actions.get("resignation_timeline") or [])
                if actions_available
                else None
            ),
            "raw_resignation_packet_count": (
                len(actions.get("raw_resignation_timeline") or [])
                if actions_available
                else None
            ),
            "age_up_research_command_count": (
                len(actions.get("age_up_research_commands") or [])
                if actions_available
                else None
            ),
            "market_command_count": len(market_commands) if actions_available else None,
            "market_command_type_counts": {
                command_type: market_type_counts[command_type]
                for command_type in sorted(market_type_counts)
            },
            "tribute_command_count": (
                len(actions.get("tribute_commands") or [])
                if actions_available
                else None
            ),
            "chat_message_count": (
                evidence.get("chat", {}).get("count")
                if isinstance(evidence.get("chat"), dict)
                else None
            ),
            "full_action_stream_lane": "candidate_output_only",
            "full_command_timeline_lane": "candidate_output_only",
        },
        "failure_signature": failure.get("signature") if failure else None,
        "receipt_truncated": False,
    }

    if len(_canonical_json(receipt).encode("utf-8")) > MAX_COMPACT_RECEIPT_JSON_BYTES:
        recorded = receipt["recorded_evidence"]
        recorded["raw_activity_by_player"] = []
        recorded["identity_normalized_activity_by_player"] = []
        recorded["activity_summary_lane"] = "candidate_output_only"
        receipt["receipt_truncated"] = True

    if len(_canonical_json(receipt).encode("utf-8")) > MAX_COMPACT_RECEIPT_JSON_BYTES:
        recorded = receipt["recorded_evidence"]
        recorded["dataset"] = {}
        recorded["diplomacy"] = {}
        recorded["map_snapshot"] = {
            key: recorded["map_snapshot"].get(key)
            for key in ("id", "name", "size", "dimension", "tile_count", "tile_sha256")
            if isinstance(recorded.get("map_snapshot"), dict)
        }
        recorded["initial_objects"]["object_type_counts"] = []
        receipt["action_type_counts"] = {}

    if len(_canonical_json(receipt).encode("utf-8")) > MAX_COMPACT_RECEIPT_JSON_BYTES:
        receipt["recorded_evidence"] = {
            "evidence_summary_lane": "candidate_output_only",
            "chat_message_count": (
                evidence.get("chat", {}).get("count")
                if isinstance(evidence.get("chat"), dict)
                else None
            ),
            "resignation_count": (
                len(actions.get("resignation_timeline") or [])
                if actions_available
                else None
            ),
            "raw_resignation_packet_count": (
                len(actions.get("raw_resignation_timeline") or [])
                if actions_available
                else None
            ),
            "age_up_research_command_count": (
                len(actions.get("age_up_research_commands") or [])
                if actions_available
                else None
            ),
            "market_command_count": len(market_commands) if actions_available else None,
            "tribute_command_count": (
                len(actions.get("tribute_commands") or [])
                if actions_available
                else None
            ),
        }
        receipt["action_type_counts"] = {}
        receipt["receipt_truncated"] = True

    return receipt
