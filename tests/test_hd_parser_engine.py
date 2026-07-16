import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.replay_engine import (  # noqa: E402
    PARSER_IMPLEMENTATION,
    PARSER_PASS_NAME,
    PARSER_PASS_VERSION,
    PARSER_SCHEMA_VERSION,
    MAX_COMPACT_RECEIPT_JSON_BYTES,
    PROVENANCE_ABSENT,
    PROVENANCE_DIRECT_ACTION,
    build_candidate_envelope,
    canonical_candidate_json,
    compact_candidate_receipt,
    normalize_failure_signature,
    parser_identity,
    pass_idempotency_key,
)
import utils.replay_parser as replay_parser  # noqa: E402
from utils.replay_parser import parse_replay_candidate_bytes  # noqa: E402


MANIFEST_PATH = Path(__file__).parent / "fixtures" / "hd5_8_golden_manifest.json"
FRAGMENT_MANIFEST_PATH = (
    Path(__file__).parent / "fixtures" / "hd_fragment_golden_manifest.json"
)
DEFAULT_GOLDEN_DIR = (
    Path.home()
    / "Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)"
    / "Steam/steamapps/common/Age2HD/SaveGame"
)
DEFAULT_FRAGMENT_GOLDEN_DIR = Path(
    "/mnt/HC_Volume_105319120/aoe2-parser-engine/golden-fixtures"
)


def _load_manifest():
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))["fixtures"]


def _golden_dir():
    configured = os.getenv("AOE2_HD_GOLDEN_DIR")
    return Path(configured).expanduser() if configured else DEFAULT_GOLDEN_DIR


def _golden_path(entry):
    fixture_dir = _golden_dir()
    suffix = Path(entry["filename"]).suffix.casefold()
    candidates = (
        fixture_dir / entry["filename"],
        fixture_dir / f'{entry["sha256"]}{suffix}',
    )
    return next((path for path in candidates if path.is_file()), candidates[0])


def _fragment_manifest():
    return json.loads(FRAGMENT_MANIFEST_PATH.read_text(encoding="utf-8"))["fixtures"]


def _fragment_golden_dir():
    configured = os.getenv("AOE2_HD_FRAGMENT_GOLDEN_DIR")
    return Path(configured).expanduser() if configured else DEFAULT_FRAGMENT_GOLDEN_DIR


def _fragment_golden_path(entry):
    return _fragment_golden_dir() / entry["filename"]


@pytest.fixture(scope="module")
def golden_candidates():
    manifest = _load_manifest()
    missing = [entry["filename"] for entry in manifest if not _golden_path(entry).is_file()]
    if missing:
        pytest.skip(
            "AoE2HD binary corpus is external immutable test data; set "
            "AOE2_HD_GOLDEN_DIR to run golden-byte tests"
        )

    candidates = {}
    for expected in manifest:
        path = _golden_path(expected)
        file_bytes = path.read_bytes()
        assert hashlib.sha256(file_bytes).hexdigest() == expected["sha256"]
        candidates[expected["filename"]] = parse_replay_candidate_bytes(
            expected["filename"],
            file_bytes,
        )
    return manifest, candidates


@pytest.fixture(scope="module")
def fragment_golden_candidates():
    manifest = _fragment_manifest()
    missing = [
        entry["filename"]
        for entry in manifest
        if not _fragment_golden_path(entry).is_file()
    ]
    if missing:
        pytest.skip(
            "HD fragment goldens are private immutable replay bytes; set "
            "AOE2_HD_FRAGMENT_GOLDEN_DIR to run them"
        )

    candidates = {}
    for expected in manifest:
        path = _fragment_golden_path(expected)
        file_bytes = path.read_bytes()
        assert hashlib.sha256(file_bytes).hexdigest() == expected["sha256"]
        candidates[expected["filename"]] = parse_replay_candidate_bytes(
            expected["filename"],
            file_bytes,
        )
    return manifest, candidates


def test_parser_pass_identity_is_explicit_and_idempotent():
    identity = parser_identity(apply_hd_early_exit_rules=True)
    assert identity["implementation"] == PARSER_IMPLEMENTATION
    assert identity["pass_name"] == PARSER_PASS_NAME
    assert identity["implementation_version"] == "1.8.51"
    assert identity["schema_version"] == PARSER_SCHEMA_VERSION == "2026-07-16.1"
    assert identity["pass_version"] == PARSER_PASS_VERSION == "3"
    assert pass_idempotency_key("a" * 64, identity) == pass_idempotency_key("a" * 64, identity)
    assert pass_idempotency_key("a" * 64, identity) != pass_idempotency_key("b" * 64, identity)


def test_golden_resolver_accepts_content_addressed_archive_names(
    golden_candidates,
    monkeypatch,
    tmp_path,
):
    manifest, _candidates = golden_candidates
    expected = manifest[0]
    file_bytes = _golden_path(expected).read_bytes()
    content_addressed = tmp_path / f'{expected["sha256"]}.aoe2record'
    content_addressed.write_bytes(file_bytes)
    monkeypatch.setenv("AOE2_HD_GOLDEN_DIR", str(tmp_path))

    assert _golden_path(expected) == content_addressed


def test_failure_signatures_group_offsets_and_paths_without_leaking_them():
    first = normalize_failure_signature(
        struct.error("unpack requires a buffer of 4 bytes at /tmp/upload-123/file.aoe2record"),
        stage="summary",
    )
    second = normalize_failure_signature(
        struct.error("unpack requires a buffer of 8 bytes at /private/tmp/upload-999/file.aoe2record"),
        stage="summary",
    )

    assert first["signature"] == second["signature"]
    assert first["category"] == "truncated_or_incomplete"
    assert first["retryable"] is True
    assert "/tmp" not in first["normalized_message"]
    assert "123" not in first["normalized_message"]


def test_failure_signatures_redact_paths_urls_and_secret_bearing_tokens():
    failure = normalize_failure_signature(
        ValueError(
            "failed reading /Users/Tony Blum/Secret Games/final replay.aoe2record "
            "near https://user:pass@example.com/replay?id=12&token=url-secret "
            "api_key=plain-secret password:'hunter two' "
            "Authorization: Bearer abcdefghijklmnopqrstuvwxyz "
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWNyZXQifQ.signaturevalue"
        ),
        stage="summary",
    )

    message = failure["normalized_message"]
    assert "<path>" in message
    assert "<url>" in message
    assert "<redacted>" in message
    for secret in (
        "tony blum",
        "secret games",
        "user:pass",
        "example.com",
        "url-secret",
        "plain-secret",
        "hunter two",
        "abcdefghijklmnopqrstuvwxyz",
        "eyjhb",
    ):
        assert secret not in message

    windows_failure = normalize_failure_signature(
        ValueError(
            r"failed reading C:\Users\Tony Blum\Secret Games\final replay.aoe2record "
            r"at offset 1234 x-api-key=windows-secret"
        ),
        stage="summary",
    )
    assert "<path>" in windows_failure["normalized_message"]
    assert "tony blum" not in windows_failure["normalized_message"]
    assert "windows-secret" not in windows_failure["normalized_message"]


def test_async_candidate_generation_failure_preserves_artifact_identity(
    monkeypatch,
    tmp_path,
):
    replay_path = tmp_path / "candidate with spaces.aoe2record"
    replay_bytes = b"immutable replay bytes"
    replay_path.write_bytes(replay_bytes)

    def fail_candidate_generation(*_args, **_kwargs):
        raise RuntimeError(
            "candidate failed at /Users/Tony Blum/private replay.aoe2record "
            "api_key=do-not-store"
        )

    monkeypatch.setattr(
        replay_parser,
        "parse_replay_candidate_bytes",
        fail_candidate_generation,
    )
    candidate = asyncio.run(replay_parser.parse_replay_candidate(str(replay_path)))

    assert candidate["artifact"]["sha256"] == hashlib.sha256(replay_bytes).hexdigest()
    assert candidate["artifact"]["byte_size"] == len(replay_bytes)
    assert candidate["run"]["idempotency_key"]
    assert candidate["run"]["parse_mode"] == "candidate_generation_failed"
    assert candidate["run"]["failure"]["stage"] == "candidate_build"
    assert candidate["run"]["failure"]["category"] == "parser_exception"
    assert "tony blum" not in candidate["run"]["failure"]["normalized_message"]
    assert "do-not-store" not in candidate["run"]["failure"]["normalized_message"]


def test_truncated_golden_bytes_produce_failed_candidate_with_stable_signature(
    golden_candidates,
):
    manifest, _candidates = golden_candidates
    expected = manifest[0]
    truncated = _golden_path(expected).read_bytes()[:64]

    first = parse_replay_candidate_bytes("partial.aoe2record", truncated)
    second = parse_replay_candidate_bytes("another-name.aoe2record", truncated)

    assert first["projection"] is None
    assert first["run"]["status"] == "failed"
    assert first["run"]["parse_mode"] == "mgz_failed"
    assert first["run"]["failure"]["category"] == "truncated_or_incomplete"
    assert first["run"]["failure"]["retryable"] is True
    assert first["run"]["failure"]["signature"] == second["run"]["failure"]["signature"]
    assert first["run"]["idempotency_key"] == second["run"]["idempotency_key"]
    assert first["candidate"]["state"] == "failed"
    assert first["candidate"]["semantic_sha256"] is None


def test_hd_fragment_goldens_recover_roster_diplomacy_and_bounded_body_truth(
    fragment_golden_candidates,
):
    manifest, candidates = fragment_golden_candidates
    for expected in manifest:
        candidate = candidates[expected["filename"]]
        projection = candidate["projection"]
        key_events = projection["key_events"]
        actions = candidate["actions"]

        assert candidate["artifact"]["sha256"] == expected["sha256"]
        assert candidate["artifact"]["byte_size"] == expected["byte_size"]
        assert candidate["run"]["status"] == "recovered"
        assert candidate["run"]["parse_mode"] == (
            "mgz_hd_fragment_header_body_fallback"
        )
        assert candidate["run"]["failure"]["stage"] == "header"
        assert key_events["header_fragment_boundary"] == "before_lobby"
        assert key_events["team_source"] == "header_initial_mutual_diplomacy"
        assert [player["name"] for player in projection["players"]] == expected[
            "players"
        ]
        assert projection["team_resolution"]["format"] == expected["format"]
        assert projection["team_resolution"]["result_trusted"] is expected[
            "result_trusted"
        ]
        assert projection["duration"] == expected["duration_seconds"]
        assert projection["map"]["id"] == expected["map_id"]
        assert projection["map"]["name"] == expected["map_name"]
        assert projection["map"]["dimension"] == expected["map_dimension"]
        assert candidate["evidence"]["initial_objects"]["object_count"] == expected[
            "initial_object_count"
        ]
        assert key_events["body_stream_recovery"] is expected[
            "body_stream_recovery"
        ]
        assert key_events["body_operation_count"] == expected[
            "body_operation_count"
        ]
        assert actions.get("count") == expected["raw_action_count"]
        assert actions.get("unique_action_identity_count") == expected[
            "unique_action_identity_count"
        ]
        assert actions.get("exact_duplicate_packet_excess") == expected[
            "exact_duplicate_packet_excess"
        ]
        assert len(actions.get("raw_resignation_timeline") or []) == expected[
            "raw_resignation_packet_count"
        ]
        assert len(actions.get("resignation_timeline") or []) == expected[
            "semantic_resignation_count"
        ]
        assert candidate["candidate"]["changes_effective_truth"] is False


def test_hd58_golden_candidates_match_exact_byte_evidence(golden_candidates):
    manifest, candidates = golden_candidates
    for expected in manifest:
        candidate = candidates[expected["filename"]]
        projection = candidate["projection"]
        result = projection["team_resolution"]
        map_snapshot = candidate["evidence"]["map_snapshot"]

        assert candidate["artifact"]["sha256"] == expected["sha256"]
        assert candidate["artifact"]["byte_size"] == expected["byte_size"]
        assert candidate["artifact"]["format"] == "aoe2_hd"
        assert candidate["run"]["status"] == "succeeded"
        assert candidate["run"]["parse_mode"] == "mgz_full_summary"
        assert candidate["candidate"]["state"] == "candidate"
        assert candidate["candidate"]["promotion_status"] == "candidate_only"
        assert candidate["candidate"]["changes_effective_truth"] is False

        assert [player["name"] for player in projection["players"]] == expected["players"]
        assert projection["game_type"] == expected["game_type"]
        assert projection["game_type"] == projection["key_events"]["settings"]["type"]
        assert not projection["game_type"].startswith("(")
        assert result["format"] == expected["format"]
        assert [team["players"] for team in result["teams"]] == expected["teams"]
        assert result["result_trusted"] is expected["result_trusted"]
        assert (
            result["winning_player_names"] if result["result_trusted"] else []
        ) == expected["trusted_winners"]
        assert projection["key_events"]["resigned_player_numbers"] == expected[
            "resigned_player_numbers"
        ]

        assert candidate["actions"]["count"] == expected["raw_action_count"]
        assert len(candidate["actions"]["stream"]) == expected["raw_action_count"]
        assert candidate["actions"]["unique_action_identity_count"] == expected[
            "unique_action_identity_count"
        ]
        assert candidate["actions"]["exact_duplicate_packet_excess"] == expected[
            "exact_duplicate_packet_excess"
        ]
        assert (
            candidate["actions"]["unique_action_identity_count"]
            + candidate["actions"]["exact_duplicate_packet_excess"]
            == candidate["actions"]["count"]
        )
        assert max(
            row["multiplicity"]
            for row in candidate["actions"]["identity_multiplicity_summary"]
        ) == expected["maximum_action_identity_multiplicity"]
        assert all(
            len(action["packet_identity_sha256"]) == 64
            for action in candidate["actions"]["stream"]
        )
        assert sum(
            activity["action_packet_count"]
            for activity in candidate["actions"]["raw_activity_by_player"]
        ) == expected["raw_attributed_action_count"]
        assert sum(
            activity["action_packet_count"]
            for activity in candidate["actions"][
                "identity_normalized_activity_by_player"
            ]
        ) == expected["identity_normalized_attributed_action_count"]
        assert len(candidate["actions"]["raw_resignation_timeline"]) == expected[
            "raw_resignation_packet_count"
        ]
        assert len(candidate["actions"]["resignation_timeline"]) == expected[
            "semantic_resignation_count"
        ]
        assert candidate["evidence"]["chat"]["count"] == expected[
            "chat_message_count"
        ]
        assert len(candidate["evidence"]["chat"]["stream"]) == expected[
            "chat_message_count"
        ]
        assert map_snapshot["id"] == expected["map_id"]
        assert map_snapshot["name"] == expected["map_name"]
        assert map_snapshot["dimension"] == expected["map_dimension"]
        assert map_snapshot["tile_count"] == expected["tile_count"]
        assert map_snapshot["tile_sha256"] == expected["tile_sha256"]
        assert candidate["evidence"]["initial_objects"]["object_count"] == expected[
            "initial_object_count"
        ]
        assert candidate["evidence"]["initial_objects"][
            "max_starting_town_centers_per_player"
        ] == expected["max_starting_town_centers_per_player"]
        assert candidate["evidence"]["initial_objects"]["snapshot_scope"] == (
            "mgz_initial_header_objects_only"
        )
        assert "not units or buildings created during gameplay" in candidate[
            "evidence"
        ]["initial_objects"]["object_count_semantics"]


def test_game_type_uses_normalized_settings_and_never_summary_version_tuple(
    golden_candidates,
    monkeypatch,
):
    from mgz.summary.full import FullSummary

    manifest, _candidates = golden_candidates
    expected = manifest[0]

    def fail_if_called(_summary):
        raise AssertionError("Summary.get_version is not an HD game-type source")

    monkeypatch.setattr(FullSummary, "get_version", fail_if_called)
    candidate = parse_replay_candidate_bytes(
        expected["filename"],
        _golden_path(expected).read_bytes(),
    )

    assert candidate["run"]["status"] == "succeeded"
    assert candidate["projection"]["game_type"] == expected["game_type"]


def test_action_packet_identities_and_semantic_resignations_are_deterministic(
    golden_candidates,
):
    _manifest, candidates = golden_candidates
    for candidate in candidates.values():
        actions = candidate["actions"]
        identity_fields = actions["identity_fields"]
        for action in actions["stream"]:
            identity_material = {
                field: action.get(field)
                for field in identity_fields
            }
            expected_identity = hashlib.sha256(
                json.dumps(
                    identity_material,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            assert action["packet_identity_sha256"] == expected_identity

        raw_by_player = {}
        for event in actions["raw_resignation_timeline"]:
            raw_by_player.setdefault(event["player_number"], []).append(event)
        assert len(actions["resignation_timeline"]) == len(raw_by_player)
        for event in actions["resignation_timeline"]:
            raw_events = raw_by_player[event["player_number"]]
            earliest = min(
                raw_events,
                key=lambda raw: (raw["timestamp_ms"], raw["ordinal"]),
            )
            assert event["timestamp_ms"] == earliest["timestamp_ms"]
            assert event["earliest_raw_ordinal"] == earliest["ordinal"]
            assert event["raw_packet_count_for_player"] == len(raw_events)

        assert all(
            activity["metric_lane"] == "raw_parsed_action_packets"
            and activity["validated_gameplay_truth"] is False
            for activity in actions["raw_activity_by_player"]
        )
        assert all(
            activity["metric_lane"]
            == "experimental_exact_packet_identity_normalized"
            and activity["validated_gameplay_truth"] is False
            for activity in actions["identity_normalized_activity_by_player"]
        )


def test_team_golden_keeps_complete_winner_and_direct_action_provenance(golden_candidates):
    _manifest, candidates = golden_candidates
    candidate = candidates["MP Replay v5.8 @2026.07.06 182842 (1).aoe2record"]
    result = candidate["projection"]["team_resolution"]

    assert result["winning_team_id"] == 1
    assert result["winning_player_names"] == ["Emaren", "Merik"]
    assert result["result_provenance"] == "complete_losing_team_resignation"
    assert result["result_trusted"] is True

    assert candidate["actions"]["raw_resignation_timeline"]
    assert candidate["actions"]["resignation_timeline"]
    assert candidate["actions"]["age_up_research_commands"]
    assert all(
        action["meaning"] == "research_command_recorded_not_completion_proof"
        for action in candidate["actions"]["age_up_research_commands"]
    )
    assert all(
        action["provenance_class"] == PROVENANCE_DIRECT_ACTION
        for action in candidate["actions"]["stream"]
    )
    queue_command = next(
        action for action in candidate["actions"]["stream"] if action["type"] == "de_queue"
    )
    assert queue_command["command_family"] == "production_queue_command"
    assert queue_command["payload"]["labels"]["unit"]
    assert all(
        activity["command_family_counts"]["production_queue_command"] > 0
        for activity in candidate["actions"]["raw_activity_by_player"]
    )


def test_jim_known_4v4_keeps_exact_teams_and_complete_winning_roster(golden_candidates):
    _manifest, candidates = golden_candidates
    candidate = candidates["MP Replay v5.8 @2026.07.12 203937 (1).aoe2record"]
    result = candidate["projection"]["team_resolution"]

    assert result["format"] == "4v4"
    assert result["teams"] == [
        {
            "team_id": 0,
            "player_keys": [
                "steam:76561198124349731",
                "steam:76561198128506495",
                "steam:76561198166409520",
                "steam:76561198216610161",
            ],
            "players": ["Scavanger_Ab", "Tekki", "Jim", "Rick"],
        },
        {
            "team_id": 1,
            "player_keys": [
                "steam:76561197984092705",
                "steam:76561198051612597",
                "steam:76561198105942599",
                "steam:76561199038985904",
            ],
            "players": ["MTR", "JakeTheSnake", "jlann85", "YELLOWJACKET"],
        },
    ]
    assert result["winning_team_id"] == 0
    assert result["winning_player_names"] == ["Scavanger_Ab", "Tekki", "Jim", "Rick"]
    assert result["result_trusted"] is True
    assert result["result_provenance"] == "complete_losing_team_resignation"


def test_unfinished_golden_games_do_not_gain_authoritative_winners(golden_candidates):
    _manifest, candidates = golden_candidates
    for filename in (
        "MP Replay v5.8 @2026.07.06 075258 (1).aoe2record",
        "MP Replay v5.8 @2026.07.08 172835 (1).aoe2record",
    ):
        candidate = candidates[filename]
        result = candidate["projection"]["team_resolution"]
        winning_team_observation = next(
            observation
            for observation in candidate["observations"]
            if observation["field"] == "result.winning_team_id"
        )
        assert result["winning_team_id"] is None
        assert result["winning_player_names"] == []
        assert result["result_trusted"] is False
        assert winning_team_observation["value"] is None
        assert winning_team_observation["provenance_class"] == PROVENANCE_ABSENT
        assert winning_team_observation["exact"] is False

        emaren_kills = next(
            observation
            for observation in candidate["observations"]
            if observation["field"] == "player.postgame.military.units_killed"
            and observation["subject"].get("player_name") == "Emaren"
        )
        assert emaren_kills["value"] is None
        assert emaren_kills["provenance_class"] == PROVENANCE_ABSENT
        assert emaren_kills["exact"] is False


def test_semantic_hash_is_stable_across_submission_filenames(golden_candidates):
    manifest, candidates = golden_candidates
    expected = manifest[0]
    original = candidates[expected["filename"]]
    file_bytes = _golden_path(expected).read_bytes()
    renamed = parse_replay_candidate_bytes("/archive/replay-copy.mgz", file_bytes)

    assert original["run"]["idempotency_key"] == renamed["run"]["idempotency_key"]
    assert original["candidate"]["semantic_sha256"] == renamed["candidate"]["semantic_sha256"]
    assert canonical_candidate_json(original) != canonical_candidate_json(renamed)
    assert original["artifact"]["original_extension"] == ".aoe2record"
    assert renamed["artifact"]["original_extension"] == ".mgz"


def test_compact_receipt_preserves_safe_metrics_without_hot_action_stream(golden_candidates):
    _manifest, candidates = golden_candidates
    candidate = candidates["MP Replay v5.8 @2026.07.06 182842 (1).aoe2record"]
    receipt = compact_candidate_receipt(candidate)

    assert receipt == candidate["projection"]["key_events"]["parser_engine"]
    assert receipt["raw_action_count"] == 5860
    assert receipt["unique_action_identity_count"] == 3944
    assert receipt["exact_duplicate_packet_excess"] == 1916
    assert receipt["recorded_evidence"]["map_snapshot"]["tile_count"] == 28224
    assert receipt["recorded_evidence"]["initial_objects"]["object_count"] == 366
    assert receipt["recorded_evidence"]["raw_activity_by_player"]
    assert receipt["recorded_evidence"]["identity_normalized_activity_by_player"]
    assert receipt["recorded_evidence"]["resignation_count"] == 2
    assert receipt["recorded_evidence"]["raw_resignation_packet_count"] == 5
    assert receipt["recorded_evidence"]["age_up_research_command_count"] > 0
    assert receipt["recorded_evidence"]["market_command_count"] > 0
    assert receipt["recorded_evidence"]["tribute_command_count"] >= 0
    assert receipt["recorded_evidence"]["chat_message_count"] == 62
    assert receipt["recorded_evidence"]["full_action_stream_lane"] == "candidate_output_only"
    assert receipt["recorded_evidence"]["full_command_timeline_lane"] == "candidate_output_only"
    assert "market_commands" not in receipt["recorded_evidence"]
    assert "tribute_commands" not in receipt["recorded_evidence"]
    assert "resignation_timeline" not in receipt["recorded_evidence"]
    assert "age_up_research_commands" not in receipt["recorded_evidence"]
    assert "stream" not in receipt
    assert "observations" not in receipt
    assert len(canonical_candidate_json(receipt).encode("utf-8")) <= MAX_COMPACT_RECEIPT_JSON_BYTES


def test_compact_receipt_enforces_size_ceiling_for_oversized_summaries(
    golden_candidates,
):
    _manifest, candidates = golden_candidates
    candidate = copy.deepcopy(
        candidates["MP Replay v5.8 @2026.07.06 182842 (1).aoe2record"]
    )
    candidate["actions"]["raw_activity_by_player"] = [
        {"player_number": number, "synthetic_summary": "x" * 10_000}
        for number in range(32)
    ]
    candidate["actions"]["identity_normalized_activity_by_player"] = [
        {"player_number": number, "synthetic_summary": "y" * 10_000}
        for number in range(32)
    ]

    receipt = compact_candidate_receipt(candidate)

    assert receipt["receipt_truncated"] is True
    assert receipt["recorded_evidence"]["raw_activity_by_player"] == []
    assert receipt["recorded_evidence"]["identity_normalized_activity_by_player"] == []
    assert len(canonical_candidate_json(receipt).encode("utf-8")) <= MAX_COMPACT_RECEIPT_JSON_BYTES


def test_projection_only_parser_skips_engine_evidence_capture(
    golden_candidates,
    monkeypatch,
):
    manifest, _candidates = golden_candidates
    expected = manifest[0]
    replay_path = _golden_path(expected)

    def fail_if_called(_summary):
        raise AssertionError("hot projection parser captured heavyweight engine evidence")

    monkeypatch.setattr(replay_parser, "capture_summary_evidence", fail_if_called)
    parsed = asyncio.run(replay_parser.parse_replay_full(str(replay_path)))

    assert parsed is not None
    assert "parser_engine" not in parsed.get("key_events", {})


def test_missing_evidence_stays_absent_instead_of_becoming_exact_zero():
    candidate = build_candidate_envelope(
        replay_path="header-only.aoe2record",
        file_bytes=b"header-only",
        projection={
            "game_version": "Version.HD",
            "game_type": "Unknown",
            "duration": 0,
            "completed": False,
            "players": [],
            "key_events": {},
            "map": {},
        },
        evidence=None,
        apply_hd_early_exit_rules=True,
        parse_mode="mgz_header_only_fallback",
    )
    observations = {
        observation["field"]: observation for observation in candidate["observations"]
    }

    for field in ("actions.raw_count", "actions.type_counts", "chat.message_count"):
        assert observations[field]["value"] is None
        assert observations[field]["provenance_class"] == PROVENANCE_ABSENT
        assert observations[field]["exact"] is False

    receipt = compact_candidate_receipt(candidate)
    assert receipt["raw_action_count"] is None
    assert receipt["recorded_evidence"]["initial_objects"]["object_count"] is None
    assert receipt["recorded_evidence"]["chat_message_count"] is None
    assert receipt["recorded_evidence"]["market_command_count"] is None


def test_local_candidate_cli_emits_canonical_candidate_json(golden_candidates, tmp_path):
    manifest, _candidates = golden_candidates
    expected = manifest[0]
    output_path = tmp_path / "candidate.json"
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/parse_replay_candidate.py",
            str(_golden_path(expected)),
            "--source-name",
            expected["filename"],
            "--expected-sha256",
            expected["sha256"],
            "--output",
            str(output_path),
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    rendered = output_path.read_text(encoding="utf-8")
    candidate = json.loads(rendered)
    assert rendered == json.dumps(
        candidate,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    assert candidate["artifact"]["sha256"] == expected["sha256"]
    assert candidate["candidate"]["promotion_status"] == "candidate_only"
    assert candidate["candidate"]["changes_effective_truth"] is False
    assert output_path.stat().st_mode & 0o777 == 0o600


def test_local_candidate_cli_emits_structured_integrity_failure(tmp_path):
    artifact_path = tmp_path / "content.aoe2record"
    artifact_path.write_bytes(b"not the expected artifact")
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/parse_replay_candidate.py",
            str(artifact_path),
            "--expected-sha256",
            "0" * 64,
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    candidate = json.loads(completed.stdout)
    assert candidate["run"]["status"] == "failed"
    assert candidate["run"]["parse_mode"] == "artifact_integrity_failed"
    assert candidate["run"]["failure"]["category"] == "artifact_integrity"
    assert candidate["candidate"]["promotion_status"] == "candidate_only"
    assert candidate["candidate"]["changes_effective_truth"] is False
