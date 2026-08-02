from __future__ import annotations

import pytest

from scripts.project_replay_candidate_results import (
    require_current_artifact_binding,
)
from scripts.report_replay_engine_room_job import (
    artifact_binding_summary,
)


def test_current_artifact_binding_accepts_exact_hash_case_insensitively():
    require_current_artifact_binding(
        {
            "replay_hash": "a" * 64,
            "input_hash": "A" * 64,
        }
    )


def test_current_artifact_binding_rejects_stale_prefix_run():
    with pytest.raises(
        RuntimeError,
        match="input_hash does not match current",
    ):
        require_current_artifact_binding(
            {
                "replay_hash": "e" * 64,
                "input_hash": "c" * 64,
            }
        )


def test_report_marks_stale_artifact_binding():
    assert artifact_binding_summary(
        {
            "current_replay_hash": "e" * 64,
            "input_hash": "c" * 64,
        }
    ) == {
        "source_artifact_sha256": "c" * 64,
        "current_replay_hash": "e" * 64,
        "matches_current": False,
    }


def test_report_marks_exact_artifact_binding():
    assert artifact_binding_summary(
        {
            "current_replay_hash": "e" * 64,
            "input_hash": "E" * 64,
        }
    )["matches_current"] is True
