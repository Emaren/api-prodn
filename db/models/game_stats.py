from datetime import datetime
import os, json
from pprint import pformat
from logging import getLogger
from sqlalchemy import (
    Column, String, Boolean, Integer, DateTime,
    UniqueConstraint, Index, ForeignKey
)
from sqlalchemy.dialects.postgresql import JSONB
from db.base import Base
from utils.extract_datetime import extract_datetime_from_filename

def is_render():
    return os.getenv("RENDER") == "1"

class GameStats(Base):
    __tablename__ = "game_stats"

    id = Column(Integer, primary_key=True)
    user_uid = Column(String(100), ForeignKey("users.uid"), nullable=True, index=True)
    replay_file = Column(String(500), nullable=False)
    replay_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    game_version = Column(String(50))
    map = Column(JSONB)
    game_type = Column(String(50))
    duration = Column(Integer)
    game_duration = Column(Integer)
    winner = Column(String(100))
    players = Column(JSONB)
    event_types = Column(JSONB)
    key_events = Column(JSONB)
    timestamp = Column(DateTime, default=datetime.utcnow)
    played_on = Column(DateTime, nullable=True)
    parse_iteration = Column(Integer, default=0, nullable=False)
    is_final = Column(Boolean, default=False, nullable=False)
    disconnect_detected = Column(Boolean, default=False, nullable=False)
    parse_source = Column(String(20), default="unknown", nullable=False)
    parse_reason = Column(String(50), default="unspecified", nullable=False)
    original_filename = Column(String(255), nullable=True)

    __table_args__ = (
        Index("ix_replay_iteration", "replay_file", "parse_iteration"),
        Index("ix_replay_hash_iteration", "replay_hash", "parse_iteration"),
        UniqueConstraint("replay_hash", "is_final", name="uq_replay_final"),
    )

    def __repr__(self):
        return f"<GameStats {self.replay_hash} - Final: {self.is_final}>"

    def _filename_played_on(self):
        for value in (self.original_filename, self.replay_file):
            if not value:
                continue
            parsed = extract_datetime_from_filename(str(value))
            if parsed is not None:
                return parsed
        return None

    def public_played_at(self):
        return (
            self.played_on
            or self._filename_played_on()
            or self.created_at
            or self.timestamp
        )

    def to_dict(self):
        logger = getLogger(__name__)
        trace_enabled = os.getenv("ENABLE_TRACE_LOGS", "true").lower() == "true"
        derived_played_on = self._filename_played_on()
        played_at = self.played_on or derived_played_on or self.created_at or self.timestamp

        try:
            map_data = json.loads(self.map) if isinstance(self.map, str) else self.map
        except Exception:
            map_data = {"name": "Unknown", "size": "Unknown"}
        if not isinstance(map_data, dict):
            map_data = {"name": "Unknown", "size": "Unknown"}

        try:
            players = json.loads(self.players) if isinstance(self.players, str) else self.players
        except Exception:
            players = []
        if not isinstance(players, list):
            players = []

        try:
            event_types = (
                json.loads(self.event_types)
                if isinstance(self.event_types, str)
                else (self.event_types or [])
            )
        except Exception:
            event_types = []
        if not isinstance(event_types, list):
            event_types = []

        try:
            key_events = (
                json.loads(self.key_events)
                if isinstance(self.key_events, str)
                else (self.key_events or {})
            )
        except Exception:
            key_events = {}
        if not isinstance(key_events, dict):
            key_events = {}

        resigns = len([e for e in event_types if e == "resign"])
        anomalies = any(k for k in key_events if "anomaly" in str(k).lower())

        trace_block = (
            "\n📊 Final Game Parsed\n"
            f"📁 File: {self.replay_file}\n"
            f"📎 Original: {self.original_filename or 'N/A'}\n"
            f"🏷️  Hash: {self.replay_hash}\n"
            f"⚙️  Source: {self.parse_source or 'unknown'}\n"
            f"🧪 Reason: {self.parse_reason or 'unspecified'}\n"
            f"⏱️  Duration: {self.duration} sec\n"
            f"🗺️  Map: {map_data.get('name', 'Unknown')} ({map_data.get('size', 'Unknown')})\n"
            f"🏆 Winner: {self.winner or 'Unknown'}\n"
            f"🧩 Iteration: {self.parse_iteration}\n"
            f"🚪 Resigns: {resigns}\n"
            f"🚨 Anomalies: {'Yes' if anomalies else 'No'}\n"
            f"❌ Disconnect: {'Yes' if self.disconnect_detected else 'No'}\n"
            f"👥 Players:\n{pformat(players)}"
        )

        if trace_enabled and not is_render():
            logger.debug(trace_block)
            try:
                trace_path = self.replay_file + ".trace"
                with open(trace_path, "w") as f:
                    f.write(trace_block + "\n")
                with open("trace.index", "a") as idx:
                    idx.write(f"{datetime.utcnow().isoformat()} - {self.replay_file}\n")
            except Exception as e:
                logger.warning(f"❌ Failed to write trace files: {e}")

        return {
            "id": self.id,
            "user_uid": self.user_uid,
            "replay_file": self.replay_file,
            "replay_hash": self.replay_hash,
            "game_version": self.game_version,
            "map": map_data,
            "game_type": self.game_type,
            "duration": self.duration,
            "game_duration": self.game_duration,
            "winner": self.winner,
            "players": players,
            "event_types": event_types,
            "key_events": key_events,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "derived_played_on": derived_played_on.isoformat() if derived_played_on else None,
            "played_on": self.played_on.isoformat() if self.played_on else None,
            "played_at": played_at.isoformat() if played_at else None,
            "parse_iteration": self.parse_iteration,
            "is_final": self.is_final,
            "disconnect_detected": self.disconnect_detected,
            "parse_source": self.parse_source,
            "parse_reason": self.parse_reason,
            "original_filename": self.original_filename,
        }
