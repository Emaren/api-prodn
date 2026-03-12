from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String

from db.base import Base


class ReplayParseAttempt(Base):
    __tablename__ = "replay_parse_attempts"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    user_uid = Column(String(100), nullable=True)
    replay_hash = Column(String(64), nullable=True)
    original_filename = Column(String(255), nullable=True)
    parse_source = Column(String(20), default="file_upload", nullable=False)
    status = Column(String(32), default="received", nullable=False)
    detail = Column(String(255), nullable=True)
    upload_mode = Column(String(20), nullable=True)
    file_size_bytes = Column(Integer, nullable=True)
    game_stats_id = Column(Integer, ForeignKey("game_stats.id"), nullable=True)
    played_on = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_replay_parse_attempts_created_at", "created_at"),
        Index("ix_replay_parse_attempts_status_created_at", "status", "created_at"),
        Index("ix_replay_parse_attempts_user_uid_created_at", "user_uid", "created_at"),
        Index("ix_replay_parse_attempts_replay_hash", "replay_hash"),
        Index("ix_replay_parse_attempts_game_stats_id", "game_stats_id"),
    )

    def __repr__(self):
        return f"<ReplayParseAttempt {self.id} {self.status} {self.original_filename or self.replay_hash}>"

    def to_dict(self):
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "user_uid": self.user_uid,
            "replay_hash": self.replay_hash,
            "original_filename": self.original_filename,
            "parse_source": self.parse_source,
            "status": self.status,
            "detail": self.detail,
            "upload_mode": self.upload_mode,
            "file_size_bytes": self.file_size_bytes,
            "game_stats_id": self.game_stats_id,
            "played_on": self.played_on.isoformat() if self.played_on else None,
        }
