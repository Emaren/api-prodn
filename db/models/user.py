from sqlalchemy import Column, Integer, String, Boolean, DateTime, func
from db.base import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    uid = Column(String(100), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=True)
    in_game_name = Column(String, unique=True, nullable=True)
    verified = Column(Boolean, default=False, nullable=False)
    wallet_address = Column(String(100), nullable=True)
    lock_name = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    token = Column(String(128), nullable=True)
    last_seen = Column(DateTime, nullable=True)
    is_admin = Column(Boolean, default=False, nullable=False)

    # NEW
    steam_id = Column(String(32), nullable=True)
    steam_persona_name = Column(String(255), nullable=True)
    verification_level = Column(Integer, default=0, nullable=False)
    verification_method = Column(String(32), default="none", nullable=False)
    verified_at = Column(DateTime, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "uid": self.uid,
            "email": self.email,
            "in_game_name": self.in_game_name,
            "verified": self.verified,
            "wallet_address": self.wallet_address,
            "lock_name": self.lock_name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "token": self.token,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "is_admin": self.is_admin,

            "steam_id": self.steam_id,
            "steam_persona_name": self.steam_persona_name,
            "verification_level": self.verification_level,
            "verification_method": self.verification_method,
            "verified_at": self.verified_at.isoformat() if self.verified_at else None,
        }