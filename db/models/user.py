# db/models/user.py

from datetime import datetime
from sqlalchemy import Column, String, Boolean, Integer, DateTime
from .base import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    uid = Column(String(100), unique=True, nullable=False)
    email = Column(String(100), unique=True, index=True)
    in_game_name = Column(String, nullable=True, unique=True)
    verified = Column(Boolean, default=False)
    wallet_address = Column(String(100), nullable=True)
    lock_name = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    token = Column(String(128), nullable=True)
    last_seen = Column(DateTime, default=None)
    is_admin = Column(Boolean, default=False)  # ✅ required for admin access

    def __repr__(self):
        return f"<User {self.uid}>"

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
        }
