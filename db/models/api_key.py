from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, func
from db.base import Base

class ApiKey(Base):
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    kind = Column(String(20), nullable=False, default="watcher")
    key_prefix = Column(String(12), nullable=False, unique=True)
    key_hash = Column(String(255), nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_used_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)