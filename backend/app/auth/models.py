import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Boolean, DateTime, Enum, ForeignKey
from sqlalchemy.orm import relationship
from enum import Enum as PyEnum
from app.core.database import Base

class UserRole(str, PyEnum):
    teacher = "teacher"
    hod     = "hod"
    admin   = "admin"

class User(Base):
    __tablename__ = "users"
    id               = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email            = Column(String, unique=True, index=True, nullable=False)
    name             = Column(String, nullable=False)
    hashed_password  = Column(String, nullable=False)
    role             = Column(Enum(UserRole), default=UserRole.teacher)
    institution      = Column(String, nullable=True)
    is_active        = Column(Boolean, default=True)
    created_at       = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    projects       = relationship("Project",      back_populates="owner",  cascade="all, delete-orphan")
    refresh_tokens = relationship("RefreshToken", back_populates="user",   cascade="all, delete-orphan")

class RefreshToken(Base):
    __tablename__ = "refresh_tokens"
    id         = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    token      = Column(String, unique=True, index=True)
    user_id    = Column(String, ForeignKey("users.id"), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    revoked    = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="refresh_tokens")
