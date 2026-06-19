import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base

class Project(Base):
    __tablename__ = "projects"
    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_id    = Column(String, ForeignKey("users.id"), nullable=False)
    name        = Column(String, nullable=False)
    subject     = Column(String, nullable=True)
    description = Column(Text,   nullable=True)
    semester    = Column(String, nullable=True)
    department  = Column(String, nullable=True)
    institution = Column(String, nullable=True)
    is_archived = Column(Boolean, default=False)
    created_at  = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at  = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                         onupdate=lambda: datetime.now(timezone.utc))

    owner            = relationship("User",           back_populates="projects")
    documents        = relationship("Document",       back_populates="project", cascade="all, delete-orphan")
    papers           = relationship("Paper",          back_populates="project", cascade="all, delete-orphan")
    templates        = relationship("Template",       back_populates="project", cascade="all, delete-orphan")
    course_outcomes  = relationship("CourseOutcome",  back_populates="project", cascade="all, delete-orphan")
