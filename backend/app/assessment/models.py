import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, Enum, ForeignKey, JSON
from sqlalchemy.orm import relationship
from enum import Enum as PyEnum
from app.core.database import Base

class BloomLevel(str, PyEnum):
    remember   = "remember"
    understand = "understand"
    apply      = "apply"
    analyze    = "analyze"
    evaluate   = "evaluate"
    create     = "create"

class Difficulty(str, PyEnum):
    easy   = "easy"
    medium = "medium"
    hard   = "hard"

class QuestionType(str, PyEnum):
    mcq        = "mcq"
    one_word   = "one_word"
    fill_blank = "fill_blank"
    short      = "short"
    long       = "long"
    case_study = "case_study"

class PaperStatus(str, PyEnum):
    draft      = "draft"
    generating = "generating"
    ready      = "ready"
    failed     = "failed"
    exported   = "exported"

class CourseOutcome(Base):
    __tablename__ = "course_outcomes"
    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id  = Column(String, ForeignKey("projects.id"), nullable=False)
    code        = Column(String, nullable=False)
    description = Column(Text,   nullable=False)
    bloom_level = Column(Enum(BloomLevel), nullable=True)
    created_at  = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    project = relationship("Project", back_populates="course_outcomes")

class Template(Base):
    __tablename__ = "templates"
    id             = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id     = Column(String, ForeignKey("projects.id"), nullable=True)
    name           = Column(String, nullable=False)
    institution    = Column(String, nullable=True)
    header_config  = Column(JSON, default=dict)
    section_config = Column(JSON, default=dict)
    marks_layout   = Column(JSON, default=dict)
    is_default     = Column(Boolean, default=False)
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    project = relationship("Project", back_populates="templates")

class Paper(Base):
    __tablename__ = "papers"
    id                   = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id           = Column(String, ForeignKey("projects.id"), nullable=False)
    title                = Column(String, nullable=False)
    status               = Column(Enum(PaperStatus), default=PaperStatus.draft)
    blueprint            = Column(JSON, default=dict)
    total_marks          = Column(Integer, default=100)
    duration_mins        = Column(Integer, default=180)
    template_id          = Column(String, ForeignKey("templates.id"), nullable=True)
    error_message        = Column(Text, nullable=True)
    pdf_path             = Column(String, nullable=True)
    docx_path            = Column(String, nullable=True)
    answer_key_pdf_path  = Column(String, nullable=True)
    answer_key_docx_path = Column(String, nullable=True)
    notes                = Column(Text, nullable=True)
    task_id              = Column(String, nullable=True)
    created_at           = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at           = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                                  onupdate=lambda: datetime.now(timezone.utc))

    project   = relationship("Project",  back_populates="papers")
    questions = relationship("Question", back_populates="paper", cascade="all, delete-orphan",
                             order_by="Question.order_index")

class Question(Base):
    __tablename__ = "questions"
    id             = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    paper_id       = Column(String, ForeignKey("papers.id"), nullable=False)
    order_index    = Column(Integer, default=0)
    question_text  = Column(Text,    nullable=False)
    answer_text    = Column(Text,    nullable=True)
    question_type  = Column(Enum(QuestionType), nullable=False)
    bloom_level    = Column(Enum(BloomLevel),   nullable=True)
    difficulty     = Column(Enum(Difficulty),   nullable=True)
    marks          = Column(Integer, default=2)
    topic          = Column(String,  nullable=True)
    subtopic       = Column(String,  nullable=True)
    co_id          = Column(String,  ForeignKey("course_outcomes.id"), nullable=True)
    options        = Column(JSON,    nullable=True)
    correct_option = Column(Integer, nullable=True)
    rubric         = Column(JSON,    nullable=True)
    quality_scores = Column(JSON,    nullable=True)
    is_accepted    = Column(Boolean, default=True)
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    paper = relationship("Paper", back_populates="questions")
    course_outcome = relationship("CourseOutcome")
