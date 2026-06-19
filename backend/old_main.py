"""
TestBoost.ai — Monolithic FastAPI Backend
=========================================
Phase 2: Celery task queue, SSE progress, real document processing
Sections:
  1.  Imports & Config
  2.  Database Models
  3.  Pydantic Schemas
  4.  Database Setup
  5.  Auth Utilities
  6.  Celery Setup & Tasks
  7.  Document Processing (PyMuPDF, pdfplumber, Tesseract)
  8.  Auth Routes         /auth/*
  9.  Project Routes      /projects/*
  10. Document Routes     /documents/*
  11. SSE Progress Route  /stream/*
  12. Paper Routes        /papers/*
  13. Question Routes     /questions/*
  14. Template Routes     /templates/*
  15. Analytics Routes    /analytics/*
  16. Health Check
"""

# ─────────────────────────────────────────────────────────────
# 1. IMPORTS & CONFIG
# ─────────────────────────────────────────────────────────────
import os
import uuid
import json
import asyncio
import logging
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Optional, List, AsyncGenerator
from enum import Enum as PyEnum
from pathlib import Path

from dotenv import load_dotenv
_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_env_path, override=True)

from fastapi import (
    FastAPI, Depends, HTTPException, status,
    UploadFile, File, Form, BackgroundTasks, Request
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from sqlalchemy import (
    create_engine, Column, String, Integer,
    Boolean, DateTime, Text, ForeignKey, JSON, Enum
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship

from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────
DATABASE_URL  = os.getenv("DATABASE_URL",  "postgresql://postgres:1234@localhost:5432/questionpaper")
SECRET_KEY    = os.getenv("SECRET_KEY",    "change-this-secret-in-production")
ALGORITHM     = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
REFRESH_TOKEN_EXPIRE_DAYS   = 30
UPLOAD_DIR    = Path(os.getenv("UPLOAD_DIR", "./uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379/0")

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".pptx", ".txt", ".png", ".jpg", ".jpeg"}
MAX_FILE_SIZE_MB   = 50


# ─────────────────────────────────────────────────────────────
# 2. DATABASE MODELS
# ─────────────────────────────────────────────────────────────
Base = declarative_base()


class UserRole(str, PyEnum):
    teacher = "teacher"
    hod     = "hod"
    admin   = "admin"


class DocumentStatus(str, PyEnum):
    pending    = "pending"
    processing = "processing"
    ready      = "ready"
    failed     = "failed"


class DocumentType(str, PyEnum):
    textbook       = "textbook"
    notes          = "notes"
    ppt            = "ppt"
    previous_paper = "previous_paper"
    syllabus       = "syllabus"
    answer_key     = "answer_key"
    other          = "other"


class PaperStatus(str, PyEnum):
    draft      = "draft"
    generating = "generating"
    ready      = "ready"
    failed     = "failed"
    exported   = "exported"


class QuestionType(str, PyEnum):
    mcq        = "mcq"
    one_word   = "one_word"
    fill_blank = "fill_blank"
    short      = "short"
    long       = "long"
    case_study = "case_study"


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


class TaskStatus(str, PyEnum):
    queued     = "queued"
    running    = "running"
    done       = "done"
    failed     = "failed"


# ── ORM Models ─────────────────────────────────────────────────

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
    question_bank    = relationship("QuestionBankItem", back_populates="project", cascade="all, delete-orphan")


class Document(Base):
    __tablename__ = "documents"
    id            = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id    = Column(String, ForeignKey("projects.id"), nullable=False)
    filename      = Column(String, nullable=False)
    original_name = Column(String, nullable=False)
    file_path     = Column(String, nullable=False)
    file_size     = Column(Integer, nullable=True)
    doc_type      = Column(Enum(DocumentType), default=DocumentType.other)
    status        = Column(Enum(DocumentStatus), default=DocumentStatus.pending)
    error_message = Column(Text, nullable=True)
    chunk_count   = Column(Integer, default=0)
    page_count    = Column(Integer, default=0)
    extracted_text = Column(Text, nullable=True)
    metadata_     = Column("metadata", JSON, default=dict)
    task_id       = Column(String, nullable=True)   # Celery task id
    uploaded_at   = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    processed_at  = Column(DateTime, nullable=True)

    project = relationship("Project", back_populates="documents")
    curriculum_nodes = relationship("CurriculumNode", cascade="all, delete-orphan")
    document_chunks = relationship("DocumentChunk", cascade="all, delete-orphan")
    pages           = relationship("DocumentPage", cascade="all, delete-orphan", back_populates="document")

class DocumentPage(Base):
    __tablename__ = "document_pages"
    id            = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    document_id   = Column(String, ForeignKey("documents.id"), nullable=False)
    physical_page = Column(Integer, nullable=False)
    text          = Column(Text, nullable=False)

    document = relationship("Document", back_populates="pages")


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
    source_doc_ids = Column(JSON,    default=list)
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    paper = relationship("Paper", back_populates="questions")


class QuestionBankItem(Base):
    __tablename__ = "question_bank"
    id            = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id    = Column(String, ForeignKey("projects.id"), nullable=False)
    question_text = Column(Text,   nullable=False)
    answer_text   = Column(Text,   nullable=True)
    question_type = Column(Enum(QuestionType), nullable=False)
    bloom_level   = Column(Enum(BloomLevel),   nullable=True)
    difficulty    = Column(Enum(Difficulty),   nullable=True)
    marks         = Column(Integer, default=2)
    topic         = Column(String,  nullable=True)
    times_used    = Column(Integer, default=1)
    feedback      = Column(String,  nullable=True)
    added_at      = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    project = relationship("Project", back_populates="question_bank")


# Task progress log — stores SSE events per task_id
class TaskProgress(Base):
    __tablename__ = "task_progress"
    id         = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    task_id    = Column(String, index=True, nullable=False)
    step       = Column(String, nullable=False)
    message    = Column(String, nullable=False)
    status     = Column(Enum(TaskStatus), default=TaskStatus.running)
    detail     = Column(JSON,  nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─────────────────────────────────────────────────────────────
# 3. PYDANTIC SCHEMAS
# ─────────────────────────────────────────────────────────────

# ── Auth ───────────────────────────────────────────────────────
class RegisterRequest(BaseModel):
    email: EmailStr
    name: str = Field(min_length=2)
    password: str = Field(min_length=8)
    institution: Optional[str] = None

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user_id: str
    name: str
    email: str
    role: str

class RefreshRequest(BaseModel):
    refresh_token: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class UserOut(BaseModel):
    id: str
    email: str
    name: str
    role: str
    institution: Optional[str]
    created_at: datetime
    class Config: from_attributes = True

# ── Projects ───────────────────────────────────────────────────
class ProjectCreate(BaseModel):
    name: str = Field(min_length=2)
    subject: Optional[str] = None
    description: Optional[str] = None
    semester: Optional[str] = None
    department: Optional[str] = None
    institution: Optional[str] = None

class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    subject: Optional[str] = None
    description: Optional[str] = None
    semester: Optional[str] = None
    department: Optional[str] = None

class ProjectOut(BaseModel):
    id: str
    name: str
    subject: Optional[str]
    description: Optional[str]
    semester: Optional[str]
    department: Optional[str]
    institution: Optional[str]
    is_archived: bool
    created_at: datetime
    updated_at: datetime
    document_count: int = 0
    paper_count: int = 0
    class Config: from_attributes = True

# ── Documents ─────────────────────────────────────────────────
class DocumentOut(BaseModel):
    id: str
    filename: str
    original_name: str
    doc_type: str
    status: str
    file_size: Optional[int]
    chunk_count: int
    page_count: int
    error_message: Optional[str]
    task_id: Optional[str]
    uploaded_at: datetime
    processed_at: Optional[datetime]
    class Config: from_attributes = True

# ── Course Outcomes ───────────────────────────────────────────
class COCreate(BaseModel):
    code: str
    description: str
    bloom_level: Optional[BloomLevel] = None

class COOut(BaseModel):
    id: str
    code: str
    description: str
    bloom_level: Optional[str]
    created_at: datetime
    class Config: from_attributes = True

# ── Templates ─────────────────────────────────────────────────
class TemplateCreate(BaseModel):
    name: str
    institution: Optional[str] = None
    header_config: dict = {}
    section_config: dict = {}
    marks_layout: dict = {}

class TemplateOut(BaseModel):
    id: str
    name: str
    institution: Optional[str]
    header_config: dict
    section_config: dict
    marks_layout: dict
    is_default: bool
    created_at: datetime
    class Config: from_attributes = True

# ── Blueprint ─────────────────────────────────────────────────
class QuestionTypeSpec(BaseModel):
    type: QuestionType
    count: int
    marks_each: int

class BloomTarget(BaseModel):
    remember: int   = 0
    understand: int = 0
    apply: int      = 0
    analyze: int    = 0
    evaluate: int   = 0
    create: int     = 0

class DifficultyTarget(BaseModel):
    easy: int   = 30
    medium: int = 50
    hard: int   = 20

class Blueprint(BaseModel):
    question_specs: List[QuestionTypeSpec]
    bloom_targets: BloomTarget
    difficulty_targets: DifficultyTarget
    topics: List[str] = []
    total_marks: int = 100
    duration_mins: int = 180

# ── Papers ────────────────────────────────────────────────────
class PaperCreate(BaseModel):
    title: str
    blueprint: Blueprint
    template_id: Optional[str] = None
    notes: Optional[str] = None

class PaperOut(BaseModel):
    id: str
    title: str
    status: str
    total_marks: int
    duration_mins: int
    blueprint: dict
    template_id: Optional[str]
    notes: Optional[str]
    pdf_path: Optional[str]
    docx_path: Optional[str]
    error_message: Optional[str]
    task_id: Optional[str]
    created_at: datetime
    updated_at: datetime
    question_count: int = 0
    class Config: from_attributes = True

# ── Questions ─────────────────────────────────────────────────
class QuestionCreate(BaseModel):
    question_text: str
    answer_text: Optional[str] = None
    question_type: QuestionType
    bloom_level: Optional[BloomLevel] = None
    difficulty: Optional[Difficulty] = None
    marks: int = 2
    topic: Optional[str] = None
    options: Optional[List[str]] = None
    correct_option: Optional[int] = None
    rubric: Optional[dict] = None
    co_id: Optional[str] = None

class QuestionUpdate(BaseModel):
    question_text: Optional[str] = None
    answer_text: Optional[str] = None
    bloom_level: Optional[BloomLevel] = None
    difficulty: Optional[Difficulty] = None
    marks: Optional[int] = None
    topic: Optional[str] = None
    is_accepted: Optional[bool] = None
    order_index: Optional[int] = None

class QuestionOut(BaseModel):
    id: str
    paper_id: str
    order_index: int
    question_text: str
    answer_text: Optional[str]
    question_type: str
    bloom_level: Optional[str]
    difficulty: Optional[str]
    marks: int
    topic: Optional[str]
    subtopic: Optional[str]
    co_id: Optional[str]
    options: Optional[List[str]]
    correct_option: Optional[int]
    rubric: Optional[dict]
    quality_scores: Optional[dict]
    is_accepted: bool
    created_at: datetime
    class Config: from_attributes = True

# ── Analytics ─────────────────────────────────────────────────
class AnalyticsSummary(BaseModel):
    total_papers: int
    total_questions: int
    bloom_distribution: dict
    difficulty_distribution: dict
    topic_distribution: dict
    question_type_distribution: dict

# ── Task Progress ─────────────────────────────────────────────
class ProgressEvent(BaseModel):
    task_id: str
    step: str
    message: str
    status: str
    detail: Optional[dict] = None
    timestamp: str


# ─────────────────────────────────────────────────────────────
# 4. DATABASE SETUP
# ─────────────────────────────────────────────────────────────
print(f"--- WORKER BOOTING: Connected to DB: {DATABASE_URL} ---")
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# 5. AUTH UTILITIES
# ─────────────────────────────────────────────────────────────
import bcrypt

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


def hash_password(password: str) -> str:
    pwd_bytes = password.encode("utf-8")
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode("utf-8")

def verify_password(plain: str, hashed: str) -> bool:
    pwd_bytes = plain.encode("utf-8")
    hashed_bytes = hashed.encode("utf-8")
    return bcrypt.checkpw(pwd_bytes, hashed_bytes)

def create_access_token(user_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    return jwt.encode({"sub": user_id, "exp": expire, "type": "access"}, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(db: Session, user_id: str) -> str:
    token   = str(uuid.uuid4())
    expires = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    db.add(RefreshToken(token=token, user_id=user_id, expires_at=expires))
    db.commit()
    return token

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if not user_id or payload.get("type") != "access":
            raise exc
    except JWTError:
        raise exc
    user = db.query(User).filter(User.id == user_id, User.is_active == True).first()
    if not user:
        raise exc
    return user

def get_project_or_404(project_id: str, user: User, db: Session) -> Project:
    project = db.query(Project).filter(
        Project.id == project_id,
        Project.owner_id == user.id,
    ).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


# ─────────────────────────────────────────────────────────────
# 6. CELERY SETUP
# ─────────────────────────────────────────────────────────────
# Celery is optional — if Redis isn't running, tasks run as
# plain background threads (good enough for local dev).
celery_app = None
CELERY_AVAILABLE = False

try:
    from celery import Celery
    celery_app = Celery(
        "questionpaper",
        broker=REDIS_URL,
        backend=os.getenv("CELERY_RESULT_BACKEND", REDIS_URL.replace("/0", "/1")),
    )
    celery_app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone="UTC",
        task_routes={
            "main.task_process_document": {"queue": "low_priority"},
            "main.task_generate_paper":   {"queue": "normal"},
        },
        task_track_started=True,
    )
    CELERY_AVAILABLE = True
    logger.info("Celery connected to Redis")
except Exception as e:
    logger.warning(f"Celery/Redis not available — using background threads: {e}")


# ─────────────────────────────────────────────────────────────
# 7. PROGRESS HELPERS
# ─────────────────────────────────────────────────────────────

def push_progress(task_id: str, step: str, message: str,
                  status: str = "running", detail: dict = None):
    """Write a progress event to the DB so SSE can stream it."""
    db = SessionLocal()
    try:
        db.add(TaskProgress(
            task_id=task_id,
            step=step,
            message=message,
            status=status,
            detail=detail or {},
        ))
        db.commit()
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# 8. DOCUMENT PROCESSING
# ─────────────────────────────────────────────────────────────

def _should_skip_page(text: str, subject: str = "") -> bool:
    if not text:
        return False
    text_lower = text.lower()
    
    banned = [
        "copyright", "isbn", "cover design", "published by", 
        "all rights reserved", "printing history", "first edition", 
        "reprinted", "publication division", "printed at",
        "chief editor", "production assistant", "textbook development team",
        "about this book", "how to use this textbook", "about education"
    ]
    if any(kw in text_lower for kw in banned):
        return True

    # Detect headings that invalidate the page
    import re
    if re.search(r'(?mi)^\s*(FOREWORD|PREFACE|ACKNOWLEDGEMENT|ACKNOWLEDGEMENTS|INDEX|APPENDIX|GLOSSARY|BIBLIOGRAPHY)\s*$', text):
        return True

    # National education pages rules
    subject_lower = subject.lower() if subject else ""
    is_civics = any(s in subject_lower for s in ["political science", "civics", "law", "constitutional"])
    if not is_civics:
        national_keywords = [
            "national anthem", "fundamental duties", 
            "constitution of india", "preamble"
        ]
        if any(nk in text_lower for nk in national_keywords):
            return True
            
    return False


def _extract_pdf(file_path: Path, subject: str = "") -> dict:
    """Extract text and tables from PDF using PyMuPDF + pdfplumber, filtering metadata pages."""
    pages_data = []
    page_count = 0
    tables = []

    # PyMuPDF — fast text extraction
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(file_path))
        page_count = len(doc)
        for i, page in enumerate(doc):
            text = page.get_text("text")
            if text.strip():
                if not _should_skip_page(text, subject):
                    pages_data.append({"page": i + 1, "text": text})
        doc.close()
    except ImportError:
        logger.warning("PyMuPDF not installed — skipping text extraction")
    except Exception as e:
        logger.warning(f"PyMuPDF error: {e}")

    # pdfplumber — table extraction
    try:
        import pdfplumber
        with pdfplumber.open(str(file_path)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if _should_skip_page(page_text, subject):
                    continue
                page_tables = page.extract_tables()
                for t in page_tables:
                    if t:
                        tables.append(t)
    except ImportError:
        logger.warning("pdfplumber not installed — skipping table extraction")
    except Exception as e:
        logger.warning(f"pdfplumber error: {e}")

    full_text = "\n\n".join([p["text"] for p in pages_data])

    # If no text found — likely scanned PDF, run OCR
    if not full_text.strip() and page_count > 0:
        pages_data = _ocr_pdf(file_path, subject)
        full_text = "\n\n".join([p["text"] for p in pages_data])

    return {
        "text": full_text,
        "pages": pages_data,
        "page_count": len(pages_data) or page_count,
        "table_count": len(tables),
        "tables": tables[:5],  # store first 5 tables as sample
    }


def _ocr_pdf(file_path: Path, subject: str = "") -> list:
    """OCR a scanned PDF using Tesseract via pytesseract, filtering metadata pages."""
    try:
        import fitz
        import pytesseract
        from PIL import Image
        import io

        doc = fitz.open(str(file_path))
        pages_data = []
        for i, page in enumerate(doc):
            pix = page.get_pixmap(dpi=200)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            text = pytesseract.image_to_string(img)
            if text.strip():
                if not _should_skip_page(text, subject):
                    pages_data.append({"page": i + 1, "text": text})
        doc.close()
        return pages_data
    except Exception as e:
        logger.warning(f"OCR failed: {e}")
        return []


def _extract_docx(file_path: Path, subject: str = "") -> dict:
    """Extract text from DOCX using python-docx, filtering metadata pages."""
    try:
        from docx import Document as DocxDocument
        doc = DocxDocument(str(file_path))
        
        pages = []
        current_page_paragraphs = []
        
        for p in doc.paragraphs:
            has_page_break = False
            for run in p.runs:
                run_xml = run._r.xml
                if "w:br" in run_xml and 'type="page"' in run_xml:
                    has_page_break = True
                    break
                if "w:lastRenderedPageBreak" in run_xml:
                    has_page_break = True
                    break
            
            if has_page_break and current_page_paragraphs:
                pages.append("\n\n".join(current_page_paragraphs))
                current_page_paragraphs = []
                
            if p.text.strip():
                current_page_paragraphs.append(p.text.strip())
                
        if current_page_paragraphs:
            pages.append("\n\n".join(current_page_paragraphs))
            
        filtered_pages = []
        for i, page_text in enumerate(pages):
            if not _should_skip_page(page_text, subject):
                filtered_pages.append({"page": i + 1, "text": page_text})
                
        table_text_parts = []
        for table in doc.tables:
            cells_text = []
            for row in table.rows:
                for cell in row.cells:
                    if cell.text.strip():
                        cells_text.append(cell.text.strip())
            table_content = "\n\n".join(cells_text)
            if not _should_skip_page(table_content, subject):
                table_text_parts.append(table_content)
                
        combined_text = "\n\n".join([p["text"] for p in filtered_pages] + table_text_parts)
        
        return {
            "text": combined_text,
            "pages": filtered_pages,
            "page_count": len(filtered_pages) or 1,
            "table_count": len(doc.tables)
        }
    except ImportError:
        return {"text": "", "pages": [], "page_count": 0, "table_count": 0, "error": "python-docx not installed"}
    except Exception as e:
        return {"text": "", "pages": [], "page_count": 0, "table_count": 0, "error": str(e)}


def _extract_pptx(file_path: Path, subject: str = "") -> dict:
    """Extract text from PPTX slide by slide, filtering slides with metadata."""
    try:
        from pptx import Presentation
        prs = Presentation(str(file_path))
        pages_data = []
        slides_text = []
        for i, slide in enumerate(prs.slides):
            slide_parts = []
            for shape in slide.shapes:
                if hasattr(shape, "text") and shape.text.strip():
                    slide_parts.append(shape.text.strip())
            if slide_parts:
                slide_content = "\n".join(slide_parts)
                if not _should_skip_page(slide_content, subject):
                    slide_str = f"[Slide {i+1}]\n" + slide_content
                    slides_text.append(slide_str)
                    pages_data.append({"page": i + 1, "text": slide_str})
        return {
            "text": "\n\n".join(slides_text),
            "pages": pages_data,
            "page_count": len(pages_data),
            "table_count": 0,
        }
    except ImportError:
        return {"text": "", "pages": [], "page_count": 0, "table_count": 0, "error": "python-pptx not installed"}
    except Exception as e:
        return {"text": "", "pages": [], "page_count": 0, "table_count": 0, "error": str(e)}


def _extract_txt(file_path: Path, subject: str = "") -> dict:
    """Extract text from TXT, splitting by page breaks and filtering metadata."""
    text = file_path.read_text(errors="ignore")
    pages = text.split("\x0c")
    filtered_pages = [page for page in pages if not _should_skip_page(page, subject)]
    return {
        "text": "\n\n".join(filtered_pages),
        "page_count": len(filtered_pages) or 1,
        "table_count": 0
    }


def _chunk_text(text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
    """
    Simple semantic chunking by paragraph, targeting ~chunk_size words per chunk.
    Phase 4 will replace this with proper sentence-transformer-based chunking.
    """
    if not text:
        return []
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks, current, word_count = [], [], 0
    for para in paragraphs:
        words = len(para.split())
        if word_count + words > chunk_size and current:
            chunks.append("\n\n".join(current))
            # Keep last paragraph as overlap
            current = current[-1:] if overlap > 0 else []
            word_count = len(current[0].split()) if current else 0
        current.append(para)
        word_count += words
    if current:
        chunks.append("\n\n".join(current))
    return chunks


def process_document_sync(document_id: str, file_path: str, task_id: str):
    """
    Synchronous document processing — runs in background thread or Celery worker.
    Phase 4 will add: embedding generation → Qdrant storage, curriculum extraction.
    """
    logger.info(f"[process_document_sync] START document_id={document_id} file_path={file_path} task_id={task_id}")
    logger.info(f"[process_document_sync] Using DATABASE_URL={DATABASE_URL[:50]}...")
    db = SessionLocal()
    try:
        doc = db.query(Document).filter(Document.id == document_id).first()
        if not doc:
            logger.error(f"[process_document_sync] Document NOT FOUND in DB: {document_id}. "
                         f"This usually means the Celery worker is connected to a different database than the API server. "
                         f"Check that both processes use the same DATABASE_URL.")
            push_progress(task_id, "error", f"Document {document_id} not found in database", "failed")
            return

        doc.status = DocumentStatus.processing
        doc.task_id = task_id
        db.commit()

        push_progress(task_id, "extract", "Reading document...", "running")

        path   = Path(file_path)
        if not path.exists():
            logger.error(f"[process_document_sync] File NOT FOUND: {file_path}")
            push_progress(task_id, "error", f"File not found at {file_path}", "failed")
            doc.status = DocumentStatus.failed
            doc.error_message = f"File not found: {file_path}"
            db.commit()
            return
        logger.info(f"[process_document_sync] File found: {file_path} ({path.stat().st_size} bytes)")
        suffix = path.suffix.lower()

        subject = doc.project.subject if doc.project else ""

        # ── Extract based on file type ──────────────────────
        if suffix == ".pdf":
            push_progress(task_id, "extract", "Extracting text from PDF...", "running")
            result = _extract_pdf(path, subject)
        elif suffix == ".docx":
            push_progress(task_id, "extract", "Extracting text from Word document...", "running")
            result = _extract_docx(path, subject)
        elif suffix in (".pptx", ".ppt"):
            push_progress(task_id, "extract", "Extracting slides from PowerPoint...", "running")
            result = _extract_pptx(path, subject)
        elif suffix == ".txt":
            push_progress(task_id, "extract", "Reading text file...", "running")
            result = _extract_txt(path, subject)
        elif suffix in (".png", ".jpg", ".jpeg"):
            push_progress(task_id, "ocr", "Running OCR on image...", "running")
            try:
                import pytesseract
                from PIL import Image
                img  = Image.open(str(path))
                text = pytesseract.image_to_string(img)
                if _should_skip_page(text, subject):
                    result = {"text": "", "page_count": 0, "table_count": 0}
                else:
                    result = {"text": text, "page_count": 1, "table_count": 0}
            except Exception as e:
                result = {"text": "", "page_count": 0, "table_count": 0, "error": str(e)}
        else:
            result = {"text": "", "page_count": 0, "table_count": 0, "error": "Unsupported file type"}

        # Remove NUL bytes which cause PostgreSQL insertion errors
        clean_text = result.get("text", "")
        if isinstance(clean_text, str):
            clean_text = clean_text.replace("\x00", "")
            result["text"] = clean_text
            
        # Store DocumentPages
        pages_data = result.get("pages", [])
        for p_data in pages_data:
            p_text = p_data.get("text", "")
            if isinstance(p_text, str):
                p_text = p_text.replace("\x00", "")
            db.add(DocumentPage(
                document_id=doc.id,
                physical_page=p_data.get("page", 1),
                text=p_text
            ))
        db.commit()
            
        text_len = len(clean_text)
        logger.info(f"[process_document_sync] Extraction complete: "
                    f"{result.get('page_count', 0)} pages, "
                    f"{text_len} chars, "
                    f"{result.get('table_count', 0)} tables"
                    f"{', ERROR: ' + result.get('error', '') if result.get('error') else ''}")

        if text_len == 0:
            logger.warning(f"[process_document_sync] No text extracted from {file_path}. "
                          f"The document may be scanned/image-only without OCR support installed.")

        push_progress(task_id, "chunk", "Chunking content...", "running",
                      {"page_count": result.get("page_count", 0)})

        # ── Chunk the text ──────────────────────────────────
        chunks = _chunk_text(result.get("text", ""))

        push_progress(task_id, "store", f"Storing {len(chunks)} chunks...", "running",
                      {"chunk_count": len(chunks)})

        # ── Update document record ──────────────────────────
        doc.status         = DocumentStatus.ready
        doc.extracted_text = result.get("text", "")[:50000]   # cap at 50k chars in DB
        doc.chunk_count    = len(chunks)
        doc.page_count     = result.get("page_count", 0)
        doc.processed_at   = datetime.now(timezone.utc)
        doc.metadata_      = {
            "table_count": result.get("table_count", 0),
            "word_count":  len(result.get("text", "").split()),
            "extractor":   suffix,
        }
        db.commit()

        push_progress(task_id, "done",
                      f"Document processed — {len(chunks)} chunks, {result.get('page_count',0)} pages",
                      "done", {"chunk_count": len(chunks), "page_count": result.get("page_count", 0)})

        logger.info(f"Document {document_id} processed: {len(chunks)} chunks")

    except Exception as e:
        logger.error(f"Document processing failed for {document_id}: {e}")
        push_progress(task_id, "error", str(e), "failed")
        try:
            doc = db.query(Document).filter(Document.id == document_id).first()
            if doc:
                doc.status = DocumentStatus.failed
                doc.error_message = str(e)
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


# Celery task wrapper (used when Redis is available)
if CELERY_AVAILABLE:
    @celery_app.task(name="main.task_process_document", bind=True)
    def task_process_document(self, document_id: str, file_path: str):
        process_document_sync(document_id, file_path, self.request.id)

    @celery_app.task(name="main.task_generate_paper", bind=True)
    def task_generate_paper(self, paper_id: str):
        """Phase 5 will fill this with the full LangGraph generation pipeline."""
        task_id = self.request.id
        push_progress(task_id, "start", "Generation pipeline starting...", "running")
        # stub — Phase 5 replaces this
        db = SessionLocal()
        try:
            paper = db.query(Paper).filter(Paper.id == paper_id).first()
            if paper:
                paper.status = PaperStatus.failed
                paper.error_message = "Generation pipeline not yet implemented (Phase 5)"
                db.commit()
        finally:
            db.close()
        push_progress(task_id, "error", "Generation pipeline not yet implemented", "failed")


# ─────────────────────────────────────────────────────────────
# 9. APP FACTORY
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="TestBoost.ai API",
    description="AI-powered assessment platform — Phase 2",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "http://localhost:3000",
    "Access-Control-Allow-Credentials": "true",
}

# ── Global exception handlers (fixes the 500 with no detail) ──
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception on {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": type(exc).__name__},
        headers=CORS_HEADERS,
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    error_messages = []
    for error in exc.errors():
        loc = error.get("loc", [])
        field = loc[-1] if loc else "field"
        msg = error.get("msg", "invalid value")
        error_messages.append(f"{field}: {msg}")
    detail_str = ", ".join(error_messages)
    return JSONResponse(
        status_code=422,
        content={"detail": detail_str, "body": str(exc.body)},
        headers=CORS_HEADERS,
    )



# ─────────────────────────────────────────────────────────────
# 10. AUTH ROUTES  /auth/*
# ─────────────────────────────────────────────────────────────

@app.post("/auth/register", response_model=LoginResponse, status_code=201, tags=["Auth"])
def register(body: RegisterRequest, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(status_code=409, detail="Email already registered")
    user = User(
        email=body.email,
        name=body.name,
        hashed_password=hash_password(body.password),
        institution=body.institution,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return LoginResponse(
        access_token=create_access_token(user.id),
        refresh_token=create_refresh_token(db, user.id),
        user_id=user.id,
        name=user.name,
        email=user.email,
        role=user.role,
    )


@app.post("/auth/token", response_model=LoginResponse, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == form.username, User.is_active == True).first()
    if not user or not verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return LoginResponse(
        access_token=create_access_token(user.id),
        refresh_token=create_refresh_token(db, user.id),
        user_id=user.id,
        name=user.name,
        email=user.email,
        role=user.role,
    )


@app.post("/auth/refresh", response_model=TokenResponse, tags=["Auth"])
def refresh_token(body: RefreshRequest, db: Session = Depends(get_db)):
    record = db.query(RefreshToken).filter(
        RefreshToken.token == body.refresh_token,
        RefreshToken.revoked == False,
    ).first()
    if not record:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
    if record.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Refresh token expired")
    record.revoked = True
    db.commit()
    create_refresh_token(db, record.user_id)
    return TokenResponse(access_token=create_access_token(record.user_id))


@app.post("/auth/logout", tags=["Auth"])
def logout(body: RefreshRequest, db: Session = Depends(get_db)):
    record = db.query(RefreshToken).filter(RefreshToken.token == body.refresh_token).first()
    if record:
        record.revoked = True
        db.commit()
    return {"message": "Logged out"}


@app.get("/auth/me", response_model=UserOut, tags=["Auth"])
def me(current_user: User = Depends(get_current_user)):
    return current_user


# ─────────────────────────────────────────────────────────────
# 11. PROJECT ROUTES  /projects/*
# ─────────────────────────────────────────────────────────────

@app.get("/projects", response_model=List[ProjectOut], tags=["Projects"])
def list_projects(
    archived: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    projects = db.query(Project).filter(
        Project.owner_id == current_user.id,
        Project.is_archived == archived,
    ).order_by(Project.updated_at.desc()).all()
    result = []
    for p in projects:
        out = ProjectOut.model_validate(p)
        out.document_count = len(p.documents)
        out.paper_count    = len(p.papers)
        result.append(out)
    return result


@app.post("/projects", response_model=ProjectOut, status_code=201, tags=["Projects"])
def create_project(
    body: ProjectCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = Project(owner_id=current_user.id, **body.model_dump())
    db.add(project)
    db.commit()
    db.refresh(project)
    out = ProjectOut.model_validate(project)
    out.document_count = 0
    out.paper_count    = 0
    return out


@app.get("/projects/{project_id}", response_model=ProjectOut, tags=["Projects"])
def get_project(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = get_project_or_404(project_id, current_user, db)
    out = ProjectOut.model_validate(project)
    out.document_count = len(project.documents)
    out.paper_count    = len(project.papers)
    return out


@app.patch("/projects/{project_id}", response_model=ProjectOut, tags=["Projects"])
def update_project(
    project_id: str,
    body: ProjectUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = get_project_or_404(project_id, current_user, db)
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(project, k, v)
    project.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(project)
    out = ProjectOut.model_validate(project)
    out.document_count = len(project.documents)
    out.paper_count    = len(project.papers)
    return out


@app.delete("/projects/{project_id}", tags=["Projects"])
def delete_project(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = get_project_or_404(project_id, current_user, db)

    # 1. Delete Qdrant vectors
    if QDRANT_AVAILABLE and qdrant_client:
        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue
            qdrant_client.delete(
                collection_name=QDRANT_COLLECTION,
                points_selector=Filter(
                    must=[
                        FieldCondition(key="project_id", match=MatchValue(value=project_id))
                    ]
                )
            )
            logger.info(f"Deleted Qdrant vectors for project {project_id}")
        except Exception as e:
            logger.error(f"Failed to delete Qdrant vectors for project {project_id}: {e}")

    # 2. Delete physical files
    import shutil
    project_dir = UPLOAD_DIR / project_id
    if project_dir.exists():
        try:
            shutil.rmtree(project_dir)
            logger.info(f"Deleted uploads directory for project {project_id}")
        except Exception as e:
            logger.error(f"Failed to delete directory {project_dir}: {e}")

    # 3. Clean up graph data explicitly (in case they lack a doc_id and miss Document cascade)
    node_ids = [row[0] for row in db.query(CurriculumNode.id).filter(CurriculumNode.project_id == project_id).all()]
    if node_ids:
        db.query(ConceptEdge).filter(
            (ConceptEdge.source_id.in_(node_ids)) | (ConceptEdge.target_id.in_(node_ids))
        ).delete(synchronize_session=False)
        db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id).delete(synchronize_session=False)

    db.delete(project)
    db.commit()
    return {"message": "Project and all associated data deleted"}


@app.post("/projects/{project_id}/archive", tags=["Projects"])
def archive_project(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = get_project_or_404(project_id, current_user, db)
    project.is_archived = True
    db.commit()
    return {"message": "Project archived"}


@app.post("/projects/{project_id}/clone", response_model=ProjectOut, tags=["Projects"])
def clone_project(
    project_id: str,
    new_name: str = Form(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    source = get_project_or_404(project_id, current_user, db)
    clone = Project(
        owner_id=current_user.id,
        name=new_name,
        subject=source.subject,
        description=source.description,
        department=source.department,
        institution=source.institution,
    )
    db.add(clone)
    db.flush()
    for tmpl in source.templates:
        db.add(Template(
            project_id=clone.id, name=tmpl.name, institution=tmpl.institution,
            header_config=tmpl.header_config, section_config=tmpl.section_config,
            marks_layout=tmpl.marks_layout,
        ))
    for item in source.question_bank:
        db.add(QuestionBankItem(
            project_id=clone.id, question_text=item.question_text,
            answer_text=item.answer_text, question_type=item.question_type,
            bloom_level=item.bloom_level, difficulty=item.difficulty,
            marks=item.marks, topic=item.topic,
        ))
    db.commit()
    db.refresh(clone)
    out = ProjectOut.model_validate(clone)
    out.document_count = 0
    out.paper_count    = 0
    return out


# ─────────────────────────────────────────────────────────────
# 12. DOCUMENT ROUTES  /projects/{id}/documents/*
# ─────────────────────────────────────────────────────────────

@app.get("/projects/{project_id}/documents", response_model=List[DocumentOut], tags=["Documents"])
def list_documents(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    return db.query(Document).filter(Document.project_id == project_id).all()


@app.post("/projects/{project_id}/documents", response_model=DocumentOut, status_code=201, tags=["Documents"])
async def upload_document(
    project_id: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    doc_type: DocumentType = Form(DocumentType.other),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)

    suffix = Path(file.filename).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"File type '{suffix}' not allowed")

    content = await file.read()
    if len(content) > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {MAX_FILE_SIZE_MB}MB limit")

    unique_name = f"{uuid.uuid4()}{suffix}"
    project_dir = UPLOAD_DIR / project_id
    project_dir.mkdir(parents=True, exist_ok=True)
    file_path = project_dir / unique_name
    file_path.write_bytes(content)

    task_id = str(uuid.uuid4())
    doc = Document(
        project_id=project_id,
        filename=unique_name,
        original_name=file.filename,
        file_path=str(file_path),
        file_size=len(content),
        doc_type=doc_type,
        status=DocumentStatus.pending,
        task_id=task_id,
    )
    db.add(doc)
    db.commit()
    db.refresh(doc)

    # Use Celery if available, else run in background thread
    if CELERY_AVAILABLE:
        task_process_document.apply_async(
            args=[doc.id, str(file_path)],
            task_id=task_id,
            queue="low_priority",
        )
    else:
        background_tasks.add_task(process_document_sync, doc.id, str(file_path), task_id)

    return doc


@app.get("/projects/{project_id}/documents/{doc_id}", response_model=DocumentOut, tags=["Documents"])
def get_document(
    project_id: str, doc_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    doc = db.query(Document).filter(Document.id == doc_id, Document.project_id == project_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return doc


@app.delete("/projects/{project_id}/documents/{doc_id}", tags=["Documents"])
def delete_document(
    project_id: str, doc_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    doc = db.query(Document).filter(Document.id == doc_id, Document.project_id == project_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    try:
        Path(doc.file_path).unlink(missing_ok=True)
    except Exception:
        pass
    db.delete(doc)
    db.commit()
    return {"message": "Document deleted"}


@app.post("/projects/{project_id}/documents/{doc_id}/reprocess", tags=["Documents"])
def reprocess_document(
    project_id: str, doc_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Re-trigger processing for a failed or pending document."""
    get_project_or_404(project_id, current_user, db)
    doc = db.query(Document).filter(Document.id == doc_id, Document.project_id == project_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    task_id = str(uuid.uuid4())
    doc.status = DocumentStatus.pending
    doc.task_id = task_id
    doc.error_message = None
    db.commit()

    if CELERY_AVAILABLE:
        task_process_document.apply_async(
            args=[doc.id, doc.file_path],
            task_id=task_id,
            queue="low_priority",
        )
    else:
        background_tasks.add_task(process_document_sync, doc.id, doc.file_path, task_id)

    return {"message": "Reprocessing started", "task_id": task_id}


# ─────────────────────────────────────────────────────────────
# 13. SSE PROGRESS ROUTE  /stream/task/{task_id}
# ─────────────────────────────────────────────────────────────

@app.get("/stream/task/{task_id}", tags=["Streaming"])
async def stream_task_progress(
    task_id: str,
    db: Session = Depends(get_db),
):
    """
    Server-Sent Events endpoint.
    Frontend connects here to get real-time progress updates.

    Usage (JavaScript):
      const es = new EventSource(`/stream/task/${taskId}`);
      es.onmessage = (e) => {
        const event = JSON.parse(e.data);
        console.log(event.step, event.message, event.status);
        if (event.status === 'done' || event.status === 'failed') es.close();
      };
    """
    async def event_generator() -> AsyncGenerator[str, None]:
        last_id   = 0
        max_polls = 120   # 2 minutes max (120 × 1s)
        polls     = 0

        while polls < max_polls:
            # Fetch new progress rows since last poll
            rows = db.query(TaskProgress).filter(
                TaskProgress.task_id == task_id,
                TaskProgress.id > last_id,
            ).order_by(TaskProgress.id).all() if last_id == 0 else []

            # Re-query properly using rowid ordering via created_at
            rows = (
                db.query(TaskProgress)
                .filter(TaskProgress.task_id == task_id)
                .order_by(TaskProgress.created_at)
                .all()
            )
            new_rows = [r for r in rows if r.created_at.timestamp() > (
                datetime.now(timezone.utc).timestamp() - (polls + 1)
            )]

            # Simpler: just send all rows on first connect, then poll for new ones
            if polls == 0:
                for row in rows:
                    event = {
                        "task_id":   task_id,
                        "step":      row.step,
                        "message":   row.message,
                        "status":    row.status,
                        "detail":    row.detail or {},
                        "timestamp": row.created_at.isoformat(),
                    }
                    yield f"data: {json.dumps(event)}\n\n"
                    if row.status in ("done", "failed"):
                        return
            else:
                # Poll for latest status
                latest = (
                    db.query(TaskProgress)
                    .filter(TaskProgress.task_id == task_id)
                    .order_by(TaskProgress.created_at.desc())
                    .first()
                )
                if latest:
                    event = {
                        "task_id":   task_id,
                        "step":      latest.step,
                        "message":   latest.message,
                        "status":    latest.status,
                        "detail":    latest.detail or {},
                        "timestamp": latest.created_at.isoformat(),
                    }
                    yield f"data: {json.dumps(event)}\n\n"
                    if latest.status in ("done", "failed"):
                        return

            polls += 1
            await asyncio.sleep(1)

        yield f"data: {json.dumps({'task_id': task_id, 'step': 'timeout', 'message': 'Stream timed out', 'status': 'failed'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",     # disables Nginx buffering
            "Connection": "keep-alive",
        },
    )


@app.get("/stream/task/{task_id}/status", tags=["Streaming"])
def get_task_status(task_id: str, db: Session = Depends(get_db)):
    """Polling fallback — returns latest progress event for a task."""
    latest = (
        db.query(TaskProgress)
        .filter(TaskProgress.task_id == task_id)
        .order_by(TaskProgress.created_at.desc())
        .first()
    )
    if not latest:
        return {"task_id": task_id, "status": "queued", "message": "Waiting to start..."}
    return {
        "task_id":   task_id,
        "step":      latest.step,
        "message":   latest.message,
        "status":    latest.status,
        "detail":    latest.detail,
        "timestamp": latest.created_at.isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# 14. COURSE OUTCOME ROUTES  /projects/{id}/cos/*
# ─────────────────────────────────────────────────────────────

@app.get("/projects/{project_id}/cos", response_model=List[COOut], tags=["Course Outcomes"])
def list_cos(project_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    return db.query(CourseOutcome).filter(CourseOutcome.project_id == project_id).all()


@app.post("/projects/{project_id}/cos", response_model=COOut, status_code=201, tags=["Course Outcomes"])
def create_co(project_id: str, body: COCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    co = CourseOutcome(project_id=project_id, **body.model_dump())
    db.add(co); db.commit(); db.refresh(co)
    return co


@app.delete("/projects/{project_id}/cos/{co_id}", tags=["Course Outcomes"])
def delete_co(project_id: str, co_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    co = db.query(CourseOutcome).filter(CourseOutcome.id == co_id, CourseOutcome.project_id == project_id).first()
    if not co:
        raise HTTPException(status_code=404, detail="Course outcome not found")
    db.delete(co); db.commit()
    return {"message": "Deleted"}


# ─────────────────────────────────────────────────────────────
# 15. TEMPLATE ROUTES  /projects/{id}/templates/*
# ─────────────────────────────────────────────────────────────

@app.get("/projects/{project_id}/templates", response_model=List[TemplateOut], tags=["Templates"])
def list_templates(project_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    return db.query(Template).filter(Template.project_id == project_id).all()


@app.post("/projects/{project_id}/templates", response_model=TemplateOut, status_code=201, tags=["Templates"])
def create_template(project_id: str, body: TemplateCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    tmpl = Template(project_id=project_id, **body.model_dump())
    db.add(tmpl); db.commit(); db.refresh(tmpl)
    return tmpl


@app.delete("/projects/{project_id}/templates/{tmpl_id}", tags=["Templates"])
def delete_template(project_id: str, tmpl_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    tmpl = db.query(Template).filter(Template.id == tmpl_id, Template.project_id == project_id).first()
    if not tmpl:
        raise HTTPException(status_code=404, detail="Template not found")
    db.delete(tmpl); db.commit()
    return {"message": "Deleted"}


# ─────────────────────────────────────────────────────────────
# 16. PAPER ROUTES  /projects/{id}/papers/*
# ─────────────────────────────────────────────────────────────

@app.get("/projects/{project_id}/papers", response_model=List[PaperOut], tags=["Papers"])
def list_papers(project_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    papers = db.query(Paper).filter(Paper.project_id == project_id).order_by(Paper.created_at.desc()).all()
    result = []
    for p in papers:
        out = PaperOut.model_validate(p)
        out.question_count = len(p.questions)
        result.append(out)
    return result


@app.post("/projects/{project_id}/papers", response_model=PaperOut, status_code=201, tags=["Papers"])
def create_paper(project_id: str, body: PaperCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    paper = Paper(
        project_id=project_id, title=body.title,
        blueprint=body.blueprint.model_dump(),
        total_marks=body.blueprint.total_marks,
        duration_mins=body.blueprint.duration_mins,
        template_id=body.template_id, notes=body.notes,
        status=PaperStatus.draft,
    )
    db.add(paper); db.commit(); db.refresh(paper)
    out = PaperOut.model_validate(paper)
    out.question_count = 0
    return out


@app.get("/projects/{project_id}/papers/{paper_id}", response_model=PaperOut, tags=["Papers"])
def get_paper(project_id: str, paper_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    paper = db.query(Paper).filter(Paper.id == paper_id, Paper.project_id == project_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
    out = PaperOut.model_validate(paper)
    out.question_count = len(paper.questions)
    return out


@app.delete("/projects/{project_id}/papers/{paper_id}", tags=["Papers"])
def delete_paper(project_id: str, paper_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    paper = db.query(Paper).filter(Paper.id == paper_id, Paper.project_id == project_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
    db.delete(paper); db.commit()
    return {"message": "Paper deleted"}


@app.post("/projects/{project_id}/papers/{paper_id}/generate", tags=["Papers"])
def trigger_generation(
    project_id: str, paper_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    paper = db.query(Paper).filter(Paper.id == paper_id, Paper.project_id == project_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
    if paper.status == PaperStatus.generating:
        raise HTTPException(status_code=409, detail="Generation already in progress")

    doc_count = db.query(Document).filter(
        Document.project_id == project_id,
        Document.status == DocumentStatus.ready,
    ).count()
    if doc_count == 0:
        raise HTTPException(status_code=400,
            detail="No processed documents found. Upload and process at least one document first.")

    if not LLM_AVAILABLE:
        raise HTTPException(status_code=400,
            detail="No LLM API key configured. Add GROQ_API_KEY or OPENAI_API_KEY to your .env file.")

    task_id = str(uuid.uuid4())
    paper.status  = PaperStatus.generating
    paper.task_id = task_id
    paper.error_message = None
    db.commit()

    background_tasks.add_task(run_generation_pipeline, paper_id, task_id)

    return {
        "message":    "Generation started",
        "paper_id":   paper_id,
        "task_id":    task_id,
        "status":     "generating",
        "stream_url": f"/stream/task/{task_id}",
    }


# ─────────────────────────────────────────────────────────────
# 17. QUESTION ROUTES  /projects/{id}/papers/{id}/questions/*
# ─────────────────────────────────────────────────────────────

@app.get("/projects/{project_id}/papers/{paper_id}/questions", response_model=List[QuestionOut], tags=["Questions"])
def list_questions(project_id: str, paper_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    return db.query(Question).filter(Question.paper_id == paper_id).order_by(Question.order_index).all()


@app.post("/projects/{project_id}/papers/{paper_id}/questions", response_model=QuestionOut, status_code=201, tags=["Questions"])
def add_question(project_id: str, paper_id: str, body: QuestionCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    paper = db.query(Paper).filter(Paper.id == paper_id, Paper.project_id == project_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
    count = db.query(Question).filter(Question.paper_id == paper_id).count()
    question = Question(paper_id=paper_id, order_index=count, **body.model_dump())
    db.add(question); db.commit(); db.refresh(question)
    return question


@app.patch("/projects/{project_id}/papers/{paper_id}/questions/{q_id}", response_model=QuestionOut, tags=["Questions"])
def update_question(project_id: str, paper_id: str, q_id: str, body: QuestionUpdate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    question = db.query(Question).filter(Question.id == q_id, Question.paper_id == paper_id).first()
    if not question:
        raise HTTPException(status_code=404, detail="Question not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(question, k, v)
    db.commit(); db.refresh(question)
    return question


@app.delete("/projects/{project_id}/papers/{paper_id}/questions/{q_id}", tags=["Questions"])
def delete_question(project_id: str, paper_id: str, q_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    question = db.query(Question).filter(Question.id == q_id, Question.paper_id == paper_id).first()
    if not question:
        raise HTTPException(status_code=404, detail="Question not found")
    db.delete(question); db.commit()
    return {"message": "Question deleted"}


@app.post("/projects/{project_id}/papers/{paper_id}/questions/{q_id}/feedback", tags=["Questions"])
def question_feedback(
    project_id: str, paper_id: str, q_id: str,
    feedback: str = Form(..., description="accepted | rejected | modified"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    question = db.query(Question).filter(Question.id == q_id, Question.paper_id == paper_id).first()
    if not question:
        raise HTTPException(status_code=404, detail="Question not found")
    question.is_accepted = feedback != "rejected"
    db.commit()
    existing = db.query(QuestionBankItem).filter(
        QuestionBankItem.project_id == project_id,
        QuestionBankItem.question_text == question.question_text,
    ).first()
    if existing:
        existing.times_used += 1
        existing.feedback = feedback
    else:
        db.add(QuestionBankItem(
            project_id=project_id, question_text=question.question_text,
            answer_text=question.answer_text, question_type=question.question_type,
            bloom_level=question.bloom_level, difficulty=question.difficulty,
            marks=question.marks, topic=question.topic, feedback=feedback,
        ))
    db.commit()
    return {"message": f"Feedback '{feedback}' recorded"}


# ─────────────────────────────────────────────────────────────
# 18. QUESTION BANK  /projects/{id}/question-bank
# ─────────────────────────────────────────────────────────────

@app.get("/projects/{project_id}/question-bank", tags=["Question Bank"])
def list_question_bank(
    project_id: str,
    topic: Optional[str] = None,
    bloom_level: Optional[BloomLevel] = None,
    difficulty: Optional[Difficulty] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    q = db.query(QuestionBankItem).filter(QuestionBankItem.project_id == project_id)
    if topic:        q = q.filter(QuestionBankItem.topic == topic)
    if bloom_level:  q = q.filter(QuestionBankItem.bloom_level == bloom_level)
    if difficulty:   q = q.filter(QuestionBankItem.difficulty == difficulty)
    return q.order_by(QuestionBankItem.times_used.desc()).all()


# ─────────────────────────────────────────────────────────────
# 19. ANALYTICS  /projects/{id}/analytics
# ─────────────────────────────────────────────────────────────

@app.get("/projects/{project_id}/analytics", response_model=AnalyticsSummary, tags=["Analytics"])
def project_analytics(project_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    get_project_or_404(project_id, current_user, db)
    papers   = db.query(Paper).filter(Paper.project_id == project_id).all()
    paper_ids = [p.id for p in papers]
    questions = db.query(Question).filter(
        Question.paper_id.in_(paper_ids), Question.is_accepted == True,
    ).all() if paper_ids else []

    total = len(questions)
    def pct(c): return round(c / total * 100, 1) if total else 0

    bloom_dist, diff_dist, topic_dist, type_dist = {}, {}, {}, {}
    for q in questions:
        for d, key in [(bloom_dist, q.bloom_level or "untagged"),
                       (diff_dist,  q.difficulty  or "untagged"),
                       (topic_dist, q.topic        or "untagged"),
                       (type_dist,  q.question_type)]:
            d[key] = d.get(key, 0) + 1

    return AnalyticsSummary(
        total_papers=len(papers), total_questions=total,
        bloom_distribution={k: pct(v) for k, v in bloom_dist.items()},
        difficulty_distribution={k: pct(v) for k, v in diff_dist.items()},
        topic_distribution={k: pct(v) for k, v in topic_dist.items()},
        question_type_distribution={k: pct(v) for k, v in type_dist.items()},
    )


# ─────────────────────────────────────────────────────────────
# 20. HEALTH CHECK
# ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health(db: Session = Depends(get_db)):
    try:
        db.execute(__import__("sqlalchemy").text("SELECT 1"))
        db_status = "ok"
    except Exception as e:
        db_status = str(e)
    return {
        "status":         "ok",
        "version":        "0.2.0",
        "phase":          "2 — Celery + SSE + Document Processing",
        "database":       db_status,
        "celery":         "connected" if CELERY_AVAILABLE else "disabled (no Redis)",
        "upload_dir":     str(UPLOAD_DIR),
    }


@app.get("/", tags=["System"])
def root():
    return {
        "name":    "TestBoost.ai API",
        "version": "0.2.0",
        "docs":    "/docs",
        "health":  "/health",
        "phase":   "2 — Celery + SSE + Document Processing",
        "new_in_phase_2": [
            "Real document processing (PDF, DOCX, PPTX, OCR)",
            "Celery task queue with priority queues",
            "SSE progress streaming at /stream/task/{task_id}",
            "Polling fallback at /stream/task/{task_id}/status",
            "Document reprocess endpoint",
            "Global exception handler (fixes silent 500s)",
            "Task progress stored in DB",
        ],
    }

# ═════════════════════════════════════════════════════════════
# PHASE 3 — CURRICULUM UNDERSTANDING ENGINE
# ═════════════════════════════════════════════════════════════
#
# New in Phase 3:
#   21. DB Models     — CurriculumNode, ConceptEdge, DocumentChunk
#   22. Schemas       — curriculum + graph schemas
#   23. Embedding     — BAAI/bge-large or OpenAI, stored in Qdrant
#   24. Curriculum    — LLM-based extraction: Subject→Chapter→Topic→LO
#   25. Graph         — concept nodes + typed edges (depends_on, related_to)
#   26. Graph-RAG     — expand topics → vector search → rerank → context
#   27. Routes        — /projects/{id}/curriculum/*
#                       /projects/{id}/graph/*
#                       /projects/{id}/search
# ═════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────
# 21. PHASE 3 DATABASE MODELS
# ─────────────────────────────────────────────────────────────

class NodeType(str, PyEnum):
    subject         = "subject"
    unit            = "unit"
    chapter         = "chapter"
    topic           = "topic"
    subtopic        = "subtopic"
    learning_outcome = "learning_outcome"
    formula         = "formula"
    definition      = "definition"
    example         = "example"


class EdgeType(str, PyEnum):
    contains     = "contains"       # chapter contains topic
    depends_on   = "depends_on"     # deadlocks depends_on resource_allocation
    related_to   = "related_to"     # scheduling related_to context_switching
    prerequisite = "prerequisite"   # process_sync prerequisite for deadlocks
    leads_to     = "leads_to"       # memory_leak leads_to system_crash


class CurriculumNode(Base):
    __tablename__ = "curriculum_nodes"
    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id  = Column(String, ForeignKey("projects.id"), nullable=False)
    doc_id      = Column(String, ForeignKey("documents.id"), nullable=True)
    node_type   = Column(Enum(NodeType), nullable=False)
    label       = Column(String, nullable=False)        # human-readable name
    description = Column(Text, nullable=True)
    bloom_level = Column(Enum(BloomLevel), nullable=True)
    parent_id   = Column(String, ForeignKey("curriculum_nodes.id"), nullable=True)
    depth       = Column(Integer, default=0)            # 0=subject, 1=chapter, 2=topic…
    metadata_   = Column("node_metadata", JSON, default=dict)
    embedding_id = Column(String, nullable=True)        # Qdrant point id
    created_at  = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    children = relationship("CurriculumNode",
                            backref=__import__("sqlalchemy.orm", fromlist=["backref"]).backref(
                                "parent", remote_side="CurriculumNode.id"))
    edges_out = relationship("ConceptEdge", foreign_keys="[ConceptEdge.source_id]", cascade="all, delete-orphan")
    edges_in = relationship("ConceptEdge", foreign_keys="[ConceptEdge.target_id]", cascade="all, delete-orphan")


class ConceptEdge(Base):
    __tablename__ = "concept_edges"
    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id  = Column(String, ForeignKey("projects.id"), nullable=False)
    source_id   = Column(String, ForeignKey("curriculum_nodes.id"), nullable=False)
    target_id   = Column(String, ForeignKey("curriculum_nodes.id"), nullable=False)
    edge_type   = Column(Enum(EdgeType), nullable=False)
    weight      = Column(Integer, default=1)     # strength of relationship
    created_at  = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class DocumentChunk(Base):
    __tablename__ = "document_chunks"
    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    doc_id      = Column(String, ForeignKey("documents.id"), nullable=False)
    project_id  = Column(String, ForeignKey("projects.id"), nullable=False)
    chunk_index = Column(Integer, nullable=False)
    text        = Column(Text, nullable=False)
    word_count  = Column(Integer, default=0)
    node_id     = Column(String, ForeignKey("curriculum_nodes.id"), nullable=True)
    embedding_id = Column(String, nullable=True)    # Qdrant point id
    metadata_   = Column("chunk_metadata", JSON, default=dict)
    created_at  = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# Create new Phase 3 tables
_p3_tables = [CurriculumNode.__table__, ConceptEdge.__table__, DocumentChunk.__table__]
for _t in _p3_tables:
    try:
        _t.create(bind=engine, checkfirst=True)
    except Exception as _e:
        logger.warning(f"Table creation skipped: {_e}")


# ─────────────────────────────────────────────────────────────
# 22. PHASE 3 SCHEMAS
# ─────────────────────────────────────────────────────────────

class CurriculumNodeOut(BaseModel):
    id: str
    node_type: str
    label: str
    description: Optional[str]
    bloom_level: Optional[str]
    parent_id: Optional[str]
    depth: int
    metadata_: Optional[dict] = None
    created_at: datetime
    class Config: from_attributes = True

class CurriculumTreeNode(BaseModel):
    id: str
    node_type: str
    label: str
    description: Optional[str]
    bloom_level: Optional[str]
    depth: int
    children: List["CurriculumTreeNode"] = []
    class Config: from_attributes = True

CurriculumTreeNode.model_rebuild()

class ConceptEdgeOut(BaseModel):
    id: str
    source_id: str
    target_id: str
    source_label: str = ""
    target_label: str = ""
    edge_type: str
    weight: int
    class Config: from_attributes = True

class GraphExpansionRequest(BaseModel):
    topic_ids: List[str]
    max_hops: int = Field(default=2, ge=1, le=3)
    edge_types: Optional[List[EdgeType]] = None

class GraphExpansionResult(BaseModel):
    seed_ids: List[str]
    expanded_ids: List[str]
    edges: List[ConceptEdgeOut]
    total_nodes: int

class SemanticSearchRequest(BaseModel):
    query: str = Field(min_length=3)
    top_k: int = Field(default=10, ge=1, le=50)
    topic_filter: Optional[List[str]] = None

class SearchResult(BaseModel):
    chunk_id: str
    doc_id: str
    text: str
    score: float
    topic: Optional[str] = None
    metadata: Optional[dict] = None

class RAGContextRequest(BaseModel):
    topic_ids: List[str]
    query: Optional[str] = None
    top_k: int = Field(default=15, ge=1, le=50)
    max_hops: int = Field(default=2, ge=1, le=3)

class RAGContextResult(BaseModel):
    topic_ids: List[str]
    expanded_topic_ids: List[str]
    chunks: List[SearchResult]
    combined_context: str
    token_estimate: int


# ─────────────────────────────────────────────────────────────
# 23. EMBEDDING ENGINE
# ─────────────────────────────────────────────────────────────

QDRANT_URL        = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY    = os.getenv("QDRANT_API_KEY", "")
EMBEDDING_MODEL   = os.getenv("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5")
EMBEDDING_DIM     = 1024   # bge-large dimension (text-embedding-3-small = 1536)
QDRANT_COLLECTION = "questionpaper_chunks"

qdrant_client  = None
embed_model    = None
QDRANT_AVAILABLE   = False
EMBEDDINGS_AVAILABLE = False


def _init_qdrant():
    global qdrant_client, QDRANT_AVAILABLE
    try:
        from qdrant_client import QdrantClient
        from qdrant_client.models import Distance, VectorParams
        qdrant_client = QdrantClient(
            url=QDRANT_URL,
            api_key=QDRANT_API_KEY or None,
            timeout=10,
        )
        # Create collection if not exists
        collections = [c.name for c in qdrant_client.get_collections().collections]
        if QDRANT_COLLECTION not in collections:
            qdrant_client.create_collection(
                collection_name=QDRANT_COLLECTION,
                vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
            )
            logger.info(f"Qdrant collection '{QDRANT_COLLECTION}' created")
        QDRANT_AVAILABLE = True
        logger.info("Qdrant connected")
    except Exception as e:
        logger.warning(f"Qdrant not available — vector search disabled: {e}")


def _init_embeddings():
    global embed_model, EMBEDDINGS_AVAILABLE
    try:
        # Try sentence-transformers (local, free)
        from sentence_transformers import SentenceTransformer
        embed_model = SentenceTransformer(EMBEDDING_MODEL)
        EMBEDDINGS_AVAILABLE = True
        logger.info(f"Embedding model loaded: {EMBEDDING_MODEL}")
    except ImportError:
        logger.warning("sentence-transformers not installed — embeddings disabled")
    except Exception as e:
        logger.warning(f"Embedding model failed to load: {e}")


# Lazy init — don't block startup if services are down
_init_qdrant()
_init_embeddings()


def embed_texts(texts: List[str]) -> Optional[List[List[float]]]:
    """Generate embeddings. Returns None if embeddings unavailable."""
    if not EMBEDDINGS_AVAILABLE or not embed_model:
        return None
    try:
        vecs = embed_model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
        return vecs.tolist()
    except Exception as e:
        logger.warning(f"Embedding failed: {e}")
        return None


def store_embeddings_in_qdrant(points: List[dict]):
    """
    Store embedding points in Qdrant.
    Each point: {"id": str_uuid, "vector": [...], "payload": {...}}
    """
    if not QDRANT_AVAILABLE or not qdrant_client:
        return False
    try:
        from qdrant_client.models import PointStruct
        qdrant_points = [
            PointStruct(id=p["id"], vector=p["vector"], payload=p.get("payload", {}))
            for p in points
        ]
        qdrant_client.upsert(collection_name=QDRANT_COLLECTION, points=qdrant_points)
        return True
    except Exception as e:
        logger.warning(f"Qdrant upsert failed: {e}")
        return False


def search_qdrant(query_vector: List[float], top_k: int = 10,
                  filter_payload: dict = None) -> List[dict]:
    """Vector search in Qdrant. Returns list of {id, score, payload}."""
    if not QDRANT_AVAILABLE or not qdrant_client:
        return []
    try:
        from qdrant_client.models import Filter, FieldCondition, MatchValue
        qdrant_filter = None
        if filter_payload:
            conditions = [
                FieldCondition(key=k, match=MatchValue(value=v))
                for k, v in filter_payload.items()
            ]
            qdrant_filter = Filter(must=conditions)

        response = qdrant_client.query_points(
            collection_name=QDRANT_COLLECTION,
            query=query_vector,
            limit=top_k,
            query_filter=qdrant_filter,
            with_payload=True,
        )
        return [{"id": str(r.id), "score": r.score, "payload": r.payload} for r in response.points]
    except Exception as e:
        logger.warning(f"Qdrant search failed: {e}")
        return []


def rerank_results(query: str, results: List[dict], top_k: int = 10) -> List[dict]:
    """
    Rerank using bge-reranker-v2 if available.
    Falls back to original vector score order.
    """
    try:
        from FlagEmbedding import FlagReranker
        reranker = FlagReranker("BAAI/bge-reranker-v2-m3", use_fp16=True)
        pairs    = [[query, r["payload"].get("text", "")] for r in results]
        scores   = reranker.compute_score(pairs)
        for i, r in enumerate(results):
            r["rerank_score"] = scores[i]
        results.sort(key=lambda x: x.get("rerank_score", x["score"]), reverse=True)
        return results[:top_k]
    except Exception:
        # Reranker not available — return by vector score
        return sorted(results, key=lambda x: x["score"], reverse=True)[:top_k]


# ─────────────────────────────────────────────────────────────
# 24. CURRICULUM EXTRACTION ENGINE
# ─────────────────────────────────────────────────────────────

GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
LLM_AVAILABLE  = False
_llm_client    = None
_llm_model     = None


def _init_llm():
    global LLM_AVAILABLE, _llm_client, _llm_model
    if GROQ_API_KEY:
        try:
            from groq import Groq
            _llm_client = Groq(api_key=GROQ_API_KEY)
            _llm_model  = "llama-3.1-8b-instant"
            LLM_AVAILABLE = True
            logger.info("LLM: Groq (llama-3.1-8b-instant)")
            return
        except Exception as e:
            logger.warning(f"Groq init failed: {e}")
    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            _llm_client = OpenAI(api_key=OPENAI_API_KEY)
            _llm_model  = "gpt-4o-mini"
            LLM_AVAILABLE = True
            logger.info("LLM: OpenAI (gpt-4o-mini)")
            return
        except Exception as e:
            logger.warning(f"OpenAI init failed: {e}")
    logger.warning("No LLM API key set — curriculum extraction disabled. Set GROQ_API_KEY or OPENAI_API_KEY in .env")


_init_llm()


def _call_llm(system: str, user: str, json_mode: bool = True) -> Optional[str]:
    """Generic LLM call. Returns text content or None on failure."""
    if not LLM_AVAILABLE or not _llm_client:
        return None
    try:
        kwargs = {"model": _llm_model, "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ], "max_tokens": 2000, "temperature": 0.1}
        # Groq and OpenAI share the same interface
        response = _llm_client.chat.completions.create(**kwargs)
        return response.choices[0].message.content
    except Exception as e:
        logger.warning(f"LLM call failed: {e}")
        return None


def extract_toc_from_text(text: str, subject_hint: str = "") -> Optional[dict]:
    """
    Use LLM to extract the Table of Contents from the beginning of the document.
    Returns structured JSON with units and chapters.
    """
    system = """You are a curriculum analysis expert. Extract the Table of Contents from the provided text.
CRITICAL RULES:
1. Identify the hierarchical structure: Units (if any) and Chapters.
2. Provide the exact chapter title as printed.
3. If the text does not contain explicit Units, you can group chapters under logical Units if appropriate, or omit Units.
4. Keep titles clean and concise. Remove leading numbers if they are just indices (e.g. "1. " -> ""), but preserve meaningful numbers.
5. Extract the printed page number for each chapter if available.

Return ONLY valid JSON in this exact format:
{
  "subject": "clean subject name",
  "units": [
    {
      "unit_title": "Unit 1: Reproduction",
      "chapters": [
        { "title": "Reproduction in Lower and Higher Plants", "printed_page": 1 },
        { "title": "Reproduction in Lower and Higher Animals", "printed_page": 18 }
      ]
    }
  ]
}
If there are no units, you can still return them under a default unit or omit the 'units' key and just return 'chapters'."""

    user = f"Subject hint: {subject_hint or 'auto-detect'}\n\nDocument text (TOC pages):\n{text[:8000]}"
    
    raw = _call_llm(system, user)
    if not raw: return None
    try:
        raw = raw.strip()
        if raw.startswith("```"): raw = raw.split("```")[1]
        if raw.startswith("json"): raw = raw[4:]
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        logger.warning(f"TOC JSON parse failed: {e}\nRaw: {raw[:200]}")
        return None

def extract_chapter_topics(chapter_title: str, chapter_text: str) -> Optional[dict]:
    """
    Use LLM to extract Topics and Subtopics for a specific chapter.
    """
    system = """You are a curriculum analysis expert. Extract the hierarchical educational structure for a specific chapter.
CRITICAL RULES:
1. Build a strict 2-layer hierarchy under the chapter: Topic -> Subtopic.
   Example: Topic: "Asexual Reproduction" -> Subtopics: ["Vegetative Propagation", "Spores"]
2. Do NOT use flat, generic topics like "Introduction" or "Fundamentals" without nesting.
3. Remove all bullet points, numbers (e.g. "1.", "1.1"), and ASCII tree characters.
4. Titles must be clean, concise names of the concepts.

Return ONLY valid JSON in this exact format:
{
  "topics": [
    {
      "title": "clean topic title",
      "subtopics": ["clean subtopic1", "clean subtopic2"],
      "learning_outcomes": ["student will be able to..."],
      "key_concepts": ["concept1", "concept2"],
      "bloom_level": "remember|understand|apply|analyze|evaluate|create"
    }
  ]
}"""
    user = f"Chapter Title: {chapter_title}\n\nChapter Text (up to 5000 chars):\n{chapter_text[:5000]}"
    
    raw = _call_llm(system, user)
    if not raw: return None
    try:
        raw = raw.strip()
        if raw.startswith("```"): raw = raw.split("```")[1]
        if raw.startswith("json"): raw = raw[4:]
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        logger.warning(f"Chapter topics JSON parse failed: {e}\nRaw: {raw[:200]}")
        return None




def extract_concept_relationships(nodes: List[dict]) -> List[dict]:
    """
    Use LLM to identify relationships between curriculum concepts.
    nodes: list of {"id": ..., "label": ...}
    Returns list of {"source": label, "target": label, "type": edge_type}
    """
    if not nodes or len(nodes) < 2:
        return []

    labels = [n["label"] for n in nodes[:30]]  # cap at 30 concepts per call
    system = """You are a knowledge graph expert. Given a list of educational concepts,
identify meaningful relationships between them.
Return ONLY valid JSON array, no markdown:
[
  {"source": "concept A", "target": "concept B", "type": "depends_on"},
  {"source": "concept C", "target": "concept D", "type": "related_to"}
]
Edge types: depends_on, related_to, prerequisite, leads_to, contains
Only include meaningful relationships. Maximum 20 edges."""

    user = f"Concepts: {json.dumps(labels)}"
    raw  = _call_llm(system, user)
    if not raw:
        return []
    try:
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception:
        return []


def build_curriculum_for_document(doc_id: str, project_id: str, subject_hint: str = ""):
    """
    Full curriculum build pipeline for one document.
    1. Load extracted text from DB
    2. Extract curriculum hierarchy via LLM
    3. Save CurriculumNodes
    4. Extract concept relationships → ConceptEdges
    5. Generate + store embeddings for each node
    6. Store chunks with embeddings in Qdrant
    """
    def clean_label(text: str) -> str:
        import re
        if not text:
            return "Unknown"
        # Remove ASCII tree chars
        text = re.sub(r'[├└│─■]', '', text)
        # Remove leading numbers/bullets (e.g., "1.", "1.1", "- ")
        text = re.sub(r'^[\d\.\-\s]+', '', text)
        return text.strip()

    db = SessionLocal()
    try:
        doc = db.query(Document).filter(Document.id == doc_id).first()
        if not doc:
            logger.warning(f"Document {doc_id} not found")
            return
            
        pages = db.query(DocumentPage).filter(DocumentPage.document_id == doc_id).order_by(DocumentPage.physical_page).all()
        if not pages:
            logger.warning(f"Document {doc_id} has no extracted pages in DocumentPage")
            return

        task_id = doc.task_id or str(uuid.uuid4())
        push_progress(task_id, "curriculum", "Extracting curriculum structure...", "running")

        # ── Step 1: Extract TOC ──────────────────────
        toc_text = "\n\n".join([f"--- PAGE {p.physical_page} ---\n{p.text}" for p in pages[:30]])
        toc = extract_toc_from_text(toc_text, subject_hint)

        if not toc:
            push_progress(task_id, "curriculum", "LLM not available or failed TOC — building basic structure", "running")
            toc = {"subject": subject_hint or doc.original_name, "chapters": [{"title": "Chapter 1"}]}
            
        push_progress(task_id, "graph", "Building knowledge graph backbone...", "running")

        # ── Step 2: Save subject node ───────────────────────
        subject_node = CurriculumNode(
            project_id=project_id, doc_id=doc_id,
            node_type=NodeType.subject,
            label=toc.get("subject", subject_hint or doc.original_name),
            depth=0,
        )
        db.add(subject_node)
        db.flush()

        all_nodes = [subject_node]
        topic_nodes = []
        
        units_data = toc.get("units", [])
        if not units_data and "chapters" in toc:
            units_data = [{"unit_title": None, "chapters": toc["chapters"]}]
            
        chapter_nodes_map = []
        
        for unit_data in units_data:
            unit_title = unit_data.get("unit_title")
            unit_node = None
            if unit_title:
                unit_node = CurriculumNode(
                    project_id=project_id, doc_id=doc_id,
                    node_type=NodeType.unit,
                    label=clean_label(unit_title),
                    parent_id=subject_node.id, depth=1,
                )
                db.add(unit_node)
                db.flush()
                all_nodes.append(unit_node)
                db.add(ConceptEdge(
                    project_id=project_id,
                    source_id=subject_node.id, target_id=unit_node.id,
                    edge_type=EdgeType.contains,
                ))
            
            parent_node = unit_node if unit_node else subject_node
            depth_for_chapter = 2 if unit_node else 1
            
            for chapter_data in unit_data.get("chapters", []):
                chapter_title = clean_label(chapter_data.get("title", "Chapter"))
                chapter_node = CurriculumNode(
                    project_id=project_id, doc_id=doc_id,
                    node_type=NodeType.chapter,
                    label=chapter_title,
                    parent_id=parent_node.id, depth=depth_for_chapter,
                )
                db.add(chapter_node)
                db.flush()
                all_nodes.append(chapter_node)
                chapter_nodes_map.append((chapter_node, chapter_title))
                
                db.add(ConceptEdge(
                    project_id=project_id,
                    source_id=parent_node.id, target_id=chapter_node.id,
                    edge_type=EdgeType.contains,
                ))
                
        # ── Step 3: Chapter Localization ─────────────
        import re
        chapter_starts = []
        for i, (chapter_node, title) in enumerate(chapter_nodes_map):
            found_page = -1
            # Pass 1: Try regex headers (most reliable)
            for p in pages:
                if re.search(r'(?mi)^\s*(CHAPTER|UNIT)\s+' + str(i+1), p.text):
                    found_page = p.physical_page
                    break
            
            # Pass 2: Try string matching on the title
            if found_page == -1:
                best_page = -1
                title_lower = title.lower()
                for p in pages:
                    # Look for title in the first 500 chars of a page
                    if title_lower in p.text.lower()[:500]:
                        best_page = p.physical_page
                        break
                if best_page != -1:
                    found_page = best_page

            if found_page != -1:
                chapter_starts.append((i, found_page))
            else:
                logger.warning(f"Could not localize chapter {title}")
                
        chapter_starts.sort(key=lambda x: x[1])
        
        # ── Step 4: Extract Topics per Chapter ─────────────
        for i, (chap_index, start_page) in enumerate(chapter_starts):
            end_page = chapter_starts[i+1][1] if i + 1 < len(chapter_starts) else pages[-1].physical_page + 1
            
            chapter_text_parts = [p.text for p in pages if start_page <= p.physical_page < end_page]
            chapter_text = "\n\n".join(chapter_text_parts)
            
            chapter_node, chapter_title = chapter_nodes_map[chap_index]
            
            push_progress(task_id, "curriculum", f"Extracting topics for {chapter_title} (Pages {start_page}-{end_page-1})...", "running")
            chapter_curriculum = extract_chapter_topics(chapter_title, chapter_text)
            
            if chapter_curriculum:
                for topic_data in chapter_curriculum.get("topics", []):
                    bloom = topic_data.get("bloom_level", "")
                    bloom_val = BloomLevel(bloom) if bloom in [b.value for b in BloomLevel] else None
                    topic_node = CurriculumNode(
                        project_id=project_id, doc_id=doc_id,
                        node_type=NodeType.topic,
                        label=clean_label(topic_data.get("title", "Topic")),
                        description=", ".join(topic_data.get("learning_outcomes", [])),
                        bloom_level=bloom_val,
                        parent_id=chapter_node.id, depth=chapter_node.depth + 1,
                        metadata_={
                            "key_concepts": topic_data.get("key_concepts", []),
                            "subtopics":    [clean_label(st) for st in topic_data.get("subtopics", [])],
                        },
                    )
                    db.add(topic_node)
                    db.flush()
                    all_nodes.append(topic_node)
                    topic_nodes.append(topic_node)

                    db.add(ConceptEdge(
                        project_id=project_id,
                        source_id=chapter_node.id, target_id=topic_node.id,
                        edge_type=EdgeType.contains,
                    ))

                    for st_label in topic_data.get("subtopics", []):
                        st_node = CurriculumNode(
                            project_id=project_id, doc_id=doc_id,
                            node_type=NodeType.subtopic,
                            label=clean_label(st_label), parent_id=topic_node.id, depth=topic_node.depth + 1,
                        )
                        db.add(st_node)
                        db.flush()
                        all_nodes.append(st_node)
                        db.add(ConceptEdge(
                            project_id=project_id,
                            source_id=topic_node.id, target_id=st_node.id,
                            edge_type=EdgeType.contains,
                        ))

        db.commit()

        # ── Step 4: Concept relationship edges ─────────────
        if len(topic_nodes) >= 2 and LLM_AVAILABLE:
            node_refs = [{"id": n.id, "label": n.label} for n in topic_nodes]
            relationships = extract_concept_relationships(node_refs)
            label_to_id   = {n.label.lower(): n.id for n in topic_nodes}

            for rel in relationships:
                src_id = label_to_id.get(rel.get("source", "").lower())
                tgt_id = label_to_id.get(rel.get("target", "").lower())
                rel_type = rel.get("type", "related_to")
                if src_id and tgt_id and rel_type in [e.value for e in EdgeType]:
                    db.add(ConceptEdge(
                        project_id=project_id,
                        source_id=src_id, target_id=tgt_id,
                        edge_type=EdgeType(rel_type),
                    ))
            db.commit()

        push_progress(task_id, "embed", "Generating embeddings...", "running",
                      {"node_count": len(all_nodes)})

        # ── Step 5: Embed nodes + store in Qdrant ──────────
        if EMBEDDINGS_AVAILABLE and QDRANT_AVAILABLE:
            node_texts = [f"{n.node_type}: {n.label}. {n.description or ''}" for n in all_nodes]
            vectors    = embed_texts(node_texts)
            if vectors:
                points = []
                for node, vec in zip(all_nodes, vectors):
                    point_id = str(uuid.uuid4())
                    node.embedding_id = point_id
                    points.append({
                        "id": point_id, "vector": vec,
                        "payload": {
                            "node_id":    node.id,
                            "project_id": project_id,
                            "doc_id":     doc_id,
                            "label":      node.label,
                            "node_type":  node.node_type,
                            "type":       "curriculum_node",
                        },
                    })
                store_embeddings_in_qdrant(points)
                db.commit()

        # ── Step 6: Embed + store document chunks ──────────
        chunks = _chunk_text(doc.extracted_text or "", chunk_size=300)
        push_progress(task_id, "embed_chunks", f"Embedding {len(chunks)} chunks...", "running",
                      {"chunk_count": len(chunks)})

        chunk_batch_size = 32
        for batch_start in range(0, len(chunks), chunk_batch_size):
            batch = chunks[batch_start:batch_start + chunk_batch_size]
            vectors = embed_texts(batch) if EMBEDDINGS_AVAILABLE else None

            qdrant_points = []
            for i, (chunk_text, vec) in enumerate(
                zip(batch, vectors if vectors else [None] * len(batch))
            ):
                chunk_idx = batch_start + i
                point_id  = str(uuid.uuid4())
                chunk_row = DocumentChunk(
                    doc_id=doc_id, project_id=project_id,
                    chunk_index=chunk_idx, text=chunk_text,
                    word_count=len(chunk_text.split()),
                    embedding_id=point_id if vec else None,
                )
                db.add(chunk_row)
                db.flush()

                if vec:
                    qdrant_points.append({
                        "id": point_id, "vector": vec,
                        "payload": {
                            "chunk_id":   chunk_row.id,
                            "doc_id":     doc_id,
                            "project_id": project_id,
                            "text":       chunk_text[:500],
                            "chunk_index": chunk_idx,
                            "type":       "chunk",
                        },
                    })
            if qdrant_points:
                store_embeddings_in_qdrant(qdrant_points)
            db.commit()

        push_progress(task_id, "done",
                      f"Curriculum built — {len(all_nodes)} nodes, {len(chunks)} chunks embedded",
                      "done", {"node_count": len(all_nodes), "chunk_count": len(chunks)})

        logger.info(f"Curriculum built for doc {doc_id}: {len(all_nodes)} nodes")

    except Exception as e:
        logger.error(f"Curriculum build failed for {doc_id}: {e}", exc_info=True)
        push_progress(doc.task_id or "", "error", f"Curriculum build failed: {e}", "failed")
    finally:
        db.close()


def _build_fallback_curriculum(text: str, name: str) -> dict:
    """
    Heuristic fallback when LLM is unavailable.
    Extracts headings from text as chapter/topic structure.
    """
    lines = text.split("\n")
    chapters = []
    current_chapter = None
    current_topics  = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Detect heading-like lines (short, title-case or ALL CAPS, no period)
        if (len(line) < 80 and not line.endswith(".")
                and (line.istitle() or line.isupper() or line.startswith("#"))):
            line = line.lstrip("#").strip()
            if len(line) > 3:
                if current_chapter:
                    chapters.append({"title": current_chapter, "topics": current_topics})
                current_chapter = line
                current_topics  = []
        elif current_chapter and len(line) > 20:
            # First sentence as a subtopic hint
            topic_hint = line[:60].split(".")[0]
            if topic_hint and topic_hint not in [t.get("title") for t in current_topics]:
                current_topics.append({
                    "title": topic_hint,
                    "subtopics": [], "learning_outcomes": [], "key_concepts": [],
                })

    if current_chapter:
        chapters.append({"title": current_chapter, "topics": current_topics})

    if not chapters:
        # Absolute fallback — one chapter from filename
        chapters = [{"title": name, "topics": [
            {"title": "Main Content", "subtopics": [],
             "learning_outcomes": [], "key_concepts": []}
        ]}]

    return {"subject": name, "chapters": chapters}


# ─────────────────────────────────────────────────────────────
# 25. GRAPH-RAG RETRIEVAL
# ─────────────────────────────────────────────────────────────

def expand_graph(project_id: str, seed_node_ids: List[str],
                 max_hops: int = 2, edge_types: List[str] = None,
                 db: Session = None) -> tuple[List[str], List[ConceptEdge]]:
    """
    Expand outward from seed nodes through the knowledge graph.
    Returns (all_node_ids, all_edges_traversed).
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        visited = set(seed_node_ids)
        frontier = list(seed_node_ids)
        all_edges = []

        for _ in range(max_hops):
            if not frontier:
                break
            q = db.query(ConceptEdge).filter(
                ConceptEdge.project_id == project_id,
                ConceptEdge.source_id.in_(frontier),
            )
            if edge_types:
                q = q.filter(ConceptEdge.edge_type.in_(edge_types))
            edges = q.all()
            all_edges.extend(edges)

            next_frontier = []
            for edge in edges:
                if edge.target_id not in visited:
                    visited.add(edge.target_id)
                    next_frontier.append(edge.target_id)
            frontier = next_frontier

        return list(visited), all_edges
    finally:
        if close_db:
            db.close()


def retrieve_rag_context(project_id: str, topic_ids: List[str],
                         query: str = "", top_k: int = 15,
                         max_hops: int = 2) -> RAGContextResult:
    """
    Full Graph-RAG retrieval pipeline:
    1. Expand topics through knowledge graph
    2. Vector search for relevant chunks
    3. Rerank results
    4. Assemble context string
    """
    db = SessionLocal()
    try:
        # Step 1 — Graph expansion
        expanded_ids, _ = expand_graph(project_id, topic_ids, max_hops=max_hops, db=db)

        # Step 2 — Build query vector
        search_results = []
        if EMBEDDINGS_AVAILABLE and QDRANT_AVAILABLE and query:
            q_vectors = embed_texts([query])
            if q_vectors:
                raw_results = search_qdrant(
                    q_vectors[0], top_k=top_k * 2,
                    filter_payload={"project_id": project_id},
                )
                # Optionally re-rank
                if query and raw_results:
                    raw_results = rerank_results(query, raw_results, top_k=top_k)
                search_results = raw_results
        else:
            # Fallback: load chunks from DB directly (no vector search)
            doc_ids = [
                row.doc_id for row in
                db.query(CurriculumNode.doc_id).filter(
                    CurriculumNode.project_id == project_id,
                    CurriculumNode.id.in_(expanded_ids),
                ).distinct().all()
            ]
            chunks = db.query(DocumentChunk).filter(
                DocumentChunk.project_id == project_id,
                DocumentChunk.doc_id.in_(doc_ids),
            ).limit(top_k).all()
            search_results = [
                {"id": c.embedding_id or c.id, "score": 1.0,
                 "payload": {"chunk_id": c.id, "doc_id": c.doc_id,
                             "text": c.text, "chunk_index": c.chunk_index}}
                for c in chunks
            ]

        # Step 3 — Build result objects
        chunk_results = []
        for r in search_results[:top_k]:
            payload = r.get("payload", {})
            chunk_results.append(SearchResult(
                chunk_id=payload.get("chunk_id", r["id"]),
                doc_id=payload.get("doc_id", ""),
                text=payload.get("text", ""),
                score=r.get("rerank_score", r["score"]),
                metadata=payload,
            ))

        # Step 4 — Assemble context
        context_parts = [f"[Chunk {i+1}]\n{r.text}" for i, r in enumerate(chunk_results)]
        combined = "\n\n---\n\n".join(context_parts)
        token_estimate = len(combined.split()) * 4 // 3   # rough token estimate

        return RAGContextResult(
            topic_ids=topic_ids,
            expanded_topic_ids=expanded_ids,
            chunks=chunk_results,
            combined_context=combined,
            token_estimate=token_estimate,
        )
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# 26. ENHANCED DOCUMENT PROCESSING (replaces Phase 2 stub)
# ─────────────────────────────────────────────────────────────

_original_process_document_sync = process_document_sync


def process_document_sync(document_id: str, file_path: str, task_id: str):
    """
    Phase 3 enhanced processing:
    Phase 2 extraction → Phase 3 curriculum + embeddings
    """
    # Run Phase 2 extraction first
    _original_process_document_sync(document_id, file_path, task_id)

    # Then build curriculum (only if document processed successfully)
    db = SessionLocal()
    try:
        doc = db.query(Document).filter(Document.id == document_id).first()
        if doc and doc.status == DocumentStatus.ready and doc.extracted_text:
            project = db.query(Project).filter(Project.id == doc.project_id).first()
            subject_hint = project.subject or "" if project else ""
            db.close()
            build_curriculum_for_document(document_id, doc.project_id, subject_hint)
        else:
            db.close()
    except Exception as e:
        try:
            db.close()
        except Exception:
            pass
        logger.error(f"Phase 3 post-processing failed for {document_id}: {e}")


# ─────────────────────────────────────────────────────────────
# 27. PHASE 3 ROUTES
# ─────────────────────────────────────────────────────────────

# ── Curriculum Routes ─────────────────────────────────────────

@app.get("/projects/{project_id}/curriculum", tags=["Curriculum"])
def get_curriculum_tree(
    project_id: str,
    doc_id: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return full curriculum tree for a project (or specific document)."""
    get_project_or_404(project_id, current_user, db)

    q = db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id)
    if doc_id:
        q = q.filter(CurriculumNode.doc_id == doc_id)
    nodes = q.order_by(CurriculumNode.depth, CurriculumNode.created_at).all()

    # Build tree structure
    node_map = {n.id: {"id": n.id, "node_type": n.node_type, "label": n.label,
                       "description": n.description, "bloom_level": n.bloom_level,
                       "depth": n.depth, "parent_id": n.parent_id, "children": []}
                for n in nodes}

    roots = []
    for n in nodes:
        if n.parent_id and n.parent_id in node_map:
            node_map[n.parent_id]["children"].append(node_map[n.id])
        elif not n.parent_id:
            roots.append(node_map[n.id])

    return {"project_id": project_id, "tree": roots, "total_nodes": len(nodes)}


@app.get("/projects/{project_id}/curriculum/nodes", response_model=List[CurriculumNodeOut], tags=["Curriculum"])
def list_curriculum_nodes(
    project_id: str,
    node_type: Optional[NodeType] = None,
    doc_id: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    q = db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id)
    if node_type:
        q = q.filter(CurriculumNode.node_type == node_type)
    if doc_id:
        q = q.filter(CurriculumNode.doc_id == doc_id)
    return q.order_by(CurriculumNode.depth, CurriculumNode.label).all()


@app.post("/projects/{project_id}/curriculum/rebuild", tags=["Curriculum"])
def rebuild_curriculum(
    project_id: str,
    doc_id: Optional[str] = None,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Manually trigger curriculum rebuild for all (or one) project document."""
    project = get_project_or_404(project_id, current_user, db)

    docs_q = db.query(Document).filter(
        Document.project_id == project_id,
        Document.status == DocumentStatus.ready,
    )
    if doc_id:
        docs_q = docs_q.filter(Document.id == doc_id)
    docs = docs_q.all()

    if not docs:
        raise HTTPException(status_code=404, detail="No ready documents found")

    task_ids = []
    for doc in docs:
        tid = str(uuid.uuid4())
        task_ids.append(tid)
        background_tasks.add_task(
            build_curriculum_for_document,
            doc.id, project_id, project.subject or ""
        )

    return {"message": f"Curriculum rebuild started for {len(docs)} document(s)", "task_ids": task_ids}


@app.delete("/projects/{project_id}/curriculum", tags=["Curriculum"])
def clear_curriculum(
    project_id: str,
    doc_id: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Clear curriculum nodes and edges for project (or specific document)."""
    get_project_or_404(project_id, current_user, db)

    node_q = db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id)
    if doc_id:
        node_q = node_q.filter(CurriculumNode.doc_id == doc_id)
    node_ids = [n.id for n in node_q.all()]

    if node_ids:
        db.query(ConceptEdge).filter(
            ConceptEdge.project_id == project_id,
            ConceptEdge.source_id.in_(node_ids),
        ).delete(synchronize_session=False)
        node_q.delete(synchronize_session=False)

    db.commit()
    return {"message": f"Cleared {len(node_ids)} curriculum nodes"}


# ── Analytics Routes ───────────────────────────────────────────

@app.get("/projects/{project_id}/analytics", tags=["Analytics"])
def get_analytics_summary(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Stub for analytics summary."""
    get_project_or_404(project_id, current_user, db)
    return {
        "total_papers": 0,
        "total_questions": 0,
        "bloom_distribution": {},
        "difficulty_distribution": {},
        "topic_distribution": {},
        "question_type_distribution": {},
    }

@app.get("/projects/{project_id}/analytics/coverage", tags=["Analytics"])
def get_curriculum_coverage(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Stub for curriculum coverage heatmap."""
    get_project_or_404(project_id, current_user, db)
    return {}


# ── Knowledge Graph Routes ─────────────────────────────────────

@app.get("/projects/{project_id}/graph/edges", response_model=List[ConceptEdgeOut], tags=["Knowledge Graph"])
def list_graph_edges(
    project_id: str,
    edge_type: Optional[EdgeType] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    q = db.query(ConceptEdge).filter(ConceptEdge.project_id == project_id)
    if edge_type:
        q = q.filter(ConceptEdge.edge_type == edge_type)
    edges = q.limit(200).all()

    # Enrich with labels
    node_ids = list({e.source_id for e in edges} | {e.target_id for e in edges})
    nodes    = db.query(CurriculumNode).filter(CurriculumNode.id.in_(node_ids)).all()
    label_map = {n.id: n.label for n in nodes}

    result = []
    for e in edges:
        out = ConceptEdgeOut.model_validate(e)
        out.source_label = label_map.get(e.source_id, "")
        out.target_label = label_map.get(e.target_id, "")
        result.append(out)
    return result


@app.post("/projects/{project_id}/graph/expand", response_model=GraphExpansionResult, tags=["Knowledge Graph"])
def expand_topic_graph(
    project_id: str,
    body: GraphExpansionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Expand outward from seed topic IDs through the knowledge graph.
    Returns all connected concept IDs + traversed edges.
    """
    get_project_or_404(project_id, current_user, db)

    edge_type_vals = [e.value for e in body.edge_types] if body.edge_types else None
    expanded_ids, edges = expand_graph(
        project_id, body.topic_ids,
        max_hops=body.max_hops,
        edge_types=edge_type_vals,
        db=db,
    )
    new_ids = [i for i in expanded_ids if i not in body.topic_ids]

    node_ids = list({e.source_id for e in edges} | {e.target_id for e in edges})
    nodes    = db.query(CurriculumNode).filter(CurriculumNode.id.in_(node_ids)).all()
    label_map = {n.id: n.label for n in nodes}

    edge_outs = []
    for e in edges:
        out = ConceptEdgeOut.model_validate(e)
        out.source_label = label_map.get(e.source_id, "")
        out.target_label = label_map.get(e.target_id, "")
        edge_outs.append(out)

    return GraphExpansionResult(
        seed_ids=body.topic_ids,
        expanded_ids=new_ids,
        edges=edge_outs,
        total_nodes=len(expanded_ids),
    )


@app.get("/projects/{project_id}/graph/stats", tags=["Knowledge Graph"])
def graph_stats(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    node_count = db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id).count()
    edge_count = db.query(ConceptEdge).filter(ConceptEdge.project_id == project_id).count()
    chunk_count = db.query(DocumentChunk).filter(DocumentChunk.project_id == project_id).count()

    type_counts = {}
    for node_type in NodeType:
        c = db.query(CurriculumNode).filter(
            CurriculumNode.project_id == project_id,
            CurriculumNode.node_type == node_type,
        ).count()
        if c:
            type_counts[node_type.value] = c

    return {
        "node_count":  node_count,
        "edge_count":  edge_count,
        "chunk_count": chunk_count,
        "node_types":  type_counts,
        "qdrant_available": QDRANT_AVAILABLE,
        "embeddings_available": EMBEDDINGS_AVAILABLE,
        "llm_available": LLM_AVAILABLE,
    }


# ── Search + RAG Routes ───────────────────────────────────────

@app.post("/projects/{project_id}/search", tags=["Search & RAG"])
def semantic_search(
    project_id: str,
    body: SemanticSearchRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Semantic search across all project document chunks.
    Uses vector search if Qdrant is available, else full-text keyword fallback.
    """
    get_project_or_404(project_id, current_user, db)

    if EMBEDDINGS_AVAILABLE and QDRANT_AVAILABLE:
        q_vec = embed_texts([body.query])
        if not q_vec:
            raise HTTPException(status_code=503, detail="Embedding service unavailable")
        raw = search_qdrant(q_vec[0], top_k=body.top_k * 2,
                            filter_payload={"project_id": project_id})
        ranked = rerank_results(body.query, raw, top_k=body.top_k)
        return {
            "query": body.query,
            "mode": "vector+rerank",
            "results": [
                SearchResult(
                    chunk_id=r["payload"].get("chunk_id", r["id"]),
                    doc_id=r["payload"].get("doc_id", ""),
                    text=r["payload"].get("text", ""),
                    score=r.get("rerank_score", r["score"]),
                    metadata=r["payload"],
                )
                for r in ranked
            ],
        }
    else:
        # Keyword fallback
        from sqlalchemy import or_
        keywords = body.query.lower().split()[:5]
        filters  = [DocumentChunk.text.ilike(f"%{kw}%") for kw in keywords]
        chunks   = db.query(DocumentChunk).filter(
            DocumentChunk.project_id == project_id,
            or_(*filters),
        ).limit(body.top_k).all()
        return {
            "query": body.query,
            "mode": "keyword_fallback",
            "results": [
                SearchResult(chunk_id=c.id, doc_id=c.doc_id, text=c.text, score=1.0)
                for c in chunks
            ],
        }


@app.post("/projects/{project_id}/rag-context", response_model=RAGContextResult, tags=["Search & RAG"])
def get_rag_context(
    project_id: str,
    body: RAGContextRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Full Graph-RAG context retrieval.
    Used by the question generation engine (Phase 5) to get
    curriculum-aware context for a set of topics.
    """
    get_project_or_404(project_id, current_user, db)

    # Validate topic IDs belong to this project
    valid = db.query(CurriculumNode).filter(
        CurriculumNode.project_id == project_id,
        CurriculumNode.id.in_(body.topic_ids),
    ).count()
    if valid == 0:
        raise HTTPException(status_code=404, detail="No valid topic IDs found for this project")

    result = retrieve_rag_context(
        project_id=project_id,
        topic_ids=body.topic_ids,
        query=body.query or "",
        top_k=body.top_k,
        max_hops=body.max_hops,
    )
    return result


# Update health endpoint to reflect Phase 3
@app.get("/phase3/status", tags=["System"])
def phase3_status():
    return {
        "phase": "3 — Curriculum Understanding + Knowledge Graph + RAG",
        "services": {
            "llm":        LLM_AVAILABLE,
            "embeddings": EMBEDDINGS_AVAILABLE,
            "qdrant":     QDRANT_AVAILABLE,
            "celery":     CELERY_AVAILABLE,
        },
        "embedding_model": EMBEDDING_MODEL if EMBEDDINGS_AVAILABLE else None,
        "new_routes": [
            "GET  /projects/{id}/curriculum              — full curriculum tree",
            "GET  /projects/{id}/curriculum/nodes        — flat node list",
            "POST /projects/{id}/curriculum/rebuild      — re-extract curriculum",
            "DELETE /projects/{id}/curriculum            — clear curriculum",
            "GET  /projects/{id}/graph/edges             — knowledge graph edges",
            "POST /projects/{id}/graph/expand            — expand topic graph",
            "GET  /projects/{id}/graph/stats             — graph statistics",
            "POST /projects/{id}/search                  — semantic search",
            "POST /projects/{id}/rag-context             — Graph-RAG context retrieval",
        ],
    }


# ═════════════════════════════════════════════════════════════
# PHASE 4 — QUESTION GENERATION ENGINE
# ═════════════════════════════════════════════════════════════
# Sections:
#   P4-A  LLM Provider layer
#   P4-B  Prompt templates
#   P4-C  Single question generator + validator
#   P4-D  Duplicate detector
#   P4-E  Blueprint slot builder
#   P4-F  Paper assembly
#   P4-G  Full generation pipeline (run_generation_pipeline)
#   P4-H  Export engine  (DOCX + PDF)
#   P4-I  Export routes  /papers/{id}/export  /papers/{id}/download
# ═════════════════════════════════════════════════════════════

import re
import io
import difflib


# ─────────────────────────────────────────────────────────────
# P4-A  LLM PROVIDER LAYER
# ─────────────────────────────────────────────────────────────

GROQ_API_KEY      = os.getenv("GROQ_API_KEY",    "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY",  "")
OPENROUTE_API_KEY = os.getenv("OPENROUTE_API_KEY", "")
OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")
DEEPSEEK_API_KEY  = os.getenv("DEEPSEEK_API_KEY", "")
MISTRAL_API_KEY   = os.getenv("MISTRAL_API_KEY", "")
CEREBRAS_API_KEY  = os.getenv("CEREBRAS_API_KEY", "")

LLM_AVAILABLE = bool(
    GEMINI_API_KEY or GROQ_API_KEY or OPENROUTE_API_KEY or 
    OPENAI_API_KEY or DEEPSEEK_API_KEY or MISTRAL_API_KEY or CEREBRAS_API_KEY
)

import requests

def check_gemini_status() -> bool:
    """Lightweight check to see if Gemini API is up and responding."""
    if not GEMINI_API_KEY:
        return False
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash?key={GEMINI_API_KEY}"
        resp = requests.get(url, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"Gemini API status check encountered an error: {e}")
        return False

def call_gemini(system: str, user: str, max_tokens: int, temperature: float) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": [{"parts": [{"text": user}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens}
    }
    for attempt in range(5):
        resp = requests.post(url, json=payload, timeout=60)
        if resp.status_code == 429:
            wait = (2 ** attempt) + random.uniform(0.5, 1.5)
            logger.warning(f"Gemini 429 rate-limit, retrying in {wait:.1f}s (attempt {attempt+1}/5)")
            time.sleep(wait)
            continue
        if resp.status_code == 503:
            wait = (2 ** attempt) + random.uniform(0.5, 1.5)
            logger.warning(f"Gemini 503 overloaded, retrying in {wait:.1f}s (attempt {attempt+1}/5)")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        # Gemini 2.5 models return multiple parts (thinking + text).
        # Extract the actual text content, skipping "thought" parts.
        candidate = data["candidates"][0]["content"]
        text_parts = []
        for part in candidate.get("parts", []):
            # Skip thinking/thought parts — they don't contain the JSON answer
            if part.get("thought", False):
                continue
            if "text" in part:
                text_parts.append(part["text"])
        if text_parts:
            return "\n".join(text_parts)
        # Fallback: return first part's text if no non-thought parts found
        return candidate["parts"][-1].get("text", "")
    raise Exception("Gemini rate-limited/overloaded after 5 retries")

def call_openai_compat(url: str, api_key: str, model: str, system: str, user: str, max_tokens: int, temperature: float) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=45)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]

def call_llm(system: str, user: str, max_tokens: int = 1500, temperature: float = 0.3, prefer_provider: str = None) -> Optional[str]:
    """
    Unified LLM call with fallback: gemini -> groq -> openai -> openroute -> deepseek -> mistral -> cerebras.
    Returns text content or None on failure.
    """
    if not LLM_AVAILABLE:
        logger.warning("No LLM API keys are configured.")
        return None

    providers = ["gemini", "groq", "openai", "openrouter", "deepseek", "mistral", "cerebras"]
    
    if prefer_provider and prefer_provider in providers:
        providers.remove(prefer_provider)
        providers.insert(0, prefer_provider)

    logger.info(f"Initiating LLM API call. Providers list: {providers}")

    if "gemini" in providers:
        logger.info("Checking Gemini API status before calling...")
        if not check_gemini_status():
            logger.warning("Gemini API is down or unreachable. Skipping Gemini.")
            providers.remove("gemini")
    for provider in providers:
        if provider == "gemini" and GEMINI_API_KEY:
            try:
                logger.info("Calling Gemini API...")
                return call_gemini(system, user, max_tokens, temperature)
            except Exception as e:
                logger.warning(f"Gemini failed, falling back... Error: {e}")
        elif provider == "groq" and GROQ_API_KEY:
            try:
                logger.info("Calling Groq API...")
                return call_openai_compat(
                    "https://api.groq.com/openai/v1/chat/completions",
                    GROQ_API_KEY,
                    "llama-3.3-70b-versatile",
                    system, user, max_tokens, temperature
                )
            except Exception as e:
                logger.warning(f"Groq failed, falling back... Error: {e}")
        elif provider == "openai" and OPENAI_API_KEY:
            try:
                logger.info("Calling OpenAI API...")
                return call_openai_compat(
                    "https://api.openai.com/v1/chat/completions",
                    OPENAI_API_KEY,
                    "gpt-4o-mini",
                    system, user, max_tokens, temperature
                )
            except Exception as e:
                logger.warning(f"OpenAI failed, falling back... Error: {e}")
        elif provider == "openrouter" and OPENROUTE_API_KEY:
            try:
                logger.info("Calling OpenRouter API...")
                return call_openai_compat(
                    "https://openrouter.ai/api/v1/chat/completions",
                    OPENROUTE_API_KEY,
                    "google/gemini-2.5-flash",
                    system, user, max_tokens, temperature
                )
            except Exception as e:
                logger.warning(f"OpenRouter failed, falling back... Error: {e}")
        elif provider == "deepseek" and DEEPSEEK_API_KEY:
            try:
                logger.info("Calling DeepSeek API...")
                return call_openai_compat(
                    "https://api.deepseek.com/chat/completions",
                    DEEPSEEK_API_KEY,
                    "deepseek-chat",
                    system, user, max_tokens, temperature
                )
            except Exception as e:
                logger.warning(f"DeepSeek failed, falling back... Error: {e}")
        elif provider == "mistral" and MISTRAL_API_KEY:
            try:
                logger.info("Calling Mistral API...")
                return call_openai_compat(
                    "https://api.mistral.ai/v1/chat/completions",
                    MISTRAL_API_KEY,
                    "mistral-small-latest",
                    system, user, max_tokens, temperature
                )
            except Exception as e:
                logger.warning(f"Mistral failed, falling back... Error: {e}")
        elif provider == "cerebras" and CEREBRAS_API_KEY:
            try:
                logger.info("Calling Cerebras API...")
                return call_openai_compat(
                    "https://api.cerebras.ai/v1/chat/completions",
                    CEREBRAS_API_KEY,
                    "llama3.1-70b",
                    system, user, max_tokens, temperature
                )
            except Exception as e:
                logger.warning(f"Cerebras failed, falling back... Error: {e}")

    logger.error("All configured LLM providers failed or none are configured.")
    return None


def parse_llm_json(raw: str) -> Optional[dict]:
    """Strip markdown fences, thinking text, and parse JSON from LLM response."""
    if not raw:
        return None
    raw = raw.strip()
    # Remove ```json ... ``` fences
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                raw = part
                break
    # Find the outermost { ... } block using brace counting
    start_idx = raw.find("{")
    if start_idx == -1:
        logger.warning(f"parse_llm_json: no '{{' found in response: {raw[:200]}")
        return None
    depth = 0
    end_idx = start_idx
    for i in range(start_idx, len(raw)):
        if raw[i] == "{":
            depth += 1
        elif raw[i] == "}":
            depth -= 1
            if depth == 0:
                end_idx = i
                break
    json_str = raw[start_idx:end_idx + 1]
    # Fix common LLM JSON issues: trailing commas before } or ]
    json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
    try:
        return json.loads(json_str, strict=False)
    except json.JSONDecodeError as e:
        logger.warning(f"parse_llm_json: JSONDecodeError: {e}\nExtracted: {json_str[:300]}")
        return None


# ─────────────────────────────────────────────────────────────
# P4-B  PROMPT TEMPLATES
# ─────────────────────────────────────────────────────────────

_QTYPE_GUIDE = {
    "mcq":        "Multiple choice with exactly 4 options (A, B, C, D). Exactly one correct answer.",
    "one_word":   "Question whose complete answer is one word or a short phrase (≤4 words).",
    "fill_blank": "Sentence with one blank (___). Answer is the missing word/phrase.",
    "short":      "Short answer requiring 2–4 sentences. Clear, concise question.",
    "long":       "Essay/long answer requiring deep analysis, comparison, or evaluation. Avoid generic textbook summaries. Use verbs like Compare, Contrast, Justify, Critique.",
    "case_study": "Presents a real-world scenario or problem. Student must analyze and respond.",
}

_BLOOM_GUIDE = {
    "remember":   "Recall facts. Use verbs: define, list, state, name, identify, what is, who invented.",
    "understand": "Explain concepts. Use verbs: explain, describe, summarize, interpret, classify.",
    "apply":      "Require the student to apply knowledge to a specific scenario, case, or problem. Provide a short scenario/context in the question. Do NOT just ask them to 'explain' with an example.",
    "analyze":    "Require the student to break down, compare, contrast, or find root causes. Avoid simple lists.",
    "evaluate":   "Require the student to judge, critique, justify, or assess a decision based on criteria.",
    "create":     "Synthesize. Use verbs: design, propose, formulate, construct, develop, create.",
}

_DIFFICULTY_GUIDE = {
    "easy":   "Straightforward basic recall or single-step reasoning (e.g., Define X).",
    "medium": "Requires solid understanding. Compare two concepts or apply to a basic problem.",
    "hard":   "Requires deep critical thinking, evaluation of trade-offs, or complex problem-solving. We will reject 'hard' questions that are just definitions.",
}

_GENERATION_SYSTEM = """You are an expert academic question paper setter with 20 years of university experience.
Generate exactly ONE examination question based on the curriculum content provided.

STRICT RULES:
1. {grounding_rule}
2. Return ONLY valid JSON — no markdown, no explanation, nothing else.
3. For MCQ: provide exactly 4 options and a correct_option index (0=A, 1=B, 2=C, 3=D).
4. For non-MCQ: set options and correct_option to null.
5. The answer must be complete and model-quality.
6. Do NOT repeat any question from the "avoid" list.
7. The question MUST be strictly about the assigned Topic or Learning Outcome. Do NOT drift into other topics mentioned in the context.
8. First, extract key concepts from the context into "extracted_concepts" to ground your reasoning.

Return this exact JSON structure:
{{
  "extracted_concepts": ["concept 1", "concept 2", "concept 3"],
  "question": "full question text here",
  "answer": "complete model answer here",
  "bloom_level": "{bloom}",
  "difficulty": "{difficulty}",
  "topic": "{topic}",
  "options": ["A) ...", "B) ...", "C) ...", "D) ..."],
  "correct_option": 0,
  "rubric": {{
    "key_points": ["point 1", "point 2", "point 3"],
    "marks_breakdown": "{marks} marks for complete answer"
  }}
}}"""


def _build_prompt(
    context: str,
    qtype: str,
    bloom: str,
    difficulty: str,
    topic: str,
    marks: int,
    avoid: List[str],
) -> tuple[str, str]:
    grounding_rule = "The question MUST be directly answerable from the provided context."
    system = _GENERATION_SYSTEM.format(
        bloom=bloom, difficulty=difficulty, topic=topic, marks=marks, grounding_rule=grounding_rule
    )

    avoid_block = ""
    if avoid:
        avoid_block = "\n\nDo NOT generate questions similar to:\n" + "\n".join(
            f"- {q[:120]}" for q in avoid[-8:]
        )

    user = (
        f"Question requirements:\n"
        f"  Type       : {qtype} — {_QTYPE_GUIDE.get(qtype, qtype)}\n"
        f"  Bloom level: {bloom.upper()} — {_BLOOM_GUIDE.get(bloom, '')}\n"
        f"  Difficulty : {difficulty.upper()} — {_DIFFICULTY_GUIDE.get(difficulty, '')}\n"
        f"  Marks      : {marks}\n"
        f"  Topic      : {topic}\n"
        f"\nSTRICT TOPIC RULE: Focus ONLY on '{topic}'. Do not include questions from unrelated topics.\n"
        f"\nCurriculum context (use this as the knowledge source):\n"
        f"{context[:3000]}"
        f"{avoid_block}"
    )
    return system, user


# ─────────────────────────────────────────────────────────────
# P4-C  SINGLE QUESTION GENERATOR + VALIDATOR
# ─────────────────────────────────────────────────────────────

_BLOOM_VERBS = {
    "remember":   ["define","list","state","name","identify","recall","what is","who","when","where"],
    "understand": ["explain","describe","summarize","interpret","discuss","classify","outline"],
    "apply":      ["solve","calculate","use","apply","demonstrate","show","compute","implement"],
    "analyze":    ["compare","contrast","differentiate","examine","why","what causes","analyze","break down"],
    "evaluate":   ["justify","critique","assess","evaluate","recommend","defend","judge","argue"],
    "create":     ["design","create","construct","formulate","propose","develop","build","plan"],
}

_STOPWORDS = {"the","a","an","is","are","of","to","in","it","that","this","and","or","for","with","be"}


def _validate_question(
    q: dict,
    bloom: str,
    difficulty: str,
    context: str,
) -> tuple[bool, str]:
    """Returns (is_valid, rejection_reason)."""
    text   = (q.get("question") or "").strip()
    answer = (q.get("answer")   or "").strip()
    qtype  = q.get("bloom_level", bloom)

    if len(text) < 12:
        return False, "Question too short"
    if len(answer) < 5:
        return False, "Answer missing or too short"
    if text.lower() == answer.lower():
        return False, "Question is identical to answer"

    # MCQ must have 4 options
    if bloom == "mcq" or q.get("options"):
        opts = q.get("options") or []
        if len(opts) != 4:
            return False, f"MCQ must have exactly 4 options, got {len(opts)}"
        co = q.get("correct_option")
        if co is None or not (0 <= int(co) <= 3):
            return False, "MCQ correct_option must be 0-3"

    q_lower = text.lower()
    
    # Strict Bloom Validation
    bloom_verbs = _BLOOM_VERBS.get(bloom, [])
    if bloom_verbs:
        if not any(v in q_lower for v in bloom_verbs):
            return False, f"Question does not match Bloom level '{bloom}' (missing required action verb)"
            
    if bloom == "apply":
        scenario_words = ["suppose", "given", "consider", "scenario", "assume", "imagine", "calculate", "determine"]
        if not any(sw in q_lower for sw in scenario_words):
            return False, "Apply-level questions require a scenario or problem (e.g., Suppose, Given, Consider, Calculate)"

    # Strict Difficulty Validation
    word_count = len(text.split())
    if difficulty == "easy":
        if word_count > 25:
            return False, "Easy questions should be direct and concise (<25 words)"
    elif difficulty == "hard":
        if bloom in ["remember", "understand"]:
            return False, f"Hard difficulty cannot be used with basic Bloom level '{bloom}'"
        if len(answer.split()) < 20:
            return False, "Hard questions should require detailed, multi-step answers (>20 words)"

    # Answerability: check both Question and Answer grounding
    if EMBEDDINGS_AVAILABLE and embed_model:
        try:
            vecs = embed_model.encode([text[:500], answer[:500], context[:2000]], normalize_embeddings=True)
            sim_q = float(vecs[0] @ vecs[2])
            sim_ans = float(vecs[1] @ vecs[2])
            if sim_q < 0.20 or sim_ans < 0.25:
                return False, f"Not semantically grounded (sim_q={sim_q:.2f}, sim_ans={sim_ans:.2f})"
        except Exception as emb_err:
            logger.debug(f"Embedding grounding check failed, using keyword fallback: {emb_err}")
            # Fall through to keyword check below
            ans_words = set(answer.lower().split()) - _STOPWORDS
            ctx_words = set(context.lower().split())
            overlap = len(ans_words & ctx_words)
            if len(ans_words) > 8 and overlap < 2:
                return False, f"Answer not grounded in context (overlap={overlap})"
    else:
        # Relaxed keyword overlap — only reject if truly zero/near-zero overlap
        ans_words = set(answer.lower().split()) - _STOPWORDS
        ctx_words = set(context.lower().split())
        overlap = len(ans_words & ctx_words)
        if len(ans_words) > 8 and overlap < 2:
            return False, f"Answer not grounded in context (overlap={overlap})"

    return True, ""


def generate_one_question(
    context: str,
    qtype: str,
    bloom: str,
    difficulty: str,
    topic: str,
    marks: int,
    project_id: str,
    avoid: List[str],
    max_retries: int = 15,
) -> Optional[dict]:
    """Generate + validate one question. Returns dict or None after retries."""
    for attempt in range(max_retries):
        system, user = _build_prompt(context, qtype, bloom, difficulty, topic, marks, avoid)
        provider = "groq" if attempt >= 3 else None
        
        raw = call_llm(system, user, max_tokens=1200, temperature=0.4 + (attempt % 5) * 0.1, prefer_provider=provider)
        if not raw:
            logger.warning(f"  Slot attempt {attempt+1}: LLM returned nothing")
            continue

        q = parse_llm_json(raw)
        if not q:
            logger.warning(f"  Slot attempt {attempt+1}: JSON parse failed. Raw (first 300 chars): {raw[:300]}")
            continue

        ok, reason = _validate_question(q, bloom, difficulty, context)
        if not ok:
            logger.info(f"  Slot attempt {attempt+1}: rejected — {reason}")
            continue

        if _is_duplicate(q.get("question",""), project_id):
            logger.info(f"  Slot attempt {attempt+1}: duplicate detected, retrying")
            avoid.append(q.get("question",""))
            continue

        q["_attempts"] = attempt + 1
        return q

    return None


# ─────────────────────────────────────────────────────────────
# P4-D  DUPLICATE DETECTOR
# ─────────────────────────────────────────────────────────────

def _is_duplicate(question_text: str, project_id: str, threshold: float = 0.82) -> bool:
    """Embedding-based duplicate detection with Jaccard fallback."""
    if not question_text:
        return False
    db = SessionLocal()
    try:
        existing = db.query(QuestionBankItem.question_text).filter(
            QuestionBankItem.project_id == project_id
        ).all()
        if not existing:
            return False

        # Try embedding similarity (more accurate, fewer false positives)
        if EMBEDDINGS_AVAILABLE and embed_model:
            try:
                all_texts = [question_text] + [e[0] for e in existing[:50]]  # cap at 50
                vecs = embed_model.encode(all_texts, normalize_embeddings=True)
                q_vec = vecs[0]
                for i in range(1, len(vecs)):
                    sim = float(q_vec @ vecs[i])
                    if sim >= 0.92:
                        logger.info(f"  Duplicate detected via embedding (sim={sim:.3f})")
                        return True
                return False
            except Exception as e:
                logger.debug(f"Embedding duplicate check failed, using Jaccard: {e}")

        # Jaccard fallback (relaxed threshold)
        q_words = set(question_text.lower().split()) - _STOPWORDS
        for (e_text,) in existing:
            e_words = set(e_text.lower().split()) - _STOPWORDS
            if not q_words or not e_words:
                continue
            union = len(q_words | e_words)
            if union == 0:
                continue
            jaccard = len(q_words & e_words) / union
            if jaccard >= 0.88:  # relaxed from 0.82 to reduce false positives
                return True
        return False
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# P4-E  BLUEPRINT SLOT BUILDER
# ─────────────────────────────────────────────────────────────

def build_slots(blueprint: dict, cos: List[CourseOutcome] = None) -> List[dict]:
    """
    Convert blueprint JSON into a flat ordered list of generation slots.
    Each slot: {type, marks, bloom, difficulty, topic, section, co_id, co_description}
    """
    specs         = blueprint.get("question_specs", [])
    bloom_targets = blueprint.get("bloom_targets",  {})
    diff_targets  = blueprint.get("difficulty_targets", {"easy":30,"medium":50,"hard":20})
    topics        = blueprint.get("topics", [])

    if isinstance(diff_targets, dict):
        diff_pcts = diff_targets
    else:
        diff_pcts = {"easy": getattr(diff_targets,"easy",30),
                     "medium": getattr(diff_targets,"medium",50),
                     "hard": getattr(diff_targets,"hard",20)}

    # Build flat slot list from specs
    slots = []
    for spec in specs:
        count = spec.get("count", 0)
        for _ in range(count):
            slots.append({
                "type":    spec.get("type", "short"),
                "marks":   spec.get("marks_each", 2),
                "section": spec.get("type", "short").upper(),
            })

    n = len(slots)
    if n == 0:
        return []

    # Distribute Bloom levels
    bloom_order = ["remember","understand","apply","analyze","evaluate","create"]
    bloom_pool  = []
    for bl in bloom_order:
        pct   = bloom_targets.get(bl, 0)
        count = round(pct * n / 100)
        bloom_pool.extend([bl] * count)
    while len(bloom_pool) < n:
        bloom_pool.append("understand")
    bloom_pool = bloom_pool[:n]

    # Distribute difficulty
    diff_pool = []
    for df in ["easy","medium","hard"]:
        pct   = diff_pcts.get(df, 0)
        count = round(pct * n / 100)
        diff_pool.extend([df] * count)
    while len(diff_pool) < n:
        diff_pool.append("medium")
    diff_pool = diff_pool[:n]

    # Assign topics round-robin
    topic_list = topics if topics else ["General"]
    co_list = cos if cos else []
    for i, slot in enumerate(slots):
        slot["bloom"]      = bloom_pool[i]
        slot["difficulty"] = diff_pool[i]
        slot["topic"]      = topic_list[i % len(topic_list)]
        if co_list:
            co = co_list[i % len(co_list)]
            slot["co_id"] = co.id
            slot["co_description"] = co.description

    return slots


# ─────────────────────────────────────────────────────────────
# P4-F  PAPER ASSEMBLY
# ─────────────────────────────────────────────────────────────

def assemble_paper(paper_id: str, generated: List[dict], db: Session):
    """
    Persist generated questions to DB in blueprint order.
    Groups by section (question type), assigns order_index.
    """
    # Group by section label
    sections: dict[str, List[dict]] = {}
    for gq in generated:
        sec = gq["slot"].get("section", "GENERAL")
        sections.setdefault(sec, []).append(gq)

    order_idx = 0
    for sec_label, sec_questions in sections.items():
        for sq in sec_questions:
            slot = sq["slot"]
            d    = sq["data"]

            qtype_val = slot["type"]
            if qtype_val not in [e.value for e in QuestionType]:
                qtype_val = "short"

            bloom_val = slot.get("bloom")
            if bloom_val not in [e.value for e in BloomLevel]:
                bloom_val = None

            diff_val = slot.get("difficulty")
            if diff_val not in [e.value for e in Difficulty]:
                diff_val = None

            q = Question(
                paper_id      = paper_id,
                order_index   = order_idx,
                question_text = d.get("question", ""),
                answer_text   = d.get("answer",   ""),
                question_type = QuestionType(qtype_val),
                bloom_level   = BloomLevel(bloom_val) if bloom_val else None,
                difficulty    = Difficulty(diff_val)  if diff_val  else None,
                marks         = slot["marks"],
                topic         = slot.get("topic", ""),
                options       = d.get("options"),
                correct_option= d.get("correct_option"),
                rubric        = d.get("rubric"),
                quality_scores= {"attempts": d.get("_attempts", 1)},
                is_accepted   = True,
            )
            db.add(q)
            order_idx += 1
    db.flush()


# ─────────────────────────────────────────────────────────────
# P4-F2  PER-SLOT CONTEXT RETRIEVAL
# ─────────────────────────────────────────────────────────────

def _get_slot_context(project_id: str, slot: dict, fallback_text: str, db: Session, used_chunks: set) -> str:
    """
    Retrieve topic/CO-specific context for a single generation slot.
    Uses embedding search when available, else keyword-filters the fallback text.
    Filters out chunks already present in `used_chunks`.
    """
    topic = slot.get("topic", "General")
    co_desc = slot.get("co_description", "")
    
    query_text = f"Learning outcome: {co_desc}" if co_desc else f"{topic} key concepts definitions examples principles"
    keyword_text = co_desc if co_desc else topic

    # Try embedding-based search for this specific topic/CO
    if EMBEDDINGS_AVAILABLE and QDRANT_AVAILABLE:
        try:
            q_vec = embed_texts([query_text])
            if q_vec:
                results = search_qdrant(
                    q_vec[0], top_k=15,
                    filter_payload={"project_id": project_id}
                )
                if results:
                    chunks = []
                    for r in results:
                        text = r["payload"].get("text", "")
                        if text and text not in used_chunks:
                            chunks.append(text)
                            used_chunks.add(text)
                        if len(chunks) >= 8:
                            break
                    if chunks:
                        context = "\n\n---\n\n".join(chunks)[:4000]
                        logger.info(f"  Per-slot context: {len(chunks)} chunks via embedding for '{keyword_text[:30]}...'")
                        return context
        except Exception as e:
            logger.debug(f"Per-slot embedding search failed for '{keyword_text[:30]}...': {e}")

    # Fallback: keyword-filter the raw text to find relevant paragraphs
    paragraphs = fallback_text.split("\n\n")
    topic_words = set(keyword_text.lower().split()) - _STOPWORDS
    if not topic_words or keyword_text.lower() == "general":
        # No specific topic — return a varied sample from the full text
        return fallback_text[:4000]

    scored = []
    for para in paragraphs:
        para = para.strip()
        if not para or len(para) < 30 or para in used_chunks:
            continue
        para_words = set(para.lower().split())
        overlap = len(topic_words & para_words)
        scored.append((overlap, para))
    scored.sort(key=lambda x: x[0], reverse=True)
    top_paras = [p for _, p in scored[:15]]
    for p in top_paras:
        used_chunks.add(p)
    if top_paras:
        result = "\n\n".join(top_paras)[:4000]
        logger.info(f"  Per-slot context: {len(top_paras)} paragraphs via keyword for '{keyword_text[:30]}...'")
        return result
    return fallback_text[:4000]


# ─────────────────────────────────────────────────────────────
# P4-G  FULL GENERATION PIPELINE
# ─────────────────────────────────────────────────────────────

def run_generation_pipeline(paper_id: str, task_id: str):
    """
    End-to-end question generation.
    Runs in FastAPI BackgroundTask (no Celery required).

    Steps:
      1. Load blueprint + documents
      2. Build Graph-RAG context (or raw text fallback)
      3. Build slot list from blueprint
      4. Generate + validate each slot (up to 3 retries each)
      5. Assemble paper in DB
      6. Accumulate to question bank
      7. Push SSE progress at each step
    """
    db = SessionLocal()
    paper = None
    try:
        paper = db.query(Paper).filter(Paper.id == paper_id).first()
        if not paper:
            return

        project_id = paper.project_id
        blueprint  = paper.blueprint or {}

        chap_count = db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id, CurriculumNode.node_type == NodeType.chapter).count()
        chap_text = f"✓ {chap_count} Chapters Identified" if chap_count else "✓ Chapters Identified"
        push_progress(task_id, "start", f"Analyzing Curriculum... {chap_text}", "running")
        time.sleep(0.5)

        concept_count = db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id).count()
        push_progress(task_id, "graph", f"Building Knowledge Graph... ✓ {concept_count} Concepts Linked", "running")
        time.sleep(0.5)

        # Try Graph-RAG first
        combined_context = ""
        topics_requested = blueprint.get("topics", [])

        try:
            topic_nodes = db.query(CurriculumNode).filter(
                CurriculumNode.project_id == project_id,
                CurriculumNode.node_type.in_([NodeType.topic, NodeType.subtopic]),
            ).all()

            seed_ids = []
            if topics_requested and topic_nodes:
                seed_ids = [
                    n.id for n in topic_nodes
                    if any(
                        t.lower() in n.label.lower() or n.label.lower() in t.lower()
                        for t in topics_requested
                    )
                ]
            if not seed_ids:
                seed_ids = [n.id for n in topic_nodes[:15]]

            if seed_ids:
                topic_labels = [n.label for n in topic_nodes if n.id in seed_ids]
                query        = "Key concepts, definitions, examples and principles for: " + \
                               ", ".join(topic_labels)
                rag = retrieve_rag_context(
                    project_id=project_id,
                    topic_ids=seed_ids,
                    query=query,
                    top_k=max(20, len(topic_labels) * 5),
                    max_hops=2,
                )
                combined_context = rag.combined_context or ""
        except Exception as e:
            logger.warning(f"Graph-RAG failed, falling back to raw text: {e}")

        # Fallback: raw extracted text from documents
        if not combined_context.strip():
            docs = db.query(Document).filter(
                Document.project_id == project_id,
                Document.status == DocumentStatus.ready,
            ).order_by(Document.uploaded_at).all()
            combined_context = "\n\n---\n\n".join(
                (d.extracted_text or "").strip() for d in docs if d.extracted_text
            )[:8000]

        if not combined_context.strip():
            push_progress(task_id, "error",
                "No document content found. Upload and process documents first.", "failed")
            paper.status = PaperStatus.failed
            paper.error_message = "No document content available for generation."
            db.commit()
            return

        word_count = len(combined_context.split())
        word_count = len(combined_context.split())

        # ── Step 2: Build slots ─────────────────────────────
        cos = db.query(CourseOutcome).filter(CourseOutcome.project_id == project_id).all()
        slots = build_slots(blueprint, cos)
        if not slots:
            push_progress(task_id, "error",
                "Blueprint has no question specs. Edit the paper blueprint first.", "failed")
            paper.status = PaperStatus.failed
            paper.error_message = "Blueprint has no question_specs."
            db.commit()
            return

        push_progress(task_id, "plan",
            f"Generating Assessment Blueprint... ✓ Difficulty Distribution Planned",
            "running", {"total_slots": len(slots)})
        time.sleep(0.5)

        # ── Step 3: Generate each slot ──────────────────────
        generated   = []
        failed_slots = 0
        avoid_texts  = []
        used_chunks  = set()

        for i, slot in enumerate(slots):
            push_progress(
                task_id, "generate",
                f"Creating Questions... ✓ {i} Questions Generated",
                "running",
                {"current": i+1, "total": len(slots),
                 "type": slot["type"], "bloom": slot["bloom"],
                 "difficulty": slot["difficulty"]},
            )

            # Per-slot topic-specific context retrieval
            slot_context = _get_slot_context(project_id, slot, combined_context, db, used_chunks)

            topic = slot.get("topic", "General")
            co_desc = slot.get("co_description", "")
            target_concept = co_desc if co_desc else topic

            q_data = None
            total_attempts_for_slot = 0
            while not q_data:
                q_data = generate_one_question(
                    context    = slot_context,
                    qtype      = slot["type"],
                    bloom      = slot["bloom"],
                    difficulty = slot["difficulty"],
                    topic      = target_concept,
                    marks      = slot["marks"],
                    project_id = project_id,
                    avoid      = avoid_texts,
                )
                if not q_data:
                    total_attempts_for_slot += 3
                    logger.warning(f"  ✗ Slot {i+1} failed retries. Retrying generation indefinitely...")
                    time.sleep(2)

            q_data['_attempts'] = q_data.get('_attempts', 1) + total_attempts_for_slot
            generated.append({"slot": slot, "data": q_data})
            avoid_texts.append(q_data.get("question", ""))
            logger.info(
                f"  ✓ Slot {i+1}: {slot['type']} | "
                f"{slot['bloom']} | {slot['difficulty']} "
                f"(attempt {q_data.get('_attempts',1)})"
            )

            # Inter-slot delay to avoid rate limiting
            if i < len(slots) - 1:
                time.sleep(1.5)

        if not generated:
            push_progress(task_id, "error",
                "All generation attempts failed. Check your API key and document content.",
                "failed")
            paper.status = PaperStatus.failed
            paper.error_message = (
                "Generation failed — verify GROQ_API_KEY or OPENAI_API_KEY in .env "
                "and ensure documents contain readable text."
            )
            db.commit()
            return

        accepted_count = len(generated)
        regenerated_count = failed_slots + sum([g.get("data", {}).get("_attempts", 1) - 1 for g in generated])

        push_progress(task_id, "validate",
            f"Validating Bloom Levels... ✓ {accepted_count} Accepted",
            "running")
        time.sleep(0.5)

        push_progress(task_id, "coverage",
            f"Checking Coverage... ✓ {regenerated_count} Questions Regenerated",
            "running")
        time.sleep(0.5)

        # ── Step 4: Assemble + persist ──────────────────────
        push_progress(task_id, "assemble",
            f"Assembling Final Paper... ✓ Complete",
            "running",
            {"generated": len(generated), "failed": failed_slots})

        assemble_paper(paper_id, generated, db)

        # ── Step 5: Accumulate question bank ────────────────
        for gq in generated:
            slot   = gq["slot"]
            d      = gq["data"]
            q_text = d.get("question", "")
            if q_text and not _is_duplicate(q_text, project_id, threshold=0.95):
                qtype_val = slot["type"]
                if qtype_val not in [e.value for e in QuestionType]:
                    qtype_val = "short"
                bloom_val = slot.get("bloom")
                if bloom_val not in [e.value for e in BloomLevel]:
                    bloom_val = None
                diff_val = slot.get("difficulty")
                if diff_val not in [e.value for e in Difficulty]:
                    diff_val = None
                db.add(QuestionBankItem(
                    project_id    = project_id,
                    question_text = q_text,
                    answer_text   = d.get("answer", ""),
                    question_type = QuestionType(qtype_val),
                    bloom_level   = BloomLevel(bloom_val) if bloom_val else None,
                    difficulty    = Difficulty(diff_val)  if diff_val  else None,
                    marks         = slot["marks"],
                    topic         = slot.get("topic", ""),
                    feedback      = "accepted",
                ))

        paper.status = PaperStatus.ready
        paper.error_message = None
        db.commit()

        push_progress(
            task_id, "done",
            f"✓ Paper ready — {len(generated)} questions generated!",
            "done",
            {"generated": len(generated), "failed": failed_slots, "paper_id": paper_id},
        )
        logger.info(f"Paper {paper_id} complete: {len(generated)}/{len(slots)} questions")

    except Exception as e:
        logger.error(f"Generation pipeline error (paper={paper_id}): {e}", exc_info=True)
        push_progress(task_id, "error", f"Pipeline error: {str(e)}", "failed")
        try:
            if paper:
                paper.status = PaperStatus.failed
                paper.error_message = str(e)
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# P4-H  EXPORT ENGINE  (DOCX + PDF)
# ─────────────────────────────────────────────────────────────

def _build_paper_docx(paper: Paper, questions: List[Question], include_answers: bool = False) -> bytes:
    """
    Generate a DOCX question paper using python-docx.
    Returns bytes of the .docx file.
    """
    try:
        from docx import Document as DocxDoc
        from docx.shared import Pt, Inches, RGBColor
        from docx.enum.text import WD_ALIGN_PARAGRAPH
    except ImportError:
        raise HTTPException(status_code=503,
            detail="python-docx not installed. Run: pip install python-docx")

    doc = DocxDoc()

    # ── Header ─────────────────────────────────────────────
    title_para = doc.add_paragraph()
    title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title_para.add_run(paper.title or "Question Paper")
    run.bold     = True
    run.font.size = Pt(16)

    meta_para = doc.add_paragraph()
    meta_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    meta_para.add_run(
        f"Total Marks: {paper.total_marks}  |  Duration: {paper.duration_mins} minutes"
    ).font.size = Pt(11)

    doc.add_paragraph()  # spacer

    # ── Group questions by type ────────────────────────────
    sections: dict[str, List[Question]] = {}
    for q in questions:
        sec = q.question_type.upper() if hasattr(q.question_type, 'upper') else str(q.question_type).upper()
        sections.setdefault(sec, []).append(q)

    section_labels = {
        "MCQ":        "Part A — Multiple Choice Questions",
        "ONE_WORD":   "Part B — One Word / Short Answer",
        "FILL_BLANK": "Part C — Fill in the Blanks",
        "SHORT":      "Part D — Short Answer Questions",
        "LONG":       "Part E — Long Answer / Essay Questions",
        "CASE_STUDY": "Part F — Case Study",
    }

    q_number = 1
    for sec_key, sec_qs in sections.items():
        label = section_labels.get(sec_key, f"Section — {sec_key.title()}")

        # Section heading
        sec_para = doc.add_paragraph()
        run = sec_para.add_run(label)
        run.bold = True
        run.font.size = Pt(13)
        sec_para.paragraph_format.space_before = Pt(12)

        for q in sec_qs:
            # Question line
            q_para = doc.add_paragraph()
            q_para.paragraph_format.space_before = Pt(4)
            run = q_para.add_run(f"Q{q_number}. ")
            run.bold = True
            q_para.add_run(q.question_text or "")
            q_para.add_run(f"  [{q.marks} mark{'s' if q.marks != 1 else ''}]").bold = True

            # MCQ options
            if q.options:
                for opt in q.options:
                    opt_para = doc.add_paragraph(style="List Bullet")
                    opt_para.add_run(str(opt))

            # Answer (if answer key mode)
            if include_answers and q.answer_text:
                ans_para = doc.add_paragraph()
                ans_run  = ans_para.add_run(f"Answer: {q.answer_text}")
                ans_run.italic = True
                ans_run.font.color.rgb = RGBColor(0x22, 0x8B, 0x22)

            q_number += 1

    # ── Footer ─────────────────────────────────────────────
    doc.add_paragraph()
    footer_para = doc.add_paragraph("— End of Question Paper —")
    footer_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    footer_para.runs[0].italic = True

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.read()


def _build_rubric_docx(paper: Paper, questions: List[Question]) -> bytes:
    """Generate a marking scheme / rubric DOCX."""
    try:
        from docx import Document as DocxDoc
        from docx.shared import Pt
        from docx.enum.text import WD_ALIGN_PARAGRAPH
    except ImportError:
        raise HTTPException(status_code=503, detail="python-docx not installed")

    doc = DocxDoc()

    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title.add_run(f"Marking Scheme — {paper.title or 'Question Paper'}")
    run.bold = True
    run.font.size = Pt(15)
    doc.add_paragraph(f"Total Marks: {paper.total_marks}").alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.add_paragraph()

    for i, q in enumerate(questions, 1):
        # Question
        q_para = doc.add_paragraph()
        q_para.paragraph_format.space_before = Pt(8)
        run = q_para.add_run(f"Q{i}. [{q.marks} mark{'s' if q.marks != 1 else ''}] ")
        run.bold = True
        q_para.add_run(q.question_text or "")

        # Answer
        if q.answer_text:
            ans = doc.add_paragraph()
            ans.add_run("Model Answer: ").bold = True
            ans.add_run(q.answer_text)

        # Rubric key points
        if q.rubric and isinstance(q.rubric, dict):
            kp = q.rubric.get("key_points", [])
            if kp:
                doc.add_paragraph("Key Points:").runs[0].bold = True
                for point in kp:
                    doc.add_paragraph(f"  • {point}")
            mb = q.rubric.get("marks_breakdown")
            if mb:
                doc.add_paragraph(f"  Marks: {mb}").runs[0].italic = True

        # MCQ correct answer
        if q.options and q.correct_option is not None:
            opts = q.options
            idx  = q.correct_option
            if 0 <= idx < len(opts):
                ca = doc.add_paragraph()
                ca.add_run("Correct Answer: ").bold = True
                ca.add_run(str(opts[idx]))

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────
# P4-I  EXPORT ROUTES
# ─────────────────────────────────────────────────────────────

from fastapi.responses import Response as FastAPIResponse


@app.post("/projects/{project_id}/papers/{paper_id}/export", tags=["Export"])
def export_paper(
    project_id: str,
    paper_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Generate and save DOCX exports for a paper.
    Creates: question paper DOCX + answer key DOCX + rubric DOCX.
    Returns paths and metadata.
    """
    get_project_or_404(project_id, current_user, db)
    paper = db.query(Paper).filter(
        Paper.id == paper_id, Paper.project_id == project_id
    ).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
    if paper.status != PaperStatus.ready:
        raise HTTPException(status_code=400,
            detail=f"Paper is not ready (status={paper.status}). Generate questions first.")

    questions = db.query(Question).filter(
        Question.paper_id == paper_id,
        Question.is_accepted == True,
    ).order_by(Question.order_index).all()

    if not questions:
        raise HTTPException(status_code=400, detail="No accepted questions found in this paper.")

    export_dir = UPLOAD_DIR / project_id / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    saved = {}

    # Question paper (no answers)
    try:
        qp_bytes = _build_paper_docx(paper, questions, include_answers=False)
        qp_path  = export_dir / f"{paper_id}_paper.docx"
        qp_path.write_bytes(qp_bytes)
        paper.docx_path = str(qp_path)
        saved["question_paper"] = str(qp_path)
    except Exception as e:
        logger.error(f"Question paper export failed: {e}")
        saved["question_paper_error"] = str(e)

    # Answer key
    try:
        ak_bytes = _build_paper_docx(paper, questions, include_answers=True)
        ak_path  = export_dir / f"{paper_id}_answer_key.docx"
        ak_path.write_bytes(ak_bytes)
        paper.answer_key_docx_path = str(ak_path)
        saved["answer_key"] = str(ak_path)
    except Exception as e:
        logger.error(f"Answer key export failed: {e}")
        saved["answer_key_error"] = str(e)

    # Rubric / marking scheme
    try:
        rb_bytes = _build_rubric_docx(paper, questions)
        rb_path  = export_dir / f"{paper_id}_rubric.docx"
        rb_path.write_bytes(rb_bytes)
        saved["rubric"] = str(rb_path)
    except Exception as e:
        logger.error(f"Rubric export failed: {e}")
        saved["rubric_error"] = str(e)

    paper.status = PaperStatus.exported
    db.commit()

    return {
        "message":       "Export complete",
        "paper_id":      paper_id,
        "question_count": len(questions),
        "files":         saved,
        "download_urls": {
            k: f"/projects/{project_id}/papers/{paper_id}/download?file={k}"
            for k in saved if not k.endswith("_error")
        },
    }


@app.get("/projects/{project_id}/papers/{paper_id}/download", tags=["Export"])
def download_paper_file(
    project_id: str,
    paper_id: str,
    file: str = "question_paper",
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Download exported DOCX files.
    file param: question_paper | answer_key | rubric
    """
    get_project_or_404(project_id, current_user, db)
    paper = db.query(Paper).filter(
        Paper.id == paper_id, Paper.project_id == project_id
    ).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    file_map = {
        "question_paper": paper.docx_path,
        "answer_key":     paper.answer_key_docx_path,
    }
    # Rubric path derived
    export_dir = UPLOAD_DIR / project_id / "exports"
    file_map["rubric"] = str(export_dir / f"{paper_id}_rubric.docx")

    file_path = file_map.get(file)
    if not file_path or not Path(file_path).exists():
        raise HTTPException(
            status_code=404,
            detail=f"File '{file}' not found. Call POST /export first to generate files."
        )

    filename_labels = {
        "question_paper": f"{paper.title or 'paper'}_questions.docx",
        "answer_key":     f"{paper.title or 'paper'}_answer_key.docx",
        "rubric":         f"{paper.title or 'paper'}_rubric.docx",
    }
    filename = filename_labels.get(file, f"{file}.docx")
    # Sanitise filename
    filename = re.sub(r'[^\w\-_. ]', '_', filename)

    return FileResponse(
        path=file_path,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=filename,
    )


@app.get("/phase4/status", tags=["System"])
def phase4_status():
    return {
        "phase":        "4 — Question Generation Engine + Export",
        "llm_available": LLM_AVAILABLE,
        "llm_provider":  _llm_provider,
        "llm_model":     _llm_model,
        "pipeline": [
            "1. Graph-RAG context (falls back to raw text)",
            "2. Blueprint → flat slot list (type, marks, bloom, difficulty, topic)",
            "3. LLM generation per slot (up to 3 retries)",
            "4. Validation: length, bloom, answerability, duplicate check",
            "5. Paper assembly + DB persistence",
            "6. Question bank accumulation",
            "7. DOCX export: question paper + answer key + rubric",
        ],
        "env_required": [
            "GROQ_API_KEY  (free at console.groq.com — recommended)",
            "or OPENAI_API_KEY",
            "or ANTHROPIC_API_KEY",
        ],
        "new_routes": [
            "POST /projects/{id}/papers/{pid}/generate   — start generation",
            "GET  /stream/task/{task_id}                 — SSE live progress",
            "POST /projects/{id}/papers/{pid}/export     — generate DOCX files",
            "GET  /projects/{id}/papers/{pid}/download   — download DOCX",
        ],
    }


# ─────────────────────────────────────────────────────────────
# 15. ANALYTICS ROUTES
# ─────────────────────────────────────────────────────────────

from pydantic import BaseModel
from typing import Dict

class AnalyticsSummary(BaseModel):
    total_papers: int
    total_questions: int
    bloom_distribution: Dict[str, int]
    difficulty_distribution: Dict[str, int]
    topic_distribution: Dict[str, int]
    question_type_distribution: Dict[str, int]

@app.get("/projects/{project_id}/analytics", response_model=AnalyticsSummary, tags=["Analytics"])
def get_analytics(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    
    total_papers = db.query(Paper).filter(Paper.project_id == project_id).count()
    
    questions = db.query(QuestionBankItem).filter(QuestionBankItem.project_id == project_id).all()
    total_questions = len(questions)
    
    bloom_dist = {}
    diff_dist = {}
    topic_dist = {}
    qtype_dist = {}
    
    for q in questions:
        # bloom
        if q.bloom_level:
            bl = q.bloom_level.value
            bloom_dist[bl] = bloom_dist.get(bl, 0) + 1
            
        # difficulty
        if q.difficulty:
            df = q.difficulty.value
            diff_dist[df] = diff_dist.get(df, 0) + 1
            
        # topic
        if q.topic:
            tp = q.topic
            topic_dist[tp] = topic_dist.get(tp, 0) + 1
            
        # type
        if q.question_type:
            qt = q.question_type.value
            qtype_dist[qt] = qtype_dist.get(qt, 0) + 1
            
    return AnalyticsSummary(
        total_papers=total_papers,
        total_questions=total_questions,
        bloom_distribution=bloom_dist,
        difficulty_distribution=diff_dist,
        topic_distribution=topic_dist,
        question_type_distribution=qtype_dist,
    )


@app.get("/projects/{project_id}/analytics/coverage", tags=["Analytics"])
def get_curriculum_coverage(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(project_id, current_user, db)
    
    nodes = db.query(CurriculumNode).filter(CurriculumNode.project_id == project_id).all()
    questions = db.query(QuestionBankItem).filter(QuestionBankItem.project_id == project_id).all()
    
    topic_counts = {}
    for q in questions:
        if q.topic:
            topic = q.topic.lower()
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            
    coverage = {}
    for node in nodes:
        node_label = node.label.lower()
        count = sum(1 for topic, c in topic_counts.items() if node_label in topic or topic in node_label)
        val = min((count / 3) * 100, 100.0) if count > 0 else 0
        coverage[node.id] = int(val)
        
    return coverage