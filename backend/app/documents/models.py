import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, Enum, ForeignKey, JSON
from sqlalchemy.orm import relationship
from enum import Enum as PyEnum
from app.core.database import Base

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
    # Note: CurriculumNodes and ConceptEdges are now moving to Neo4j. We might not need this cascade here.
    # We will remove the curriculum_nodes relationship later when we fully transition to Neo4j.
    # curriculum_nodes = relationship("CurriculumNode", cascade="all, delete-orphan")
    pages           = relationship("DocumentPage", cascade="all, delete-orphan", back_populates="document")

class DocumentPage(Base):
    __tablename__ = "document_pages"
    id            = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    document_id   = Column(String, ForeignKey("documents.id"), nullable=False)
    physical_page = Column(Integer, nullable=False)
    text          = Column(Text, nullable=False)

    document = relationship("Document", back_populates="pages")
