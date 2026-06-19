import uuid
from pathlib import Path
from sqlalchemy.orm import Session
from fastapi import HTTPException, UploadFile
from app.documents.models import Document, DocumentStatus, DocumentType
from app.core.config import settings
from app.core.events import publish_event
import logging

logger = logging.getLogger(__name__)

def get_document_or_404(db: Session, document_id: str, project_id: str) -> Document:
    doc = db.query(Document).filter(
        Document.id == document_id,
        Document.project_id == project_id
    ).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return doc

def list_documents(db: Session, project_id: str):
    return db.query(Document).filter(Document.project_id == project_id).all()

async def upload_document(db: Session, project_id: str, file: UploadFile, doc_type: DocumentType) -> Document:
    suffix = Path(file.filename).suffix.lower()
    if suffix not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"File type '{suffix}' not allowed")

    content = await file.read()
    if len(content) > settings.MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File exceeds {settings.MAX_FILE_SIZE_MB}MB limit")

    unique_name = f"{uuid.uuid4()}{suffix}"
    project_dir = settings.UPLOAD_DIR / project_id
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

    # Emit the event!
    publish_event("document.uploaded", {
        "document_id": doc.id,
        "project_id": project_id,
        "file_path": str(file_path),
        "task_id": task_id
    })

    return doc

def delete_document(db: Session, document_id: str, project_id: str):
    doc = get_document_or_404(db, document_id, project_id)
    # TODO: Also delete associated chunks from Qdrant, nodes from Neo4j
    file_path = Path(doc.file_path)
    if file_path.exists():
        try:
            file_path.unlink()
        except Exception as e:
            logger.error(f"Failed to delete file {file_path}: {e}")
            
    db.delete(doc)
    db.commit()
    return {"message": "Document deleted"}
