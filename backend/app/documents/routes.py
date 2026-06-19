from fastapi import APIRouter, Depends, UploadFile, File, Form
from typing import List
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.auth.dependencies import get_current_user
from app.auth.models import User
from app.projects.service import get_project_or_404
from app.documents import schemas, service
from app.documents.models import DocumentType

router = APIRouter(prefix="/projects/{project_id}/documents", tags=["Documents"])

@router.get("", response_model=List[schemas.DocumentOut])
def list_documents(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(db, project_id, current_user.id)
    return service.list_documents(db, project_id)

@router.post("", response_model=schemas.DocumentOut, status_code=201)
async def upload_document(
    project_id: str,
    file: UploadFile = File(...),
    doc_type: DocumentType = Form(DocumentType.other),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(db, project_id, current_user.id)
    return await service.upload_document(db, project_id, file, doc_type)

@router.delete("/{document_id}")
def delete_document(
    project_id: str,
    document_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(db, project_id, current_user.id)
    return service.delete_document(db, document_id, project_id)
