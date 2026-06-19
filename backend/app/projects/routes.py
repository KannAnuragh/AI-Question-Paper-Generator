from fastapi import APIRouter, Depends, Form
from typing import List
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.auth.dependencies import get_current_user
from app.auth.models import User
from app.projects import schemas, service

router = APIRouter(prefix="/projects", tags=["Projects"])

@router.get("", response_model=List[schemas.ProjectOut])
def list_projects(
    archived: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    projects = service.list_projects(db, current_user.id, archived)
    result = []
    for p in projects:
        out = schemas.ProjectOut.model_validate(p)
        out.document_count = len(p.documents)
        out.paper_count = len(p.papers)
        result.append(out)
    return result

@router.post("", response_model=schemas.ProjectOut, status_code=201)
def create_project(
    body: schemas.ProjectCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = service.create_project(db, current_user.id, body)
    out = schemas.ProjectOut.model_validate(project)
    out.document_count = 0
    out.paper_count = 0
    return out

@router.get("/{project_id}", response_model=schemas.ProjectOut)
def get_project(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = service.get_project_or_404(db, project_id, current_user.id)
    out = schemas.ProjectOut.model_validate(project)
    out.document_count = len(project.documents)
    out.paper_count = len(project.papers)
    return out

@router.patch("/{project_id}", response_model=schemas.ProjectOut)
def update_project(
    project_id: str,
    body: schemas.ProjectUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = service.update_project(db, project_id, current_user.id, body)
    out = schemas.ProjectOut.model_validate(project)
    out.document_count = len(project.documents)
    out.paper_count = len(project.papers)
    return out

@router.delete("/{project_id}")
def delete_project(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return service.delete_project(db, project_id, current_user.id)

@router.post("/{project_id}/archive")
def archive_project(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return service.archive_project(db, project_id, current_user.id)

@router.post("/{project_id}/clone", response_model=schemas.ProjectOut)
def clone_project(
    project_id: str,
    new_name: str = Form(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = service.clone_project(db, project_id, current_user.id, new_name)
    out = schemas.ProjectOut.model_validate(project)
    out.document_count = 0
    out.paper_count = 0
    return out
