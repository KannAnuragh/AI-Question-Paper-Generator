from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.auth.dependencies import get_current_user
from app.auth.models import User
from app.projects.service import get_project_or_404
from app.export import service

router = APIRouter(prefix="/projects/{project_id}/papers/{paper_id}/export", tags=["Export"])

@router.post("")
def export_paper(
    project_id: str,
    paper_id: str,
    format: str = Query("pdf", description="Format to export (pdf or docx)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    get_project_or_404(db, project_id, current_user.id)
    return service.trigger_export(db, project_id, paper_id, format)
