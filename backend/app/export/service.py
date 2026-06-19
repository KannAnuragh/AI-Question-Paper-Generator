from sqlalchemy.orm import Session
from fastapi import HTTPException
import logging
from app.assessment.models import Paper, PaperStatus
from app.core.events import publish_event
import uuid

logger = logging.getLogger(__name__)

def trigger_export(db: Session, project_id: str, paper_id: str, format: str) -> dict:
    paper = db.query(Paper).filter(Paper.id == paper_id, Paper.project_id == project_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
        
    if paper.status not in [PaperStatus.ready, PaperStatus.exported]:
        raise HTTPException(status_code=400, detail="Paper must be in 'ready' state to export")

    task_id = str(uuid.uuid4())
    
    # Emit export event
    publish_event("paper.export", {
        "paper_id": paper_id,
        "project_id": project_id,
        "format": format,
        "task_id": task_id
    })

    return {
        "message": f"Export to {format} started",
        "task_id": task_id,
        "status": "exporting"
    }
