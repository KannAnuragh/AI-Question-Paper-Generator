from fastapi import APIRouter, Depends, Form
from typing import List, Optional
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.auth.dependencies import get_current_user
from app.auth.models import User
from app.projects.service import get_project_or_404
from app.assessment import schemas, service

router = APIRouter(prefix="/projects/{project_id}", tags=["Assessment"])

def verify_project(project_id: str, db: Session, current_user: User):
    get_project_or_404(db, project_id, current_user.id)

# --- Course Outcomes ---

@router.get("/cos", response_model=List[schemas.COOut], tags=["Course Outcomes"])
def list_cos(project_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.list_cos(db, project_id)

@router.post("/cos", response_model=schemas.COOut, status_code=201, tags=["Course Outcomes"])
def create_co(project_id: str, body: schemas.COCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.create_co(db, project_id, body)

@router.delete("/cos/{co_id}", tags=["Course Outcomes"])
def delete_co(project_id: str, co_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.delete_co(db, project_id, co_id)

# --- Templates ---

@router.get("/templates", response_model=List[schemas.TemplateOut], tags=["Templates"])
def list_templates(project_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.list_templates(db, project_id)

@router.post("/templates", response_model=schemas.TemplateOut, status_code=201, tags=["Templates"])
def create_template(project_id: str, body: schemas.TemplateCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.create_template(db, project_id, body)

@router.delete("/templates/{tmpl_id}", tags=["Templates"])
def delete_template(project_id: str, tmpl_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.delete_template(db, project_id, tmpl_id)

# --- Papers ---

@router.get("/papers", response_model=List[schemas.PaperOut], tags=["Papers"])
def list_papers(project_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    papers = service.list_papers(db, project_id)
    result = []
    for p in papers:
        out = schemas.PaperOut.model_validate(p)
        out.question_count = len(p.questions)
        result.append(out)
    return result

@router.post("/papers", response_model=schemas.PaperOut, status_code=201, tags=["Papers"])
def create_paper(project_id: str, body: schemas.PaperCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    paper = service.create_paper(db, project_id, body)
    out = schemas.PaperOut.model_validate(paper)
    out.question_count = 0
    return out

@router.get("/papers/{paper_id}", response_model=schemas.PaperOut, tags=["Papers"])
def get_paper(project_id: str, paper_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    paper = service.get_paper(db, project_id, paper_id)
    out = schemas.PaperOut.model_validate(paper)
    out.question_count = len(paper.questions)
    return out

@router.delete("/papers/{paper_id}", tags=["Papers"])
def delete_paper(project_id: str, paper_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.delete_paper(db, project_id, paper_id)

@router.post("/papers/{paper_id}/generate", tags=["Papers"])
def trigger_generation(project_id: str, paper_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.trigger_generation(db, project_id, paper_id)

# --- Questions ---

@router.get("/papers/{paper_id}/questions", response_model=List[schemas.QuestionOut], tags=["Questions"])
def list_questions(project_id: str, paper_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.list_questions(db, project_id, paper_id)

@router.post("/papers/{paper_id}/questions", response_model=schemas.QuestionOut, status_code=201, tags=["Questions"])
def add_question(project_id: str, paper_id: str, body: schemas.QuestionCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.add_question(db, project_id, paper_id, body)

@router.patch("/papers/{paper_id}/questions/{q_id}", response_model=schemas.QuestionOut, tags=["Questions"])
def update_question(project_id: str, paper_id: str, q_id: str, body: schemas.QuestionUpdate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.update_question(db, project_id, paper_id, q_id, body)

@router.delete("/papers/{paper_id}/questions/{q_id}", tags=["Questions"])
def delete_question(project_id: str, paper_id: str, q_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    verify_project(project_id, db, current_user)
    return service.delete_question(db, project_id, paper_id, q_id)

@router.post("/papers/{paper_id}/questions/{q_id}/feedback", tags=["Questions"])
def question_feedback(
    project_id: str, paper_id: str, q_id: str,
    feedback: str = Form(..., description="accepted | rejected | modified"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    verify_project(project_id, db, current_user)
    return service.question_feedback(db, project_id, paper_id, q_id, feedback)
