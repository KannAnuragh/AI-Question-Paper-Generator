from sqlalchemy.orm import Session
from fastapi import HTTPException
from datetime import datetime, timezone
import uuid
import logging
from app.assessment.models import CourseOutcome, Template, Paper, PaperStatus, Question
from app.assessment.schemas import COCreate, TemplateCreate, PaperCreate, QuestionCreate, QuestionUpdate
from app.documents.models import Document, DocumentStatus
from app.projects.service import get_project_or_404
from app.core.events import publish_event
from app.core.config import settings

logger = logging.getLogger(__name__)

# --- Course Outcomes ---

def list_cos(db: Session, project_id: str):
    return db.query(CourseOutcome).filter(CourseOutcome.project_id == project_id).all()

def create_co(db: Session, project_id: str, body: COCreate) -> CourseOutcome:
    co = CourseOutcome(project_id=project_id, **body.model_dump())
    db.add(co)
    db.commit()
    db.refresh(co)
    return co

def delete_co(db: Session, project_id: str, co_id: str):
    co = db.query(CourseOutcome).filter(CourseOutcome.id == co_id, CourseOutcome.project_id == project_id).first()
    if not co:
        raise HTTPException(status_code=404, detail="Course outcome not found")
    db.delete(co)
    db.commit()
    return {"message": "Deleted"}

# --- Templates ---

def list_templates(db: Session, project_id: str):
    return db.query(Template).filter(Template.project_id == project_id).all()

def create_template(db: Session, project_id: str, body: TemplateCreate) -> Template:
    tmpl = Template(project_id=project_id, **body.model_dump())
    db.add(tmpl)
    db.commit()
    db.refresh(tmpl)
    return tmpl

def delete_template(db: Session, project_id: str, tmpl_id: str):
    tmpl = db.query(Template).filter(Template.id == tmpl_id, Template.project_id == project_id).first()
    if not tmpl:
        raise HTTPException(status_code=404, detail="Template not found")
    db.delete(tmpl)
    db.commit()
    return {"message": "Deleted"}

# --- Papers ---

def list_papers(db: Session, project_id: str):
    return db.query(Paper).filter(Paper.project_id == project_id).order_by(Paper.created_at.desc()).all()

def create_paper(db: Session, project_id: str, body: PaperCreate) -> Paper:
    paper = Paper(
        project_id=project_id, title=body.title,
        blueprint=body.blueprint.model_dump(),
        total_marks=body.blueprint.total_marks,
        duration_mins=body.blueprint.duration_mins,
        template_id=body.template_id, notes=body.notes,
        status=PaperStatus.draft,
    )
    db.add(paper)
    db.commit()
    db.refresh(paper)
    return paper

def get_paper(db: Session, project_id: str, paper_id: str) -> Paper:
    paper = db.query(Paper).filter(Paper.id == paper_id, Paper.project_id == project_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
    return paper

def delete_paper(db: Session, project_id: str, paper_id: str):
    paper = get_paper(db, project_id, paper_id)
    db.delete(paper)
    db.commit()
    return {"message": "Paper deleted"}

def trigger_generation(db: Session, project_id: str, paper_id: str) -> dict:
    paper = get_paper(db, project_id, paper_id)
    if paper.status == PaperStatus.generating:
        raise HTTPException(status_code=409, detail="Generation already in progress")

    doc_count = db.query(Document).filter(
        Document.project_id == project_id,
        Document.status == DocumentStatus.ready,
    ).count()
    if doc_count == 0:
        raise HTTPException(status_code=400,
            detail="No processed documents found. Upload and process at least one document first.")

    task_id = str(uuid.uuid4())
    paper.status  = PaperStatus.generating
    paper.task_id = task_id
    paper.error_message = None
    db.commit()

    # Emit event to worker
    publish_event("paper.generate", {
        "paper_id": paper_id,
        "project_id": project_id,
        "task_id": task_id
    })

    return {
        "message":    "Generation started",
        "paper_id":   paper_id,
        "task_id":    task_id,
        "status":     "generating",
        "stream_url": f"/stream/task/{task_id}",
    }

# --- Questions ---

def list_questions(db: Session, project_id: str, paper_id: str):
    return db.query(Question).filter(Question.paper_id == paper_id).order_by(Question.order_index).all()

def add_question(db: Session, project_id: str, paper_id: str, body: QuestionCreate) -> Question:
    paper = get_paper(db, project_id, paper_id)
    count = db.query(Question).filter(Question.paper_id == paper_id).count()
    question = Question(paper_id=paper_id, order_index=count, **body.model_dump())
    db.add(question)
    db.commit()
    db.refresh(question)
    return question

def update_question(db: Session, project_id: str, paper_id: str, q_id: str, body: QuestionUpdate) -> Question:
    question = db.query(Question).filter(Question.id == q_id, Question.paper_id == paper_id).first()
    if not question:
        raise HTTPException(status_code=404, detail="Question not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(question, k, v)
    db.commit()
    db.refresh(question)
    return question

def delete_question(db: Session, project_id: str, paper_id: str, q_id: str):
    question = db.query(Question).filter(Question.id == q_id, Question.paper_id == paper_id).first()
    if not question:
        raise HTTPException(status_code=404, detail="Question not found")
    db.delete(question)
    db.commit()
    return {"message": "Question deleted"}

def question_feedback(db: Session, project_id: str, paper_id: str, q_id: str, feedback: str):
    question = db.query(Question).filter(Question.id == q_id, Question.paper_id == paper_id).first()
    if not question:
        raise HTTPException(status_code=404, detail="Question not found")
    question.is_accepted = feedback != "rejected"
    db.commit()
    return {"message": f"Feedback '{feedback}' recorded"}
