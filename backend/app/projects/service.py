from sqlalchemy.orm import Session
from fastapi import HTTPException
from datetime import datetime, timezone
import shutil
import logging
from app.projects.models import Project
from app.projects.schemas import ProjectCreate, ProjectUpdate
from app.core.config import settings
from app.core.qdrant_db import get_qdrant_client

logger = logging.getLogger(__name__)

def get_project_or_404(db: Session, project_id: str, owner_id: str) -> Project:
    project = db.query(Project).filter(
        Project.id == project_id,
        Project.owner_id == owner_id,
    ).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project

def list_projects(db: Session, owner_id: str, archived: bool = False):
    return db.query(Project).filter(
        Project.owner_id == owner_id,
        Project.is_archived == archived,
    ).order_by(Project.updated_at.desc()).all()

def create_project(db: Session, owner_id: str, body: ProjectCreate) -> Project:
    project = Project(owner_id=owner_id, **body.model_dump())
    db.add(project)
    db.commit()
    db.refresh(project)
    return project

def update_project(db: Session, project_id: str, owner_id: str, body: ProjectUpdate) -> Project:
    project = get_project_or_404(db, project_id, owner_id)
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(project, k, v)
    project.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(project)
    return project

def delete_project(db: Session, project_id: str, owner_id: str):
    project = get_project_or_404(db, project_id, owner_id)

    # 1. Delete Qdrant vectors
    qdrant_client = get_qdrant_client()
    if qdrant_client:
        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue
            qdrant_client.delete(
                collection_name="chunks", # Assume default collection name
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
    project_dir = settings.UPLOAD_DIR / project_id
    if project_dir.exists():
        try:
            shutil.rmtree(project_dir)
            logger.info(f"Deleted uploads directory for project {project_id}")
        except Exception as e:
            logger.error(f"Failed to delete directory {project_dir}: {e}")

    # 3. Graph data (Neo4j)
    # The new architecture uses Neo4j for the graph. We should delete project nodes from Neo4j here.
    from app.core.neo4j_db import get_neo4j_driver
    driver = get_neo4j_driver()
    if driver:
        try:
            with driver.session() as session:
                session.run("MATCH (n {project_id: $pid}) DETACH DELETE n", pid=project_id)
            logger.info(f"Deleted Neo4j nodes for project {project_id}")
        except Exception as e:
            logger.error(f"Failed to delete Neo4j nodes for project {project_id}: {e}")

    db.delete(project)
    db.commit()
    return {"message": "Project and all associated data deleted"}

def archive_project(db: Session, project_id: str, owner_id: str):
    project = get_project_or_404(db, project_id, owner_id)
    project.is_archived = True
    db.commit()
    return {"message": "Project archived"}

def clone_project(db: Session, project_id: str, owner_id: str, new_name: str) -> Project:
    source = get_project_or_404(db, project_id, owner_id)
    clone = Project(
        owner_id=owner_id,
        name=new_name,
        subject=source.subject,
        description=source.description,
        department=source.department,
        institution=source.institution,
    )
    db.add(clone)
    db.flush()
    
    # We will need to clone Templates.
    # For now, we will leave this as a TODO to import the Template model properly,
    # as it belongs to the assessment domain.
    # TODO: Import models and clone associated entities
    db.commit()
    db.refresh(clone)
    return clone
