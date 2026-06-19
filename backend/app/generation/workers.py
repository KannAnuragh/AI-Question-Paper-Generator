from celery import shared_task
import logging

logger = logging.getLogger(__name__)

@shared_task(name="app.generation.workers.handle_paper_generate")
def handle_paper_generate(payload: dict):
    """
    Worker task that handles question paper generation via LLM.
    """
    logger.info(f"Worker received paper.generate: {payload}")
    paper_id = payload.get("paper_id")
    project_id = payload.get("project_id")
    
    # TODO: Implement actual generation pipeline
    logger.info(f"Generating paper {paper_id} for project {project_id}")
    
    return {"status": "success", "paper_id": paper_id}
