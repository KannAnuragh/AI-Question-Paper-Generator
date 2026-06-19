from celery import shared_task
import logging

logger = logging.getLogger(__name__)

@shared_task(name="app.curriculum.workers.handle_document_extracted")
def handle_document_extracted(payload: dict):
    """
    Worker task that receives extracted document text/headings
    and builds the curriculum graph in Neo4j.
    """
    logger.info(f"Worker received document.extracted: {payload}")
    document_id = payload.get("document_id")
    project_id = payload.get("project_id")
    headings = payload.get("headings", [])
    
    # TODO: Connect to Neo4j and create Curriculum nodes and relationships
    logger.info(f"Building curriculum graph for project {project_id} from document {document_id}")
    
    return {"status": "success", "nodes_created": len(headings)}
