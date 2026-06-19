from celery import shared_task
import logging
from app.core.events import publish_event
import time

logger = logging.getLogger(__name__)

@shared_task(name="app.documents.workers.handle_document_uploaded")
def handle_document_uploaded(payload: dict):
    """
    Worker task that processes a document after it's uploaded.
    Extracts text (and deterministic headings/TOC).
    """
    logger.info(f"Worker received document.uploaded: {payload}")
    document_id = payload.get("document_id")
    project_id = payload.get("project_id")
    file_path = payload.get("file_path")

    # TODO: Implement actual document processing (PyMuPDF/PDFPlumber)
    # This is where the statistical/deterministic heading detection will go
    logger.info(f"Processing document {document_id} at {file_path}")
    
    # Simulating processing time
    time.sleep(2)
    
    logger.info(f"Document {document_id} processed successfully.")

    # Emit the next event in the pipeline
    publish_event("document.extracted", {
        "document_id": document_id,
        "project_id": project_id,
        "extracted_text": "Simulated extracted text content...",
        "headings": [{"title": "Chapter 1", "page": 1, "level": 1}]
    })
    
    return {"status": "success", "document_id": document_id}
