from celery import Celery
import logging
from .config import settings

logger = logging.getLogger(__name__)

# Initialize Celery app
celery_app = Celery(
    "questionpaper_events",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.documents.workers",
        "app.curriculum.workers",
        "app.generation.workers"
    ]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Route tasks to specific queues if needed
    task_routes={
        "app.documents.workers.*": {"queue": "document_queue"},
        "app.curriculum.workers.*": {"queue": "curriculum_queue"},
        "app.generation.workers.*": {"queue": "generation_queue"},
    }
)

def publish_event(event_name: str, payload: dict):
    """
    Publish an event to the appropriate Celery worker.
    This acts as a simple event bus abstraction.
    """
    logger.info(f"Publishing event: {event_name} with payload: {payload}")
    
    if event_name == "document.uploaded":
        celery_app.send_task("app.documents.workers.handle_document_uploaded", kwargs={"payload": payload}, queue="document_queue")
    elif event_name == "document.extracted":
        celery_app.send_task("app.curriculum.workers.handle_document_extracted", kwargs={"payload": payload}, queue="curriculum_queue")
    elif event_name == "paper.generate":
        celery_app.send_task("app.generation.workers.handle_paper_generate", kwargs={"payload": payload}, queue="generation_queue")
    else:
        logger.warning(f"Unknown event name: {event_name}")
