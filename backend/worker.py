from app.core.events import celery_app

# This file is the entrypoint for running Celery workers.
# Run with: celery -A worker.celery_app worker --loglevel=info

if __name__ == "__main__":
    celery_app.start()
