import logging
from qdrant_client import QdrantClient
from .config import settings

logger = logging.getLogger(__name__)

class QdrantConnection:
    def __init__(self):
        self.client: QdrantClient | None = None
        
    def connect(self):
        try:
            self.client = QdrantClient(url=settings.QDRANT_URL)
            logger.info("Connected to Qdrant successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Qdrant: {e}")
            self.client = None
            
qdrant_conn = QdrantConnection()

def get_qdrant_client() -> QdrantClient | None:
    if not qdrant_conn.client:
        qdrant_conn.connect()
    return qdrant_conn.client
