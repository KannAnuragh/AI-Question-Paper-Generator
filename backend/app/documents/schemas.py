from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class DocumentOut(BaseModel):
    id: str
    filename: str
    original_name: str
    doc_type: str
    status: str
    file_size: Optional[int]
    chunk_count: int
    page_count: int
    error_message: Optional[str]
    task_id: Optional[str]
    uploaded_at: datetime
    processed_at: Optional[datetime]
    class Config: from_attributes = True
