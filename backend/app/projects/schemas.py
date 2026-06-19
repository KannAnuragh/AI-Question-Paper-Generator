from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class ProjectCreate(BaseModel):
    name: str = Field(min_length=2)
    subject: Optional[str] = None
    description: Optional[str] = None
    semester: Optional[str] = None
    department: Optional[str] = None
    institution: Optional[str] = None

class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    subject: Optional[str] = None
    description: Optional[str] = None
    semester: Optional[str] = None
    department: Optional[str] = None

class ProjectOut(BaseModel):
    id: str
    name: str
    subject: Optional[str]
    description: Optional[str]
    semester: Optional[str]
    department: Optional[str]
    institution: Optional[str]
    is_archived: bool
    created_at: datetime
    updated_at: datetime
    document_count: int = 0
    paper_count: int = 0
    class Config: from_attributes = True
