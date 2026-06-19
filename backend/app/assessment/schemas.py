from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from app.assessment.models import BloomLevel, Difficulty, QuestionType

class COCreate(BaseModel):
    code: str
    description: str
    bloom_level: Optional[BloomLevel] = None

class COOut(BaseModel):
    id: str
    code: str
    description: str
    bloom_level: Optional[str]
    created_at: datetime
    class Config: from_attributes = True

class TemplateCreate(BaseModel):
    name: str
    institution: Optional[str] = None
    header_config: dict = {}
    section_config: dict = {}
    marks_layout: dict = {}

class TemplateOut(BaseModel):
    id: str
    name: str
    institution: Optional[str]
    header_config: dict
    section_config: dict
    marks_layout: dict
    is_default: bool
    created_at: datetime
    class Config: from_attributes = True

class QuestionTypeSpec(BaseModel):
    type: QuestionType
    count: int
    marks_each: int

class BloomTarget(BaseModel):
    remember: int   = 0
    understand: int = 0
    apply: int      = 0
    analyze: int    = 0
    evaluate: int   = 0
    create: int     = 0

class DifficultyTarget(BaseModel):
    easy: int   = 30
    medium: int = 50
    hard: int   = 20

class Blueprint(BaseModel):
    question_specs: List[QuestionTypeSpec]
    bloom_targets: BloomTarget
    difficulty_targets: DifficultyTarget
    topics: List[str] = []
    total_marks: int = 100
    duration_mins: int = 180

class PaperCreate(BaseModel):
    title: str
    blueprint: Blueprint
    template_id: Optional[str] = None
    notes: Optional[str] = None

class PaperOut(BaseModel):
    id: str
    title: str
    status: str
    total_marks: int
    duration_mins: int
    blueprint: dict
    template_id: Optional[str]
    notes: Optional[str]
    pdf_path: Optional[str]
    docx_path: Optional[str]
    error_message: Optional[str]
    task_id: Optional[str]
    created_at: datetime
    updated_at: datetime
    question_count: int = 0
    class Config: from_attributes = True

class QuestionCreate(BaseModel):
    question_text: str
    answer_text: Optional[str] = None
    question_type: QuestionType
    bloom_level: Optional[BloomLevel] = None
    difficulty: Optional[Difficulty] = None
    marks: int = 2
    topic: Optional[str] = None
    options: Optional[List[str]] = None
    correct_option: Optional[int] = None
    rubric: Optional[dict] = None
    co_id: Optional[str] = None

class QuestionUpdate(BaseModel):
    question_text: Optional[str] = None
    answer_text: Optional[str] = None
    bloom_level: Optional[BloomLevel] = None
    difficulty: Optional[Difficulty] = None
    marks: Optional[int] = None
    topic: Optional[str] = None
    is_accepted: Optional[bool] = None
    order_index: Optional[int] = None

class QuestionOut(BaseModel):
    id: str
    paper_id: str
    order_index: int
    question_text: str
    answer_text: Optional[str]
    question_type: str
    bloom_level: Optional[str]
    difficulty: Optional[str]
    marks: int
    topic: Optional[str]
    subtopic: Optional[str]
    co_id: Optional[str]
    options: Optional[List[str]]
    correct_option: Optional[int]
    rubric: Optional[dict]
    quality_scores: Optional[dict]
    is_accepted: bool
    created_at: datetime
    class Config: from_attributes = True
