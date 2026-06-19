from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import logging

from app.core.config import settings
from app.core.database import Base, engine

# Import routers
from app.auth.routes import router as auth_router
from app.projects.routes import router as projects_router
from app.documents.routes import router as documents_router
from app.assessment.routes import router as assessment_router
from app.export.routes import router as export_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="TestBoost.ai API (Modular)",
    description="AI-powered assessment platform — Modular Monolith",
    version="0.3.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "http://localhost:3000",
    "Access-Control-Allow-Credentials": "true",
}

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception on {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": type(exc).__name__},
        headers=CORS_HEADERS,
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    error_messages = []
    for error in exc.errors():
        loc = error.get("loc", [])
        field = loc[-1] if loc else "field"
        msg = error.get("msg", "invalid value")
        error_messages.append(f"{field}: {msg}")
    detail_str = ", ".join(error_messages)
    return JSONResponse(
        status_code=422,
        content={"detail": detail_str, "body": str(exc.body)},
        headers=CORS_HEADERS,
    )

# Include routers
app.include_router(auth_router)
app.include_router(projects_router)
app.include_router(documents_router)
app.include_router(assessment_router)
app.include_router(export_router)

@app.get("/health", tags=["System"])
def health():
    return {
        "status": "ok",
        "version": "0.3.0",
        "phase": "Modular Monolith Transition"
    }

@app.get("/", tags=["System"])
def root():
    return {
        "name": "TestBoost.ai API (Modular)",
        "version": "0.3.0",
        "docs": "/docs",
        "health": "/health",
    }
