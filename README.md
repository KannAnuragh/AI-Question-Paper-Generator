# QuestionPaper.ai (AI Question Paper Generator)

An advanced, AI-powered assessment generation platform designed to build balanced, syllabus-aligned question papers. It processes reference documents (textbooks, syllabus, slide decks, notes) and utilizes RAG (Retrieval-Augmented Generation) coupled with Knowledge Graphs to map questions against Bloom's Taxonomy levels, course outcomes (COs), and difficulty targets.

---

## 🚀 Key Features

*   **Document Analysis**: Automated parsing of textbooks, notes, syllabi, and past question papers (PDF, DOCX, PPTX, TXT) with automatic metadata/copyright page filtering and scanned PDF OCR.
*   **Vector Search & Graph RAG**: Combines vector indexing (Qdrant) and semantic graph mapping (Neo4j) to query course topics accurately.
*   **Balanced Blueprint Alignment**: Generate question papers matching strict blueprints (e.g., specific marks per section, difficulty distributions, target Bloom levels).
*   **Async Task Processing**: Celery background queues process intensive document chunking, embeddings, and paper generation reliably.
*   **Modular Monolith Architecture**: Clean division of concerns with FastAPI (modular routers/services) and a Next.js frontend.

---

## 🛠️ Tech Stack

### Backend
*   **Core Framework**: [FastAPI](https://fastapi.tiangolo.com/) (Python 3.10+)
*   **Database & ORM**: PostgreSQL & [SQLAlchemy](https://www.sqlalchemy.org/)
*   **Vector Engine**: [Qdrant](https://qdrant.tech/)
*   **Graph Engine**: [Neo4j](https://neo4j.com/)
*   **Task Queue**: [Celery](https://docs.celeryq.dev/) & [Redis](https://redis.io/)
*   **AI Frameworks**: LangChain, LangGraph, Groq / Gemini / OpenAI APIs

### Frontend
*   **Framework**: [Next.js](https://nextjs.org/) (App Router, React 19)
*   **Styling**: Tailwind CSS v4 & Lucide Icons

---

## 📁 Repository Structure

```text
├── backend/                  # FastAPI Application
│   ├── app/                  # Application Modules
│   │   ├── assessment/       # Question papers, blueprints & grading schemas
│   │   ├── auth/             # User signup, login & session tokens
│   │   ├── core/             # DB clients, base configs, security utils
│   │   ├── curriculum/       # Course outcome mappings & schemas
│   │   ├── documents/        # PDF extraction, OCR, and storage
│   │   └── export/           # Document formatting & export (PDF/DOCX)
│   ├── Dockerfile            # Container definition for api & workers
│   ├── requirements.txt      # Python dependencies
│   ├── worker.py             # Celery worker bootstrapper
│   └── .env.example          # Template configuration
│
├── frontend/                 # Next.js Application
│   ├── app/                  # Pages, layouts, dashboards
│   ├── package.json          # Node dependencies & scripts
│   └── tsconfig.json         # TypeScript compiler configurations
│
├── docker-compose.yml        # Development environment services
├── nginx.conf                # Local reverse proxy setup
└── terminalcode              # Quick command snippets for local dev
```

---

## ⚙️ Setup & Installation

### Option A: Run via Docker Compose (Recommended)

Docker Compose is configured with optional service profiles. You can launch standard databases or spin up the entire AI and task-monitoring stacks.

1.  **Configure environment variables**:
    Copy the example config in the backend folder and fill in your API keys (e.g., Gemini, OpenAI, or Groq):
    ```bash
    cp backend/.env.example backend/.env
    ```
2.  **Spin up the base stack** (FastAPI, PostgreSQL, Redis):
    ```bash
    docker compose up -d
    ```
3.  **Spin up with Celery & AI components** (Celery Workers, Qdrant, Neo4j):
    ```bash
    docker compose --profile celery --profile ai up -d
    ```
    *Available profiles:*
    *   `celery`: Launches the Celery worker and the Flower monitoring tool.
    *   `ai`: Launches Qdrant Vector DB and Neo4j Graph DB.
    *   `storage`: Launches MinIO object storage.
    *   `proxy`: Launches an Nginx reverse proxy.

---

### Option B: Local Manual Development

#### 1. Running the Backend (FastAPI)

1.  Navigate to the `backend` directory:
    ```bash
    cd backend
    ```
2.  Create and activate a virtual environment:
    ```bash
    python -m venv venv
    # Windows:
    .\venv\Scripts\activate
    # macOS/Linux:
    source venv/bin/activate
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4.  Configure `.env`:
    Create a `.env` file based on `.env.example` and set up database/API parameters.
5.  Start the FastAPI application:
    ```bash
    uvicorn app.main:app --reload
    ```
6.  *(Optional)* Start the Celery worker (in a separate terminal inside active virtual environment):
    ```bash
    celery -A worker.celery_app worker --loglevel=info --pool=solo
    ```

#### 2. Running the Frontend (Next.js)

1.  Navigate to the `frontend` directory:
    ```bash
    cd frontend
    ```
2.  Install dependencies:
    ```bash
    npm install
    ```
3.  Start the development server:
    ```bash
    npm run dev
    ```
4.  Access the web UI at `http://localhost:3000`.

---

## 🔗 Interface URLs

When using the default ports:

*   **Frontend**: `http://localhost:3000`
*   **FastAPI API Docs**: `http://localhost:8000/docs` (Swagger UI) or `/redoc` (ReDoc)
*   **Flower (Celery Dashboard)**: `http://localhost:5555`
*   **Qdrant Console**: `http://localhost:6333/dashboard`
*   **Neo4j Console**: `http://localhost:7474`
