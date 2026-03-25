# Data Quality Monitor

A production-ready data quality monitoring system built with FastAPI, PostgreSQL, React, and GPT-4o-mini.

## Features

### Core Validation
- CSV file upload and REST API data ingestion
- Comprehensive validation checks:
  - Missing values & null percentage by column
  - Duplicate detection
  - Schema mismatch
  - Outlier detection (IQR method)
  - Type consistency
  - Unexpected categorical values
  - Numeric range violations
  - Timestamp freshness checks
  - Primary key uniqueness

### Dataset Relationships
- Define foreign key relationships between datasets
- Validate referential integrity
- Detect orphaned values and missing references

### Data Lineage
- Track data sources (e.g., Salesforce, SAP)
- Record transformations between datasets
- Visualize data flow

### AI Analysis (GPT-4o-mini)
- Automatic issue analysis
- Human-readable explanations
- Root cause identification
- Remediation suggestions
- Executive summaries

### Visualization
- Interactive dependency graph
- Quality score dashboards
- Issue breakdown charts
- Validation history

## Tech Stack

- **Backend**: FastAPI, SQLAlchemy (async), PostgreSQL
- **Frontend**: React, Tailwind CSS, Shadcn/UI, ReactFlow
- **AI**: OpenAI GPT-4o-mini
- **Database**: PostgreSQL 15

## Quick Start with Docker

1. Clone the repository
2. Create `.env` file in root directory:
   ```
   EMERGENT_LLM_KEY=your_openai_api_key
   ```

3. Start all services:
   ```bash
   docker-compose up -d
   ```

4. Access the application:
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8001
   - API Docs: http://localhost:8001/docs

## Local Development Setup

### Prerequisites
- Python 3.11+
- Node.js 20+
- PostgreSQL 15+
- Yarn

### Backend Setup

1. Create PostgreSQL database:
   ```bash
   createdb dataquality_db
   createuser dataquality -P  # Set password: dataquality123
   ```

2. Setup backend:
   ```bash
   cd backend
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   cp .env.example .env
   # Edit .env with your settings
   ```

3. Run backend:
   ```bash
   uvicorn server:app --reload --host 0.0.0.0 --port 8001
   ```

### Frontend Setup

1. Install dependencies:
   ```bash
   cd frontend
   yarn install
   cp .env.example .env
   ```

2. Run frontend:
   ```bash
   yarn start
   ```

## API Endpoints

### Datasets
- `POST /api/datasets/upload` - Upload CSV dataset
- `POST /api/datasets/json` - Create dataset from JSON
- `GET /api/datasets` - List all datasets
- `GET /api/datasets/{id}` - Get dataset details
- `DELETE /api/datasets/{id}` - Delete dataset

### Validation
- `POST /api/validation/run/{dataset_id}` - Trigger validation
- `GET /api/validation/jobs` - List validation jobs
- `GET /api/validation/results/{job_id}` - Get validation results
- `POST /api/validation/integrity/{job_id}` - Run referential integrity check

### Relationships
- `POST /api/relationships` - Create relationship
- `GET /api/relationships` - List all relationships
- `DELETE /api/relationships/{id}` - Delete relationship

### Lineage
- `POST /api/lineage` - Create lineage entry
- `GET /api/lineage` - List all lineage entries
- `GET /api/lineage/dataset/{id}` - Get dataset lineage

### AI Analysis
- `POST /api/ai/analyze/{job_id}` - Trigger AI analysis

### Graph
- `GET /api/graph/dependencies` - Get dependency graph data

## Project Structure

```
data-quality-monitor/
├── backend/
│   ├── server.py          # FastAPI application
│   ├── database.py        # PostgreSQL connection
│   ├── models.py          # SQLAlchemy models
│   ├── schemas.py         # Pydantic schemas
│   ├── validator.py       # Data validation engine
│   ├── ai_service.py      # GPT-4o-mini integration
│   ├── requirements.txt   # Python dependencies
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── pages/         # Page components
│   │   ├── components/    # Reusable components
│   │   ├── api.js         # API client
│   │   ├── App.js         # Main app
│   │   └── index.css      # Global styles
│   ├── package.json
│   └── Dockerfile
├── docker-compose.yml
└── README.md
```

## License

MIT
