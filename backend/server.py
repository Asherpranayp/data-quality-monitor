"""FastAPI Backend for Data Quality Monitor"""
from fastapi import FastAPI, APIRouter, UploadFile, File, Form, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
import os
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timezone, timedelta
import pandas as pd
import io
import json
import asyncio

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from sqlalchemy.orm import selectinload

from database import get_db, init_db, engine
from models import (
    Dataset, ValidationJob, ValidationIssue, ValidationMetric, ScheduledJob,
    DatasetRelationship, ReferentialIntegrityResult, DatasetLineage, ValidationHistory
)
from schemas import (
    DatasetResponse, DatasetListResponse, DatasetCreate, DatasetUpdate,
    JSONDataInput, ValidationJobResponse, ValidationJobWithDataset,
    ValidationIssueResponse, ValidationMetricResponse, ValidationResultsResponse,
    ScheduledJobCreate, ScheduledJobResponse, DashboardStats,
    ReferentialIntegrityCheck,
    DatasetRelationshipCreate, DatasetRelationshipResponse, DatasetRelationshipWithNames,
    ReferentialIntegrityResultResponse,
    DatasetLineageCreate, DatasetLineageResponse, DatasetLineageWithNames,
    ValidationHistoryResponse,
    GraphNode, GraphEdge, DependencyGraphResponse,
    ValidationResultsWithIntegrity
)
from validator import DataValidator, check_referential_integrity
from ai_service import AIAnalysisService

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Create the main app
app = FastAPI(
    title="Data Quality Monitor API",
    description="Production-ready data quality monitoring system",
    version="1.0.0"
)

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize AI service
ai_service = AIAnalysisService()

# Data storage directory
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


# ============== Health Check ==============
@api_router.get("/")
async def root():
    return {"message": "Data Quality Monitor API", "status": "healthy"}


@api_router.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ============== Dataset Endpoints ==============
@api_router.post("/datasets/upload", response_model=DatasetResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    name: str = Form(...),
    description: Optional[str] = Form(None),
    primary_key_columns: Optional[str] = Form(None),
    expected_schema: Optional[str] = Form(None),
    categorical_columns: Optional[str] = Form(None),
    numeric_ranges: Optional[str] = Form(None),
    freshness_columns: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db)
):
    """Upload a CSV dataset for quality monitoring"""
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    
    try:
        # Read CSV content
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Generate schema info
        schema_info = {col: str(df[col].dtype) for col in df.columns}
        
        # Get sample data (first 5 rows) - replace NaN with None for JSON compatibility
        sample_df = df.head(5).fillna('')
        sample_data = sample_df.to_dict('records')
        
        # Save file
        file_path = DATA_DIR / f"{name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(file_path, 'wb') as f:
            f.write(content)
        
        # Parse optional JSON fields
        pk_cols = json.loads(primary_key_columns) if primary_key_columns else None
        exp_schema = json.loads(expected_schema) if expected_schema else None
        cat_cols = json.loads(categorical_columns) if categorical_columns else None
        num_ranges = json.loads(numeric_ranges) if numeric_ranges else None
        fresh_cols = json.loads(freshness_columns) if freshness_columns else None
        
        # Create dataset record
        dataset = Dataset(
            name=name,
            description=description,
            source_type="csv",
            row_count=len(df),
            column_count=len(df.columns),
            schema_info=schema_info,
            sample_data=sample_data,
            file_path=str(file_path),
            primary_key_columns=pk_cols,
            expected_schema=exp_schema,
            categorical_columns=cat_cols,
            numeric_ranges=num_ranges,
            freshness_columns=fresh_cols
        )
        
        db.add(dataset)
        await db.commit()
        await db.refresh(dataset)
        
        logger.info(f"Dataset uploaded: {name} ({len(df)} rows)")
        
        return DatasetResponse(
            id=dataset.id,
            name=dataset.name,
            description=dataset.description,
            source_type=dataset.source_type,
            row_count=dataset.row_count,
            column_count=dataset.column_count,
            schema_info=dataset.schema_info,
            sample_data=dataset.sample_data,
            primary_key_columns=dataset.primary_key_columns,
            expected_schema=dataset.expected_schema,
            categorical_columns=dataset.categorical_columns,
            numeric_ranges=dataset.numeric_ranges,
            freshness_columns=dataset.freshness_columns,
            created_at=dataset.created_at,
            updated_at=dataset.updated_at
        )
        
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty")
    except pd.errors.ParserError as e:
        raise HTTPException(status_code=400, detail=f"CSV parsing error: {str(e)}")
    except Exception as e:
        logger.error(f"Error uploading dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/datasets/json", response_model=DatasetResponse)
async def create_dataset_from_json(
    data_input: JSONDataInput,
    db: AsyncSession = Depends(get_db)
):
    """Create a dataset from JSON data via REST API"""
    
    try:
        df = pd.DataFrame(data_input.data)
        
        if df.empty:
            raise HTTPException(status_code=400, detail="Data cannot be empty")
        
        # Generate schema info
        schema_info = {col: str(df[col].dtype) for col in df.columns}
        
        # Get sample data
        sample_data = df.head(5).to_dict('records')
        
        # Save as CSV
        file_path = DATA_DIR / f"{data_input.name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(file_path, index=False)
        
        # Create dataset record
        dataset = Dataset(
            name=data_input.name,
            description=data_input.description,
            source_type="api",
            row_count=len(df),
            column_count=len(df.columns),
            schema_info=schema_info,
            sample_data=sample_data,
            file_path=str(file_path),
            primary_key_columns=data_input.primary_key_columns,
            expected_schema=data_input.expected_schema
        )
        
        db.add(dataset)
        await db.commit()
        await db.refresh(dataset)
        
        logger.info(f"Dataset created from JSON: {data_input.name} ({len(df)} rows)")
        
        return DatasetResponse(
            id=dataset.id,
            name=dataset.name,
            description=dataset.description,
            source_type=dataset.source_type,
            row_count=dataset.row_count,
            column_count=dataset.column_count,
            schema_info=dataset.schema_info,
            sample_data=dataset.sample_data,
            primary_key_columns=dataset.primary_key_columns,
            expected_schema=dataset.expected_schema,
            categorical_columns=dataset.categorical_columns,
            numeric_ranges=dataset.numeric_ranges,
            freshness_columns=dataset.freshness_columns,
            created_at=dataset.created_at,
            updated_at=dataset.updated_at
        )
        
    except Exception as e:
        logger.error(f"Error creating dataset from JSON: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/datasets", response_model=List[DatasetListResponse])
async def list_datasets(db: AsyncSession = Depends(get_db)):
    """List all datasets"""
    result = await db.execute(select(Dataset).order_by(desc(Dataset.created_at)))
    datasets = result.scalars().all()
    
    return [
        DatasetListResponse(
            id=d.id,
            name=d.name,
            source_type=d.source_type,
            row_count=d.row_count,
            column_count=d.column_count,
            created_at=d.created_at
        )
        for d in datasets
    ]


@api_router.get("/datasets/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Get dataset details"""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    return DatasetResponse(
        id=dataset.id,
        name=dataset.name,
        description=dataset.description,
        source_type=dataset.source_type,
        row_count=dataset.row_count,
        column_count=dataset.column_count,
        schema_info=dataset.schema_info,
        sample_data=dataset.sample_data,
        primary_key_columns=dataset.primary_key_columns,
        expected_schema=dataset.expected_schema,
        categorical_columns=dataset.categorical_columns,
        numeric_ranges=dataset.numeric_ranges,
        freshness_columns=dataset.freshness_columns,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at
    )


@api_router.put("/datasets/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(
    dataset_id: str,
    update_data: DatasetUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update dataset configuration"""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Update fields
    for field, value in update_data.model_dump(exclude_unset=True).items():
        setattr(dataset, field, value)
    
    dataset.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(dataset)
    
    return DatasetResponse(
        id=dataset.id,
        name=dataset.name,
        description=dataset.description,
        source_type=dataset.source_type,
        row_count=dataset.row_count,
        column_count=dataset.column_count,
        schema_info=dataset.schema_info,
        sample_data=dataset.sample_data,
        primary_key_columns=dataset.primary_key_columns,
        expected_schema=dataset.expected_schema,
        categorical_columns=dataset.categorical_columns,
        numeric_ranges=dataset.numeric_ranges,
        freshness_columns=dataset.freshness_columns,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at
    )


@api_router.delete("/datasets/{dataset_id}")
async def delete_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a dataset"""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Delete file if exists
    if dataset.file_path and os.path.exists(dataset.file_path):
        os.remove(dataset.file_path)
    
    await db.delete(dataset)
    await db.commit()
    
    return {"message": "Dataset deleted successfully"}


# ============== Validation Endpoints ==============
async def run_validation_job(job_id: str, dataset_id: str):
    """Background task to run validation"""
    from database import async_session_maker
    
    async with async_session_maker() as db:
        try:
            # Get job and dataset
            result = await db.execute(select(ValidationJob).where(ValidationJob.id == job_id))
            job = result.scalar_one_or_none()
            
            result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
            dataset = result.scalar_one_or_none()
            
            if not job or not dataset:
                return
            
            # Update job status
            job.status = "running"
            job.started_at = datetime.utcnow()
            await db.commit()
            
            # Load data
            df = pd.read_csv(dataset.file_path)
            
            # Build config
            config = {
                "primary_key_columns": dataset.primary_key_columns or [],
                "expected_schema": dataset.expected_schema or {},
                "categorical_columns": dataset.categorical_columns or {},
                "numeric_ranges": dataset.numeric_ranges or {},
                "freshness_columns": dataset.freshness_columns or {}
            }
            
            # Run validation
            validator = DataValidator(df, config)
            issues, metrics, quality_score = validator.run_all_checks()
            
            # Count issues by severity
            severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
            for issue in issues:
                severity = issue.get("severity", "low")
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            # Update job
            job.total_issues = len(issues)
            job.critical_issues = severity_counts["critical"]
            job.high_issues = severity_counts["high"]
            job.medium_issues = severity_counts["medium"]
            job.low_issues = severity_counts["low"]
            job.quality_score = quality_score
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            
            # Save issues
            for issue_data in issues:
                issue = ValidationIssue(
                    job_id=job_id,
                    issue_type=issue_data["issue_type"],
                    severity=issue_data["severity"],
                    column_name=issue_data.get("column_name"),
                    affected_rows=issue_data["affected_rows"],
                    affected_percentage=issue_data["affected_percentage"],
                    description=issue_data["description"],
                    sample_values=issue_data.get("sample_values")
                )
                db.add(issue)
            
            # Save metrics
            for metric_data in metrics:
                metric = ValidationMetric(
                    job_id=job_id,
                    metric_name=metric_data["metric_name"],
                    metric_value=metric_data["metric_value"],
                    column_name=metric_data.get("column_name"),
                    details=metric_data.get("details")
                )
                db.add(metric)
            
            # Save validation history
            completeness_metric = next((m for m in metrics if m["metric_name"] == "completeness"), None)
            duplicate_metric = next((m for m in metrics if m["metric_name"] == "duplicate_rows"), None)
            
            history = ValidationHistory(
                dataset_id=dataset_id,
                job_id=job_id,
                quality_score=quality_score,
                total_issues=len(issues),
                critical_issues=severity_counts["critical"],
                high_issues=severity_counts["high"],
                medium_issues=severity_counts["medium"],
                low_issues=severity_counts["low"],
                completeness=completeness_metric["metric_value"] if completeness_metric else None,
                duplicate_percentage=duplicate_metric["metric_value"] if duplicate_metric else None,
                validated_at=datetime.utcnow()
            )
            db.add(history)
            
            await db.commit()
            
            logger.info(f"Validation completed for job {job_id}: {len(issues)} issues, score {quality_score}")
            
        except Exception as e:
            logger.error(f"Validation failed for job {job_id}: {e}")
            
            result = await db.execute(select(ValidationJob).where(ValidationJob.id == job_id))
            job = result.scalar_one_or_none()
            if job:
                job.status = "failed"
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                await db.commit()


@api_router.post("/validation/run/{dataset_id}", response_model=ValidationJobResponse)
async def trigger_validation(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Trigger a validation job for a dataset"""
    
    # Verify dataset exists
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Create job
    job = ValidationJob(
        dataset_id=dataset_id,
        status="pending"
    )
    
    db.add(job)
    await db.commit()
    await db.refresh(job)
    
    # Run validation in background
    background_tasks.add_task(run_validation_job, job.id, dataset_id)
    
    return ValidationJobResponse(
        id=job.id,
        dataset_id=job.dataset_id,
        status=job.status,
        started_at=job.started_at,
        completed_at=job.completed_at,
        total_issues=job.total_issues,
        critical_issues=job.critical_issues,
        high_issues=job.high_issues,
        medium_issues=job.medium_issues,
        low_issues=job.low_issues,
        quality_score=job.quality_score,
        error_message=job.error_message,
        ai_summary=job.ai_summary,
        ai_analysis_completed=job.ai_analysis_completed,
        created_at=job.created_at
    )


@api_router.get("/validation/jobs", response_model=List[ValidationJobWithDataset])
async def list_validation_jobs(
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """List all validation jobs"""
    result = await db.execute(
        select(ValidationJob, Dataset.name)
        .join(Dataset, ValidationJob.dataset_id == Dataset.id)
        .order_by(desc(ValidationJob.created_at))
        .limit(limit)
    )
    rows = result.all()
    
    return [
        ValidationJobWithDataset(
            id=job.id,
            dataset_id=job.dataset_id,
            dataset_name=name,
            status=job.status,
            started_at=job.started_at,
            completed_at=job.completed_at,
            total_issues=job.total_issues,
            critical_issues=job.critical_issues,
            high_issues=job.high_issues,
            medium_issues=job.medium_issues,
            low_issues=job.low_issues,
            quality_score=job.quality_score,
            error_message=job.error_message,
            ai_summary=job.ai_summary,
            ai_analysis_completed=job.ai_analysis_completed,
            created_at=job.created_at
        )
        for job, name in rows
    ]


@api_router.get("/validation/results/{job_id}", response_model=ValidationResultsResponse)
async def get_validation_results(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get full validation results for a job"""
    
    # Get job
    result = await db.execute(select(ValidationJob).where(ValidationJob.id == job_id))
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Validation job not found")
    
    # Get issues
    result = await db.execute(
        select(ValidationIssue)
        .where(ValidationIssue.job_id == job_id)
        .order_by(ValidationIssue.severity)  # Simple ordering
    )
    issues = result.scalars().all()
    
    # Sort issues by severity priority in Python
    severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    issues = sorted(issues, key=lambda x: severity_order.get(x.severity, 4))
    
    # Get metrics
    result = await db.execute(
        select(ValidationMetric).where(ValidationMetric.job_id == job_id)
    )
    metrics = result.scalars().all()
    
    return ValidationResultsResponse(
        job=ValidationJobResponse(
            id=job.id,
            dataset_id=job.dataset_id,
            status=job.status,
            started_at=job.started_at,
            completed_at=job.completed_at,
            total_issues=job.total_issues,
            critical_issues=job.critical_issues,
            high_issues=job.high_issues,
            medium_issues=job.medium_issues,
            low_issues=job.low_issues,
            quality_score=job.quality_score,
            error_message=job.error_message,
            ai_summary=job.ai_summary,
            ai_analysis_completed=job.ai_analysis_completed,
            created_at=job.created_at
        ),
        issues=[
            ValidationIssueResponse(
                id=i.id,
                job_id=i.job_id,
                issue_type=i.issue_type,
                severity=i.severity,
                column_name=i.column_name,
                affected_rows=i.affected_rows,
                affected_percentage=i.affected_percentage,
                description=i.description,
                sample_values=i.sample_values,
                ai_explanation=i.ai_explanation,
                ai_cause=i.ai_cause,
                ai_remediation=i.ai_remediation,
                created_at=i.created_at
            )
            for i in issues
        ],
        metrics=[
            ValidationMetricResponse(
                id=m.id,
                job_id=m.job_id,
                metric_name=m.metric_name,
                metric_value=m.metric_value,
                column_name=m.column_name,
                details=m.details,
                created_at=m.created_at
            )
            for m in metrics
        ]
    )


# ============== AI Analysis Endpoints ==============
async def run_ai_analysis(job_id: str):
    """Background task to run AI analysis on validation results"""
    from database import async_session_maker
    
    async with async_session_maker() as db:
        try:
            # Get job and issues
            result = await db.execute(select(ValidationJob).where(ValidationJob.id == job_id))
            job = result.scalar_one_or_none()
            
            if not job:
                return
            
            # Get dataset info
            result = await db.execute(select(Dataset).where(Dataset.id == job.dataset_id))
            dataset = result.scalar_one_or_none()
            
            if not dataset:
                return
            
            dataset_info = {
                "name": dataset.name,
                "row_count": dataset.row_count,
                "column_count": dataset.column_count
            }
            
            # Get issues
            result = await db.execute(
                select(ValidationIssue).where(ValidationIssue.job_id == job_id)
            )
            issues = result.scalars().all()
            
            # Get metrics
            result = await db.execute(
                select(ValidationMetric).where(ValidationMetric.job_id == job_id)
            )
            metrics = result.scalars().all()
            
            # Prepare issue data for AI
            issue_data = [
                {
                    "issue_type": i.issue_type,
                    "severity": i.severity,
                    "column_name": i.column_name,
                    "affected_rows": i.affected_rows,
                    "affected_percentage": i.affected_percentage,
                    "description": i.description,
                    "sample_values": i.sample_values
                }
                for i in issues
            ]
            
            metric_data = [
                {
                    "metric_name": m.metric_name,
                    "metric_value": m.metric_value,
                    "column_name": m.column_name,
                    "details": m.details
                }
                for m in metrics
            ]
            
            # Run AI analysis on issues
            analyzed_issues = await ai_service.analyze_issues(issue_data, dataset_info)
            
            # Update issues with AI analysis
            for i, issue in enumerate(issues):
                if i < len(analyzed_issues):
                    issue.ai_explanation = analyzed_issues[i].get("ai_explanation")
                    issue.ai_cause = analyzed_issues[i].get("ai_cause")
                    issue.ai_remediation = analyzed_issues[i].get("ai_remediation")
            
            # Generate summary
            summary = await ai_service.generate_summary(
                analyzed_issues, metric_data, job.quality_score, dataset_info
            )
            
            job.ai_summary = summary
            job.ai_analysis_completed = True
            
            await db.commit()
            
            logger.info(f"AI analysis completed for job {job_id}")
            
        except Exception as e:
            logger.error(f"AI analysis failed for job {job_id}: {e}")


@api_router.post("/ai/analyze/{job_id}")
async def trigger_ai_analysis(
    job_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Trigger AI analysis for a completed validation job"""
    
    # Verify job exists and is completed
    result = await db.execute(select(ValidationJob).where(ValidationJob.id == job_id))
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Validation job not found")
    
    if job.status != "completed":
        raise HTTPException(status_code=400, detail="Job must be completed before AI analysis")
    
    # Run AI analysis in background
    background_tasks.add_task(run_ai_analysis, job_id)
    
    return {"message": "AI analysis started", "job_id": job_id}


# ============== Referential Integrity ==============
@api_router.post("/validation/referential-integrity")
async def check_referential_integrity_endpoint(
    check: ReferentialIntegrityCheck,
    db: AsyncSession = Depends(get_db)
):
    """Check referential integrity between two datasets"""
    
    # Get source dataset
    result = await db.execute(select(Dataset).where(Dataset.id == check.source_dataset_id))
    source_dataset = result.scalar_one_or_none()
    
    if not source_dataset:
        raise HTTPException(status_code=404, detail="Source dataset not found")
    
    # Get target dataset
    result = await db.execute(select(Dataset).where(Dataset.id == check.target_dataset_id))
    target_dataset = result.scalar_one_or_none()
    
    if not target_dataset:
        raise HTTPException(status_code=404, detail="Target dataset not found")
    
    # Load data
    source_df = pd.read_csv(source_dataset.file_path)
    target_df = pd.read_csv(target_dataset.file_path)
    
    # Check referential integrity
    result = check_referential_integrity(
        source_df, check.source_column,
        target_df, check.target_column
    )
    
    return result


# ============== Scheduled Jobs ==============
@api_router.post("/schedules", response_model=ScheduledJobResponse)
async def create_schedule(
    schedule: ScheduledJobCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a scheduled validation job"""
    
    # Verify dataset exists
    result = await db.execute(select(Dataset).where(Dataset.id == schedule.dataset_id))
    dataset = result.scalar_one_or_none()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Calculate next run (daily at midnight UTC)
    now = datetime.utcnow()
    next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    scheduled_job = ScheduledJob(
        dataset_id=schedule.dataset_id,
        schedule_type=schedule.schedule_type,
        cron_expression=schedule.cron_expression,
        next_run=next_run
    )
    
    db.add(scheduled_job)
    await db.commit()
    await db.refresh(scheduled_job)
    
    return ScheduledJobResponse(
        id=scheduled_job.id,
        dataset_id=scheduled_job.dataset_id,
        schedule_type=scheduled_job.schedule_type,
        cron_expression=scheduled_job.cron_expression,
        is_active=scheduled_job.is_active,
        last_run=scheduled_job.last_run,
        next_run=scheduled_job.next_run,
        created_at=scheduled_job.created_at
    )


@api_router.get("/schedules", response_model=List[ScheduledJobResponse])
async def list_schedules(db: AsyncSession = Depends(get_db)):
    """List all scheduled jobs"""
    result = await db.execute(select(ScheduledJob).order_by(desc(ScheduledJob.created_at)))
    schedules = result.scalars().all()
    
    return [
        ScheduledJobResponse(
            id=s.id,
            dataset_id=s.dataset_id,
            schedule_type=s.schedule_type,
            cron_expression=s.cron_expression,
            is_active=s.is_active,
            last_run=s.last_run,
            next_run=s.next_run,
            created_at=s.created_at
        )
        for s in schedules
    ]


@api_router.delete("/schedules/{schedule_id}")
async def delete_schedule(schedule_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a scheduled job"""
    result = await db.execute(select(ScheduledJob).where(ScheduledJob.id == schedule_id))
    schedule = result.scalar_one_or_none()
    
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    
    await db.delete(schedule)
    await db.commit()
    
    return {"message": "Schedule deleted successfully"}


# ============== Dashboard Stats ==============
@api_router.get("/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats(db: AsyncSession = Depends(get_db)):
    """Get dashboard statistics"""
    
    # Total datasets
    result = await db.execute(select(func.count(Dataset.id)))
    total_datasets = result.scalar() or 0
    
    # Total jobs
    result = await db.execute(select(func.count(ValidationJob.id)))
    total_jobs = result.scalar() or 0
    
    # Total issues
    result = await db.execute(select(func.count(ValidationIssue.id)))
    total_issues = result.scalar() or 0
    
    # Average quality score
    result = await db.execute(
        select(func.avg(ValidationJob.quality_score))
        .where(ValidationJob.status == "completed")
    )
    avg_quality = result.scalar() or 100.0
    
    # Recent jobs with dataset names
    result = await db.execute(
        select(ValidationJob, Dataset.name)
        .join(Dataset, ValidationJob.dataset_id == Dataset.id)
        .order_by(desc(ValidationJob.created_at))
        .limit(10)
    )
    recent_jobs_data = result.all()
    
    recent_jobs = [
        ValidationJobWithDataset(
            id=job.id,
            dataset_id=job.dataset_id,
            dataset_name=name,
            status=job.status,
            started_at=job.started_at,
            completed_at=job.completed_at,
            total_issues=job.total_issues,
            critical_issues=job.critical_issues,
            high_issues=job.high_issues,
            medium_issues=job.medium_issues,
            low_issues=job.low_issues,
            quality_score=job.quality_score,
            error_message=job.error_message,
            ai_summary=job.ai_summary,
            ai_analysis_completed=job.ai_analysis_completed,
            created_at=job.created_at
        )
        for job, name in recent_jobs_data
    ]
    
    # Issues by type
    result = await db.execute(
        select(ValidationIssue.issue_type, func.count(ValidationIssue.id))
        .group_by(ValidationIssue.issue_type)
    )
    issues_by_type = {row[0]: row[1] for row in result.all()}
    
    # Issues by severity
    result = await db.execute(
        select(ValidationIssue.severity, func.count(ValidationIssue.id))
        .group_by(ValidationIssue.severity)
    )
    issues_by_severity = {row[0]: row[1] for row in result.all()}
    
    return DashboardStats(
        total_datasets=total_datasets,
        total_jobs=total_jobs,
        total_issues=total_issues,
        avg_quality_score=round(avg_quality, 1),
        recent_jobs=recent_jobs,
        issues_by_type=issues_by_type,
        issues_by_severity=issues_by_severity
    )


# ============== Dataset Relationships ==============
@api_router.post("/relationships", response_model=DatasetRelationshipResponse)
async def create_relationship(
    relationship: DatasetRelationshipCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a foreign key relationship between datasets"""
    
    # Verify both datasets exist
    source = await db.execute(select(Dataset).where(Dataset.id == relationship.source_dataset_id))
    if not source.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Source dataset not found")
    
    target = await db.execute(select(Dataset).where(Dataset.id == relationship.target_dataset_id))
    if not target.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Target dataset not found")
    
    # Create relationship
    db_relationship = DatasetRelationship(
        source_dataset_id=relationship.source_dataset_id,
        source_column=relationship.source_column,
        target_dataset_id=relationship.target_dataset_id,
        target_column=relationship.target_column,
        relationship_name=relationship.relationship_name,
        relationship_type=relationship.relationship_type,
        is_required=relationship.is_required
    )
    
    db.add(db_relationship)
    await db.commit()
    await db.refresh(db_relationship)
    
    logger.info(f"Created relationship: {relationship.source_column} -> {relationship.target_column}")
    
    return DatasetRelationshipResponse(
        id=db_relationship.id,
        source_dataset_id=db_relationship.source_dataset_id,
        source_column=db_relationship.source_column,
        target_dataset_id=db_relationship.target_dataset_id,
        target_column=db_relationship.target_column,
        relationship_name=db_relationship.relationship_name,
        relationship_type=db_relationship.relationship_type,
        is_required=db_relationship.is_required,
        created_at=db_relationship.created_at
    )


@api_router.get("/relationships", response_model=List[DatasetRelationshipWithNames])
async def list_relationships(db: AsyncSession = Depends(get_db)):
    """List all dataset relationships"""
    result = await db.execute(
        select(DatasetRelationship)
        .order_by(desc(DatasetRelationship.created_at))
    )
    relationships = result.scalars().all()
    
    response = []
    for rel in relationships:
        # Get dataset names
        source_result = await db.execute(select(Dataset.name).where(Dataset.id == rel.source_dataset_id))
        target_result = await db.execute(select(Dataset.name).where(Dataset.id == rel.target_dataset_id))
        
        source_name = source_result.scalar() or "Unknown"
        target_name = target_result.scalar() or "Unknown"
        
        response.append(DatasetRelationshipWithNames(
            id=rel.id,
            source_dataset_id=rel.source_dataset_id,
            source_column=rel.source_column,
            target_dataset_id=rel.target_dataset_id,
            target_column=rel.target_column,
            relationship_name=rel.relationship_name,
            relationship_type=rel.relationship_type,
            is_required=rel.is_required,
            created_at=rel.created_at,
            source_dataset_name=source_name,
            target_dataset_name=target_name
        ))
    
    return response


@api_router.get("/relationships/dataset/{dataset_id}", response_model=List[DatasetRelationshipWithNames])
async def get_dataset_relationships(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Get all relationships for a specific dataset"""
    result = await db.execute(
        select(DatasetRelationship)
        .where(
            (DatasetRelationship.source_dataset_id == dataset_id) |
            (DatasetRelationship.target_dataset_id == dataset_id)
        )
    )
    relationships = result.scalars().all()
    
    response = []
    for rel in relationships:
        source_result = await db.execute(select(Dataset.name).where(Dataset.id == rel.source_dataset_id))
        target_result = await db.execute(select(Dataset.name).where(Dataset.id == rel.target_dataset_id))
        
        response.append(DatasetRelationshipWithNames(
            id=rel.id,
            source_dataset_id=rel.source_dataset_id,
            source_column=rel.source_column,
            target_dataset_id=rel.target_dataset_id,
            target_column=rel.target_column,
            relationship_name=rel.relationship_name,
            relationship_type=rel.relationship_type,
            is_required=rel.is_required,
            created_at=rel.created_at,
            source_dataset_name=source_result.scalar() or "Unknown",
            target_dataset_name=target_result.scalar() or "Unknown"
        ))
    
    return response


@api_router.delete("/relationships/{relationship_id}")
async def delete_relationship(relationship_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a dataset relationship"""
    result = await db.execute(
        select(DatasetRelationship).where(DatasetRelationship.id == relationship_id)
    )
    relationship = result.scalar_one_or_none()
    
    if not relationship:
        raise HTTPException(status_code=404, detail="Relationship not found")
    
    await db.delete(relationship)
    await db.commit()
    
    return {"message": "Relationship deleted successfully"}


# ============== Referential Integrity Validation ==============
@api_router.post("/validation/integrity/{job_id}")
async def validate_referential_integrity(
    job_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Run referential integrity checks for all relationships of a dataset"""
    
    # Get job and dataset
    result = await db.execute(select(ValidationJob).where(ValidationJob.id == job_id))
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Validation job not found")
    
    # Run integrity checks in background
    background_tasks.add_task(run_integrity_checks, job_id, job.dataset_id)
    
    return {"message": "Referential integrity validation started", "job_id": job_id}


async def run_integrity_checks(job_id: str, dataset_id: str):
    """Background task to run referential integrity checks"""
    from database import async_session_maker
    
    async with async_session_maker() as db:
        try:
            # Get all relationships where this dataset is the source
            result = await db.execute(
                select(DatasetRelationship)
                .where(DatasetRelationship.source_dataset_id == dataset_id)
            )
            relationships = result.scalars().all()
            
            if not relationships:
                logger.info(f"No relationships found for dataset {dataset_id}")
                return
            
            # Get source dataset
            result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
            source_dataset = result.scalar_one_or_none()
            
            if not source_dataset or not source_dataset.file_path:
                return
            
            source_df = pd.read_csv(source_dataset.file_path)
            
            for rel in relationships:
                # Get target dataset
                result = await db.execute(select(Dataset).where(Dataset.id == rel.target_dataset_id))
                target_dataset = result.scalar_one_or_none()
                
                if not target_dataset or not target_dataset.file_path:
                    continue
                
                target_df = pd.read_csv(target_dataset.file_path)
                
                # Run integrity check
                check_result = check_referential_integrity(
                    source_df, rel.source_column,
                    target_df, rel.target_column
                )
                
                # Save result
                integrity_result = ReferentialIntegrityResult(
                    job_id=job_id,
                    relationship_id=rel.id,
                    is_valid=check_result["valid"],
                    orphaned_count=check_result.get("orphaned_count", 0),
                    orphaned_rows=check_result.get("orphaned_rows", 0),
                    orphaned_values=check_result.get("orphaned_values"),
                    source_unique_values=check_result.get("source_unique_values", 0),
                    target_unique_values=check_result.get("target_unique_values", 0)
                )
                
                db.add(integrity_result)
                
                # If integrity fails, add issue to validation job
                if not check_result["valid"] and rel.is_required:
                    issue = ValidationIssue(
                        job_id=job_id,
                        issue_type="referential_integrity",
                        severity="high" if check_result.get("orphaned_rows", 0) > 10 else "medium",
                        column_name=rel.source_column,
                        affected_rows=check_result.get("orphaned_rows", 0),
                        affected_percentage=(check_result.get("orphaned_rows", 0) / len(source_df) * 100) if len(source_df) > 0 else 0,
                        description=f"Found {check_result.get('orphaned_count', 0)} orphaned values in '{rel.source_column}' that don't exist in target dataset",
                        sample_values=check_result.get("orphaned_values", [])[:10]
                    )
                    db.add(issue)
            
            await db.commit()
            logger.info(f"Referential integrity checks completed for job {job_id}")
            
        except Exception as e:
            logger.error(f"Referential integrity check failed: {e}")


@api_router.get("/validation/integrity/{job_id}", response_model=List[ReferentialIntegrityResultResponse])
async def get_integrity_results(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get referential integrity results for a validation job"""
    result = await db.execute(
        select(ReferentialIntegrityResult)
        .where(ReferentialIntegrityResult.job_id == job_id)
    )
    results = result.scalars().all()
    
    response = []
    for r in results:
        # Get relationship details
        rel_result = await db.execute(
            select(DatasetRelationship).where(DatasetRelationship.id == r.relationship_id)
        )
        rel = rel_result.scalar_one_or_none()
        
        response.append(ReferentialIntegrityResultResponse(
            id=r.id,
            job_id=r.job_id,
            relationship_id=r.relationship_id,
            is_valid=r.is_valid,
            orphaned_count=r.orphaned_count,
            orphaned_rows=r.orphaned_rows,
            orphaned_values=r.orphaned_values,
            source_unique_values=r.source_unique_values,
            target_unique_values=r.target_unique_values,
            created_at=r.created_at,
            relationship=DatasetRelationshipResponse(
                id=rel.id,
                source_dataset_id=rel.source_dataset_id,
                source_column=rel.source_column,
                target_dataset_id=rel.target_dataset_id,
                target_column=rel.target_column,
                relationship_name=rel.relationship_name,
                relationship_type=rel.relationship_type,
                is_required=rel.is_required,
                created_at=rel.created_at
            ) if rel else None
        ))
    
    return response


# ============== Data Lineage ==============
@api_router.post("/lineage", response_model=DatasetLineageResponse)
async def create_lineage(
    lineage: DatasetLineageCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a data lineage entry"""
    
    # Verify dataset exists
    result = await db.execute(select(Dataset).where(Dataset.id == lineage.dataset_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Verify parent dataset if provided
    if lineage.parent_dataset_id:
        result = await db.execute(select(Dataset).where(Dataset.id == lineage.parent_dataset_id))
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Parent dataset not found")
    
    db_lineage = DatasetLineage(
        dataset_id=lineage.dataset_id,
        lineage_type=lineage.lineage_type,
        parent_dataset_id=lineage.parent_dataset_id,
        transformation_description=lineage.transformation_description,
        transformation_query=lineage.transformation_query,
        source_system=lineage.source_system,
        source_details=lineage.source_details
    )
    
    db.add(db_lineage)
    await db.commit()
    await db.refresh(db_lineage)
    
    return DatasetLineageResponse(
        id=db_lineage.id,
        dataset_id=db_lineage.dataset_id,
        lineage_type=db_lineage.lineage_type,
        parent_dataset_id=db_lineage.parent_dataset_id,
        transformation_description=db_lineage.transformation_description,
        transformation_query=db_lineage.transformation_query,
        source_system=db_lineage.source_system,
        source_details=db_lineage.source_details,
        created_at=db_lineage.created_at,
        updated_at=db_lineage.updated_at
    )


@api_router.get("/lineage", response_model=List[DatasetLineageWithNames])
async def list_lineage(db: AsyncSession = Depends(get_db)):
    """List all data lineage entries"""
    result = await db.execute(
        select(DatasetLineage).order_by(desc(DatasetLineage.created_at))
    )
    lineages = result.scalars().all()
    
    response = []
    for lin in lineages:
        # Get dataset names
        ds_result = await db.execute(select(Dataset.name).where(Dataset.id == lin.dataset_id))
        dataset_name = ds_result.scalar() or "Unknown"
        
        parent_name = None
        if lin.parent_dataset_id:
            parent_result = await db.execute(select(Dataset.name).where(Dataset.id == lin.parent_dataset_id))
            parent_name = parent_result.scalar()
        
        response.append(DatasetLineageWithNames(
            id=lin.id,
            dataset_id=lin.dataset_id,
            lineage_type=lin.lineage_type,
            parent_dataset_id=lin.parent_dataset_id,
            transformation_description=lin.transformation_description,
            transformation_query=lin.transformation_query,
            source_system=lin.source_system,
            source_details=lin.source_details,
            created_at=lin.created_at,
            updated_at=lin.updated_at,
            dataset_name=dataset_name,
            parent_dataset_name=parent_name
        ))
    
    return response


@api_router.get("/lineage/dataset/{dataset_id}", response_model=List[DatasetLineageWithNames])
async def get_dataset_lineage(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Get lineage for a specific dataset"""
    result = await db.execute(
        select(DatasetLineage)
        .where(
            (DatasetLineage.dataset_id == dataset_id) |
            (DatasetLineage.parent_dataset_id == dataset_id)
        )
    )
    lineages = result.scalars().all()
    
    response = []
    for lin in lineages:
        ds_result = await db.execute(select(Dataset.name).where(Dataset.id == lin.dataset_id))
        dataset_name = ds_result.scalar() or "Unknown"
        
        parent_name = None
        if lin.parent_dataset_id:
            parent_result = await db.execute(select(Dataset.name).where(Dataset.id == lin.parent_dataset_id))
            parent_name = parent_result.scalar()
        
        response.append(DatasetLineageWithNames(
            id=lin.id,
            dataset_id=lin.dataset_id,
            lineage_type=lin.lineage_type,
            parent_dataset_id=lin.parent_dataset_id,
            transformation_description=lin.transformation_description,
            transformation_query=lin.transformation_query,
            source_system=lin.source_system,
            source_details=lin.source_details,
            created_at=lin.created_at,
            updated_at=lin.updated_at,
            dataset_name=dataset_name,
            parent_dataset_name=parent_name
        ))
    
    return response


@api_router.delete("/lineage/{lineage_id}")
async def delete_lineage(lineage_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a lineage entry"""
    result = await db.execute(select(DatasetLineage).where(DatasetLineage.id == lineage_id))
    lineage = result.scalar_one_or_none()
    
    if not lineage:
        raise HTTPException(status_code=404, detail="Lineage entry not found")
    
    await db.delete(lineage)
    await db.commit()
    
    return {"message": "Lineage entry deleted successfully"}


# ============== Validation History ==============
@api_router.get("/history/dataset/{dataset_id}", response_model=List[ValidationHistoryResponse])
async def get_validation_history(
    dataset_id: str,
    limit: int = 30,
    db: AsyncSession = Depends(get_db)
):
    """Get validation history for a dataset"""
    result = await db.execute(
        select(ValidationHistory)
        .where(ValidationHistory.dataset_id == dataset_id)
        .order_by(desc(ValidationHistory.validated_at))
        .limit(limit)
    )
    history = result.scalars().all()
    
    return [
        ValidationHistoryResponse(
            id=h.id,
            dataset_id=h.dataset_id,
            job_id=h.job_id,
            quality_score=h.quality_score,
            total_issues=h.total_issues,
            critical_issues=h.critical_issues,
            high_issues=h.high_issues,
            medium_issues=h.medium_issues,
            low_issues=h.low_issues,
            completeness=h.completeness,
            duplicate_percentage=h.duplicate_percentage,
            referential_integrity_score=h.referential_integrity_score,
            validated_at=h.validated_at,
            created_at=h.created_at
        )
        for h in history
    ]


# ============== Dependency Graph ==============
@api_router.get("/graph/dependencies", response_model=DependencyGraphResponse)
async def get_dependency_graph(db: AsyncSession = Depends(get_db)):
    """Get the full dependency graph of all datasets"""
    
    # Get all datasets
    result = await db.execute(select(Dataset))
    datasets = result.scalars().all()
    
    # Get all relationships
    result = await db.execute(select(DatasetRelationship))
    relationships = result.scalars().all()
    
    # Get all lineage entries
    result = await db.execute(select(DatasetLineage))
    lineages = result.scalars().all()
    
    # Get latest validation job for each dataset
    latest_scores = {}
    for ds in datasets:
        result = await db.execute(
            select(ValidationJob)
            .where(ValidationJob.dataset_id == ds.id)
            .where(ValidationJob.status == "completed")
            .order_by(desc(ValidationJob.completed_at))
            .limit(1)
        )
        job = result.scalar_one_or_none()
        if job:
            latest_scores[ds.id] = {
                "quality_score": job.quality_score,
                "has_issues": job.total_issues > 0
            }
    
    # Build nodes
    nodes = []
    for ds in datasets:
        score_info = latest_scores.get(ds.id, {})
        nodes.append(GraphNode(
            id=ds.id,
            name=ds.name,
            type="dataset",
            row_count=ds.row_count,
            quality_score=score_info.get("quality_score"),
            has_issues=score_info.get("has_issues", False)
        ))
    
    # Add source nodes from lineage
    source_nodes = set()
    for lin in lineages:
        if lin.lineage_type == "source" and lin.source_system:
            source_id = f"source_{lin.source_system.replace(' ', '_')}"
            if source_id not in source_nodes:
                source_nodes.add(source_id)
                nodes.append(GraphNode(
                    id=source_id,
                    name=lin.source_system,
                    type="source",
                    row_count=0
                ))
    
    # Build edges
    edges = []
    
    # Relationship edges
    for rel in relationships:
        # Get latest integrity result
        result = await db.execute(
            select(ReferentialIntegrityResult)
            .where(ReferentialIntegrityResult.relationship_id == rel.id)
            .order_by(desc(ReferentialIntegrityResult.created_at))
            .limit(1)
        )
        integrity = result.scalar_one_or_none()
        
        edges.append(GraphEdge(
            id=f"rel_{rel.id}",
            source=rel.source_dataset_id,
            target=rel.target_dataset_id,
            label=f"{rel.source_column} → {rel.target_column}",
            type="relationship",
            is_valid=integrity.is_valid if integrity else None
        ))
    
    # Lineage edges
    for lin in lineages:
        if lin.lineage_type == "source" and lin.source_system:
            source_id = f"source_{lin.source_system.replace(' ', '_')}"
            edges.append(GraphEdge(
                id=f"lin_{lin.id}",
                source=source_id,
                target=lin.dataset_id,
                label=lin.transformation_description or "ingestion",
                type="lineage"
            ))
        elif lin.parent_dataset_id:
            edges.append(GraphEdge(
                id=f"lin_{lin.id}",
                source=lin.parent_dataset_id,
                target=lin.dataset_id,
                label=lin.transformation_description or "derived",
                type="lineage"
            ))
    
    return DependencyGraphResponse(nodes=nodes, edges=edges)


# Include the router in the main app
app.include_router(api_router)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    logger.info("Initializing database...")
    await init_db()
    logger.info("Database initialized successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await engine.dispose()
    logger.info("Database connections closed")
