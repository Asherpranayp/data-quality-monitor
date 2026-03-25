"""Pydantic Schemas for API Request/Response Models"""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class ValidationStatusEnum(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class IssueSeverityEnum(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IssueTypeEnum(str, Enum):
    MISSING_VALUES = "missing_values"
    DUPLICATES = "duplicates"
    SCHEMA_MISMATCH = "schema_mismatch"
    OUTLIERS = "outliers"
    TYPE_INCONSISTENCY = "type_inconsistency"
    UNEXPECTED_CATEGORICAL = "unexpected_categorical"
    RANGE_VIOLATION = "range_violation"
    FRESHNESS_ISSUE = "freshness_issue"
    PRIMARY_KEY_VIOLATION = "primary_key_violation"
    REFERENTIAL_INTEGRITY = "referential_integrity"


# Dataset Schemas
class DatasetBase(BaseModel):
    name: str
    description: Optional[str] = None


class DatasetCreate(DatasetBase):
    primary_key_columns: Optional[List[str]] = None
    expected_schema: Optional[Dict[str, str]] = None
    categorical_columns: Optional[Dict[str, List[str]]] = None
    numeric_ranges: Optional[Dict[str, Dict[str, float]]] = None
    freshness_columns: Optional[Dict[str, int]] = None  # Column: max_age_hours


class DatasetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    primary_key_columns: Optional[List[str]] = None
    expected_schema: Optional[Dict[str, str]] = None
    categorical_columns: Optional[Dict[str, List[str]]] = None
    numeric_ranges: Optional[Dict[str, Dict[str, float]]] = None
    freshness_columns: Optional[Dict[str, int]] = None


class DatasetResponse(DatasetBase):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    source_type: str
    row_count: int
    column_count: int
    schema_info: Optional[Dict[str, Any]] = None
    sample_data: Optional[List[Dict[str, Any]]] = None
    primary_key_columns: Optional[List[str]] = None
    expected_schema: Optional[Dict[str, str]] = None
    categorical_columns: Optional[Dict[str, List[str]]] = None
    numeric_ranges: Optional[Dict[str, Dict[str, float]]] = None
    freshness_columns: Optional[Dict[str, int]] = None
    created_at: datetime
    updated_at: datetime


class DatasetListResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    name: str
    source_type: str
    row_count: int
    column_count: int
    created_at: datetime


# JSON Data Input
class JSONDataInput(BaseModel):
    name: str
    description: Optional[str] = None
    data: List[Dict[str, Any]]
    primary_key_columns: Optional[List[str]] = None
    expected_schema: Optional[Dict[str, str]] = None


# Validation Job Schemas
class ValidationJobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    dataset_id: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_issues: int
    critical_issues: int
    high_issues: int
    medium_issues: int
    low_issues: int
    quality_score: float
    error_message: Optional[str] = None
    ai_summary: Optional[str] = None
    ai_analysis_completed: bool
    created_at: datetime


class ValidationJobWithDataset(ValidationJobResponse):
    dataset_name: str


# Validation Issue Schemas
class ValidationIssueResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    job_id: str
    issue_type: str
    severity: str
    column_name: Optional[str] = None
    affected_rows: int
    affected_percentage: float
    description: str
    sample_values: Optional[List[Any]] = None
    ai_explanation: Optional[str] = None
    ai_cause: Optional[str] = None
    ai_remediation: Optional[str] = None
    created_at: datetime


# Validation Metric Schemas
class ValidationMetricResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    job_id: str
    metric_name: str
    metric_value: float
    column_name: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    created_at: datetime


# Full Validation Results
class ValidationResultsResponse(BaseModel):
    job: ValidationJobResponse
    issues: List[ValidationIssueResponse]
    metrics: List[ValidationMetricResponse]


# Scheduled Job Schemas
class ScheduledJobCreate(BaseModel):
    dataset_id: str
    schedule_type: str = "daily"
    cron_expression: Optional[str] = None


class ScheduledJobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    dataset_id: str
    schedule_type: str
    cron_expression: Optional[str] = None
    is_active: bool
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    created_at: datetime


# Dashboard Stats
class DashboardStats(BaseModel):
    total_datasets: int
    total_jobs: int
    total_issues: int
    avg_quality_score: float
    recent_jobs: List[ValidationJobWithDataset]
    issues_by_type: Dict[str, int]
    issues_by_severity: Dict[str, int]


# AI Analysis Request
class AIAnalysisRequest(BaseModel):
    job_id: str


# Referential Integrity Check
class ReferentialIntegrityCheck(BaseModel):
    source_dataset_id: str
    source_column: str
    target_dataset_id: str
    target_column: str


# ============== Dataset Relationship Schemas ==============
class DatasetRelationshipCreate(BaseModel):
    source_dataset_id: str
    source_column: str
    target_dataset_id: str
    target_column: str
    relationship_name: Optional[str] = None
    relationship_type: str = "foreign_key"
    is_required: bool = True


class DatasetRelationshipResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    source_dataset_id: str
    source_column: str
    target_dataset_id: str
    target_column: str
    relationship_name: Optional[str] = None
    relationship_type: str
    is_required: bool
    created_at: datetime


class DatasetRelationshipWithNames(DatasetRelationshipResponse):
    source_dataset_name: str
    target_dataset_name: str


class ReferentialIntegrityResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    job_id: str
    relationship_id: str
    is_valid: bool
    orphaned_count: int
    orphaned_rows: int
    orphaned_values: Optional[List[Any]] = None
    source_unique_values: int
    target_unique_values: int
    created_at: datetime
    # Include relationship details
    relationship: Optional[DatasetRelationshipResponse] = None


# ============== Data Lineage Schemas ==============
class DatasetLineageCreate(BaseModel):
    dataset_id: str
    lineage_type: str  # source, transformation, output
    parent_dataset_id: Optional[str] = None
    transformation_description: Optional[str] = None
    transformation_query: Optional[str] = None
    source_system: Optional[str] = None
    source_details: Optional[Dict[str, Any]] = None


class DatasetLineageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    dataset_id: str
    lineage_type: str
    parent_dataset_id: Optional[str] = None
    transformation_description: Optional[str] = None
    transformation_query: Optional[str] = None
    source_system: Optional[str] = None
    source_details: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime


class DatasetLineageWithNames(DatasetLineageResponse):
    dataset_name: str
    parent_dataset_name: Optional[str] = None


# ============== Validation History Schemas ==============
class ValidationHistoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    dataset_id: str
    job_id: str
    quality_score: float
    total_issues: int
    critical_issues: int
    high_issues: int
    medium_issues: int
    low_issues: int
    completeness: Optional[float] = None
    duplicate_percentage: Optional[float] = None
    referential_integrity_score: Optional[float] = None
    validated_at: datetime
    created_at: datetime


# ============== Dependency Graph Schemas ==============
class GraphNode(BaseModel):
    id: str
    name: str
    type: str  # dataset, source, output
    row_count: int = 0
    quality_score: Optional[float] = None
    has_issues: bool = False


class GraphEdge(BaseModel):
    id: str
    source: str  # source node id
    target: str  # target node id
    label: Optional[str] = None
    type: str  # relationship, lineage
    is_valid: Optional[bool] = None  # For relationship edges


class DependencyGraphResponse(BaseModel):
    nodes: List[GraphNode]
    edges: List[GraphEdge]


# Extended Validation Results with Integrity
class ValidationResultsWithIntegrity(ValidationResultsResponse):
    integrity_results: List[ReferentialIntegrityResultResponse] = []
