"""SQLAlchemy Database Models for Data Quality Monitor"""
from sqlalchemy import Column, String, Integer, Float, Text, DateTime, JSON, ForeignKey, Enum, Boolean
from sqlalchemy.orm import relationship
from database import Base
import enum
from datetime import datetime
import uuid


def generate_uuid():
    return str(uuid.uuid4())


def utc_now():
    """Return current UTC time as timezone-naive datetime for PostgreSQL compatibility"""
    return datetime.utcnow()


class ValidationStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class IssueSeverity(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IssueType(str, enum.Enum):
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


class Dataset(Base):
    __tablename__ = "datasets"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    source_type = Column(String(50), nullable=False)  # 'csv' or 'api'
    row_count = Column(Integer, default=0)
    column_count = Column(Integer, default=0)
    schema_info = Column(JSON, nullable=True)  # Store column names and types
    sample_data = Column(JSON, nullable=True)  # First few rows for preview
    file_path = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now())
    updated_at = Column(DateTime, default=lambda: datetime.now(), onupdate=lambda: datetime.now())
    
    # Configuration for validation
    primary_key_columns = Column(JSON, nullable=True)  # List of column names
    expected_schema = Column(JSON, nullable=True)  # Expected column types
    categorical_columns = Column(JSON, nullable=True)  # Columns with expected categories
    numeric_ranges = Column(JSON, nullable=True)  # Min/max for numeric columns
    freshness_columns = Column(JSON, nullable=True)  # Timestamp columns with freshness rules
    
    # Relationships
    validation_jobs = relationship("ValidationJob", back_populates="dataset", cascade="all, delete-orphan")


class ValidationJob(Base):
    __tablename__ = "validation_jobs"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    status = Column(String(20), default=ValidationStatus.PENDING.value)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    total_issues = Column(Integer, default=0)
    critical_issues = Column(Integer, default=0)
    high_issues = Column(Integer, default=0)
    medium_issues = Column(Integer, default=0)
    low_issues = Column(Integer, default=0)
    quality_score = Column(Float, default=100.0)  # 0-100 score
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now())
    
    # AI Analysis
    ai_summary = Column(Text, nullable=True)
    ai_analysis_completed = Column(Boolean, default=False)
    
    # Relationships
    dataset = relationship("Dataset", back_populates="validation_jobs")
    issues = relationship("ValidationIssue", back_populates="job", cascade="all, delete-orphan")
    metrics = relationship("ValidationMetric", back_populates="job", cascade="all, delete-orphan")


class ValidationIssue(Base):
    __tablename__ = "validation_issues"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    job_id = Column(String, ForeignKey("validation_jobs.id"), nullable=False)
    issue_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)
    column_name = Column(String(255), nullable=True)
    affected_rows = Column(Integer, default=0)
    affected_percentage = Column(Float, default=0.0)
    description = Column(Text, nullable=False)
    sample_values = Column(JSON, nullable=True)  # Example problematic values
    
    # AI-generated fields
    ai_explanation = Column(Text, nullable=True)
    ai_cause = Column(Text, nullable=True)
    ai_remediation = Column(Text, nullable=True)
    
    created_at = Column(DateTime, default=lambda: datetime.now())
    
    # Relationships
    job = relationship("ValidationJob", back_populates="issues")


class ValidationMetric(Base):
    __tablename__ = "validation_metrics"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    job_id = Column(String, ForeignKey("validation_jobs.id"), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Float, nullable=False)
    column_name = Column(String(255), nullable=True)
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now())
    
    # Relationships
    job = relationship("ValidationJob", back_populates="metrics")


class ScheduledJob(Base):
    __tablename__ = "scheduled_jobs"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    schedule_type = Column(String(50), default="daily")  # daily, hourly, cron
    cron_expression = Column(String(100), nullable=True)
    is_active = Column(Boolean, default=True)
    last_run = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now())
    updated_at = Column(DateTime, default=lambda: datetime.now(), onupdate=lambda: datetime.now())


class DatasetRelationship(Base):
    """Defines foreign key relationships between datasets"""
    __tablename__ = "dataset_relationships"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    source_dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    source_column = Column(String(255), nullable=False)
    target_dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    target_column = Column(String(255), nullable=False)
    relationship_name = Column(String(255), nullable=True)
    relationship_type = Column(String(50), default="foreign_key")  # foreign_key, lookup, derived
    is_required = Column(Boolean, default=True)  # If true, all source values must exist in target
    created_at = Column(DateTime, default=utc_now)
    
    # Relationships
    source_dataset = relationship("Dataset", foreign_keys=[source_dataset_id], backref="outgoing_relationships")
    target_dataset = relationship("Dataset", foreign_keys=[target_dataset_id], backref="incoming_relationships")


class ReferentialIntegrityResult(Base):
    """Stores results of referential integrity checks"""
    __tablename__ = "referential_integrity_results"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    job_id = Column(String, ForeignKey("validation_jobs.id"), nullable=False)
    relationship_id = Column(String, ForeignKey("dataset_relationships.id"), nullable=False)
    is_valid = Column(Boolean, default=True)
    orphaned_count = Column(Integer, default=0)
    orphaned_rows = Column(Integer, default=0)
    orphaned_values = Column(JSON, nullable=True)  # Sample of orphaned values
    source_unique_values = Column(Integer, default=0)
    target_unique_values = Column(Integer, default=0)
    created_at = Column(DateTime, default=utc_now)
    
    # Relationships
    job = relationship("ValidationJob", backref="integrity_results")
    relationship = relationship("DatasetRelationship", backref="integrity_results")


class DatasetLineage(Base):
    """Tracks data lineage - source → transformations → outputs"""
    __tablename__ = "dataset_lineage"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    lineage_type = Column(String(50), nullable=False)  # source, transformation, output
    parent_dataset_id = Column(String, ForeignKey("datasets.id"), nullable=True)  # For derived datasets
    transformation_description = Column(Text, nullable=True)
    transformation_query = Column(Text, nullable=True)  # SQL or code used
    source_system = Column(String(255), nullable=True)  # External source name
    source_details = Column(JSON, nullable=True)  # Connection info, file path, etc.
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)
    
    # Relationships
    dataset = relationship("Dataset", foreign_keys=[dataset_id], backref="lineage_entries")
    parent_dataset = relationship("Dataset", foreign_keys=[parent_dataset_id], backref="derived_datasets")


class ValidationHistory(Base):
    """Stores summary of validation history for trend analysis"""
    __tablename__ = "validation_history"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    job_id = Column(String, ForeignKey("validation_jobs.id"), nullable=False)
    quality_score = Column(Float, nullable=False)
    total_issues = Column(Integer, default=0)
    critical_issues = Column(Integer, default=0)
    high_issues = Column(Integer, default=0)
    medium_issues = Column(Integer, default=0)
    low_issues = Column(Integer, default=0)
    completeness = Column(Float, nullable=True)  # Data completeness percentage
    duplicate_percentage = Column(Float, nullable=True)
    referential_integrity_score = Column(Float, nullable=True)  # % of valid relationships
    validated_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=utc_now)
    
    # Relationships
    dataset = relationship("Dataset", backref="validation_history")
    job = relationship("ValidationJob", backref="history_entry")
