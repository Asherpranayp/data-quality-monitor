"""Data Validation Engine - Comprehensive Data Quality Checks"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone, timedelta
from schemas import IssueSeverityEnum, IssueTypeEnum


class DataValidator:
    """Performs comprehensive data quality validation checks"""
    
    def __init__(self, df: pd.DataFrame, config: Dict[str, Any] = None):
        self.df = df
        self.config = config or {}
        self.issues: List[Dict[str, Any]] = []
        self.metrics: List[Dict[str, Any]] = []
        self.row_count = len(df)
        self.column_count = len(df.columns)
    
    def run_all_checks(self) -> Tuple[List[Dict], List[Dict], float]:
        """Run all validation checks and return issues, metrics, and quality score"""
        self.issues = []
        self.metrics = []
        
        # Run all validation checks
        self._check_missing_values()
        self._check_duplicates()
        self._check_schema_consistency()
        self._check_type_consistency()
        self._check_outliers()
        self._check_categorical_values()
        self._check_numeric_ranges()
        self._check_freshness()
        self._check_primary_key_uniqueness()
        
        # Calculate quality score
        quality_score = self._calculate_quality_score()
        
        return self.issues, self.metrics, quality_score
    
    def _add_issue(
        self,
        issue_type: IssueTypeEnum,
        severity: IssueSeverityEnum,
        description: str,
        column_name: Optional[str] = None,
        affected_rows: int = 0,
        sample_values: Optional[List] = None
    ):
        """Add an issue to the issues list"""
        affected_percentage = (affected_rows / self.row_count * 100) if self.row_count > 0 else 0
        self.issues.append({
            "issue_type": issue_type.value,
            "severity": severity.value,
            "description": description,
            "column_name": column_name,
            "affected_rows": affected_rows,
            "affected_percentage": round(affected_percentage, 2),
            "sample_values": sample_values[:10] if sample_values else None
        })
    
    def _add_metric(
        self,
        metric_name: str,
        metric_value: float,
        column_name: Optional[str] = None,
        details: Optional[Dict] = None
    ):
        """Add a metric to the metrics list"""
        self.metrics.append({
            "metric_name": metric_name,
            "metric_value": round(metric_value, 4),
            "column_name": column_name,
            "details": details
        })
    
    def _check_missing_values(self):
        """Check for missing values and null percentages by column"""
        total_nulls = 0
        
        for col in self.df.columns:
            null_count = self.df[col].isna().sum()
            null_percentage = (null_count / self.row_count * 100) if self.row_count > 0 else 0
            
            # Add metric for each column
            self._add_metric(
                "null_percentage",
                null_percentage,
                col,
                {"null_count": int(null_count), "total_rows": self.row_count}
            )
            
            total_nulls += null_count
            
            # Create issue if null percentage exceeds threshold
            if null_percentage > 0:
                severity = IssueSeverityEnum.LOW
                if null_percentage > 50:
                    severity = IssueSeverityEnum.CRITICAL
                elif null_percentage > 25:
                    severity = IssueSeverityEnum.HIGH
                elif null_percentage > 10:
                    severity = IssueSeverityEnum.MEDIUM
                
                if null_percentage > 5:  # Only report if more than 5%
                    self._add_issue(
                        IssueTypeEnum.MISSING_VALUES,
                        severity,
                        f"Column '{col}' has {null_percentage:.1f}% missing values ({null_count} rows)",
                        col,
                        int(null_count)
                    )
        
        # Overall completeness metric
        total_cells = self.row_count * self.column_count
        completeness = ((total_cells - total_nulls) / total_cells * 100) if total_cells > 0 else 100
        self._add_metric("completeness", completeness, details={"total_nulls": int(total_nulls)})
    
    def _check_duplicates(self):
        """Check for duplicate rows"""
        duplicate_count = self.df.duplicated().sum()
        duplicate_percentage = (duplicate_count / self.row_count * 100) if self.row_count > 0 else 0
        
        self._add_metric("duplicate_rows", duplicate_percentage, details={"count": int(duplicate_count)})
        
        if duplicate_count > 0:
            severity = IssueSeverityEnum.MEDIUM
            if duplicate_percentage > 20:
                severity = IssueSeverityEnum.HIGH
            elif duplicate_percentage > 50:
                severity = IssueSeverityEnum.CRITICAL
            
            # Get sample of duplicate values
            duplicates = self.df[self.df.duplicated(keep=False)]
            sample = duplicates.head(5).to_dict('records')
            
            self._add_issue(
                IssueTypeEnum.DUPLICATES,
                severity,
                f"Found {duplicate_count} duplicate rows ({duplicate_percentage:.1f}%)",
                affected_rows=int(duplicate_count),
                sample_values=sample
            )
    
    def _check_schema_consistency(self):
        """Check if schema matches expected schema"""
        expected_schema = self.config.get("expected_schema", {})
        
        if not expected_schema:
            return
        
        actual_schema = {col: str(self.df[col].dtype) for col in self.df.columns}
        
        # Check for missing columns
        for col, expected_type in expected_schema.items():
            if col not in self.df.columns:
                self._add_issue(
                    IssueTypeEnum.SCHEMA_MISMATCH,
                    IssueSeverityEnum.CRITICAL,
                    f"Expected column '{col}' not found in dataset",
                    col
                )
            elif expected_type.lower() not in actual_schema[col].lower():
                self._add_issue(
                    IssueTypeEnum.SCHEMA_MISMATCH,
                    IssueSeverityEnum.HIGH,
                    f"Column '{col}' has type '{actual_schema[col]}' but expected '{expected_type}'",
                    col,
                    self.row_count
                )
        
        # Check for extra columns
        for col in self.df.columns:
            if col not in expected_schema:
                self._add_issue(
                    IssueTypeEnum.SCHEMA_MISMATCH,
                    IssueSeverityEnum.LOW,
                    f"Unexpected column '{col}' found in dataset",
                    col
                )
    
    def _check_type_consistency(self):
        """Check for data type consistency within columns"""
        for col in self.df.columns:
            if self.df[col].dtype == 'object':
                # Check if column should be numeric
                try:
                    numeric_values = pd.to_numeric(self.df[col], errors='coerce')
                    non_numeric = numeric_values.isna() & ~self.df[col].isna()
                    non_numeric_count = non_numeric.sum()
                    
                    if non_numeric_count > 0 and non_numeric_count < len(self.df[col]) * 0.9:
                        # Mixed types detected
                        sample_non_numeric = self.df[col][non_numeric].head(5).tolist()
                        self._add_issue(
                            IssueTypeEnum.TYPE_INCONSISTENCY,
                            IssueSeverityEnum.MEDIUM,
                            f"Column '{col}' has mixed numeric and non-numeric values",
                            col,
                            int(non_numeric_count),
                            sample_non_numeric
                        )
                except Exception:
                    pass
                
                # Check for mixed datetime types
                try:
                    datetime_values = pd.to_datetime(self.df[col], errors='coerce')
                    non_datetime = datetime_values.isna() & ~self.df[col].isna()
                    non_datetime_count = non_datetime.sum()
                    
                    # If some but not all are valid dates
                    valid_dates = (~datetime_values.isna()).sum()
                    if valid_dates > 0 and non_datetime_count > 0 and valid_dates > len(self.df[col]) * 0.1:
                        self._add_issue(
                            IssueTypeEnum.TYPE_INCONSISTENCY,
                            IssueSeverityEnum.MEDIUM,
                            f"Column '{col}' has mixed date and non-date values",
                            col,
                            int(non_datetime_count)
                        )
                except Exception:
                    pass
    
    def _check_outliers(self):
        """Detect outliers in numeric columns using IQR method"""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            data = self.df[col].dropna()
            if len(data) < 4:
                continue
            
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = data[(data < lower_bound) | (data > upper_bound)]
            outlier_count = len(outliers)
            outlier_percentage = (outlier_count / len(data) * 100) if len(data) > 0 else 0
            
            self._add_metric(
                "outlier_percentage",
                outlier_percentage,
                col,
                {"count": outlier_count, "lower_bound": float(lower_bound), "upper_bound": float(upper_bound)}
            )
            
            if outlier_count > 0:
                severity = IssueSeverityEnum.LOW
                if outlier_percentage > 10:
                    severity = IssueSeverityEnum.MEDIUM
                elif outlier_percentage > 25:
                    severity = IssueSeverityEnum.HIGH
                
                self._add_issue(
                    IssueTypeEnum.OUTLIERS,
                    severity,
                    f"Column '{col}' has {outlier_count} outliers ({outlier_percentage:.1f}%) outside range [{lower_bound:.2f}, {upper_bound:.2f}]",
                    col,
                    outlier_count,
                    outliers.head(5).tolist()
                )
    
    def _check_categorical_values(self):
        """Check for unexpected categorical values"""
        categorical_columns = self.config.get("categorical_columns", {})
        
        for col, expected_values in categorical_columns.items():
            if col not in self.df.columns:
                continue
            
            actual_values = set(self.df[col].dropna().unique())
            expected_set = set(expected_values)
            unexpected = actual_values - expected_set
            
            if unexpected:
                unexpected_count = self.df[col].isin(unexpected).sum()
                self._add_issue(
                    IssueTypeEnum.UNEXPECTED_CATEGORICAL,
                    IssueSeverityEnum.MEDIUM,
                    f"Column '{col}' has {len(unexpected)} unexpected values: {list(unexpected)[:5]}",
                    col,
                    int(unexpected_count),
                    list(unexpected)[:10]
                )
    
    def _check_numeric_ranges(self):
        """Check if numeric values are within expected ranges"""
        numeric_ranges = self.config.get("numeric_ranges", {})
        
        for col, ranges in numeric_ranges.items():
            if col not in self.df.columns:
                continue
            
            min_val = ranges.get("min")
            max_val = ranges.get("max")
            
            violations = pd.Series([False] * len(self.df))
            
            if min_val is not None:
                violations |= self.df[col] < min_val
            if max_val is not None:
                violations |= self.df[col] > max_val
            
            violation_count = violations.sum()
            
            if violation_count > 0:
                sample = self.df[col][violations].head(5).tolist()
                self._add_issue(
                    IssueTypeEnum.RANGE_VIOLATION,
                    IssueSeverityEnum.MEDIUM,
                    f"Column '{col}' has {violation_count} values outside expected range [{min_val}, {max_val}]",
                    col,
                    int(violation_count),
                    sample
                )
    
    def _check_freshness(self):
        """Check freshness of timestamp/date columns"""
        freshness_columns = self.config.get("freshness_columns", {})
        current_time = datetime.now(timezone.utc)
        
        for col, max_age_hours in freshness_columns.items():
            if col not in self.df.columns:
                continue
            
            try:
                dates = pd.to_datetime(self.df[col], errors='coerce')
                
                # Make dates timezone-aware if they aren't
                if dates.dt.tz is None:
                    dates = dates.dt.tz_localize('UTC')
                
                threshold = current_time - timedelta(hours=max_age_hours)
                stale_count = (dates < threshold).sum()
                
                self._add_metric(
                    "freshness_stale_records",
                    float(stale_count),
                    col,
                    {"max_age_hours": max_age_hours, "threshold": threshold.isoformat()}
                )
                
                if stale_count > 0:
                    stale_percentage = (stale_count / self.row_count * 100) if self.row_count > 0 else 0
                    severity = IssueSeverityEnum.LOW
                    if stale_percentage > 50:
                        severity = IssueSeverityEnum.HIGH
                    elif stale_percentage > 25:
                        severity = IssueSeverityEnum.MEDIUM
                    
                    self._add_issue(
                        IssueTypeEnum.FRESHNESS_ISSUE,
                        severity,
                        f"Column '{col}' has {stale_count} records older than {max_age_hours} hours ({stale_percentage:.1f}%)",
                        col,
                        int(stale_count)
                    )
            except Exception as e:
                self._add_issue(
                    IssueTypeEnum.FRESHNESS_ISSUE,
                    IssueSeverityEnum.LOW,
                    f"Could not check freshness for column '{col}': {str(e)}",
                    col
                )
    
    def _check_primary_key_uniqueness(self):
        """Check if primary key columns have unique values"""
        pk_columns = self.config.get("primary_key_columns", [])
        
        if not pk_columns:
            return
        
        # Check if all PK columns exist
        missing_cols = [col for col in pk_columns if col not in self.df.columns]
        if missing_cols:
            self._add_issue(
                IssueTypeEnum.PRIMARY_KEY_VIOLATION,
                IssueSeverityEnum.CRITICAL,
                f"Primary key columns not found: {missing_cols}",
                affected_rows=0
            )
            return
        
        # Check for duplicates in primary key
        duplicates = self.df.duplicated(subset=pk_columns, keep=False)
        duplicate_count = duplicates.sum()
        
        self._add_metric(
            "primary_key_duplicates",
            float(duplicate_count),
            details={"columns": pk_columns}
        )
        
        if duplicate_count > 0:
            sample = self.df[duplicates][pk_columns].head(5).to_dict('records')
            self._add_issue(
                IssueTypeEnum.PRIMARY_KEY_VIOLATION,
                IssueSeverityEnum.CRITICAL,
                f"Primary key columns {pk_columns} have {duplicate_count} duplicate entries",
                affected_rows=int(duplicate_count),
                sample_values=sample
            )
        
        # Check for nulls in primary key
        null_count = self.df[pk_columns].isna().any(axis=1).sum()
        if null_count > 0:
            self._add_issue(
                IssueTypeEnum.PRIMARY_KEY_VIOLATION,
                IssueSeverityEnum.CRITICAL,
                f"Primary key columns {pk_columns} have {null_count} rows with null values",
                affected_rows=int(null_count)
            )
    
    def _calculate_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)"""
        if not self.issues:
            return 100.0
        
        # Weight by severity
        severity_weights = {
            "critical": 25,
            "high": 15,
            "medium": 8,
            "low": 3
        }
        
        total_penalty = 0
        for issue in self.issues:
            severity = issue.get("severity", "low")
            affected_pct = issue.get("affected_percentage", 0)
            
            # Base penalty from severity
            base_penalty = severity_weights.get(severity, 3)
            
            # Scale by affected percentage
            scaled_penalty = base_penalty * (1 + affected_pct / 100)
            total_penalty += scaled_penalty
        
        # Cap penalty at 100
        total_penalty = min(total_penalty, 100)
        
        return round(100 - total_penalty, 1)


def check_referential_integrity(
    source_df: pd.DataFrame,
    source_column: str,
    target_df: pd.DataFrame,
    target_column: str
) -> Dict[str, Any]:
    """Check referential integrity between two datasets"""
    
    if source_column not in source_df.columns:
        return {
            "valid": False,
            "error": f"Source column '{source_column}' not found"
        }
    
    if target_column not in target_df.columns:
        return {
            "valid": False,
            "error": f"Target column '{target_column}' not found"
        }
    
    source_values = set(source_df[source_column].dropna().unique())
    target_values = set(target_df[target_column].dropna().unique())
    
    orphaned = source_values - target_values
    orphaned_count = source_df[source_df[source_column].isin(orphaned)].shape[0]
    
    return {
        "valid": len(orphaned) == 0,
        "orphaned_values": list(orphaned)[:20],
        "orphaned_count": len(orphaned),
        "orphaned_rows": orphaned_count,
        "source_unique_values": len(source_values),
        "target_unique_values": len(target_values)
    }
