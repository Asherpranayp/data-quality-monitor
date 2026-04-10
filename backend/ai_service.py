from typing import Any, Dict, List


class AIAnalysisService:
    def __init__(self) -> None:
        pass

    async def analyze_validation_results(
        self,
        dataset_name: str,
        validation_results: List[Dict[str, Any]],
        quality_score: float,
    ) -> Dict[str, Any]:
        issue_count = len(validation_results)

        if quality_score >= 90:
            severity = "low"
            summary = f"{dataset_name} looks healthy overall with only minor data quality concerns."
        elif quality_score >= 70:
            severity = "medium"
            summary = f"{dataset_name} has moderate data quality issues that should be reviewed."
        else:
            severity = "high"
            summary = f"{dataset_name} has significant data quality issues that may impact downstream systems."

        likely_causes = []
        suggested_fixes = []

        for issue in validation_results:
            issue_type = issue.get("issue_type", "unknown")

            if issue_type == "missing_values":
                likely_causes.append("Incomplete source records or ingestion issues")
                suggested_fixes.append("Review source completeness and apply null-handling rules")
            elif issue_type == "duplicates":
                likely_causes.append("Duplicate ingestion or missing uniqueness constraints")
                suggested_fixes.append("Add deduplication rules and enforce unique keys")
            elif issue_type == "schema_mismatch":
                likely_causes.append("Upstream schema changes")
                suggested_fixes.append("Update schema validation and transformation logic")
            elif issue_type == "outliers":
                likely_causes.append("Unexpected source values or faulty transformations")
                suggested_fixes.append("Investigate abnormal values and add range validation")
            elif issue_type == "referential_integrity":
                likely_causes.append("Broken relationships between datasets")
                suggested_fixes.append("Validate foreign keys and upstream load ordering")
            else:
                likely_causes.append("General data quality issue")
                suggested_fixes.append("Inspect records and review validation logic")

        return {
            "summary": summary,
            "severity": severity,
            "issue_count": issue_count,
            "likely_causes": list(dict.fromkeys(likely_causes)),
            "suggested_fixes": list(dict.fromkeys(suggested_fixes)),
        }
