"""AI Service for Data Quality Analysis using GPT-4o-mini"""
import os
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from pathlib import Path

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

logger = logging.getLogger(__name__)

# Import emergent integrations for LLM
from emergentintegrations.llm.chat import LlmChat, UserMessage


class AIAnalysisService:
    """AI-powered analysis service for data quality issues"""
    
    def __init__(self):
        self.api_key = os.environ.get('EMERGENT_LLM_KEY')
        if not self.api_key:
            logger.warning("EMERGENT_LLM_KEY not found in environment")
    
    async def analyze_issues(
        self,
        issues: List[Dict[str, Any]],
        dataset_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Analyze each issue and add AI explanations"""
        
        if not self.api_key:
            logger.warning("No API key available for AI analysis")
            return issues
        
        analyzed_issues = []
        
        for issue in issues:
            try:
                analysis = await self._analyze_single_issue(issue, dataset_info)
                issue.update(analysis)
            except Exception as e:
                logger.error(f"Error analyzing issue: {e}")
                issue["ai_explanation"] = "Analysis unavailable"
                issue["ai_cause"] = "Unable to determine"
                issue["ai_remediation"] = "Manual review recommended"
            
            analyzed_issues.append(issue)
        
        return analyzed_issues
    
    async def _analyze_single_issue(
        self,
        issue: Dict[str, Any],
        dataset_info: Dict[str, Any]
    ) -> Dict[str, str]:
        """Analyze a single issue and return AI insights"""
        
        chat = LlmChat(
            api_key=self.api_key,
            session_id=f"issue-analysis-{issue.get('issue_type', 'unknown')}",
            system_message="""You are a data quality expert. Analyze data quality issues and provide:
1. A clear explanation of what the issue means
2. The likely cause of the issue
3. A specific, actionable remediation step

Be concise but thorough. Focus on practical advice."""
        ).with_model("openai", "gpt-4o-mini")
        
        prompt = f"""Analyze this data quality issue:

Issue Type: {issue.get('issue_type', 'Unknown')}
Severity: {issue.get('severity', 'Unknown')}
Column: {issue.get('column_name', 'N/A')}
Description: {issue.get('description', 'No description')}
Affected Rows: {issue.get('affected_rows', 0)}
Affected Percentage: {issue.get('affected_percentage', 0)}%
Sample Values: {issue.get('sample_values', [])}

Dataset Context:
- Total Rows: {dataset_info.get('row_count', 'Unknown')}
- Total Columns: {dataset_info.get('column_count', 'Unknown')}

Respond in this exact format:
EXPLANATION: [1-2 sentence explanation of the issue]
CAUSE: [Most likely cause of this issue]
REMEDIATION: [Specific action to fix this issue]"""

        user_message = UserMessage(text=prompt)
        response = await chat.send_message(user_message)
        
        # Parse response
        explanation = ""
        cause = ""
        remediation = ""
        
        lines = response.strip().split('\n')
        current_field = None
        
        for line in lines:
            line = line.strip()
            if line.startswith('EXPLANATION:'):
                current_field = 'explanation'
                explanation = line.replace('EXPLANATION:', '').strip()
            elif line.startswith('CAUSE:'):
                current_field = 'cause'
                cause = line.replace('CAUSE:', '').strip()
            elif line.startswith('REMEDIATION:'):
                current_field = 'remediation'
                remediation = line.replace('REMEDIATION:', '').strip()
            elif current_field:
                if current_field == 'explanation':
                    explanation += ' ' + line
                elif current_field == 'cause':
                    cause += ' ' + line
                elif current_field == 'remediation':
                    remediation += ' ' + line
        
        return {
            "ai_explanation": explanation.strip() or "Analysis unavailable",
            "ai_cause": cause.strip() or "Unable to determine",
            "ai_remediation": remediation.strip() or "Manual review recommended"
        }
    
    async def generate_summary(
        self,
        issues: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
        quality_score: float,
        dataset_info: Dict[str, Any]
    ) -> str:
        """Generate an executive summary of the validation results"""
        
        if not self.api_key:
            return self._generate_basic_summary(issues, metrics, quality_score, dataset_info)
        
        try:
            chat = LlmChat(
                api_key=self.api_key,
                session_id=f"validation-summary",
                system_message="""You are a data quality expert writing executive summaries. 
Be concise, focus on the most important findings, and prioritize actionable insights.
Use bullet points for clarity. Maximum 200 words."""
            ).with_model("openai", "gpt-4o-mini")
            
            # Summarize issues by type and severity
            issues_by_severity = {"critical": 0, "high": 0, "medium": 0, "low": 0}
            issues_by_type = {}
            
            for issue in issues:
                severity = issue.get("severity", "low")
                issues_by_severity[severity] = issues_by_severity.get(severity, 0) + 1
                
                issue_type = issue.get("issue_type", "unknown")
                issues_by_type[issue_type] = issues_by_type.get(issue_type, 0) + 1
            
            # Get key metrics
            completeness = next((m["metric_value"] for m in metrics if m["metric_name"] == "completeness"), 100)
            
            prompt = f"""Generate an executive summary for this data quality validation:

Dataset: {dataset_info.get('name', 'Unknown')}
Rows: {dataset_info.get('row_count', 0):,}
Columns: {dataset_info.get('column_count', 0)}

Quality Score: {quality_score}/100
Data Completeness: {completeness:.1f}%

Issues Found:
- Critical: {issues_by_severity['critical']}
- High: {issues_by_severity['high']}
- Medium: {issues_by_severity['medium']}
- Low: {issues_by_severity['low']}

Issue Types: {issues_by_type}

Top 3 Issues:
{self._format_top_issues(issues[:3])}

Provide:
1. Overall assessment (1 sentence)
2. Top 3 priority actions (bullet points)
3. Risk assessment (low/medium/high with brief explanation)"""

            user_message = UserMessage(text=prompt)
            response = await chat.send_message(user_message)
            return response.strip()
            
        except Exception as e:
            logger.error(f"Error generating AI summary: {e}")
            return self._generate_basic_summary(issues, metrics, quality_score, dataset_info)
    
    def _format_top_issues(self, issues: List[Dict]) -> str:
        """Format top issues for the prompt"""
        formatted = []
        for i, issue in enumerate(issues, 1):
            formatted.append(f"{i}. [{issue.get('severity', 'unknown').upper()}] {issue.get('description', 'No description')}")
        return '\n'.join(formatted) if formatted else "No issues found"
    
    def _generate_basic_summary(
        self,
        issues: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
        quality_score: float,
        dataset_info: Dict[str, Any]
    ) -> str:
        """Generate a basic summary without AI"""
        
        critical = sum(1 for i in issues if i.get("severity") == "critical")
        high = sum(1 for i in issues if i.get("severity") == "high")
        
        completeness = next((m["metric_value"] for m in metrics if m["metric_name"] == "completeness"), 100)
        
        summary = f"""## Data Quality Summary

**Dataset:** {dataset_info.get('name', 'Unknown')}
**Quality Score:** {quality_score}/100
**Completeness:** {completeness:.1f}%

### Issues Overview
- Total Issues: {len(issues)}
- Critical: {critical}
- High Priority: {high}

### Recommendation
{"Immediate attention required due to critical issues." if critical > 0 else "Review high-priority issues before production use." if high > 0 else "Data quality is acceptable for most use cases."}
"""
        return summary
