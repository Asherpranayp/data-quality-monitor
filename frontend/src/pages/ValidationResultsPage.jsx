import { useState, useEffect, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  ArrowLeft,
  Robot,
  Spinner,
  Warning,
  CheckCircle,
  Lightbulb,
  Target,
  Wrench,
} from "@phosphor-icons/react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  QualityScore,
  StatusBadge,
  SeverityBadge,
  IssueTypeLabel,
  MetricCard,
  LoadingSpinner,
} from "@/components/DataQualityComponents";
import { getValidationResults, triggerAIAnalysis } from "@/api";
import { toast } from "sonner";
import { format } from "date-fns";

export default function ValidationResultsPage() {
  const { jobId } = useParams();
  const navigate = useNavigate();
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(true);
  const [aiAnalyzing, setAiAnalyzing] = useState(false);
  const [selectedIssue, setSelectedIssue] = useState(null);

  const fetchResults = useCallback(async () => {
    try {
      const data = await getValidationResults(jobId);
      setResults(data);
      
      // Auto-select first issue if available
      if (data.issues?.length > 0 && !selectedIssue) {
        setSelectedIssue(data.issues[0]);
      }
    } catch (error) {
      console.error("Failed to fetch results:", error);
      toast.error("Failed to load validation results");
    } finally {
      setLoading(false);
    }
  }, [jobId, selectedIssue]);

  useEffect(() => {
    fetchResults();
    
    // Poll if job is still running
    const interval = setInterval(() => {
      if (results?.job?.status === "running" || results?.job?.status === "pending") {
        fetchResults();
      }
    }, 3000);
    
    return () => clearInterval(interval);
  }, [fetchResults, results?.job?.status]);

  const handleRunAIAnalysis = async () => {
    setAiAnalyzing(true);
    try {
      await triggerAIAnalysis(jobId);
      toast.success("AI analysis started");
      
      // Poll for completion
      const checkInterval = setInterval(async () => {
        const updated = await getValidationResults(jobId);
        if (updated.job.ai_analysis_completed) {
          clearInterval(checkInterval);
          setResults(updated);
          setAiAnalyzing(false);
          toast.success("AI analysis completed");
          if (updated.issues?.length > 0) {
            setSelectedIssue(updated.issues[0]);
          }
        }
      }, 3000);
      
      // Timeout after 2 minutes
      setTimeout(() => {
        clearInterval(checkInterval);
        setAiAnalyzing(false);
      }, 120000);
    } catch (error) {
      console.error("AI analysis error:", error);
      toast.error("Failed to start AI analysis");
      setAiAnalyzing(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (!results) {
    return (
      <div className="p-8 text-center">
        <p>Validation job not found</p>
        <Button onClick={() => navigate("/jobs")} className="mt-4">
          Back to Jobs
        </Button>
      </div>
    );
  }

  const { job, issues, metrics } = results;

  // Group metrics by type
  const completeness = metrics.find((m) => m.metric_name === "completeness");
  const nullMetrics = metrics.filter((m) => m.metric_name === "null_percentage");
  const outlierMetrics = metrics.filter((m) => m.metric_name === "outlier_percentage");

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="validation-results-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-4">
        <div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => navigate("/jobs")}
            className="mb-2 -ml-2"
            data-testid="back-btn"
          >
            <ArrowLeft className="w-4 h-4 mr-1" />
            Back to Jobs
          </Button>
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">
            Validation Results
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            Job ID: <span className="font-mono">{job.id.slice(0, 8)}...</span>
          </p>
        </div>
        <div className="flex items-center gap-3">
          <StatusBadge status={job.status} />
          {job.status === "completed" && !job.ai_analysis_completed && (
            <Button
              onClick={handleRunAIAnalysis}
              disabled={aiAnalyzing}
              className="bg-black text-white hover:bg-gray-800"
              data-testid="run-ai-analysis-btn"
            >
              {aiAnalyzing ? (
                <>
                  <Spinner className="w-4 h-4 mr-2 animate-spin" />
                  Analyzing...
                </>
              ) : (
                <>
                  <Robot className="w-4 h-4 mr-2" />
                  Run AI Analysis
                </>
              )}
            </Button>
          )}
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card className="border border-border shadow-none bg-white col-span-1" data-testid="quality-card">
          <CardContent className="p-4">
            <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-2">
              Quality Score
            </p>
            <QualityScore score={job.quality_score} size="lg" />
          </CardContent>
        </Card>

        <MetricCard
          label="Total Issues"
          value={job.total_issues}
          icon={Warning}
        />

        <MetricCard
          label="Data Completeness"
          value={`${completeness?.metric_value?.toFixed(1) || 100}%`}
          icon={CheckCircle}
        />

        <MetricCard
          label="Critical Issues"
          value={job.critical_issues}
          icon={Target}
          className={job.critical_issues > 0 ? "border-red-200 bg-red-50" : ""}
        />
      </div>

      {/* AI Summary */}
      {job.ai_summary && (
        <Card className="border border-border shadow-none bg-white" data-testid="ai-summary-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg font-bold flex items-center gap-2">
              <Robot className="w-5 h-5" />
              AI Summary
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="prose prose-sm max-w-none font-mono text-sm whitespace-pre-wrap bg-secondary p-4">
              {job.ai_summary}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Issues and Metrics Tabs */}
      <Tabs defaultValue="issues" className="space-y-4">
        <TabsList className="bg-secondary p-1">
          <TabsTrigger value="issues" className="data-[state=active]:bg-white">
            Issues ({issues.length})
          </TabsTrigger>
          <TabsTrigger value="metrics" className="data-[state=active]:bg-white">
            Metrics ({metrics.length})
          </TabsTrigger>
        </TabsList>

        {/* Issues Tab */}
        <TabsContent value="issues" className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            {/* Issues List */}
            <Card className="border border-border shadow-none bg-white lg:col-span-2">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg font-bold">Issues Found</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <ScrollArea className="h-[500px]">
                  <Table>
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead className="text-xs uppercase tracking-wider">Severity</TableHead>
                        <TableHead className="text-xs uppercase tracking-wider">Type</TableHead>
                        <TableHead className="text-xs uppercase tracking-wider">Column</TableHead>
                        <TableHead className="text-xs uppercase tracking-wider">Affected</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {issues.map((issue) => (
                        <TableRow
                          key={issue.id}
                          className={`cursor-pointer hover:bg-secondary/50 ${
                            selectedIssue?.id === issue.id ? "bg-secondary" : ""
                          }`}
                          onClick={() => setSelectedIssue(issue)}
                          data-testid={`issue-row-${issue.id}`}
                        >
                          <TableCell>
                            <SeverityBadge severity={issue.severity} />
                          </TableCell>
                          <TableCell>
                            <IssueTypeLabel type={issue.issue_type} />
                          </TableCell>
                          <TableCell className="font-mono text-sm">
                            {issue.column_name || "—"}
                          </TableCell>
                          <TableCell>
                            <span className="font-mono">{issue.affected_rows}</span>
                            <span className="text-muted-foreground text-xs ml-1">
                              ({issue.affected_percentage.toFixed(1)}%)
                            </span>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </CardContent>
            </Card>

            {/* Issue Detail Panel */}
            <Card className="border border-border shadow-none bg-white" data-testid="issue-detail-panel">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg font-bold">Issue Details</CardTitle>
              </CardHeader>
              <CardContent>
                {selectedIssue ? (
                  <div className="space-y-4">
                    <div>
                      <SeverityBadge severity={selectedIssue.severity} />
                      <p className="mt-2 text-sm">{selectedIssue.description}</p>
                    </div>

                    {selectedIssue.sample_values && selectedIssue.sample_values.length > 0 && (
                      <div>
                        <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-2">
                          Sample Values
                        </p>
                        <div className="bg-secondary p-3 font-mono text-xs overflow-x-auto">
                          {JSON.stringify(selectedIssue.sample_values.slice(0, 5), null, 2)}
                        </div>
                      </div>
                    )}

                    {/* AI Analysis */}
                    {selectedIssue.ai_explanation && (
                      <div className="space-y-3 pt-3 border-t border-border">
                        <div className="flex items-center gap-2 text-sm font-semibold">
                          <Robot className="w-4 h-4" />
                          AI Analysis
                        </div>

                        <div className="space-y-3">
                          <div>
                            <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-1">
                              <Lightbulb className="w-3.5 h-3.5" />
                              Explanation
                            </div>
                            <p className="text-sm bg-blue-50 p-3 border border-blue-100">
                              {selectedIssue.ai_explanation}
                            </p>
                          </div>

                          <div>
                            <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-1">
                              <Target className="w-3.5 h-3.5" />
                              Likely Cause
                            </div>
                            <p className="text-sm bg-orange-50 p-3 border border-orange-100">
                              {selectedIssue.ai_cause}
                            </p>
                          </div>

                          <div>
                            <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-1">
                              <Wrench className="w-3.5 h-3.5" />
                              Remediation
                            </div>
                            <p className="text-sm bg-green-50 p-3 border border-green-100">
                              {selectedIssue.ai_remediation}
                            </p>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    Select an issue to view details
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Metrics Tab */}
        <TabsContent value="metrics">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Null Percentage by Column */}
            <Card className="border border-border shadow-none bg-white">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg font-bold">Null Percentage by Column</CardTitle>
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-64">
                  <Table className="table-dense">
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead className="text-xs uppercase tracking-wider">Column</TableHead>
                        <TableHead className="text-xs uppercase tracking-wider text-right">
                          Null %
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {nullMetrics.map((metric) => (
                        <TableRow key={metric.id} className="hover:bg-secondary/50">
                          <TableCell className="font-mono text-sm">
                            {metric.column_name}
                          </TableCell>
                          <TableCell className="text-right">
                            <span
                              className={`font-mono ${
                                metric.metric_value > 25
                                  ? "text-red-600"
                                  : metric.metric_value > 10
                                  ? "text-orange-600"
                                  : metric.metric_value > 0
                                  ? "text-yellow-600"
                                  : "text-green-600"
                              }`}
                            >
                              {metric.metric_value.toFixed(1)}%
                            </span>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </CardContent>
            </Card>

            {/* Outlier Percentage by Column */}
            <Card className="border border-border shadow-none bg-white">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg font-bold">Outliers by Column</CardTitle>
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-64">
                  <Table className="table-dense">
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead className="text-xs uppercase tracking-wider">Column</TableHead>
                        <TableHead className="text-xs uppercase tracking-wider text-right">
                          Outlier %
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {outlierMetrics.map((metric) => (
                        <TableRow key={metric.id} className="hover:bg-secondary/50">
                          <TableCell className="font-mono text-sm">
                            {metric.column_name}
                          </TableCell>
                          <TableCell className="text-right">
                            <span
                              className={`font-mono ${
                                metric.metric_value > 10
                                  ? "text-red-600"
                                  : metric.metric_value > 5
                                  ? "text-orange-600"
                                  : "text-green-600"
                              }`}
                            >
                              {metric.metric_value.toFixed(1)}%
                            </span>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
