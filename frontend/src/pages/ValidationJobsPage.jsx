import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowRight, ClockCounterClockwise } from "@phosphor-icons/react";
import { Card, CardContent } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  StatusBadge,
  QualityScore,
  EmptyState,
  LoadingSpinner,
} from "@/components/DataQualityComponents";
import { getValidationJobs } from "@/api";
import { toast } from "sonner";
import { format } from "date-fns";

export default function ValidationJobsPage() {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  const fetchJobs = async () => {
    try {
      const data = await getValidationJobs();
      setJobs(data);
    } catch (error) {
      console.error("Failed to fetch jobs:", error);
      toast.error("Failed to load validation jobs");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
    const interval = setInterval(fetchJobs, 10000); // Refresh for running jobs
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="validation-jobs-page">
      {/* Header */}
      <div>
        <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">Validation Jobs</h1>
        <p className="text-sm text-muted-foreground mt-1">
          View all validation runs and their results
        </p>
      </div>

      {/* Jobs Table */}
      <Card className="border border-border shadow-none bg-white">
        <CardContent className="p-0">
          {jobs.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-xs uppercase tracking-wider">Dataset</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Status</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Quality</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Issues</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Critical</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">AI Analysis</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Started</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Duration</TableHead>
                  <TableHead></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {jobs.map((job) => {
                  const duration =
                    job.completed_at && job.started_at
                      ? Math.round(
                          (new Date(job.completed_at) - new Date(job.started_at)) / 1000
                        )
                      : null;

                  return (
                    <TableRow
                      key={job.id}
                      className="cursor-pointer hover:bg-secondary/50"
                      onClick={() => navigate(`/jobs/${job.id}`)}
                      data-testid={`job-row-${job.id}`}
                    >
                      <TableCell className="font-medium">{job.dataset_name}</TableCell>
                      <TableCell>
                        <StatusBadge status={job.status} />
                      </TableCell>
                      <TableCell>
                        {job.status === "completed" ? (
                          <QualityScore score={job.quality_score} size="sm" />
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="font-mono">{job.total_issues}</TableCell>
                      <TableCell>
                        {job.critical_issues > 0 ? (
                          <span className="px-2 py-0.5 text-xs font-bold bg-red-100 text-red-700">
                            {job.critical_issues}
                          </span>
                        ) : (
                          <span className="text-muted-foreground">0</span>
                        )}
                      </TableCell>
                      <TableCell>
                        {job.ai_analysis_completed ? (
                          <span className="px-2 py-0.5 text-xs font-mono bg-green-100 text-green-700">
                            READY
                          </span>
                        ) : (
                          <span className="text-muted-foreground text-xs">Not run</span>
                        )}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm">
                        {job.started_at
                          ? format(new Date(job.started_at), "MMM d, HH:mm")
                          : "—"}
                      </TableCell>
                      <TableCell className="font-mono text-sm">
                        {duration !== null ? `${duration}s` : "—"}
                      </TableCell>
                      <TableCell>
                        <ArrowRight className="w-4 h-4 text-muted-foreground" />
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          ) : (
            <EmptyState
              title="No validation jobs"
              description="Run a validation on any dataset to see results here."
              icon={ClockCounterClockwise}
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
