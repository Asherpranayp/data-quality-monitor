import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  Database,
  ClockCounterClockwise,
  Warning,
  Gauge,
  ArrowRight,
  Plus,
} from "@phosphor-icons/react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  MetricCard,
  QualityScore,
  StatusBadge,
  EmptyState,
  LoadingSpinner,
} from "@/components/DataQualityComponents";
import UploadDatasetModal from "@/components/UploadDatasetModal";
import { getDashboardStats } from "@/api";
import { toast } from "sonner";
import { format } from "date-fns";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts";

export default function Dashboard() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploadModalOpen, setUploadModalOpen] = useState(false);
  const navigate = useNavigate();

  const fetchStats = async () => {
    try {
      const data = await getDashboardStats();
      setStats(data);
    } catch (error) {
      console.error("Failed to fetch stats:", error);
      toast.error("Failed to load dashboard data");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 30000); // Refresh every 30s
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  const issuesByTypeData = stats?.issues_by_type
    ? Object.entries(stats.issues_by_type).map(([name, value]) => ({
        name: name.replace(/_/g, " "),
        value,
      }))
    : [];

  const issuesBySeverityData = stats?.issues_by_severity
    ? Object.entries(stats.issues_by_severity).map(([name, value]) => ({
        name,
        value,
      }))
    : [];

  const severityColors = {
    critical: "#dc2626",
    high: "#ea580c",
    medium: "#ca8a04",
    low: "#2563eb",
  };

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Monitor data quality across all datasets
          </p>
        </div>
        <Button
          onClick={() => setUploadModalOpen(true)}
          className="bg-black text-white hover:bg-gray-800"
          data-testid="upload-dataset-btn"
        >
          <Plus className="w-4 h-4 mr-2" />
          Upload Dataset
        </Button>
      </div>

      {/* North Star Metric */}
      <Card className="border border-border shadow-none bg-white" data-testid="quality-score-card">
        <CardContent className="p-6">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-6">
            <div>
              <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-2">
                Average Quality Score
              </p>
              <QualityScore score={stats?.avg_quality_score || 0} size="xl" />
              <p className="text-sm text-muted-foreground mt-2">
                Across {stats?.total_datasets || 0} datasets
              </p>
            </div>
            <div className="w-48 h-48">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={issuesBySeverityData}
                    dataKey="value"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={70}
                    paddingAngle={2}
                  >
                    {issuesBySeverityData.map((entry, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={severityColors[entry.name] || "#94a3b8"}
                      />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      border: "1px solid #e5e5e5",
                      borderRadius: 0,
                      fontSize: 12,
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Metrics Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard
          label="Total Datasets"
          value={stats?.total_datasets || 0}
          icon={Database}
        />
        <MetricCard
          label="Validation Jobs"
          value={stats?.total_jobs || 0}
          icon={ClockCounterClockwise}
        />
        <MetricCard
          label="Total Issues"
          value={stats?.total_issues || 0}
          icon={Warning}
        />
        <MetricCard
          label="Quality Score"
          value={`${stats?.avg_quality_score?.toFixed(1) || 0}%`}
          icon={Gauge}
        />
      </div>

      {/* Issues by Type Chart */}
      {issuesByTypeData.length > 0 && (
        <Card className="border border-border shadow-none bg-white">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg font-bold">Issues by Type</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={issuesByTypeData} layout="vertical">
                  <XAxis type="number" />
                  <YAxis
                    dataKey="name"
                    type="category"
                    width={150}
                    tick={{ fontSize: 11 }}
                  />
                  <Tooltip
                    contentStyle={{
                      border: "1px solid #e5e5e5",
                      borderRadius: 0,
                      fontSize: 12,
                    }}
                  />
                  <Bar dataKey="value" fill="#000" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Recent Jobs Table */}
      <Card className="border border-border shadow-none bg-white">
        <CardHeader className="pb-2 flex flex-row items-center justify-between">
          <CardTitle className="text-lg font-bold">Recent Validation Jobs</CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={() => navigate("/jobs")}
            className="border-border"
            data-testid="view-all-jobs-btn"
          >
            View All
            <ArrowRight className="w-4 h-4 ml-2" />
          </Button>
        </CardHeader>
        <CardContent>
          {stats?.recent_jobs?.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-xs uppercase tracking-wider">Dataset</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Status</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Score</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Issues</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Created</TableHead>
                  <TableHead></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {stats.recent_jobs.slice(0, 5).map((job) => (
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
                      <QualityScore score={job.quality_score} size="sm" />
                    </TableCell>
                    <TableCell>
                      <span className="font-mono">{job.total_issues}</span>
                      {job.critical_issues > 0 && (
                        <span className="ml-2 text-xs text-red-600">
                          ({job.critical_issues} critical)
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {format(new Date(job.created_at), "MMM d, HH:mm")}
                    </TableCell>
                    <TableCell>
                      <ArrowRight className="w-4 h-4 text-muted-foreground" />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <EmptyState
              title="No validation jobs yet"
              description="Upload a dataset and run your first validation to see results here."
              icon={ClockCounterClockwise}
              action={
                <Button
                  onClick={() => navigate("/datasets")}
                  className="bg-black text-white hover:bg-gray-800"
                >
                  Go to Datasets
                </Button>
              }
            />
          )}
        </CardContent>
      </Card>

      <UploadDatasetModal
        open={uploadModalOpen}
        onClose={() => setUploadModalOpen(false)}
        onSuccess={() => {
          fetchStats();
          navigate("/datasets");
        }}
      />
    </div>
  );
}
