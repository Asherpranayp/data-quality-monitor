import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  Plus,
  Play,
  Trash,
  Eye,
  CalendarPlus,
  Spinner,
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
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { EmptyState, LoadingSpinner } from "@/components/DataQualityComponents";
import UploadDatasetModal from "@/components/UploadDatasetModal";
import { getDatasets, deleteDataset, triggerValidation, createSchedule } from "@/api";
import { toast } from "sonner";
import { format } from "date-fns";

export default function DatasetsPage() {
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [uploadModalOpen, setUploadModalOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState(null);
  const [runningValidation, setRunningValidation] = useState(null);
  const navigate = useNavigate();

  const fetchDatasets = async () => {
    try {
      const data = await getDatasets();
      setDatasets(data);
    } catch (error) {
      console.error("Failed to fetch datasets:", error);
      toast.error("Failed to load datasets");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDatasets();
  }, []);

  const handleDelete = async () => {
    if (!selectedDataset) return;

    try {
      await deleteDataset(selectedDataset.id);
      toast.success(`Dataset "${selectedDataset.name}" deleted`);
      setDatasets(datasets.filter((d) => d.id !== selectedDataset.id));
    } catch (error) {
      console.error("Delete error:", error);
      toast.error("Failed to delete dataset");
    } finally {
      setDeleteDialogOpen(false);
      setSelectedDataset(null);
    }
  };

  const handleRunValidation = async (dataset) => {
    setRunningValidation(dataset.id);
    try {
      const job = await triggerValidation(dataset.id);
      toast.success("Validation job started");
      navigate(`/jobs/${job.id}`);
    } catch (error) {
      console.error("Validation error:", error);
      toast.error("Failed to start validation");
    } finally {
      setRunningValidation(null);
    }
  };

  const handleSchedule = async (dataset) => {
    try {
      await createSchedule({
        dataset_id: dataset.id,
        schedule_type: "daily",
      });
      toast.success(`Daily validation scheduled for "${dataset.name}"`);
    } catch (error) {
      console.error("Schedule error:", error);
      toast.error("Failed to create schedule");
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="datasets-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">Datasets</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Manage and monitor your data sources
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

      {/* Datasets Table */}
      <Card className="border border-border shadow-none bg-white">
        <CardContent className="p-0">
          {datasets.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-xs uppercase tracking-wider">Name</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Source</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Rows</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Columns</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Created</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider text-right">
                    Actions
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {datasets.map((dataset) => (
                  <TableRow
                    key={dataset.id}
                    className="hover:bg-secondary/50"
                    data-testid={`dataset-row-${dataset.id}`}
                  >
                    <TableCell className="font-medium">{dataset.name}</TableCell>
                    <TableCell>
                      <span className="px-2 py-0.5 text-xs font-mono bg-secondary">
                        {dataset.source_type.toUpperCase()}
                      </span>
                    </TableCell>
                    <TableCell className="font-mono">{dataset.row_count.toLocaleString()}</TableCell>
                    <TableCell className="font-mono">{dataset.column_count}</TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {format(new Date(dataset.created_at), "MMM d, yyyy")}
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex items-center justify-end gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handleRunValidation(dataset)}
                          disabled={runningValidation === dataset.id}
                          className="border-border"
                          data-testid={`run-validation-${dataset.id}`}
                        >
                          {runningValidation === dataset.id ? (
                            <Spinner className="w-4 h-4 animate-spin" />
                          ) : (
                            <Play className="w-4 h-4" />
                          )}
                          <span className="ml-1.5 hidden sm:inline">Validate</span>
                        </Button>

                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button variant="outline" size="sm" className="border-border">
                              <Eye className="w-4 h-4" />
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end" className="border-border">
                            <DropdownMenuItem onClick={() => handleSchedule(dataset)}>
                              <CalendarPlus className="w-4 h-4 mr-2" />
                              Schedule Daily
                            </DropdownMenuItem>
                            <DropdownMenuItem
                              onClick={() => {
                                setSelectedDataset(dataset);
                                setDeleteDialogOpen(true);
                              }}
                              className="text-red-600 focus:text-red-600"
                            >
                              <Trash className="w-4 h-4 mr-2" />
                              Delete
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <EmptyState
              title="No datasets yet"
              description="Upload your first CSV dataset to start monitoring data quality."
              action={
                <Button
                  onClick={() => setUploadModalOpen(true)}
                  className="bg-black text-white hover:bg-gray-800"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Upload Dataset
                </Button>
              }
            />
          )}
        </CardContent>
      </Card>

      {/* Upload Modal */}
      <UploadDatasetModal
        open={uploadModalOpen}
        onClose={() => setUploadModalOpen(false)}
        onSuccess={fetchDatasets}
      />

      {/* Delete Confirmation */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent className="border border-border">
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Dataset</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete "{selectedDataset?.name}"? This will also delete all
              validation history. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel className="border-border">Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              className="bg-red-600 hover:bg-red-700 text-white"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
