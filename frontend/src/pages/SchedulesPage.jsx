import { useState, useEffect } from "react";
import { Trash, CalendarCheck, Plus, Spinner } from "@phosphor-icons/react";
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
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { EmptyState, LoadingSpinner } from "@/components/DataQualityComponents";
import { getSchedules, deleteSchedule, createSchedule, getDatasets } from "@/api";
import { toast } from "sonner";
import { format } from "date-fns";

export default function SchedulesPage() {
  const [schedules, setSchedules] = useState([]);
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [selectedDatasetId, setSelectedDatasetId] = useState("");
  const [creating, setCreating] = useState(false);

  const fetchData = async () => {
    try {
      const [schedulesData, datasetsData] = await Promise.all([
        getSchedules(),
        getDatasets(),
      ]);
      setSchedules(schedulesData);
      setDatasets(datasetsData);
    } catch (error) {
      console.error("Failed to fetch data:", error);
      toast.error("Failed to load schedules");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleCreate = async () => {
    if (!selectedDatasetId) {
      toast.error("Please select a dataset");
      return;
    }

    setCreating(true);
    try {
      await createSchedule({
        dataset_id: selectedDatasetId,
        schedule_type: "daily",
      });
      toast.success("Daily schedule created");
      setCreateModalOpen(false);
      setSelectedDatasetId("");
      fetchData();
    } catch (error) {
      console.error("Create error:", error);
      toast.error("Failed to create schedule");
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (scheduleId) => {
    try {
      await deleteSchedule(scheduleId);
      toast.success("Schedule deleted");
      setSchedules(schedules.filter((s) => s.id !== scheduleId));
    } catch (error) {
      console.error("Delete error:", error);
      toast.error("Failed to delete schedule");
    }
  };

  // Get dataset name helper
  const getDatasetName = (datasetId) => {
    const dataset = datasets.find((d) => d.id === datasetId);
    return dataset?.name || "Unknown";
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="schedules-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">Scheduled Jobs</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Automated daily validation schedules
          </p>
        </div>
        <Button
          onClick={() => setCreateModalOpen(true)}
          className="bg-black text-white hover:bg-gray-800"
          data-testid="create-schedule-btn"
        >
          <Plus className="w-4 h-4 mr-2" />
          Create Schedule
        </Button>
      </div>

      {/* Schedules Table */}
      <Card className="border border-border shadow-none bg-white">
        <CardContent className="p-0">
          {schedules.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-xs uppercase tracking-wider">Dataset</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Schedule</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Status</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Last Run</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Next Run</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider text-right">
                    Actions
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {schedules.map((schedule) => (
                  <TableRow
                    key={schedule.id}
                    className="hover:bg-secondary/50"
                    data-testid={`schedule-row-${schedule.id}`}
                  >
                    <TableCell className="font-medium">
                      {getDatasetName(schedule.dataset_id)}
                    </TableCell>
                    <TableCell>
                      <span className="px-2 py-0.5 text-xs font-mono bg-secondary uppercase">
                        {schedule.schedule_type}
                      </span>
                    </TableCell>
                    <TableCell>
                      {schedule.is_active ? (
                        <span className="px-2 py-0.5 text-xs font-semibold bg-green-100 text-green-700">
                          ACTIVE
                        </span>
                      ) : (
                        <span className="px-2 py-0.5 text-xs font-semibold bg-gray-100 text-gray-700">
                          PAUSED
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {schedule.last_run
                        ? format(new Date(schedule.last_run), "MMM d, HH:mm")
                        : "Never"}
                    </TableCell>
                    <TableCell className="text-sm">
                      {schedule.next_run
                        ? format(new Date(schedule.next_run), "MMM d, HH:mm")
                        : "—"}
                    </TableCell>
                    <TableCell className="text-right">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleDelete(schedule.id)}
                        className="border-border text-red-600 hover:text-red-700 hover:bg-red-50"
                        data-testid={`delete-schedule-${schedule.id}`}
                      >
                        <Trash className="w-4 h-4" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <EmptyState
              title="No scheduled jobs"
              description="Create a schedule to automatically validate datasets on a daily basis."
              icon={CalendarCheck}
              action={
                <Button
                  onClick={() => setCreateModalOpen(true)}
                  className="bg-black text-white hover:bg-gray-800"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Create Schedule
                </Button>
              }
            />
          )}
        </CardContent>
      </Card>

      {/* Create Schedule Modal */}
      <Dialog open={createModalOpen} onOpenChange={setCreateModalOpen}>
        <DialogContent className="sm:max-w-md border border-border shadow-none">
          <DialogHeader>
            <DialogTitle className="text-xl font-bold">Create Schedule</DialogTitle>
          </DialogHeader>

          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Select Dataset
              </Label>
              <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
                <SelectTrigger className="border-border" data-testid="dataset-select">
                  <SelectValue placeholder="Choose a dataset" />
                </SelectTrigger>
                <SelectContent>
                  {datasets.map((dataset) => (
                    <SelectItem key={dataset.id} value={dataset.id}>
                      {dataset.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Schedule Type
              </Label>
              <div className="px-3 py-2 bg-secondary text-sm">
                Daily (runs at midnight UTC)
              </div>
              <p className="text-xs text-muted-foreground">
                Cron-based scheduling can be added later
              </p>
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setCreateModalOpen(false)}
              disabled={creating}
              className="border-border"
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreate}
              disabled={!selectedDatasetId || creating}
              className="bg-black text-white hover:bg-gray-800"
              data-testid="confirm-create-schedule"
            >
              {creating ? (
                <>
                  <Spinner className="w-4 h-4 mr-2 animate-spin" />
                  Creating...
                </>
              ) : (
                "Create Schedule"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
