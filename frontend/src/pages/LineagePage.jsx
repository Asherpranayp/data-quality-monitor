import { useState, useEffect } from "react";
import {
  Plus,
  Trash,
  GitBranch,
  CloudArrowDown,
  ArrowsLeftRight,
  Export,
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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { EmptyState, LoadingSpinner } from "@/components/DataQualityComponents";
import { getLineage, createLineage, deleteLineage, getDatasets } from "@/api";
import { toast } from "sonner";
import { format } from "date-fns";

const LINEAGE_TYPES = [
  { value: "source", label: "Source", icon: CloudArrowDown, description: "External data source" },
  { value: "transformation", label: "Transformation", icon: ArrowsLeftRight, description: "Derived from another dataset" },
  { value: "output", label: "Output", icon: Export, description: "Final output/export" },
];

export default function LineagePage() {
  const [lineages, setLineages] = useState([]);
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [creating, setCreating] = useState(false);

  // Form state
  const [datasetId, setDatasetId] = useState("");
  const [lineageType, setLineageType] = useState("source");
  const [parentDatasetId, setParentDatasetId] = useState("");
  const [sourceSystem, setSourceSystem] = useState("");
  const [transformationDescription, setTransformationDescription] = useState("");
  const [transformationQuery, setTransformationQuery] = useState("");

  const fetchData = async () => {
    try {
      const [linData, dsData] = await Promise.all([getLineage(), getDatasets()]);
      setLineages(linData);
      setDatasets(dsData);
    } catch (error) {
      console.error("Failed to fetch data:", error);
      toast.error("Failed to load lineage data");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleCreate = async () => {
    if (!datasetId || !lineageType) {
      toast.error("Please fill in all required fields");
      return;
    }

    if (lineageType === "source" && !sourceSystem) {
      toast.error("Please specify the source system");
      return;
    }

    if (lineageType === "transformation" && !parentDatasetId) {
      toast.error("Please select the parent dataset");
      return;
    }

    setCreating(true);
    try {
      await createLineage({
        dataset_id: datasetId,
        lineage_type: lineageType,
        parent_dataset_id: lineageType === "transformation" ? parentDatasetId : null,
        source_system: lineageType === "source" ? sourceSystem : null,
        transformation_description: transformationDescription || null,
        transformation_query: transformationQuery || null,
      });
      toast.success("Lineage entry created successfully");
      setCreateModalOpen(false);
      resetForm();
      fetchData();
    } catch (error) {
      console.error("Create error:", error);
      toast.error(error.response?.data?.detail || "Failed to create lineage entry");
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (id) => {
    try {
      await deleteLineage(id);
      toast.success("Lineage entry deleted");
      setLineages(lineages.filter((l) => l.id !== id));
    } catch (error) {
      console.error("Delete error:", error);
      toast.error("Failed to delete lineage entry");
    }
  };

  const resetForm = () => {
    setDatasetId("");
    setLineageType("source");
    setParentDatasetId("");
    setSourceSystem("");
    setTransformationDescription("");
    setTransformationQuery("");
  };

  const getTypeIcon = (type) => {
    const config = LINEAGE_TYPES.find((t) => t.value === type);
    if (!config) return null;
    const Icon = config.icon;
    return <Icon className="w-4 h-4" weight="bold" />;
  };

  const getTypeLabel = (type) => {
    const config = LINEAGE_TYPES.find((t) => t.value === type);
    return config?.label || type;
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="lineage-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">Data Lineage</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Track data flow: sources → transformations → outputs
          </p>
        </div>
        <Button
          onClick={() => setCreateModalOpen(true)}
          className="bg-black text-white hover:bg-gray-800"
          data-testid="create-lineage-btn"
        >
          <Plus className="w-4 h-4 mr-2" />
          Add Lineage Entry
        </Button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {LINEAGE_TYPES.map((type) => {
          const count = lineages.filter((l) => l.lineage_type === type.value).length;
          const Icon = type.icon;
          return (
            <Card key={type.value} className="border border-border shadow-none bg-white">
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className={`w-10 h-10 flex items-center justify-center ${
                    type.value === "source" ? "bg-blue-100" :
                    type.value === "transformation" ? "bg-purple-100" : "bg-green-100"
                  }`}>
                    <Icon className={`w-5 h-5 ${
                      type.value === "source" ? "text-blue-600" :
                      type.value === "transformation" ? "text-purple-600" : "text-green-600"
                    }`} weight="bold" />
                  </div>
                  <div>
                    <p className="text-2xl font-bold font-mono">{count}</p>
                    <p className="text-xs text-muted-foreground uppercase tracking-wider">
                      {type.label}s
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Lineage Table */}
      <Card className="border border-border shadow-none bg-white">
        <CardContent className="p-0">
          {lineages.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-xs uppercase tracking-wider">Type</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Dataset</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Origin</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Description</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Created</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider text-right">
                    Actions
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {lineages.map((lin) => (
                  <TableRow
                    key={lin.id}
                    className="hover:bg-secondary/50"
                    data-testid={`lineage-row-${lin.id}`}
                  >
                    <TableCell>
                      <div className={`inline-flex items-center gap-1.5 px-2 py-1 text-xs font-semibold ${
                        lin.lineage_type === "source" ? "bg-blue-100 text-blue-700" :
                        lin.lineage_type === "transformation" ? "bg-purple-100 text-purple-700" :
                        "bg-green-100 text-green-700"
                      }`}>
                        {getTypeIcon(lin.lineage_type)}
                        {getTypeLabel(lin.lineage_type)}
                      </div>
                    </TableCell>
                    <TableCell className="font-medium">{lin.dataset_name}</TableCell>
                    <TableCell>
                      {lin.lineage_type === "source" ? (
                        <span className="text-blue-600 font-medium">{lin.source_system}</span>
                      ) : lin.parent_dataset_name ? (
                        <span className="font-mono text-sm">{lin.parent_dataset_name}</span>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </TableCell>
                    <TableCell className="max-w-[200px] truncate">
                      {lin.transformation_description || (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {format(new Date(lin.created_at), "MMM d, yyyy")}
                    </TableCell>
                    <TableCell className="text-right">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleDelete(lin.id)}
                        className="border-border text-red-600 hover:text-red-700 hover:bg-red-50"
                        data-testid={`delete-lineage-${lin.id}`}
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
              title="No lineage entries"
              description="Add lineage entries to track the flow of data from sources through transformations to outputs."
              icon={GitBranch}
              action={
                <Button
                  onClick={() => setCreateModalOpen(true)}
                  className="bg-black text-white hover:bg-gray-800"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Add Lineage Entry
                </Button>
              }
            />
          )}
        </CardContent>
      </Card>

      {/* Create Lineage Modal */}
      <Dialog open={createModalOpen} onOpenChange={setCreateModalOpen}>
        <DialogContent className="sm:max-w-lg border border-border shadow-none">
          <DialogHeader>
            <DialogTitle className="text-xl font-bold">Add Lineage Entry</DialogTitle>
          </DialogHeader>

          <div className="space-y-6 py-4">
            {/* Dataset Selection */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Dataset *
              </Label>
              <Select value={datasetId} onValueChange={setDatasetId}>
                <SelectTrigger className="border-border" data-testid="lineage-dataset-select">
                  <SelectValue placeholder="Select dataset" />
                </SelectTrigger>
                <SelectContent>
                  {datasets.map((ds) => (
                    <SelectItem key={ds.id} value={ds.id}>
                      {ds.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Lineage Type */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Lineage Type *
              </Label>
              <Tabs value={lineageType} onValueChange={setLineageType}>
                <TabsList className="w-full bg-secondary p-1">
                  {LINEAGE_TYPES.map((type) => {
                    const Icon = type.icon;
                    return (
                      <TabsTrigger
                        key={type.value}
                        value={type.value}
                        className="flex-1 data-[state=active]:bg-white"
                      >
                        <Icon className="w-4 h-4 mr-1.5" />
                        {type.label}
                      </TabsTrigger>
                    );
                  })}
                </TabsList>
              </Tabs>
            </div>

            {/* Source System (for source type) */}
            {lineageType === "source" && (
              <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider">
                  Source System *
                </Label>
                <Input
                  value={sourceSystem}
                  onChange={(e) => setSourceSystem(e.target.value)}
                  placeholder="e.g., Salesforce, PostgreSQL, S3 Bucket"
                  className="border-border"
                  data-testid="source-system-input"
                />
              </div>
            )}

            {/* Parent Dataset (for transformation type) */}
            {lineageType === "transformation" && (
              <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider">
                  Parent Dataset *
                </Label>
                <Select value={parentDatasetId} onValueChange={setParentDatasetId}>
                  <SelectTrigger className="border-border" data-testid="parent-dataset-select">
                    <SelectValue placeholder="Select parent dataset" />
                  </SelectTrigger>
                  <SelectContent>
                    {datasets
                      .filter((ds) => ds.id !== datasetId)
                      .map((ds) => (
                        <SelectItem key={ds.id} value={ds.id}>
                          {ds.name}
                        </SelectItem>
                      ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {/* Transformation Description */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Description
              </Label>
              <Input
                value={transformationDescription}
                onChange={(e) => setTransformationDescription(e.target.value)}
                placeholder="Brief description of the transformation"
                className="border-border"
                data-testid="transformation-description-input"
              />
            </div>

            {/* Transformation Query */}
            {lineageType === "transformation" && (
              <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider">
                  SQL/Code (Optional)
                </Label>
                <Textarea
                  value={transformationQuery}
                  onChange={(e) => setTransformationQuery(e.target.value)}
                  placeholder="SELECT * FROM parent_table WHERE ..."
                  className="border-border font-mono text-sm min-h-[100px]"
                  data-testid="transformation-query-input"
                />
              </div>
            )}
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setCreateModalOpen(false);
                resetForm();
              }}
              disabled={creating}
              className="border-border"
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreate}
              disabled={!datasetId || creating}
              className="bg-black text-white hover:bg-gray-800"
              data-testid="confirm-create-lineage"
            >
              {creating ? (
                <>
                  <Spinner className="w-4 h-4 mr-2 animate-spin" />
                  Creating...
                </>
              ) : (
                "Add Entry"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
