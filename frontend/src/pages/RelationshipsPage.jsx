import { useState, useEffect } from "react";
import {
  Plus,
  Trash,
  LinkSimple,
  ArrowRight,
  CheckCircle,
  XCircle,
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
import { Switch } from "@/components/ui/switch";
import { EmptyState, LoadingSpinner } from "@/components/DataQualityComponents";
import {
  getRelationships,
  createRelationship,
  deleteRelationship,
  getDatasets,
  getDataset,
} from "@/api";
import { toast } from "sonner";
import { format } from "date-fns";

export default function RelationshipsPage() {
  const [relationships, setRelationships] = useState([]);
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [creating, setCreating] = useState(false);

  // Form state
  const [sourceDatasetId, setSourceDatasetId] = useState("");
  const [sourceColumn, setSourceColumn] = useState("");
  const [targetDatasetId, setTargetDatasetId] = useState("");
  const [targetColumn, setTargetColumn] = useState("");
  const [relationshipName, setRelationshipName] = useState("");
  const [isRequired, setIsRequired] = useState(true);

  // Schema info for column selection
  const [sourceSchema, setSourceSchema] = useState(null);
  const [targetSchema, setTargetSchema] = useState(null);

  const fetchData = async () => {
    try {
      const [relData, dsData] = await Promise.all([
        getRelationships(),
        getDatasets(),
      ]);
      setRelationships(relData);
      setDatasets(dsData);
    } catch (error) {
      console.error("Failed to fetch data:", error);
      toast.error("Failed to load relationships");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  // Load source schema when source dataset changes
  useEffect(() => {
    const loadSourceSchema = async () => {
      if (sourceDatasetId) {
        try {
          const ds = await getDataset(sourceDatasetId);
          setSourceSchema(ds.schema_info);
        } catch (error) {
          console.error("Failed to load source schema");
        }
      } else {
        setSourceSchema(null);
      }
    };
    loadSourceSchema();
  }, [sourceDatasetId]);

  // Load target schema when target dataset changes
  useEffect(() => {
    const loadTargetSchema = async () => {
      if (targetDatasetId) {
        try {
          const ds = await getDataset(targetDatasetId);
          setTargetSchema(ds.schema_info);
        } catch (error) {
          console.error("Failed to load target schema");
        }
      } else {
        setTargetSchema(null);
      }
    };
    loadTargetSchema();
  }, [targetDatasetId]);

  const handleCreate = async () => {
    if (!sourceDatasetId || !sourceColumn || !targetDatasetId || !targetColumn) {
      toast.error("Please fill in all required fields");
      return;
    }

    setCreating(true);
    try {
      await createRelationship({
        source_dataset_id: sourceDatasetId,
        source_column: sourceColumn,
        target_dataset_id: targetDatasetId,
        target_column: targetColumn,
        relationship_name: relationshipName || null,
        relationship_type: "foreign_key",
        is_required: isRequired,
      });
      toast.success("Relationship created successfully");
      setCreateModalOpen(false);
      resetForm();
      fetchData();
    } catch (error) {
      console.error("Create error:", error);
      toast.error(error.response?.data?.detail || "Failed to create relationship");
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (id) => {
    try {
      await deleteRelationship(id);
      toast.success("Relationship deleted");
      setRelationships(relationships.filter((r) => r.id !== id));
    } catch (error) {
      console.error("Delete error:", error);
      toast.error("Failed to delete relationship");
    }
  };

  const resetForm = () => {
    setSourceDatasetId("");
    setSourceColumn("");
    setTargetDatasetId("");
    setTargetColumn("");
    setRelationshipName("");
    setIsRequired(true);
    setSourceSchema(null);
    setTargetSchema(null);
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="relationships-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">
            Dataset Relationships
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            Define foreign key relationships between datasets
          </p>
        </div>
        <Button
          onClick={() => setCreateModalOpen(true)}
          className="bg-black text-white hover:bg-gray-800"
          data-testid="create-relationship-btn"
        >
          <Plus className="w-4 h-4 mr-2" />
          Create Relationship
        </Button>
      </div>

      {/* Relationships Table */}
      <Card className="border border-border shadow-none bg-white">
        <CardContent className="p-0">
          {relationships.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-xs uppercase tracking-wider">Name</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Source</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider"></TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Target</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Required</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider">Created</TableHead>
                  <TableHead className="text-xs uppercase tracking-wider text-right">
                    Actions
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {relationships.map((rel) => (
                  <TableRow
                    key={rel.id}
                    className="hover:bg-secondary/50"
                    data-testid={`relationship-row-${rel.id}`}
                  >
                    <TableCell className="font-medium">
                      {rel.relationship_name || (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <div>
                        <p className="font-medium">{rel.source_dataset_name}</p>
                        <p className="text-xs font-mono text-muted-foreground">
                          {rel.source_column}
                        </p>
                      </div>
                    </TableCell>
                    <TableCell>
                      <ArrowRight className="w-5 h-5 text-muted-foreground" />
                    </TableCell>
                    <TableCell>
                      <div>
                        <p className="font-medium">{rel.target_dataset_name}</p>
                        <p className="text-xs font-mono text-muted-foreground">
                          {rel.target_column}
                        </p>
                      </div>
                    </TableCell>
                    <TableCell>
                      {rel.is_required ? (
                        <span className="px-2 py-0.5 text-xs font-semibold bg-red-100 text-red-700">
                          Required
                        </span>
                      ) : (
                        <span className="px-2 py-0.5 text-xs font-semibold bg-gray-100 text-gray-600">
                          Optional
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {format(new Date(rel.created_at), "MMM d, yyyy")}
                    </TableCell>
                    <TableCell className="text-right">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleDelete(rel.id)}
                        className="border-border text-red-600 hover:text-red-700 hover:bg-red-50"
                        data-testid={`delete-relationship-${rel.id}`}
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
              title="No relationships defined"
              description="Create foreign key relationships between datasets to enable referential integrity validation."
              icon={LinkSimple}
              action={
                <Button
                  onClick={() => setCreateModalOpen(true)}
                  className="bg-black text-white hover:bg-gray-800"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Create Relationship
                </Button>
              }
            />
          )}
        </CardContent>
      </Card>

      {/* Create Relationship Modal */}
      <Dialog open={createModalOpen} onOpenChange={setCreateModalOpen}>
        <DialogContent className="sm:max-w-lg border border-border shadow-none">
          <DialogHeader>
            <DialogTitle className="text-xl font-bold">
              Create Relationship
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-6 py-4">
            {/* Relationship Name */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Relationship Name (Optional)
              </Label>
              <Input
                value={relationshipName}
                onChange={(e) => setRelationshipName(e.target.value)}
                placeholder="e.g., orders_customers"
                className="border-border"
                data-testid="relationship-name-input"
              />
            </div>

            {/* Source Dataset */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Source Dataset *
              </Label>
              <Select value={sourceDatasetId} onValueChange={setSourceDatasetId}>
                <SelectTrigger className="border-border" data-testid="source-dataset-select">
                  <SelectValue placeholder="Select source dataset" />
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

            {/* Source Column */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Source Column *
              </Label>
              <Select
                value={sourceColumn}
                onValueChange={setSourceColumn}
                disabled={!sourceSchema}
              >
                <SelectTrigger className="border-border" data-testid="source-column-select">
                  <SelectValue placeholder="Select source column" />
                </SelectTrigger>
                <SelectContent>
                  {sourceSchema &&
                    Object.keys(sourceSchema).map((col) => (
                      <SelectItem key={col} value={col}>
                        <span className="font-mono">{col}</span>
                        <span className="ml-2 text-xs text-muted-foreground">
                          ({sourceSchema[col]})
                        </span>
                      </SelectItem>
                    ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center justify-center">
              <ArrowRight className="w-6 h-6 text-muted-foreground" />
            </div>

            {/* Target Dataset */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Target Dataset *
              </Label>
              <Select value={targetDatasetId} onValueChange={setTargetDatasetId}>
                <SelectTrigger className="border-border" data-testid="target-dataset-select">
                  <SelectValue placeholder="Select target dataset" />
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

            {/* Target Column */}
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider">
                Target Column *
              </Label>
              <Select
                value={targetColumn}
                onValueChange={setTargetColumn}
                disabled={!targetSchema}
              >
                <SelectTrigger className="border-border" data-testid="target-column-select">
                  <SelectValue placeholder="Select target column" />
                </SelectTrigger>
                <SelectContent>
                  {targetSchema &&
                    Object.keys(targetSchema).map((col) => (
                      <SelectItem key={col} value={col}>
                        <span className="font-mono">{col}</span>
                        <span className="ml-2 text-xs text-muted-foreground">
                          ({targetSchema[col]})
                        </span>
                      </SelectItem>
                    ))}
                </SelectContent>
              </Select>
            </div>

            {/* Is Required */}
            <div className="flex items-center justify-between">
              <div>
                <Label className="text-xs font-semibold uppercase tracking-wider">
                  Required Relationship
                </Label>
                <p className="text-xs text-muted-foreground mt-1">
                  If enabled, orphaned values will be flagged as issues
                </p>
              </div>
              <Switch
                checked={isRequired}
                onCheckedChange={setIsRequired}
                data-testid="is-required-switch"
              />
            </div>
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
              disabled={
                !sourceDatasetId ||
                !sourceColumn ||
                !targetDatasetId ||
                !targetColumn ||
                creating
              }
              className="bg-black text-white hover:bg-gray-800"
              data-testid="confirm-create-relationship"
            >
              {creating ? (
                <>
                  <Spinner className="w-4 h-4 mr-2 animate-spin" />
                  Creating...
                </>
              ) : (
                "Create Relationship"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
