import { useState, useCallback } from "react";
import { useDropzone } from "react-dropzone";
import { UploadSimple, X, File, Spinner } from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { uploadDataset } from "@/api";
import { toast } from "sonner";

export default function UploadDatasetModal({ open, onClose, onSuccess }) {
  const [file, setFile] = useState(null);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [primaryKeyColumns, setPrimaryKeyColumns] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const onDrop = useCallback((acceptedFiles) => {
    const csvFile = acceptedFiles.find((f) => f.name.endsWith(".csv"));
    if (csvFile) {
      setFile(csvFile);
      if (!name) {
        setName(csvFile.name.replace(".csv", ""));
      }
    } else {
      toast.error("Please upload a CSV file");
    }
  }, [name]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { "text/csv": [".csv"] },
    maxFiles: 1,
  });

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!file || !name) {
      toast.error("Please provide a file and name");
      return;
    }

    setIsLoading(true);
    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("name", name);
      formData.append("description", description);

      if (primaryKeyColumns.trim()) {
  const pkCols = primaryKeyColumns
    .split(",")
    .map((c) => c.trim())
    .filter(Boolean);

  pkCols.forEach((col) => {
    formData.append("primary_key_columns", col);
  });
}

      const dataset = await uploadDataset(formData);
      toast.success(`Dataset "${dataset.name}" uploaded successfully`);
      onSuccess?.(dataset);
      handleClose();
    } catch (error) {
      console.error("Upload error:", error);
      toast.error(error.response?.data?.detail || "Failed to upload dataset");
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    setFile(null);
    setName("");
    setDescription("");
    setPrimaryKeyColumns("");
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-lg border border-border shadow-none">
        <DialogHeader>
          <DialogTitle className="text-xl font-bold">Upload Dataset</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Dropzone */}
          <div
            {...getRootProps()}
            className={`dropzone p-8 text-center cursor-pointer ${
              isDragActive ? "drag-over" : ""
            } ${file ? "border-green-400 bg-green-50" : ""}`}
            data-testid="upload-dropzone"
          >
            <input {...getInputProps()} data-testid="file-input" />
            {file ? (
              <div className="flex items-center justify-center gap-3">
                <File className="w-8 h-8 text-green-600" weight="fill" />
                <div className="text-left">
                  <p className="font-medium">{file.name}</p>
                  <p className="text-sm text-muted-foreground">
                    {(file.size / 1024).toFixed(1)} KB
                  </p>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={(e) => {
                    e.stopPropagation();
                    setFile(null);
                  }}
                >
                  <X className="w-4 h-4" />
                </Button>
              </div>
            ) : (
              <>
                <UploadSimple className="w-12 h-12 mx-auto text-muted-foreground mb-3" />
                <p className="text-sm font-medium">
                  {isDragActive
                    ? "Drop the CSV file here"
                    : "Drag & drop a CSV file, or click to select"}
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  Only .csv files are supported
                </p>
              </>
            )}
          </div>

          {/* Name */}
          <div className="space-y-2">
            <Label htmlFor="name" className="text-xs font-semibold uppercase tracking-wider">
              Dataset Name *
            </Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Enter dataset name"
              required
              className="border-border"
              data-testid="dataset-name-input"
            />
          </div>

          {/* Description */}
          <div className="space-y-2">
            <Label htmlFor="description" className="text-xs font-semibold uppercase tracking-wider">
              Description
            </Label>
            <Textarea
              id="description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional description"
              rows={2}
              className="border-border resize-none"
              data-testid="dataset-description-input"
            />
          </div>

          {/* Primary Key Columns */}
          <div className="space-y-2">
            <Label htmlFor="pk" className="text-xs font-semibold uppercase tracking-wider">
              Primary Key Columns
            </Label>
            <Input
              id="pk"
              value={primaryKeyColumns}
              onChange={(e) => setPrimaryKeyColumns(e.target.value)}
              placeholder="e.g., id, user_id (comma-separated)"
              className="border-border"
              data-testid="pk-columns-input"
            />
            <p className="text-xs text-muted-foreground">
              Used for uniqueness validation
            </p>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={handleClose}
              disabled={isLoading}
              className="border-border"
            >
              Cancel
            </Button>
            <Button
              type="submit"
              disabled={!file || !name || isLoading}
              className="bg-black text-white hover:bg-gray-800"
              data-testid="upload-submit-btn"
            >
              {isLoading ? (
                <>
                  <Spinner className="w-4 h-4 mr-2 animate-spin" />
                  Uploading...
                </>
              ) : (
                "Upload Dataset"
              )}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
