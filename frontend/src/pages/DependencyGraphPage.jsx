import { useState, useEffect, useCallback, useMemo } from "react";
import {
  ReactFlow,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
  Panel,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  Database,
  CloudArrowDown,
  LinkSimple,
  WarningCircle,
  CheckCircle,
  GitBranch,
} from "@phosphor-icons/react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSpinner, EmptyState } from "@/components/DataQualityComponents";
import { getDependencyGraph } from "@/api";
import { toast } from "sonner";

// Custom node component for datasets
const DatasetNode = ({ data }) => {
  const getNodeStyle = () => {
    if (data.type === "source") {
      return "bg-blue-50 border-blue-300";
    }
    if (data.has_issues) {
      return "bg-red-50 border-red-300";
    }
    if (data.quality_score !== null && data.quality_score < 80) {
      return "bg-yellow-50 border-yellow-300";
    }
    return "bg-white border-border";
  };

  const getIcon = () => {
    if (data.type === "source") {
      return <CloudArrowDown className="w-4 h-4 text-blue-600" weight="bold" />;
    }
    return <Database className="w-4 h-4" weight="bold" />;
  };

  return (
    <div
      className={`px-4 py-3 border-2 min-w-[160px] ${getNodeStyle()}`}
      data-testid={`graph-node-${data.label}`}
    >
      <div className="flex items-center gap-2 mb-1">
        {getIcon()}
        <span className="font-bold text-sm truncate max-w-[120px]">{data.label}</span>
      </div>
      {data.type === "dataset" && (
        <div className="space-y-1 text-xs">
          <div className="flex justify-between text-muted-foreground">
            <span>Rows:</span>
            <span className="font-mono">{data.row_count?.toLocaleString() || 0}</span>
          </div>
          {data.quality_score !== null && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">Quality:</span>
              <span
                className={`font-mono font-bold ${
                  data.quality_score >= 80
                    ? "text-green-600"
                    : data.quality_score >= 60
                    ? "text-yellow-600"
                    : "text-red-600"
                }`}
              >
                {data.quality_score.toFixed(1)}%
              </span>
            </div>
          )}
        </div>
      )}
      {data.type === "source" && (
        <div className="text-xs text-blue-600 font-medium">External Source</div>
      )}
    </div>
  );
};

const nodeTypes = {
  dataset: DatasetNode,
};

export default function DependencyGraphPage() {
  const [graphData, setGraphData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const fetchGraph = useCallback(async () => {
    try {
      const data = await getDependencyGraph();
      setGraphData(data);
      
      // Convert to ReactFlow format
      const flowNodes = data.nodes.map((node, index) => ({
        id: node.id,
        type: "dataset",
        position: { x: (index % 4) * 250, y: Math.floor(index / 4) * 180 },
        data: {
          label: node.name,
          type: node.type,
          row_count: node.row_count,
          quality_score: node.quality_score,
          has_issues: node.has_issues,
        },
      }));

      const flowEdges = data.edges.map((edge) => ({
        id: edge.id,
        source: edge.source,
        target: edge.target,
        label: edge.label,
        type: "smoothstep",
        animated: edge.type === "lineage",
        style: {
          stroke: edge.is_valid === false ? "#dc2626" : edge.type === "lineage" ? "#3b82f6" : "#000",
          strokeWidth: 2,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: edge.is_valid === false ? "#dc2626" : edge.type === "lineage" ? "#3b82f6" : "#000",
        },
        labelStyle: { fontSize: 10 },
        labelBgStyle: { fill: "#fff", fillOpacity: 0.9 },
      }));

      setNodes(flowNodes);
      setEdges(flowEdges);
    } catch (error) {
      console.error("Failed to fetch graph:", error);
      toast.error("Failed to load dependency graph");
    } finally {
      setLoading(false);
    }
  }, [setNodes, setEdges]);

  useEffect(() => {
    fetchGraph();
  }, [fetchGraph]);

  // Stats
  const stats = useMemo(() => {
    if (!graphData) return null;
    
    const datasets = graphData.nodes.filter((n) => n.type === "dataset");
    const sources = graphData.nodes.filter((n) => n.type === "source");
    const relationships = graphData.edges.filter((e) => e.type === "relationship");
    const lineages = graphData.edges.filter((e) => e.type === "lineage");
    const brokenRelationships = graphData.edges.filter(
      (e) => e.type === "relationship" && e.is_valid === false
    );

    return {
      datasetCount: datasets.length,
      sourceCount: sources.length,
      relationshipCount: relationships.length,
      lineageCount: lineages.length,
      brokenCount: brokenRelationships.length,
    };
  }, [graphData]);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 sm:p-8 space-y-6" data-testid="dependency-graph-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">
            Dependency Graph
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            Visualize dataset relationships and data lineage
          </p>
        </div>
        <Button
          onClick={fetchGraph}
          variant="outline"
          className="border-border"
          data-testid="refresh-graph-btn"
        >
          <GitBranch className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 sm:grid-cols-5 gap-4">
          <Card className="border border-border shadow-none bg-white">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <Database className="w-5 h-5 text-muted-foreground" />
                <div>
                  <p className="text-2xl font-bold font-mono">{stats.datasetCount}</p>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">
                    Datasets
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="border border-border shadow-none bg-white">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <CloudArrowDown className="w-5 h-5 text-blue-600" />
                <div>
                  <p className="text-2xl font-bold font-mono">{stats.sourceCount}</p>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">
                    Sources
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="border border-border shadow-none bg-white">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <LinkSimple className="w-5 h-5 text-muted-foreground" />
                <div>
                  <p className="text-2xl font-bold font-mono">{stats.relationshipCount}</p>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">
                    Relationships
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card className="border border-border shadow-none bg-white">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <GitBranch className="w-5 h-5 text-blue-600" />
                <div>
                  <p className="text-2xl font-bold font-mono">{stats.lineageCount}</p>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">
                    Lineage
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card
            className={`border shadow-none ${
              stats.brokenCount > 0
                ? "border-red-200 bg-red-50"
                : "border-border bg-white"
            }`}
          >
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                {stats.brokenCount > 0 ? (
                  <WarningCircle className="w-5 h-5 text-red-600" weight="bold" />
                ) : (
                  <CheckCircle className="w-5 h-5 text-green-600" weight="bold" />
                )}
                <div>
                  <p className="text-2xl font-bold font-mono">{stats.brokenCount}</p>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">
                    Broken Links
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Graph Canvas */}
      <Card className="border border-border shadow-none bg-white">
        <CardContent className="p-0">
          {nodes.length > 0 ? (
            <div className="h-[600px]" data-testid="graph-canvas">
              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                nodeTypes={nodeTypes}
                fitView
                fitViewOptions={{ padding: 0.2 }}
                minZoom={0.3}
                maxZoom={2}
              >
                <Controls />
                <Background color="#e5e5e5" gap={20} />
                <Panel position="bottom-left" className="bg-white border border-border p-3">
                  <div className="space-y-2 text-xs">
                    <div className="flex items-center gap-2">
                      <div className="w-4 h-0.5 bg-black" />
                      <span>Foreign Key</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-4 h-0.5 bg-blue-500" style={{ backgroundImage: 'repeating-linear-gradient(90deg, #3b82f6, #3b82f6 4px, transparent 4px, transparent 8px)' }} />
                      <span>Data Lineage</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-4 h-0.5 bg-red-500" />
                      <span>Broken Link</span>
                    </div>
                  </div>
                </Panel>
              </ReactFlow>
            </div>
          ) : (
            <EmptyState
              title="No dependencies found"
              description="Create relationships between datasets or add lineage entries to visualize the dependency graph."
              icon={GitBranch}
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
