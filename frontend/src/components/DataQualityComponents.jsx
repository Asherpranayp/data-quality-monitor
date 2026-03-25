import {
  Warning,
  WarningCircle,
  WarningOctagon,
  Info,
  CheckCircle,
  XCircle,
  Clock,
  Spinner,
} from "@phosphor-icons/react";

export function SeverityBadge({ severity }) {
  const config = {
    critical: {
      icon: WarningOctagon,
      className: "severity-critical border",
      label: "Critical",
    },
    high: {
      icon: WarningCircle,
      className: "severity-high border",
      label: "High",
    },
    medium: {
      icon: Warning,
      className: "severity-medium border",
      label: "Medium",
    },
    low: {
      icon: Info,
      className: "severity-low border",
      label: "Low",
    },
  };

  const { icon: Icon, className, label } = config[severity] || config.low;

  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2 py-1 text-xs font-semibold uppercase tracking-wide ${className}`}
      data-testid={`severity-badge-${severity}`}
    >
      <Icon className="w-3.5 h-3.5" weight="bold" />
      {label}
    </span>
  );
}

export function StatusBadge({ status }) {
  const config = {
    completed: {
      icon: CheckCircle,
      className: "status-completed",
      label: "Completed",
    },
    running: {
      icon: Spinner,
      className: "status-running",
      label: "Running",
      animate: true,
    },
    pending: {
      icon: Clock,
      className: "status-pending",
      label: "Pending",
    },
    failed: {
      icon: XCircle,
      className: "status-failed",
      label: "Failed",
    },
  };

  const { icon: Icon, className, label, animate } = config[status] || config.pending;

  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2 py-1 text-xs font-semibold uppercase tracking-wide ${className}`}
      data-testid={`status-badge-${status}`}
    >
      <Icon className={`w-3.5 h-3.5 ${animate ? "animate-spin" : ""}`} weight="bold" />
      {label}
    </span>
  );
}

export function QualityScore({ score, size = "md" }) {
  const getColorClass = (s) => {
    if (s >= 90) return "quality-excellent";
    if (s >= 75) return "quality-good";
    if (s >= 60) return "quality-moderate";
    if (s >= 40) return "quality-poor";
    return "quality-critical";
  };

  const sizeClasses = {
    sm: "text-lg",
    md: "text-2xl",
    lg: "text-4xl",
    xl: "text-5xl",
  };

  return (
    <div className="flex items-baseline gap-1" data-testid="quality-score">
      <span className={`font-bold font-mono ${sizeClasses[size]} ${getColorClass(score)}`}>
        {score.toFixed(1)}
      </span>
      <span className="text-muted-foreground text-sm">/100</span>
    </div>
  );
}

export function MetricCard({ label, value, icon: Icon, trend, className = "" }) {
  return (
    <div
      className={`bg-white border border-border p-4 transition-lift hover:border-gray-300 ${className}`}
      data-testid={`metric-card-${label.toLowerCase().replace(/\s/g, "-")}`}
    >
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-1">
            {label}
          </p>
          <p className="text-2xl font-bold font-mono">{value}</p>
          {trend && (
            <p className={`text-xs mt-1 ${trend > 0 ? "text-green-600" : "text-red-600"}`}>
              {trend > 0 ? "+" : ""}{trend}% from last run
            </p>
          )}
        </div>
        {Icon && (
          <div className="w-10 h-10 bg-secondary flex items-center justify-center">
            <Icon className="w-5 h-5 text-muted-foreground" weight="regular" />
          </div>
        )}
      </div>
    </div>
  );
}

export function IssueTypeLabel({ type }) {
  const labels = {
    missing_values: "Missing Values",
    duplicates: "Duplicates",
    schema_mismatch: "Schema Mismatch",
    outliers: "Outliers",
    type_inconsistency: "Type Inconsistency",
    unexpected_categorical: "Unexpected Categories",
    range_violation: "Range Violation",
    freshness_issue: "Freshness Issue",
    primary_key_violation: "PK Violation",
    referential_integrity: "Referential Integrity",
  };

  return <span className="font-mono text-sm">{labels[type] || type}</span>;
}

export function EmptyState({ title, description, action, icon: Icon }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      {Icon && (
        <div className="w-16 h-16 bg-secondary flex items-center justify-center mb-4">
          <Icon className="w-8 h-8 text-muted-foreground" />
        </div>
      )}
      <h3 className="text-lg font-bold mb-2">{title}</h3>
      <p className="text-sm text-muted-foreground max-w-md mb-6">{description}</p>
      {action}
    </div>
  );
}

export function LoadingSpinner({ size = "md" }) {
  const sizeClasses = {
    sm: "w-4 h-4",
    md: "w-8 h-8",
    lg: "w-12 h-12",
  };

  return (
    <div className="flex items-center justify-center p-8">
      <Spinner className={`${sizeClasses[size]} animate-spin text-muted-foreground`} />
    </div>
  );
}
