import { NavLink, useLocation } from "react-router-dom";
import {
  ChartBar,
  Database,
  ClockCounterClockwise,
  CalendarCheck,
  Gauge,
  LinkSimple,
  GitBranch,
  Graph,
} from "@phosphor-icons/react";

const navItems = [
  { path: "/", label: "Dashboard", icon: Gauge },
  { path: "/datasets", label: "Datasets", icon: Database },
  { path: "/jobs", label: "Validation Jobs", icon: ClockCounterClockwise },
  { path: "/relationships", label: "Relationships", icon: LinkSimple },
  { path: "/lineage", label: "Data Lineage", icon: GitBranch },
  { path: "/graph", label: "Dependency Graph", icon: Graph },
  { path: "/schedules", label: "Schedules", icon: CalendarCheck },
];

export default function Sidebar() {
  const location = useLocation();

  return (
    <aside
      className="fixed left-0 top-0 h-screen w-64 bg-white border-r border-border flex flex-col"
      data-testid="sidebar"
    >
      {/* Logo / Brand */}
      <div className="p-6 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-black flex items-center justify-center">
            <ChartBar className="w-6 h-6 text-white" weight="bold" />
          </div>
          <div>
            <h1 className="text-lg font-bold tracking-tight">Data Quality</h1>
            <p className="text-xs text-muted-foreground uppercase tracking-widest">
              Monitor
            </p>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4">
        <ul className="space-y-1">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = location.pathname === item.path;

            return (
              <li key={item.path}>
                <NavLink
                  to={item.path}
                  className={`flex items-center gap-3 px-4 py-3 text-sm font-medium transition-all duration-200 border ${
                    isActive
                      ? "bg-black text-white border-black"
                      : "bg-white text-foreground border-transparent hover:bg-secondary hover:border-border"
                  }`}
                  data-testid={`nav-${item.label.toLowerCase().replace(" ", "-")}`}
                >
                  <Icon className="w-5 h-5" weight={isActive ? "bold" : "regular"} />
                  {item.label}
                </NavLink>
              </li>
            );
          })}
        </ul>
      </nav>

      {/* Footer */}
      <div className="p-4 border-t border-border">
        <p className="text-xs text-muted-foreground text-center">
          PostgreSQL • FastAPI • React
        </p>
      </div>
    </aside>
  );
}
