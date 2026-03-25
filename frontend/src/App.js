import { useEffect, useState, useCallback } from "react";
import "@/App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Toaster } from "@/components/ui/sonner";
import Dashboard from "@/pages/Dashboard.jsx";
import DatasetsPage from "@/pages/DatasetsPage.jsx";
import ValidationJobsPage from "@/pages/ValidationJobsPage.jsx";
import ValidationResultsPage from "@/pages/ValidationResultsPage.jsx";
import SchedulesPage from "@/pages/SchedulesPage.jsx";
import RelationshipsPage from "@/pages/RelationshipsPage.jsx";
import LineagePage from "@/pages/LineagePage.jsx";
import DependencyGraphPage from "@/pages/DependencyGraphPage.jsx";
import Sidebar from "@/components/Sidebar.jsx";

function App() {
  return (
    <div className="App min-h-screen bg-background">
      <Toaster position="top-right" />
      <BrowserRouter>
        <div className="flex min-h-screen">
          <Sidebar />
          <main className="flex-1 ml-64">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/datasets" element={<DatasetsPage />} />
              <Route path="/jobs" element={<ValidationJobsPage />} />
              <Route path="/jobs/:jobId" element={<ValidationResultsPage />} />
              <Route path="/schedules" element={<SchedulesPage />} />
              <Route path="/relationships" element={<RelationshipsPage />} />
              <Route path="/lineage" element={<LineagePage />} />
              <Route path="/graph" element={<DependencyGraphPage />} />
            </Routes>
          </main>
        </div>
      </BrowserRouter>
    </div>
  );
}

export default App;
