import { useState } from "react";
import AirQualityMapDashboard from "./pages/AirQualityMapDashboard";
import RealtimeDashboard from "./pages/RealtimeDashboard";
import HistoricalDashboard from "./pages/HistoricalDashboard";
import "./index.css";

function App() {
  const [page, setPage] = useState("map");

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h2>AIS UI</h2>
        <button
          className={page === "map" ? "nav-btn active" : "nav-btn"}
          onClick={() => setPage("map")}
        >
          Air Quality Map
        </button>
        <button
          className={page === "realtime" ? "nav-btn active" : "nav-btn"}
          onClick={() => setPage("realtime")}
        >
          Realtime Dashboard
        </button>
        <button
          className={page === "history" ? "nav-btn active" : "nav-btn"}
          onClick={() => setPage("history")}
        >
          Historical Dashboard
        </button>
      </aside>

      <main className="main-content">
        {page === "map" && <AirQualityMapDashboard />}
        {page === "realtime" && <RealtimeDashboard />}
        {page === "history" && <HistoricalDashboard />}
      </main>
    </div>
  );
}

export default App;
