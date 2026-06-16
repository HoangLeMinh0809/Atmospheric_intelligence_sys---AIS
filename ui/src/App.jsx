// File nay: entrypoint/cau hinh build frontend React/Vite.
import { useEffect, useState } from "react";
import AirQualityMapDashboard from "./pages/AirQualityMapDashboard";
import StatisticsDashboard from "./pages/StatisticsDashboard";
import "./index.css";

// Render component App va gan state/props cho UI.
function App() {
  const [route, setRoute] = useState(window.location.hash);

  useEffect(() => {
    // Khai bao class onHashChange de gom state, cau hinh hoac hanh vi lien quan.
    const onHashChange = () => setRoute(window.location.hash);
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return route === "#/statistics" ? <StatisticsDashboard /> : <AirQualityMapDashboard />;
}

export default App;
