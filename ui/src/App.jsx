import { useEffect, useState } from "react";
import AirQualityMapDashboard from "./pages/AirQualityMapDashboard";
import StatisticsDashboard from "./pages/StatisticsDashboard";
import "./index.css";

function App() {
  const [route, setRoute] = useState(window.location.hash);

  useEffect(() => {
    const onHashChange = () => setRoute(window.location.hash);
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return route === "#/statistics" ? <StatisticsDashboard /> : <AirQualityMapDashboard />;
}

export default App;
