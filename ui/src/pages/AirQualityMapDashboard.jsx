import { useEffect, useMemo, useState } from "react";
import PM25ForecastChart from "../components/charts/PM25ForecastChart";
import DateSelector from "../components/map/DateSelector";
import ForecastPanel from "../components/map/ForecastPanel";
import FreshnessBadge from "../components/map/FreshnessBadge";
import LayerControl from "../components/map/LayerControl";
import MapCanvas from "../components/map/MapCanvas";
import MapPopup from "../components/map/MapPopup";
import SourceAttributionPanel from "../components/map/SourceAttributionPanel";
import TimeSelector from "../components/map/TimeSelector";
import {
  getBackwardTrajectoriesLatest,
  getForecastLatest,
  getForwardPlumeLatest,
  getHeatmapLatest,
  getManifestLatest,
  getPM25TimeseriesLatest,
  getSourceAttributionLatest,
  getStationsLatest,
} from "../services/visualizationApi";

export default function AirQualityMapDashboard() {
  const [horizon, setHorizon] = useState(Number(import.meta.env.VITE_DEFAULT_HORIZON_H || 0));
  const [selectedDate, setSelectedDate] = useState("");
  const [enabled, setEnabled] = useState({
    heatmap: true,
    trajectories: true,
    plume: false,
    sources: true,
    stations: true,
  });
  const [selected, setSelected] = useState(null);
  const [status, setStatus] = useState({ loading: true, error: "" });
  const [data, setData] = useState({
    manifest: null,
    heatmap: null,
    trajectories: null,
    plume: null,
    forecast: null,
    timeseries: null,
    sources: null,
    stations: null,
  });

  useEffect(() => {
    let active = true;
    Promise.all([
      getManifestLatest(selectedDate),
      getHeatmapLatest(horizon, selectedDate),
      getBackwardTrajectoriesLatest(selectedDate),
      getForwardPlumeLatest(horizon === 0 ? 6 : horizon, selectedDate),
      getForecastLatest("hanoi", selectedDate),
      getPM25TimeseriesLatest("hanoi", selectedDate),
      getSourceAttributionLatest("hanoi", selectedDate),
      getStationsLatest(selectedDate),
    ])
      .then(([manifest, heatmap, trajectories, plume, forecast, timeseries, sources, stations]) => {
        if (!active) return;
        setData({ manifest, heatmap, trajectories, plume, forecast, timeseries, sources, stations });
        setStatus({ loading: false, error: "" });
      })
      .catch((error) => {
        if (!active) return;
        setStatus({ loading: false, error: error.message });
      });
    return () => {
      active = false;
    };
  }, [horizon, selectedDate]);

  const layerData = useMemo(
    () => ({
      heatmap: data.heatmap,
      trajectories: data.trajectories,
      plume: data.plume?.available === false ? { type: "FeatureCollection", features: [] } : data.plume,
      sources: data.sources,
      stations: data.stations,
    }),
    [data],
  );

  function toggleLayer(key) {
    setEnabled((current) => ({ ...current, [key]: !current[key] }));
  }

  function changeHorizon(nextHorizon) {
    setStatus({ loading: true, error: "" });
    setHorizon(nextHorizon);
  }

  function changeDate(nextDate) {
    setStatus({ loading: true, error: "" });
    setSelectedDate(nextDate);
  }

  return (
    <div className="air-map-page">
      <MapCanvas layers={layerData} enabled={enabled} onSelect={setSelected} />
      <div className="topbar">
        <div>
          <h1>AIS Air Quality</h1>
          <span>Northern Vietnam PM2.5 visualization</span>
        </div>
        <DateSelector value={selectedDate} availableDates={data.manifest?.available_dates} onChange={changeDate} />
        <TimeSelector horizon={horizon} onChange={changeHorizon} />
        <FreshnessBadge forecast={data.forecast} />
      </div>

      <aside className="left-stack">
        <ForecastPanel forecast={data.forecast} />
        <SourceAttributionPanel sourceAttribution={data.sources} plume={data.plume} />
      </aside>

      <aside className="right-stack">
        <LayerControl enabled={enabled} onToggle={toggleLayer} />
        <PM25ForecastChart timeseries={data.timeseries} />
      </aside>

      {status.loading && <div className="status-toast">Loading visualization cache</div>}
      {status.error && <div className="status-toast error">{status.error}</div>}
      <MapPopup feature={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
