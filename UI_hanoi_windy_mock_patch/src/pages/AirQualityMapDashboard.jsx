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

function heatmapStats(heatmap) {
  const values = (heatmap?.features || [])
    .map((feature) => Number(feature.properties?.pm25_value ?? feature.properties?.pm25 ?? feature.properties?.value))
    .filter((value) => Number.isFinite(value));
  if (!values.length) return { avg: "-", max: "-", count: 0 };
  return {
    avg: (values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(1),
    max: Math.max(...values).toFixed(1),
    count: values.length,
  };
}

export default function AirQualityMapDashboard() {
  const [horizon, setHorizon] = useState(Number(import.meta.env.VITE_DEFAULT_HORIZON_H || 0));
  const [selectedDate, setSelectedDate] = useState(import.meta.env.VITE_DEFAULT_VIS_DATE || "");
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

  const stats = useMemo(() => heatmapStats(data.heatmap), [data.heatmap]);

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
    <div className="air-map-page windy-shell">
      <MapCanvas layers={layerData} enabled={enabled} onSelect={setSelected} />

      <header className="windy-topbar">
        <div className="windy-brand">
          <div className="windy-logo">AIS</div>
          <div>
            <h1>Hà Nội Air Flow</h1>
            <span>PM2.5 heatmap · backward trajectories · station observations</span>
          </div>
        </div>
        <div className="windy-top-controls">
          <DateSelector value={selectedDate} availableDates={data.manifest?.available_dates} onChange={changeDate} />
          <TimeSelector horizon={horizon} onChange={changeHorizon} />
          <FreshnessBadge forecast={data.forecast} />
        </div>
      </header>

      <aside className="windy-left-rail" aria-label="Quick map statistics">
        <div className="mini-stat active"><span>Avg PM2.5</span><strong>{stats.avg}</strong><em>µg/m³</em></div>
        <div className="mini-stat"><span>Max cell</span><strong>{stats.max}</strong><em>µg/m³</em></div>
        <div className="mini-stat"><span>Grid</span><strong>{stats.count}</strong><em>cells</em></div>
      </aside>

      <aside className="windy-right-panel">
        <LayerControl enabled={enabled} onToggle={toggleLayer} />
        <ForecastPanel forecast={data.forecast} />
        <SourceAttributionPanel sourceAttribution={data.sources} plume={data.plume} />
        <PM25ForecastChart timeseries={data.timeseries} />
      </aside>

      <div className="windy-legend" aria-label="PM2.5 color legend">
        <span>PM2.5</span>
        <div className="legend-ramp" />
        <div className="legend-ticks"><b>0</b><b>35</b><b>55</b><b>75</b><b>150+</b></div>
      </div>

      <div className="windy-bottom-timeline">
        <div className="timeline-caption">
          <strong>{horizon === 0 ? "Latest observed/nowcast" : `Forecast +${horizon}h`}</strong>
          <span>{selectedDate || data.manifest?.selected_date || "latest cache"}</span>
        </div>
        <TimeSelector horizon={horizon} onChange={changeHorizon} />
      </div>

      {status.loading && <div className="status-toast windy-status">Loading Hanoi visualization cache</div>}
      {status.error && <div className="status-toast error windy-status">{status.error}</div>}
      <MapPopup feature={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
