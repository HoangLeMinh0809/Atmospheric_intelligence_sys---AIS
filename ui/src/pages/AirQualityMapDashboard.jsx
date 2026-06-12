import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
  getLiveHeatmapLatest,
  getManifestLatest,
  getPM25TimeseriesLatest,
  getSourceAttributionLatest,
  getStationsLatest,
} from "../services/visualizationApi";

export default function AirQualityMapDashboard() {
  const [horizon, setHorizon] = useState(Number(import.meta.env.VITE_DEFAULT_HORIZON_H || 0));
  const [selectedDate, setSelectedDate] = useState(import.meta.env.VITE_DEFAULT_VIS_DATE || "");
  const refreshMs = Number(import.meta.env.VITE_VIS_REFRESH_MS || 60000);
  const [refreshTick, setRefreshTick] = useState(0);
  const [enabled, setEnabled] = useState({
    heatmap: true,
    trajectories: true,
    plume: false,
    sources: true,
    stations: true,
  });
  const [selected, setSelected] = useState(null);
  const [status, setStatus] = useState({ loading: true, error: "" });
  const [mapStats, setMapStats] = useState({ avg: "-", max: "-", count: 0, source: "" });
  const [data, setData] = useState({
    manifest: null,
    heatmap: null,
    liveHeatmap: null,
    trajectories: null,
    plume: null,
    heatmapsByHorizon: {},
    forecast: null,
    timeseries: null,
    sources: null,
    stations: null,
  });
  const horizonRef = useRef(horizon);
  const fullLoadRequestIdRef = useRef(0);
  const horizonRequestIdRef = useRef(0);

  useEffect(() => {
    horizonRef.current = horizon;
  }, [horizon]);

  useEffect(() => {
    if (!Number.isFinite(refreshMs) || refreshMs <= 0) return undefined;
    const timer = window.setInterval(() => {
      setRefreshTick((current) => current + 1);
    }, refreshMs);
    return () => window.clearInterval(timer);
  }, [refreshMs]);

  useEffect(() => {
    let active = true;
    const requestId = ++fullLoadRequestIdRef.current;
    const requestHorizon = horizonRef.current;
    const plumeHorizon = requestHorizon === 0 ? 6 : requestHorizon;
    Promise.all([
      getManifestLatest(selectedDate),
      getHeatmapLatest(requestHorizon, selectedDate),
      getLiveHeatmapLatest("hanoi", selectedDate).catch(() => null),
      getBackwardTrajectoriesLatest(selectedDate),
      getForwardPlumeLatest(plumeHorizon, selectedDate),
      getForecastLatest("hanoi", selectedDate),
      getPM25TimeseriesLatest("hanoi", selectedDate),
      getSourceAttributionLatest("hanoi", selectedDate),
      getStationsLatest(selectedDate),
    ])
      .then(([manifest, cachedHeatmap, liveHeatmap, trajectories, plume, forecast, timeseries, sources, stations]) => {
        if (!active || requestId !== fullLoadRequestIdRef.current) return;
        const heatmap = requestHorizon === 0 && liveHeatmap ? liveHeatmap : cachedHeatmap;
        setData((current) => ({
          manifest,
          heatmap: horizonRef.current === requestHorizon ? heatmap : current.heatmap,
          liveHeatmap: liveHeatmap || current.liveHeatmap,
          heatmapsByHorizon: {
            ...(current.heatmapsByHorizon || {}),
            [requestHorizon]: heatmap,
            ...(liveHeatmap ? { 0: liveHeatmap } : {}),
          },
          trajectories,
          plume,
          forecast,
          timeseries,
          sources,
          stations,
        }));
        setStatus({ loading: false, error: "" });
      })
      .catch((error) => {
        if (!active || requestId !== fullLoadRequestIdRef.current) return;
        setStatus({ loading: false, error: error.message });
      });
    return () => {
      active = false;
    };
  }, [selectedDate, refreshTick]);

  const layerData = useMemo(
    () => ({
      heatmap: data.heatmap,
      heatmapsByHorizon: data.heatmapsByHorizon,
      trajectories: data.trajectories,
      plume: data.plume?.available === false ? { type: "FeatureCollection", features: [] } : data.plume,
      sources: data.sources,
      stations: data.stations,
    }),
    [data],
  );

  const handleMapStats = useCallback((nextStats) => {
    setMapStats((current) =>
      current.avg === nextStats.avg &&
      current.max === nextStats.max &&
      current.count === nextStats.count &&
      current.source === nextStats.source
        ? current
        : nextStats,
    );
  }, []);

  function toggleLayer(key) {
    setEnabled((current) => ({ ...current, [key]: !current[key] }));
  }

  function changeHorizon(nextHorizon) {
    horizonRef.current = nextHorizon;
    const requestId = ++horizonRequestIdRef.current;
    const cached = nextHorizon === 0 ? null : data.heatmapsByHorizon?.[nextHorizon];
    setStatus({ loading: !cached, error: "" });
    setHorizon(nextHorizon);
    setData((current) => ({
      ...current,
      heatmap: current.heatmapsByHorizon?.[nextHorizon] || current.heatmap,
    }));
    Promise.all([
      nextHorizon === 0
        ? getLiveHeatmapLatest("hanoi", selectedDate).catch(() => getHeatmapLatest(0, selectedDate))
        : cached ? Promise.resolve(cached) : getHeatmapLatest(nextHorizon, selectedDate),
      getForwardPlumeLatest(nextHorizon === 0 ? 6 : nextHorizon, selectedDate),
    ])
      .then(([heatmap, plume]) => {
        if (requestId !== horizonRequestIdRef.current || horizonRef.current !== nextHorizon) return;
        setData((current) => ({
          ...current,
          heatmap,
          liveHeatmap: nextHorizon === 0 ? heatmap : current.liveHeatmap,
          plume,
          heatmapsByHorizon: {
            ...(current.heatmapsByHorizon || {}),
            [nextHorizon]: heatmap,
          },
        }));
        setStatus({ loading: false, error: "" });
      })
      .catch((error) => {
        if (requestId !== horizonRequestIdRef.current || horizonRef.current !== nextHorizon) return;
        setStatus({ loading: false, error: error.message });
      });
  }

  function changeDate(nextDate) {
    horizonRequestIdRef.current += 1;
    setStatus({ loading: true, error: "" });
    setData((current) => ({
      ...current,
      heatmap: null,
      liveHeatmap: null,
      heatmapsByHorizon: {},
      plume: null,
    }));
    setSelectedDate(nextDate);
  }

  return (
    <div className="air-map-page windy-shell">
      <MapCanvas layers={layerData} enabled={enabled} onSelect={setSelected} onStats={handleMapStats} />

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
        <div className="mini-stat active"><span>Map avg</span><strong>{mapStats.avg}</strong><em>µg/m³</em></div>
        <div className="mini-stat"><span>Map max</span><strong>{mapStats.max}</strong><em>µg/m³</em></div>
        <div className="mini-stat"><span>Grid</span><strong>{mapStats.count}</strong><em>cells</em></div>
      </aside>

      <aside className="windy-right-panel">
        <LayerControl enabled={enabled} onToggle={toggleLayer} />
        <ForecastPanel forecast={data.forecast} />
        <SourceAttributionPanel sourceAttribution={data.sources} plume={data.plume} />
        <PM25ForecastChart timeseries={data.timeseries} />
      </aside>

      <div className="windy-legend" aria-label="PM2.5 color legend">
        <span>PM2.5 local scale</span>
        <div className="legend-ramp" />
        <div className="legend-ticks"><b>15</b><b>25</b><b>35</b><b>55</b><b>80+</b></div>
      </div>

      <div className="windy-bottom-timeline">
        <div className="timeline-caption">
          <strong>{horizon === 0 ? "Latest observed/nowcast" : `Forecast +${horizon}h`}</strong>
          <span>{selectedDate || data.manifest?.selected_date || "latest cache"}</span>
        </div>
        <TimeSelector horizon={horizon} onChange={changeHorizon} />
      </div>

      {status.loading && <div className="status-toast windy-status">Loading Hanoi live map</div>}
      {status.error && <div className="status-toast error windy-status">{status.error}</div>}
      <MapPopup feature={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
