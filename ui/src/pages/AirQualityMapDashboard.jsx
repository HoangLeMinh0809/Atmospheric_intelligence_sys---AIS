// File nay: page React gom API calls, state va layout man hinh.
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import PM25ForecastChart from "../components/charts/PM25ForecastChart";
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
  getLiveHeatmapLatest,
  getPM25TimeseriesLatest,
  getSourceAttributionLatest,
  getStationsLatest,
} from "../services/visualizationApi";

// Doi PM2.5 thanh risk key dung chung cho forecast panel.
function riskForPm25(value) {
  if (value == null || Number.isNaN(Number(value))) return "unknown";
  const pm25 = Number(value);
  if (pm25 < 20) return "low";
  if (pm25 < 30) return "medium";
  if (pm25 < 40) return "medium";
  if (pm25 < 55) return "high";
  return "very_high";
}

// Rut gia tri hien tai tu heatmap live de chen vao forecast card.
function liveNowFromHeatmap(liveHeatmap) {
  const summary = liveHeatmap?.summary || {};
  const value = Number(summary.pm25_mean ?? summary.pm25_median);
  if (Number.isFinite(value)) return Number(value.toFixed(1));
  const values = (liveHeatmap?.features || [])
    .map((feature) => Number(feature?.properties?.pm25_value))
    .filter((item) => Number.isFinite(item));
  if (!values.length) return null;
  return Number((values.reduce((sum, item) => sum + item, 0) / values.length).toFixed(1));
}

// Ghep du lieu "bay gio" vao payload forecast de chart co diem bat dau tu realtime.
function mergeLiveNowForecast(forecast, liveHeatmap) {
  const liveNow = liveNowFromHeatmap(liveHeatmap);
  if (liveNow == null) return forecast;
  return {
    ...(forecast || {}),
    source: forecast?.source || "cassandra",
    base_hour: liveHeatmap?.base_hour || forecast?.base_hour,
    generated_at: liveHeatmap?.generated_at || forecast?.generated_at,
    forecast: {
      ...(forecast?.forecast || {}),
      now: { pm25: liveNow, risk: riskForPm25(liveNow), source: "live_cassandra_heatmap" },
    },
    freshness: {
      ...(forecast?.freshness || {}),
      source: "cassandra_live_heatmap",
      base_hour: liveHeatmap?.base_hour || forecast?.freshness?.base_hour,
      generated_at: liveHeatmap?.generated_at || forecast?.freshness?.generated_at,
    },
  };
}

const DEFAULT_RECEPTOR = { name: "Hoàn Kiếm", lon: 105.852, lat: 21.029 };
const TRAJECTORY_RECEPTORS = [
  { name: "Tây Hồ", lon: 105.817, lat: 21.068 },
  { name: "Cầu Giấy", lon: 105.79, lat: 21.036 },
  { name: "Ba Đình", lon: 105.828, lat: 21.035 },
  DEFAULT_RECEPTOR,
  { name: "Đống Đa", lon: 105.832, lat: 21.014 },
  { name: "Hai Bà Trưng", lon: 105.859, lat: 21.0 },
  { name: "Long Biên", lon: 105.886, lat: 21.038 },
  { name: "Thanh Xuân", lon: 105.805, lat: 20.996 },
];

// Chuyen ten receptor hien thi thanh location id ma backend co the query.
function locationIdFromName(name) {
  return String(name || "hanoi")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
}

// Tao request backward trajectory tu receptor dang duoc chon tren map.
function trajectoryRequest(receptor) {
  return getBackwardTrajectoriesLatest({
    locationId: locationIdFromName(receptor?.name),
    locationName: receptor?.name,
    lon: receptor?.lon,
    lat: receptor?.lat,
  });
}

// Tom tat payload trajectory de panel hien dung trang thai actual/proxy.
function trajectorySummary(payload) {
  const features = payload?.features || [];
  const proxyCount = features.filter((feature) => feature?.properties?.trajectory_kind === "proxy_ensemble").length;
  const hotCount = features.filter((feature) => Number(feature?.properties?.risk_score ?? feature?.properties?.pollution_score) >= 0.7).length;
  const matched = payload?.selected_location?.matched_cached_trajectory;
  return {
    count: features.length,
    hotCount,
    mode: matched || (features.length && proxyCount === 0) ? "HYSPLIT" : "Proxy",
  };
}

// Dieu phoi API realtime va render man hinh ban do chat luong khong khi.
export default function AirQualityMapDashboard() {
  const [horizon, setHorizon] = useState(Number(import.meta.env.VITE_DEFAULT_HORIZON_H || 0));
  const refreshMs = Number(import.meta.env.VITE_VIS_REFRESH_MS || 15000);
  const [refreshTick, setRefreshTick] = useState(0);
  const [enabled, setEnabled] = useState({
    heatmap: true,
    trajectories: true,
    plume: false,
    sources: true,
    stations: true,
  });
  const [selected, setSelected] = useState(null);
  const [selectedReceptor, setSelectedReceptor] = useState(DEFAULT_RECEPTOR);
  const [status, setStatus] = useState({ loading: true, error: "" });
  const [mapStats, setMapStats] = useState({ avg: "-", max: "-", count: 0, source: "" });
  const [data, setData] = useState({
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
    // Tai song song nhieu payload de giam do tre cho man hinh.
    Promise.all([
      getLiveHeatmapLatest("hanoi"),
      trajectoryRequest(selectedReceptor),
      getForwardPlumeLatest(plumeHorizon),
      getForecastLatest("hanoi"),
      getPM25TimeseriesLatest("hanoi"),
      getSourceAttributionLatest("hanoi"),
      getStationsLatest(),
    ])
      .then(([liveHeatmap, trajectories, plume, forecast, timeseries, sources, stations]) => {
        if (!active || requestId !== fullLoadRequestIdRef.current) return;
        setData((current) => ({
          heatmap: horizonRef.current === requestHorizon ? liveHeatmap : current.heatmap,
          liveHeatmap: liveHeatmap || current.liveHeatmap,
          heatmapsByHorizon: {
            ...(current.heatmapsByHorizon || {}),
            [requestHorizon]: liveHeatmap,
            0: liveHeatmap,
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
  }, [refreshTick, selectedReceptor]);

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
  const displayForecast = useMemo(
    () => mergeLiveNowForecast(data.forecast, data.liveHeatmap),
    [data.forecast, data.liveHeatmap],
  );
  const trajectoryStats = useMemo(() => trajectorySummary(data.trajectories), [data.trajectories]);

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

  // Bat/tat mot overlay layer tren ban do.
  function toggleLayer(key) {
    setEnabled((current) => ({ ...current, [key]: !current[key] }));
  }

  // Doi horizon forecast va load/reuse heatmap tuong ung.
  function changeHorizon(nextHorizon) {
    horizonRef.current = nextHorizon;
    const requestId = ++horizonRequestIdRef.current;
    const existingLiveHeatmap = data.heatmapsByHorizon?.[nextHorizon];
    setStatus({ loading: !existingLiveHeatmap, error: "" });
    setHorizon(nextHorizon);
    setData((current) => ({
      ...current,
      heatmap: current.heatmapsByHorizon?.[nextHorizon] || current.heatmap,
    }));
    // Tai song song nhieu payload de giam do tre cho man hinh.
    Promise.all([
      existingLiveHeatmap ? Promise.resolve(existingLiveHeatmap) : getLiveHeatmapLatest("hanoi"),
      getForwardPlumeLatest(nextHorizon === 0 ? 6 : nextHorizon),
    ])
      .then(([heatmap, plume]) => {
        if (requestId !== horizonRequestIdRef.current || horizonRef.current !== nextHorizon) return;
        setData((current) => ({
          ...current,
          heatmap,
          liveHeatmap: heatmap,
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

  // Chon receptor trajectory va reload duong backward tu backend.
  function chooseTrajectoryReceptor(receptor) {
    setSelectedReceptor(receptor);
    setSelected({
      type: "Feature",
      geometry: { type: "Point", coordinates: [receptor.lon, receptor.lat] },
      properties: {
        layer_name: "Backward receptor",
        location_name: receptor.name,
        description: `Backward trajectory target: ${receptor.name}`,
      },
    });
  }

  return (
    <div className="air-map-page windy-shell">
      <MapCanvas
        layers={layerData}
        enabled={enabled}
        selectedReceptor={selectedReceptor}
        onReceptorChange={setSelectedReceptor}
        onSelect={setSelected}
        onStats={handleMapStats}
      />

      <header className="windy-topbar">
        <div className="windy-brand">
          <div className="windy-logo">AIS</div>
          <div>
            <h1>Hà Nội Air Flow</h1>
            <span>PM2.5 heatmap · backward trajectories · station observations</span>
          </div>
        </div>
        <div className="windy-top-controls">
          <a className="dashboard-link" href="#/statistics">Thống kê</a>
          <TimeSelector horizon={horizon} onChange={changeHorizon} />
          <FreshnessBadge forecast={displayForecast} />
        </div>
      </header>

      <aside className="windy-left-rail" aria-label="Quick map statistics">
        <div className="mini-stat active"><span>Map avg</span><strong>{mapStats.avg}</strong><em>µg/m³</em></div>
        <div className="mini-stat"><span>Map max</span><strong>{mapStats.max}</strong><em>µg/m³</em></div>
        <div className="mini-stat"><span>Grid</span><strong>{mapStats.count}</strong><em>cells</em></div>
      </aside>

      <aside className="windy-right-panel">
        <LayerControl enabled={enabled} onToggle={toggleLayer} />
        <section className="map-panel trajectory-target-panel">
          <div className="panel-heading">
            <h2>Backward trajectory</h2>
            <span>{selectedReceptor.name}</span>
          </div>
          <div className="trajectory-target-grid">
            {TRAJECTORY_RECEPTORS.map((receptor) => (
              <button
                type="button"
                key={receptor.name}
                className={selectedReceptor.name === receptor.name ? "active" : ""}
                onClick={() => chooseTrajectoryReceptor(receptor)}
              >
                {receptor.name}
              </button>
            ))}
          </div>
          <div className="trajectory-summary-row">
            <span>{trajectoryStats.mode}</span>
            <strong>{trajectoryStats.count} paths</strong>
            <em>{trajectoryStats.hotCount} red</em>
          </div>
        </section>
        <ForecastPanel forecast={displayForecast} />
        <SourceAttributionPanel sourceAttribution={data.sources} plume={data.plume} />
        <PM25ForecastChart timeseries={data.timeseries} />
      </aside>

      <div className="windy-legend" aria-label="PM2.5 color legend">
        <span>PM2.5 scale</span>
        <div className="legend-ramp" />
        <div className="legend-ticks"><b>0</b><b>30</b><b>60</b><b>80+</b></div>
      </div>

      <div className="windy-bottom-timeline">
        <div className="timeline-caption">
          <strong>{horizon === 0 ? "Latest observed/nowcast" : `Forecast +${horizon}h`}</strong>
          <span>{data.liveHeatmap?.base_hour || data.liveHeatmap?.generated_at || "live serving"}</span>
        </div>
        <TimeSelector horizon={horizon} onChange={changeHorizon} />
      </div>

      {status.loading && <div className="status-toast windy-status">Loading Hanoi live map</div>}
      {status.error && <div className="status-toast error windy-status">{status.error}</div>}
      <MapPopup feature={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
