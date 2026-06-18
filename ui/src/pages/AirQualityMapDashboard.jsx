// File nay: page React gom API calls, state va layout man hinh.
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import PM25ForecastChart from "../components/charts/PM25ForecastChart";
import ForecastPanel from "../components/map/ForecastPanel";
import FreshnessBadge from "../components/map/FreshnessBadge";
import LayerControl from "../components/map/LayerControl";
import MapCanvas from "../components/map/MapCanvas";
import MapPopup from "../components/map/MapPopup";
import TimeSelector from "../components/map/TimeSelector";
import {
  getBackwardTrajectoriesLatest,
  getForecastLatest,
  getForwardPlumeLatest,
  getHeatmapLatest,
  getLiveHeatmapLatest,
  getPM25TimeseriesLatest,
  getSourceAttributionLatest,
  getStationsLatest,
} from "../services/visualizationApi";

const FORECAST_HORIZONS = [6, 12, 24];

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

// Lay gia tri PM2.5 tu feature heatmap/station theo cac schema payload dang co.
function pm25FromFeature(feature) {
  const props = feature?.properties || feature || {};
  const value = [props.pm25_value, props.pm25, props.value, props.forecast_pm25, props.pm25_mean, props.pm25_ugm3]
    .find((item) => item !== undefined && item !== null && item !== "");
  return value == null ? null : Number(value);
}

// Lay diem dai dien cua feature heatmap de noi receptor voi cell gan nhat.
function lonLatFromFeature(feature) {
  const props = feature?.properties || {};
  if (Number.isFinite(Number(props.lon)) && Number.isFinite(Number(props.lat))) {
    return [Number(props.lon), Number(props.lat)];
  }
  if (Number.isFinite(Number(props.longitude)) && Number.isFinite(Number(props.latitude))) {
    return [Number(props.longitude), Number(props.latitude)];
  }
  if (Number.isFinite(Number(props.lon_min)) && Number.isFinite(Number(props.lon_max)) && Number.isFinite(Number(props.lat_min)) && Number.isFinite(Number(props.lat_max))) {
    return [(Number(props.lon_min) + Number(props.lon_max)) / 2, (Number(props.lat_min) + Number(props.lat_max)) / 2];
  }
  const geometry = feature?.geometry;
  if (!geometry) return null;
  if (geometry.type === "Point") return [Number(geometry.coordinates[0]), Number(geometry.coordinates[1])];
  const ring = geometry.coordinates?.[0] || [];
  const points = ring.filter(([lon, lat]) => Number.isFinite(Number(lon)) && Number.isFinite(Number(lat)));
  if (!points.length) return null;
  const lon = points.reduce((sum, point) => sum + Number(point[0]), 0) / points.length;
  const lat = points.reduce((sum, point) => sum + Number(point[1]), 0) / points.length;
  return [lon, lat];
}

// Tim PM2.5 gan receptor nhat trong heatmap cua tung horizon.
function nearestPm25At(heatmap, receptor) {
  if (!Number.isFinite(Number(receptor?.lon)) || !Number.isFinite(Number(receptor?.lat))) return null;
  let bestValue = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const feature of heatmap?.features || []) {
    const value = pm25FromFeature(feature);
    const lonLat = lonLatFromFeature(feature);
    if (!lonLat || !Number.isFinite(value)) continue;
    const dx = (lonLat[0] - Number(receptor.lon)) * Math.cos((Number(receptor.lat) * Math.PI) / 180);
    const dy = lonLat[1] - Number(receptor.lat);
    const distance = Math.hypot(dx, dy);
    if (distance < bestDistance) {
      bestDistance = distance;
      bestValue = value;
    }
  }
  return bestValue == null ? null : Number(bestValue.toFixed(1));
}

// Ghep forecast tai receptor dang chon tu cac heatmap horizon, tranh hien trung binh toan thanh pho.
function mergePointForecast(forecast, heatmapsByHorizon, receptor) {
  const pointForecast = { ...(forecast?.forecast || {}) };
  const heatmapValues = Object.fromEntries(
    [[0, "now"], [6, "6h"], [12, "12h"], [24, "24h"]].map(([horizonKey, cardKey]) => [
      cardKey,
      nearestPm25At(heatmapsByHorizon?.[horizonKey], receptor),
    ]),
  );
  const forecastHeatmapValues = ["6h", "12h", "24h"]
    .map((key) => heatmapValues[key])
    .filter((value) => value != null && Number.isFinite(value));
  const heatmapHasHorizonSignal =
    forecastHeatmapValues.length >= 2 && Math.max(...forecastHeatmapValues) - Math.min(...forecastHeatmapValues) > 0.2;
  const localNow = heatmapValues.now;
  const cityNow = Number(forecast?.forecast?.now?.pm25 ?? forecast?.pm25_now);

  for (const [horizonKey, cardKey] of [[0, "now"], [6, "6h"], [12, "12h"], [24, "24h"]]) {
    let value = heatmapValues[cardKey];
    const cityForecastValue = Number(forecast?.forecast?.[cardKey]?.pm25);
    if (
      horizonKey > 0 &&
      !heatmapHasHorizonSignal &&
      localNow != null &&
      Number.isFinite(cityNow) &&
      Number.isFinite(cityForecastValue)
    ) {
      value = Number(Math.max(1, localNow + (cityForecastValue - cityNow)).toFixed(1));
    }
    if (value == null) continue;
    pointForecast[cardKey] = {
      ...(pointForecast[cardKey] || {}),
      pm25: value,
      risk: riskForPm25(value),
      source: horizonKey > 0 && !heatmapHasHorizonSignal ? "local_now_plus_model_delta" : `heatmap_horizon_${horizonKey}`,
    };
  }
  return {
    ...(forecast || {}),
    location: locationIdFromName(receptor?.name),
    location_name: receptor?.name,
    base_hour: heatmapsByHorizon?.[0]?.base_hour || forecast?.base_hour,
    generated_at: heatmapsByHorizon?.[0]?.generated_at || forecast?.generated_at,
    forecast: pointForecast,
    freshness: {
      ...(forecast?.freshness || {}),
      source: "selected_receptor_heatmap",
      base_hour: heatmapsByHorizon?.[0]?.base_hour || forecast?.freshness?.base_hour,
      generated_at: heatmapsByHorizon?.[0]?.generated_at || forecast?.freshness?.generated_at,
    },
  };
}

function latestHeatmapRequest(horizonH) {
  return horizonH === 0 ? getLiveHeatmapLatest("hanoi") : getHeatmapLatest(horizonH);
}

function requestOrNull(request) {
  return request.catch(() => null);
}

function requestLocationPayload(factory, locationId) {
  if (locationId === "hanoi") return factory("hanoi");
  return factory(locationId).catch(() => factory("hanoi"));
}

function timeseriesValue(point) {
  const value = point?.pm25_value ?? point?.pm25 ?? point?.value ?? point?.pm25_mean;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

// Backend hien chi co history cap city; dich baseline do theo PM2.5 now tai receptor dang chon.
function timeseriesForReceptor(timeseries, forecast, locationName) {
  const points = timeseries?.points || [];
  const localNow = Number(forecast?.forecast?.now?.pm25);
  const latestPointValue = [...points].reverse().map(timeseriesValue).find((value) => value != null);
  if (!Number.isFinite(localNow) || latestPointValue == null) {
    return { ...(timeseries || {}), location_name: locationName };
  }
  const delta = localNow - latestPointValue;
  return {
    ...(timeseries || {}),
    location_name: locationName,
    derived_from_location_id: timeseries?.location_id || "hanoi",
    receptor_adjustment_delta: Number(delta.toFixed(3)),
    points: points.map((point) => {
      const value = timeseriesValue(point);
      if (value == null) return point;
      const adjusted = Math.max(1, value + delta);
      return {
        ...point,
        pm25_value: Number(adjusted.toFixed(1)),
        pm25: Number(adjusted.toFixed(1)),
        derived_for_receptor: locationName,
      };
    }),
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
  const selectedLocationId = useMemo(() => locationIdFromName(selectedReceptor.name), [selectedReceptor.name]);

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
      ...FORECAST_HORIZONS.map((item) => requestOrNull(getHeatmapLatest(item))),
      trajectoryRequest(selectedReceptor),
      getForwardPlumeLatest(plumeHorizon),
      requestLocationPayload(getForecastLatest, selectedLocationId),
      requestLocationPayload(getPM25TimeseriesLatest, selectedLocationId),
      requestLocationPayload(getSourceAttributionLatest, selectedLocationId),
      getStationsLatest(),
    ])
      .then(([liveHeatmap, heatmap6h, heatmap12h, heatmap24h, trajectories, plume, forecast, timeseries, sources, stations]) => {
        if (!active || requestId !== fullLoadRequestIdRef.current) return;
        const fetchedHeatmapsByHorizon = [
          [0, liveHeatmap],
          [6, heatmap6h],
          [12, heatmap12h],
          [24, heatmap24h],
        ].reduce((result, [key, value]) => (value ? { ...result, [key]: value } : result), {});
        setData((current) => ({
          heatmap: horizonRef.current === requestHorizon ? fetchedHeatmapsByHorizon[requestHorizon] || liveHeatmap || current.heatmap : current.heatmap,
          liveHeatmap: liveHeatmap || current.liveHeatmap,
          heatmapsByHorizon: {
            ...(current.heatmapsByHorizon || {}),
            ...fetchedHeatmapsByHorizon,
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
  }, [refreshTick, selectedLocationId, selectedReceptor]);

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
    () => mergePointForecast(mergeLiveNowForecast(data.forecast, data.liveHeatmap), data.heatmapsByHorizon, selectedReceptor),
    [data.forecast, data.heatmapsByHorizon, data.liveHeatmap, selectedReceptor],
  );
  const displayTimeseries = useMemo(
    () => timeseriesForReceptor(data.timeseries, displayForecast, selectedReceptor.name),
    [data.timeseries, displayForecast, selectedReceptor.name],
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
    setStatus({ loading: true, error: "" });
    setHorizon(nextHorizon);
    setData((current) => ({
      ...current,
      heatmap: current.heatmapsByHorizon?.[nextHorizon] || current.heatmap,
    }));
    // Tai song song nhieu payload de giam do tre cho man hinh.
    Promise.all([
      latestHeatmapRequest(nextHorizon),
      getForwardPlumeLatest(nextHorizon === 0 ? 6 : nextHorizon),
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
        <ForecastPanel forecast={displayForecast} locationName={selectedReceptor.name} />
        <PM25ForecastChart timeseries={displayTimeseries} locationName={selectedReceptor.name} />
      </aside>

      <div className="windy-legend" aria-label="PM2.5 color legend">
        <span>PM2.5 scale</span>
        <div className="legend-ramp" />
        <div className="legend-ticks"><b>0</b><b>30</b><b>60</b><b>80+</b></div>
      </div>

      {status.loading && <div className="status-toast windy-status">Loading Hanoi live map</div>}
      {status.error && <div className="status-toast error windy-status">{status.error}</div>}
      <MapPopup feature={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
