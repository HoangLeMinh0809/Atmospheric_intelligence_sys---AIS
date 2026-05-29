const API_BASE = import.meta.env.VITE_VIS_API_BASE || "/api/v1/visualization";
const USE_MOCK_DATA = String(import.meta.env.VITE_USE_MOCK_DATA || "false").toLowerCase() === "true";

async function requestJson(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${text}`);
  }
  return response.json();
}

function withDate(path, date) {
  if (!date) return path;
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}date=${encodeURIComponent(date)}`;
}

const mockFeatureCollection = { type: "FeatureCollection", features: [] };

const mockDashboard = {
  location_id: "hanoi",
  location_name: "Hanoi",
  base_hour: new Date().toISOString(),
  generated_at: new Date().toISOString(),
  freshness: { prediction_freshness_minutes: 0, observation_freshness_minutes: 0 },
  forecast: {
    now: { pm25: 42, risk: "medium" },
    "6h": { pm25: 55, risk: "medium" },
    "12h": { pm25: 78, risk: "high" },
    "24h": { pm25: 64, risk: "medium" },
  },
  model: { model_version: "mock", feature_version: "mock" },
};

export function getManifestLatest(date) {
  return USE_MOCK_DATA ? Promise.resolve({ layers: [], generated_at: new Date().toISOString() }) : requestJson(withDate("/manifest/latest", date));
}

export function getHeatmapLatest(horizonH, date) {
  return USE_MOCK_DATA ? Promise.resolve(mockFeatureCollection) : requestJson(withDate(`/pm25/heatmap/latest?horizon_h=${horizonH}`, date));
}

export function getBackwardTrajectoriesLatest(date) {
  return USE_MOCK_DATA ? Promise.resolve(mockFeatureCollection) : requestJson(withDate("/trajectories/backward/latest", date));
}

export function getForwardPlumeLatest(horizonH, date) {
  return USE_MOCK_DATA
    ? Promise.resolve({ available: false, reason: "mock_disabled_for_plume", horizon_h: horizonH })
    : requestJson(withDate(`/plume/forward/latest?horizon_h=${horizonH}`, date));
}

export function getForecastLatest(locationId = "hanoi", date) {
  return USE_MOCK_DATA ? Promise.resolve(mockDashboard) : requestJson(withDate(`/forecast/latest?location_id=${locationId}`, date));
}

export function getPM25TimeseriesLatest(locationId = "hanoi", date) {
  return USE_MOCK_DATA ? Promise.resolve({ points: [] }) : requestJson(withDate(`/timeseries/latest?location_id=${locationId}`, date));
}

export function getSourceAttributionLatest(locationId = "hanoi", date) {
  return USE_MOCK_DATA ? Promise.resolve(mockFeatureCollection) : requestJson(withDate(`/source-attribution/latest?location_id=${locationId}`, date));
}

export function getStationsLatest(date) {
  return USE_MOCK_DATA ? Promise.resolve(mockFeatureCollection) : requestJson(withDate("/stations/latest", date));
}
