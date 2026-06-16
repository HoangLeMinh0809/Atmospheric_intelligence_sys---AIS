const API_BASE = import.meta.env.VITE_VIS_API_BASE || "/api/v1/visualization";

async function requestJson(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Visualization API ${response.status}: ${text}`);
  }
  return response.json();
}

function withDate(path, date) {
  if (!date) return path;
  return `${path}${path.includes("?") ? "&" : "?"}date=${encodeURIComponent(date)}`;
}

function withParams(path, params = {}) {
  const pairs = Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== "");
  if (!pairs.length) return path;
  const search = new URLSearchParams();
  for (const [key, value] of pairs) {
    search.set(key, value);
  }
  return `${path}${path.includes("?") ? "&" : "?"}${search.toString()}`;
}

function withCacheBust(path) {
  return `${path}${path.includes("?") ? "&" : "?"}_=${Date.now()}`;
}

export function getManifestLatest(date) {
  return requestJson(withDate("/manifest/latest", date));
}

export function getHeatmapLatest(horizonH, date) {
  return requestJson(withDate(`/pm25/heatmap/latest?horizon_h=${horizonH}`, date));
}

export function getLiveHeatmapLatest(locationId = "hanoi", date) {
  return requestJson(
    withCacheBust(withDate(`/live/pm25/heatmap/latest?location_id=${encodeURIComponent(locationId)}`, date)),
  );
}

export function getBackwardTrajectoriesLatest(options = {}) {
  const date = typeof options === "string" ? options : options.date;
  const params = typeof options === "string" ? {} : {
    location_id: options.locationId,
    location_name: options.locationName,
    lon: options.lon,
    lat: options.lat,
  };
  return requestJson(withParams(withDate("/trajectories/backward/latest", date), params));
}

export function getForwardPlumeLatest(horizonH, date) {
  return requestJson(withDate(`/plume/forward/latest?horizon_h=${horizonH}`, date));
}

export function getForecastLatest(locationId = "hanoi", date) {
  return requestJson(withDate(`/forecast/latest?location_id=${encodeURIComponent(locationId)}`, date));
}

export function getPM25TimeseriesLatest(locationId = "hanoi", date) {
  return requestJson(withDate(`/timeseries/latest?location_id=${encodeURIComponent(locationId)}`, date));
}

export function getSourceAttributionLatest(locationId = "hanoi", date) {
  return requestJson(withDate(`/source-attribution/latest?location_id=${encodeURIComponent(locationId)}`, date));
}

export function getStationsLatest(date) {
  return requestJson(withDate("/stations/latest", date));
}
