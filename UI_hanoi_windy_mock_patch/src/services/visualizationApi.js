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

function polygonCell(west, south, east, north) {
  return [
    [west, south],
    [east, south],
    [east, north],
    [west, north],
    [west, south],
  ];
}

function pm25At(lon, lat, horizonH = 0) {
  const centerPulse = 62 * Math.exp(-(((lon - 105.84) ** 2) / 0.018 + ((lat - 21.02) ** 2) / 0.014));
  const westSource = 44 * Math.exp(-(((lon - 105.56) ** 2) / 0.028 + ((lat - 21.08) ** 2) / 0.02));
  const northSource = 25 * Math.exp(-(((lon - 105.92) ** 2) / 0.02 + ((lat - 21.24) ** 2) / 0.018));
  const horizonDrift = Number(horizonH || 0) * 0.72;
  const wave = 5 * Math.sin((lon - 105.25) * 16) + 4 * Math.cos((lat - 20.55) * 12);
  return Math.max(8, Math.min(145, 19 + centerPulse + westSource + northSource + horizonDrift + wave));
}

function riskOf(pm25) {
  if (pm25 <= 12) return "good";
  if (pm25 <= 35) return "moderate";
  if (pm25 <= 55) return "sensitive";
  if (pm25 <= 75) return "high";
  return "very_high";
}

function makeMockHeatmap(horizonH = 0) {
  const west = 105.34;
  const east = 106.04;
  const south = 20.68;
  const north = 21.32;
  const cols = 11;
  const rows = 10;
  const features = [];
  for (let row = 0; row < rows; row += 1) {
    for (let col = 0; col < cols; col += 1) {
      const x1 = west + ((east - west) / cols) * col;
      const x2 = west + ((east - west) / cols) * (col + 1);
      const y1 = south + ((north - south) / rows) * row;
      const y2 = south + ((north - south) / rows) * (row + 1);
      const lon = (x1 + x2) / 2;
      const lat = (y1 + y2) / 2;
      const pm25 = pm25At(lon, lat, horizonH);
      features.push({
        type: "Feature",
        geometry: { type: "Polygon", coordinates: [polygonCell(x1, y1, x2, y2)] },
        properties: {
          grid_id: `mock-${row}-${col}`,
          pm25_value: Number(pm25.toFixed(1)),
          horizon_h: horizonH,
          risk: riskOf(pm25),
        },
      });
    }
  }
  return { type: "FeatureCollection", features };
}

function makeMockBackwardTrajectories() {
  const endpoints = [
    { id: "NW-01", color: "#7dd3fc", coords: [[104.92, 21.38], [105.1, 21.31], [105.34, 21.22], [105.58, 21.12], [105.84, 21.03]] },
    { id: "W-02", color: "#a7f3d0", coords: [[105.04, 20.92], [105.22, 20.96], [105.44, 21.0], [105.64, 21.03], [105.84, 21.03]] },
    { id: "NE-03", color: "#fde68a", coords: [[106.12, 21.33], [106.02, 21.24], [105.95, 21.15], [105.9, 21.08], [105.84, 21.03]] },
    { id: "S-04", color: "#fca5a5", coords: [[105.76, 20.56], [105.78, 20.72], [105.81, 20.86], [105.84, 20.96], [105.84, 21.03]] },
    { id: "SW-05", color: "#c4b5fd", coords: [[105.31, 20.72], [105.46, 20.82], [105.6, 20.9], [105.72, 20.98], [105.84, 21.03]] },
  ];
  return {
    type: "FeatureCollection",
    features: endpoints.map((item, index) => ({
      type: "Feature",
      geometry: { type: "LineString", coordinates: item.coords },
      properties: {
        traj_id: item.id,
        hours_back: 24 + index * 6,
        style_color: item.color,
        endpoint: "Hanoi urban core",
        wind_hint: "Backward trajectory drawn as wind stream",
      },
    })),
  };
}

function makeMockPlume(horizonH = 6) {
  return {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        geometry: {
          type: "Polygon",
          coordinates: [
            [
              [105.76, 21.08],
              [105.9, 21.12],
              [106.03, 21.04],
              [105.98, 20.94],
              [105.82, 20.96],
              [105.76, 21.08],
            ],
          ],
        },
        properties: { layer_name: "Forward plume", horizon_h: horizonH, confidence: 0.62 },
      },
    ],
  };
}

function makeMockSources() {
  return {
    type: "FeatureCollection",
    features: [
      { type: "Feature", geometry: { type: "Point", coordinates: [105.57, 21.08] }, properties: { source_label: "Western industrial/upwind cluster", contribution_score: 0.78, confidence: 0.71, explanation_vi: "Cụm nguồn phía tây trùng với hướng gió đi về lõi đô thị Hà Nội." } },
      { type: "Feature", geometry: { type: "Point", coordinates: [105.94, 21.21] }, properties: { source_label: "Northern transport corridor", contribution_score: 0.43, confidence: 0.58, explanation_vi: "Tín hiệu vệ tinh và quỹ đạo gió cho thấy đóng góp vừa phải từ phía bắc." } },
    ],
  };
}

function makeMockStations() {
  const stations = [
    ["Hoàn Kiếm", 105.8542, 21.0285, 64],
    ["Cầu Giấy", 105.7903, 21.0362, 79],
    ["Tây Hồ", 105.8177, 21.0688, 57],
    ["Long Biên", 105.8998, 21.0397, 48],
    ["Hà Đông", 105.7625, 20.9714, 72],
    ["Sóc Sơn", 105.8492, 21.2578, 41],
  ];
  return {
    type: "FeatureCollection",
    features: stations.map(([name, lon, lat, pm25]) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [lon, lat] },
      properties: { station_name: name, pm25_value: pm25, risk: riskOf(pm25), coverage_pct: 100 },
    })),
  };
}

function makeMockForecast() {
  return {
    location_id: "hanoi",
    location_name: "Hà Nội",
    base_hour: "2026-05-30T23:00:00Z",
    generated_at: new Date().toISOString(),
    freshness: { prediction_freshness_minutes: 14, observation_freshness_minutes: 22 },
    forecast: {
      now: { pm25: 64.2, risk: "high" },
      "6h": { pm25: 70.5, risk: "high" },
      "12h": { pm25: 58.6, risk: "sensitive" },
      "24h": { pm25: 44.8, risk: "moderate" },
    },
    model: { model_version: "mock-hanoi-v1", feature_version: "hanoi_pm25_core_v1" },
  };
}

function makeMockTimeseries() {
  const base = Date.parse("2026-05-30T00:00:00Z");
  return {
    points: Array.from({ length: 25 }).map((_, index) => {
      const pm25 = 46 + 18 * Math.sin(index / 3) + (index > 17 ? 11 : 0);
      return {
        timestamp: new Date(base + index * 60 * 60 * 1000).toISOString(),
        pm25: Number(pm25.toFixed(1)),
        is_forecast: index > 18,
      };
    }),
  };
}

export function getManifestLatest(date) {
  return USE_MOCK_DATA
    ? Promise.resolve({
        layers: ["heatmap", "backward_trajectories", "forward_plume", "source_attribution", "stations", "forecast", "timeseries"],
        generated_at: new Date().toISOString(),
        selected_date: date || "2026-05-30",
        available_dates: ["2026-05-23", "2026-05-24", "2026-05-25", "2026-05-26", "2026-05-27", "2026-05-28", "2026-05-29", "2026-05-30"],
      })
    : requestJson(withDate("/manifest/latest", date));
}

export function getHeatmapLatest(horizonH, date) {
  return USE_MOCK_DATA ? Promise.resolve(makeMockHeatmap(horizonH)) : requestJson(withDate(`/pm25/heatmap/latest?horizon_h=${horizonH}`, date));
}

export function getBackwardTrajectoriesLatest(date) {
  return USE_MOCK_DATA ? Promise.resolve(makeMockBackwardTrajectories()) : requestJson(withDate("/trajectories/backward/latest", date));
}

export function getForwardPlumeLatest(horizonH, date) {
  return USE_MOCK_DATA ? Promise.resolve(makeMockPlume(horizonH)) : requestJson(withDate(`/plume/forward/latest?horizon_h=${horizonH}`, date));
}

export function getForecastLatest(locationId = "hanoi", date) {
  return USE_MOCK_DATA ? Promise.resolve(makeMockForecast()) : requestJson(withDate(`/forecast/latest?location_id=${locationId}`, date));
}

export function getPM25TimeseriesLatest(locationId = "hanoi", date) {
  return USE_MOCK_DATA ? Promise.resolve(makeMockTimeseries()) : requestJson(withDate(`/timeseries/latest?location_id=${locationId}`, date));
}

export function getSourceAttributionLatest(locationId = "hanoi", date) {
  return USE_MOCK_DATA ? Promise.resolve(makeMockSources()) : requestJson(withDate(`/source-attribution/latest?location_id=${locationId}`, date));
}

export function getStationsLatest(date) {
  return USE_MOCK_DATA ? Promise.resolve(makeMockStations()) : requestJson(withDate("/stations/latest", date));
}
