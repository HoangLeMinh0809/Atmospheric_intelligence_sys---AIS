const API_BASE = import.meta.env.VITE_VIS_API_BASE || "/api/v1/visualization";
const USE_MOCK_DATA = String(import.meta.env.VITE_USE_MOCK_DATA || "false").toLowerCase() === "true";
const INNER_HANOI = {
  west: 105.75,
  east: 105.95,
  south: 20.95,
  north: 21.1,
};

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
  const centerPulse = 31 * Math.exp(-(((lon - 105.842) ** 2) / 0.0011 + ((lat - 21.026) ** 2) / 0.00075));
  const westRoad = 27 * Math.exp(-(((lon - 105.793) ** 2) / 0.0009 + ((lat - 21.035) ** 2) / 0.0008));
  const southWest = 22 * Math.exp(-(((lon - 105.771) ** 2) / 0.001 + ((lat - 20.973) ** 2) / 0.00085));
  const riverEdge = 17 * Math.exp(-(((lon - 105.897) ** 2) / 0.0012 + ((lat - 21.045) ** 2) / 0.00095));
  const horizonDrift = Number(horizonH || 0) * 0.48;
  const wave = 4.8 * Math.sin((lon - 105.73) * 98) + 3.5 * Math.cos((lat - 20.91) * 115);
  return Math.max(15, Math.min(92, 24 + centerPulse + westRoad + southWest + riverEdge + horizonDrift + wave));
}

function riskOf(pm25) {
  if (pm25 <= 12) return "good";
  if (pm25 <= 35) return "moderate";
  if (pm25 <= 55) return "sensitive";
  if (pm25 <= 75) return "high";
  return "very_high";
}

function makeMockHeatmap(horizonH = 0) {
  const { west, east, south, north } = INNER_HANOI;
  const cols = 80;
  const rows = 60;
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

function interpolateRoute(points, steps = 52) {
  const coords = [];
  const segments = points.length - 1;
  for (let index = 0; index < steps; index += 1) {
    const t = index / (steps - 1);
    const scaled = Math.min(segments - 0.0001, t * segments);
    const segment = Math.floor(scaled);
    const local = scaled - segment;
    const [x1, y1] = points[segment];
    const [x2, y2] = points[segment + 1];
    const wave = Math.sin(t * Math.PI * 3) * 0.0025;
    coords.push([
      Number((x1 + (x2 - x1) * local + wave).toFixed(5)),
      Number((y1 + (y2 - y1) * local + Math.cos(t * Math.PI * 2) * 0.0018).toFixed(5)),
    ]);
  }
  return coords;
}

function makeMockBackwardTrajectories() {
  const baseTime = "2026-05-30T23:00:00Z";
  const ensemble = [
    { id: "bt-ens-01", color: "#e0f2fe", coords: [[105.758, 21.086], [105.785, 21.073], [105.813, 21.054], [105.837, 21.036], [105.8542, 21.0285]] },
    { id: "bt-ens-02", color: "#bae6fd", coords: [[105.754, 21.074], [105.781, 21.064], [105.807, 21.05], [105.834, 21.035], [105.8542, 21.0285]] },
    { id: "bt-ens-03", color: "#7dd3fc", coords: [[105.762, 21.061], [105.79, 21.056], [105.817, 21.047], [105.839, 21.034], [105.8542, 21.0285]] },
    { id: "bt-ens-04", color: "#67e8f9", coords: [[105.772, 21.096], [105.796, 21.078], [105.819, 21.057], [105.84, 21.037], [105.8542, 21.0285]] },
  ];
  return {
    type: "FeatureCollection",
    features: ensemble.map((item, index) => ({
      type: "Feature",
      geometry: { type: "LineString", coordinates: interpolateRoute(item.coords, 62 - index * 4) },
      properties: {
        traj_id: item.id,
        base_time: baseTime,
        hours_back: 24,
        ensemble_member: index + 1,
        style_color: item.color,
        endpoint: "Hoàn Kiếm receptor",
        wind_hint: "Backward ensemble for one base time and receptor",
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
              [105.9, 21.095],
              [105.945, 21.045],
              [105.905, 20.972],
              [105.805, 20.965],
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
      { type: "Feature", geometry: { type: "Point", coordinates: [105.792, 21.036] }, properties: { source_label: "Cầu Giấy traffic/upwind cluster", contribution_score: 0.78, confidence: 0.71, explanation_vi: "Cụm nguồn phía tây trùng với hướng gió đi về lõi đô thị Hà Nội." } },
      { type: "Feature", geometry: { type: "Point", coordinates: [105.898, 21.047] }, properties: { source_label: "Long Biên river corridor", contribution_score: 0.43, confidence: 0.58, explanation_vi: "Tín hiệu vệ tinh và quỹ đạo gió cho thấy đóng góp vừa phải từ hành lang phía đông." } },
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
    ["Ba Đình", 105.8342, 21.0378, 68],
    ["Đống Đa", 105.8316, 21.0173, 61],
    ["Hai Bà Trưng", 105.8606, 21.0065, 54],
    ["Thanh Xuân", 105.8056, 20.9952, 75],
    ["Gia Lâm edge", 105.934, 21.026, 42],
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

export function getLiveHeatmapLatest(locationId = "hanoi", date) {
  return USE_MOCK_DATA
    ? Promise.resolve(makeMockHeatmap(0))
    : requestJson(withDate(`/live/pm25/heatmap/latest?location_id=${encodeURIComponent(locationId)}`, date));
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
