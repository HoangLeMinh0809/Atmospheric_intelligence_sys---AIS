import { useEffect, useMemo, useState } from "react";
import { geoCentroid } from "d3";
import hanoiBoundariesRaw from "../../assets/maps/hanoi_boundaries.geojson?raw";
import hanoiLabels from "../../assets/maps/hanoi_labels.json";
import hanoiRoadsRaw from "../../assets/maps/hanoi_roads.geojson?raw";
import hanoiWaterRaw from "../../assets/maps/hanoi_water.geojson?raw";

const INNER_BBOX = [105.738, 20.941, 105.96, 21.115];
const HANOI_CENTER = [105.8542, 21.0285];
const MAP_WIDTH = 1280;
const MAP_HEIGHT = 820;
const BASE_GRID_COLS = 42;
const BASE_GRID_ROWS = 32;
const MAX_IDW_SAMPLES = 420;
const MAX_BASEMAP_ROADS = 700;

function parseGeoJson(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { type: "FeatureCollection", features: [] };
  }
}

const hanoiBoundaries = parseGeoJson(hanoiBoundariesRaw);
const hanoiRoads = parseGeoJson(hanoiRoadsRaw);
const hanoiWater = parseGeoJson(hanoiWaterRaw);

const HANOI_BBOX = {
  type: "Feature",
  geometry: {
    type: "Polygon",
    coordinates: [
      [
        [INNER_BBOX[0], INNER_BBOX[1]],
        [INNER_BBOX[2], INNER_BBOX[1]],
        [INNER_BBOX[2], INNER_BBOX[3]],
        [INNER_BBOX[0], INNER_BBOX[3]],
        [INNER_BBOX[0], INNER_BBOX[1]],
      ],
    ],
  },
  properties: { name: "Hà Nội urban bbox" },
};

const DISTRICTS = hanoiLabels.map((item) => [item.name, item.lon, item.lat]);
const DEFAULT_RECEPTOR = hanoiLabels.find((item) => item.name === "Hoàn Kiếm") || { name: "Hoàn Kiếm", lon: 105.8542, lat: 21.0285 };

function project(lon, lat, width, height) {
  const [west, south, east, north] = INNER_BBOX;
  const pad = { left: 38, top: 30, right: 38, bottom: 36 };
  const usableWidth = width - pad.left - pad.right;
  const usableHeight = height - pad.top - pad.bottom;
  const x = pad.left + ((Number(lon) - west) / (east - west)) * usableWidth;
  const y = pad.top + ((north - Number(lat)) / (north - south)) * usableHeight;
  return [x, y];
}

function valueOf(feature) {
  const props = feature?.properties || feature || {};
  const candidates = [props.pm25_value, props.pm25, props.value, props.forecast_pm25, props.pm25_mean, props.pm25_ugm3];
  const value = candidates.find((item) => item !== undefined && item !== null && item !== "");
  return value == null ? null : Number(value);
}

function pointFromGeometry(geometry, width, height) {
  if (!geometry) return project(HANOI_CENTER[0], HANOI_CENTER[1], width, height);
  if (geometry.type === "Point") return project(geometry.coordinates[0], geometry.coordinates[1], width, height);
  const center = geoCentroid({ type: "Feature", geometry });
  return project(center[0], center[1], width, height);
}

function lonLatFromFeature(feature) {
  const props = feature?.properties || {};
  if (Number.isFinite(Number(props.lon)) && Number.isFinite(Number(props.lat))) {
    return [Number(props.lon), Number(props.lat)];
  }
  const geometry = feature?.geometry;
  if (!geometry) return null;
  if (geometry.type === "Point") return [Number(geometry.coordinates[0]), Number(geometry.coordinates[1])];
  const center = geoCentroid({ type: "Feature", geometry });
  return [Number(center[0]), Number(center[1])];
}

function featureDistanceTo(feature, lon, lat) {
  const lonLat = lonLatFromFeature(feature);
  if (!lonLat) return Number.POSITIVE_INFINITY;
  const dx = (lonLat[0] - lon) * Math.cos((lat * Math.PI) / 180);
  const dy = lonLat[1] - lat;
  return Math.hypot(dx, dy);
}

function nearestPm25(heatmap, lon, lat) {
  const features = heatmap?.features || [];
  let best = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const feature of features) {
    const value = valueOf(feature);
    if (!Number.isFinite(value)) continue;
    const distance = featureDistanceTo(feature, lon, lat);
    if (distance < bestDistance) {
      bestDistance = distance;
      best = value;
    }
  }
  return best;
}

function inUrbanBbox(lon, lat, pad = 0.025) {
  return lon >= INNER_BBOX[0] - pad && lon <= INNER_BBOX[2] + pad && lat >= INNER_BBOX[1] - pad && lat <= INNER_BBOX[3] + pad;
}

function samplesFromFeatures(features) {
  const samples = (features || [])
    .map((feature) => {
      const value = valueOf(feature);
      const lonLat = lonLatFromFeature(feature);
      if (!lonLat || !Number.isFinite(value)) return null;
      const [lon, lat] = lonLat;
      if (!inUrbanBbox(lon, lat)) return null;
      return { lon, lat, pm25: value, feature };
    })
    .filter(Boolean);
  if (samples.length <= MAX_IDW_SAMPLES) return samples;
  const stride = Math.ceil(samples.length / MAX_IDW_SAMPLES);
  return samples.filter((_, index) => index % stride === 0).slice(0, MAX_IDW_SAMPLES);
}

function idwPm25(lon, lat, samples) {
  if (!samples.length) return null;
  let weightSum = 0;
  let valueSum = 0;
  for (const sample of samples) {
    const dx = (lon - sample.lon) * Math.cos((lat * Math.PI) / 180);
    const dy = lat - sample.lat;
    const distance = Math.max(0.0012, Math.hypot(dx, dy));
    const weight = 1 / distance ** 2.15;
    weightSum += weight;
    valueSum += sample.pm25 * weight;
  }
  const interpolated = valueSum / weightSum;
  return Math.max(0, interpolated);
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, (sorted.length - 1) * p));
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  const frac = idx - lo;
  return sorted[lo] * (1 - frac) + sorted[hi] * frac;
}

function colorScale(values = []) {
  const finite = values.filter((value) => Number.isFinite(value));
  if (finite.length >= 6) {
    const min = Math.max(0, percentile(finite, 0.05));
    const p50 = percentile(finite, 0.5);
    const p80 = percentile(finite, 0.8);
    const max = Math.max(percentile(finite, 0.96), p80 + 4, p50 + 8);
    return { min, p50, p80, max };
  }
  return {
    min: 0,
    p50: 30,
    p80: 40,
    max: 55,
  };
}

function mix(a, b, t) {
  return Math.round(a + (b - a) * t);
}

function rgbaBetween(left, right, t, alpha) {
  return `rgba(${mix(left[0], right[0], t)},${mix(left[1], right[1], t)},${mix(left[2], right[2], t)},${alpha})`;
}

function pm25Color(value, scale, alpha = 0.9) {
  if (value == null || Number.isNaN(value)) return `rgba(148,163,184,${alpha * 0.25})`;
  const min = Number.isFinite(scale?.min) ? scale.min : 0;
  const p50 = Number.isFinite(scale?.p50) ? scale.p50 : 30;
  const p80 = Number.isFinite(scale?.p80) ? scale.p80 : 40;
  const max = Number.isFinite(scale?.max) ? scale.max : 55;
  const span = Math.max(8, max - min);
  const stops = [
    [min, [56, 189, 248]],
    [Math.min(p50, min + span * 0.35), [20, 184, 166]],
    [p50, [132, 204, 22]],
    [p80, [217, 119, 6]],
    [max, [190, 18, 60]],
    [max + span * 0.65, [136, 19, 55]],
  ];
  for (let i = 1; i < stops.length; i += 1) {
    if (value <= stops[i][0]) {
      const [v0, c0] = stops[i - 1];
      const [v1, c1] = stops[i];
      const t = Math.max(0, Math.min(1, (value - v0) / Math.max(0.001, v1 - v0)));
      return rgbaBetween(c0, c1, t, alpha);
    }
  }
  return `rgba(136,19,55,${alpha})`;
}

function riskLabel(value) {
  if (value == null || Number.isNaN(value)) return "Unknown";
  if (value < 20) return "Low";
  if (value < 30) return "Moderate";
  if (value < 40) return "Elevated";
  if (value < 55) return "High";
  return "Very high";
}

function buildPixelGrid(heatmapFeatures, stationFeatures, cols = BASE_GRID_COLS, rows = BASE_GRID_ROWS) {
  const apiSamples = samplesFromFeatures(heatmapFeatures);
  const stationSamples = samplesFromFeatures(stationFeatures);
  const samples = [...apiSamples, ...stationSamples];
  const cells = [];
  const values = [];
  const [west, south, east, north] = INNER_BBOX;
  for (let row = 0; row < rows; row += 1) {
    for (let col = 0; col < cols; col += 1) {
      const lon0 = west + ((east - west) * col) / cols;
      const lon1 = west + ((east - west) * (col + 1)) / cols;
      const lat0 = south + ((north - south) * row) / rows;
      const lat1 = south + ((north - south) * (row + 1)) / rows;
      const lon = (lon0 + lon1) / 2;
      const lat = (lat0 + lat1) / 2;
      const pm25 = idwPm25(lon, lat, samples);
      if (pm25 == null) continue;
      values.push(pm25);
      cells.push({ row, col, lon0, lon1, lat0, lat1, lon, lat, pm25 });
    }
  }
  const scale = colorScale(values);
  return {
    cells: cells.map((cell) => ({
      ...cell,
      normalizedValue: Math.max(0, Math.min(1, (cell.pm25 - scale.min) / Math.max(1, scale.max - scale.min))),
      color: pm25Color(cell.pm25, scale, 0.82),
    })),
    scale,
    usingFallback: false,
  };
}

function rectPath(cell, width, height) {
  const p1 = project(cell.lon0, cell.lat0, width, height);
  const p2 = project(cell.lon1, cell.lat0, width, height);
  const p3 = project(cell.lon1, cell.lat1, width, height);
  const p4 = project(cell.lon0, cell.lat1, width, height);
  return `M ${p1[0].toFixed(1)} ${p1[1].toFixed(1)} L ${p2[0].toFixed(1)} ${p2[1].toFixed(1)} L ${p3[0].toFixed(1)} ${p3[1].toFixed(1)} L ${p4[0].toFixed(1)} ${p4[1].toFixed(1)} Z`;
}

function rectBox(width, height) {
  const [x0, y0] = project(INNER_BBOX[0], INNER_BBOX[3], width, height);
  const [x1, y1] = project(INNER_BBOX[2], INNER_BBOX[1], width, height);
  return {
    x: Math.min(x0, x1),
    y: Math.min(y0, y1),
    width: Math.abs(x1 - x0),
    height: Math.abs(y1 - y0),
  };
}

function polygonPath(geometry, width, height) {
  const ring = geometry?.coordinates?.[0] || [];
  return ring
    .map(([lon, lat], idx) => {
      const [x, y] = project(lon, lat, width, height);
      return `${idx === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function ringPath(ring, width, height) {
  if (!ring?.length) return "";
  const path = ring
    .map(([lon, lat], idx) => {
      const [x, y] = project(lon, lat, width, height);
      return `${idx === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
  return `${path} Z`;
}

function lineCoordsPath(coords, width, height) {
  return (coords || [])
    .filter(([lon, lat]) => Number.isFinite(Number(lon)) && Number.isFinite(Number(lat)))
    .map(([lon, lat], idx) => {
      const [x, y] = project(lon, lat, width, height);
      return `${idx === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function geometryPaths(geometry, width, height) {
  if (!geometry) return [];
  if (geometry.type === "LineString") return [lineCoordsPath(geometry.coordinates, width, height)].filter(Boolean);
  if (geometry.type === "MultiLineString") return geometry.coordinates.map((coords) => lineCoordsPath(coords, width, height)).filter(Boolean);
  if (geometry.type === "Polygon") return [geometry.coordinates.map((ring) => ringPath(ring, width, height)).join(" ")].filter(Boolean);
  if (geometry.type === "MultiPolygon") {
    return geometry.coordinates
      .map((polygon) => polygon.map((ring) => ringPath(ring, width, height)).join(" "))
      .filter(Boolean);
  }
  return [];
}

function linePath(geometry, width, height) {
  const coords = geometry?.coordinates || [];
  return coords
    .filter(([lon, lat]) => Number.isFinite(Number(lon)) && Number.isFinite(Number(lat)))
    .map(([lon, lat], idx) => {
      const [x, y] = project(lon, lat, width, height);
      return `${idx === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function featureList(collection) {
  return Array.isArray(collection?.features) ? collection.features : [];
}

function roadClass(feature) {
  const highway = feature.properties?.highway || "tertiary";
  if (["motorway", "trunk"].includes(highway)) return "road-major";
  if (highway === "primary" || highway === "primary_link") return "road-primary";
  if (highway === "secondary" || highway === "secondary_link") return "road-secondary";
  return "road-tertiary";
}

function roadPriority(feature) {
  const highway = feature.properties?.highway || "tertiary";
  if (["motorway", "trunk"].includes(highway)) return 0;
  if (highway === "primary" || highway === "primary_link") return 1;
  if (highway === "secondary" || highway === "secondary_link") return 2;
  if (highway === "tertiary") return 3;
  return 4;
}

const BASEMAP_WATER = featureList(hanoiWater);
const BASEMAP_BOUNDARIES = featureList(hanoiBoundaries);
const BASEMAP_ROADS = featureList(hanoiRoads)
  .filter((feature) => roadPriority(feature) <= 3)
  .sort((left, right) => roadPriority(left) - roadPriority(right))
  .slice(0, MAX_BASEMAP_ROADS);
const BASEMAP_WATER_PATHS = BASEMAP_WATER.flatMap((feature, index) =>
  geometryPaths(feature.geometry, MAP_WIDTH, MAP_HEIGHT).map((d, partIndex) => ({
    key: `water-${index}-${partIndex}`,
    d,
    className: feature.geometry?.type?.includes("Polygon") ? "water-polygon" : "water-line",
  })),
);
const BASEMAP_ROAD_PATHS = BASEMAP_ROADS.flatMap((feature, index) =>
  geometryPaths(feature.geometry, MAP_WIDTH, MAP_HEIGHT).map((d, partIndex) => ({
    key: `road-${index}-${partIndex}`,
    d,
    className: roadClass(feature),
  })),
);
const BASEMAP_BOUNDARY_PATHS = BASEMAP_BOUNDARIES.flatMap((feature, index) =>
  geometryPaths(feature.geometry, MAP_WIDTH, MAP_HEIGHT).map((d, partIndex) => ({
    key: `boundary-${index}-${partIndex}`,
    d,
  })),
);

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d");
}

function featureMatchesReceptor(feature, receptor) {
  const props = feature.properties || {};
  const haystack = [
    props.endpoint,
    props.receptor_name,
    props.receptor_id,
    props.location_name,
    props.location_id,
    props.station_name,
    props.target_name,
  ].map(normalizeText).join(" ");
  return haystack.includes(normalizeText(receptor.name));
}

function latestTrajectoryGroup(features, receptor = DEFAULT_RECEPTOR) {
  if (!features.length) return features;
  const baseTimes = features
    .map((feature) => feature.properties?.base_time || feature.properties?.base_hour || feature.properties?.timestamp)
    .filter(Boolean)
    .sort();
  const selectedBaseTime = baseTimes[baseTimes.length - 1];
  const sameBaseTime = selectedBaseTime
    ? features.filter((feature) => {
        const props = feature.properties || {};
        return (props.base_time || props.base_hour || props.timestamp) === selectedBaseTime;
      })
    : features;
  const receptorMatches = sameBaseTime.filter((feature) => featureMatchesReceptor(feature, receptor));
  const candidates = receptorMatches.length ? receptorMatches : normalizeText(receptor.name).includes("hoan kiem") ? sameBaseTime : [];
  const endpointGroups = new Map();
  for (const feature of candidates) {
    const props = feature.properties || {};
    const key = props.endpoint || props.receptor_id || props.location_id || "default";
    if (!endpointGroups.has(key)) endpointGroups.set(key, []);
    endpointGroups.get(key).push(feature);
  }
  if (!endpointGroups.size) return [];
  return [...endpointGroups.values()].sort((a, b) => b.length - a.length)[0].slice(0, 8);
}

function trajectoriesForReceptor(features, receptor = DEFAULT_RECEPTOR) {
  const matched = latestTrajectoryGroup(features, receptor);
  if (matched.length) return matched.map((feature) => ({ ...feature, properties: { ...(feature.properties || {}), derived_for_receptor: false } }));
  const latest = latestTrajectoryGroup(features, DEFAULT_RECEPTOR);
  return latest.map((feature, index) => translateTrajectoryToReceptor(feature, receptor, index)).filter(Boolean);
}

function translateTrajectoryToReceptor(feature, receptor, index = 0) {
  const coords = feature?.geometry?.coordinates || [];
  if (coords.length < 2 || !Number.isFinite(Number(receptor?.lon)) || !Number.isFinite(Number(receptor?.lat))) return null;
  const [anchorLon, anchorLat] = coords[coords.length - 1];
  const dx = Number(receptor.lon) - Number(anchorLon);
  const dy = Number(receptor.lat) - Number(anchorLat);
  return {
    ...feature,
    geometry: {
      type: "LineString",
      coordinates: coords.map(([lon, lat]) => [Number(lon) + dx, Number(lat) + dy]),
    },
    properties: {
      ...(feature.properties || {}),
      endpoint: receptor.name,
      receptor_name: receptor.name,
      location_name: receptor.name,
      derived_for_receptor: true,
      style_color: feature.properties?.style_color || ["#93c5fd", "#67e8f9", "#99f6e4", "#fde68a"][index % 4],
    },
  };
}

function densifyTrajectory(feature) {
  const coords = feature?.geometry?.coordinates || [];
  if (coords.length < 2) return feature;
  const dense = [];
  for (let i = 0; i < coords.length - 1; i += 1) {
    const [x0, y0] = coords[i];
    const [x1, y1] = coords[i + 1];
    for (let step = 0; step < 8; step += 1) {
      const t = step / 8;
      const wave = Math.sin((i + t) * Math.PI) * 0.0035;
      dense.push([x0 + (x1 - x0) * t, y0 + (y1 - y0) * t + wave]);
    }
  }
  dense.push(coords[coords.length - 1]);
  return { ...feature, geometry: { ...feature.geometry, coordinates: dense } };
}

function latestHeatmapStats(cells) {
  const values = cells.map((cell) => cell.pm25).filter((v) => v != null && !Number.isNaN(v));
  if (!values.length) return { avg: null, max: null };
  return {
    avg: values.reduce((sum, value) => sum + value, 0) / values.length,
    max: Math.max(...values),
  };
}

function stationRadius(feature) {
  const value = valueOf(feature);
  if (value == null || Number.isNaN(value)) return 7;
  return Math.max(7, Math.min(18, 5 + value / 7));
}

function cellGradientRadius(cell, width, height) {
  const [x0, y0] = project(cell.lon0, cell.lat0, width, height);
  const [x1, y1] = project(cell.lon1, cell.lat1, width, height);
  return Math.max(18, Math.hypot(x1 - x0, y1 - y0) * 1.65);
}

function clampZoom(value) {
  return Math.max(0.8, Math.min(5, Number(value)));
}

export default function MapCanvas({
  layers = {},
  enabled = {},
  selectedReceptor: controlledReceptor = null,
  onReceptorChange = () => {},
  onSelect = () => {},
  onStats = () => {},
}) {
  const width = MAP_WIDTH;
  const height = MAP_HEIGHT;
  const [zoom, setZoom] = useState(1);
  const [localSelectedReceptor, setLocalSelectedReceptor] = useState(DEFAULT_RECEPTOR);
  const selectedReceptor = controlledReceptor || localSelectedReceptor;
  const rawHeatmap = useMemo(() => layers.heatmap?.features || [], [layers.heatmap]);
  const rawStations = useMemo(() => layers.stations?.features || [], [layers.stations]);
  const plume = layers.plume?.features || [];
  const rawTrajectories = useMemo(() => layers.trajectories?.features || [], [layers.trajectories]);
  const trajectories = useMemo(
    () => (selectedReceptor ? trajectoriesForReceptor(rawTrajectories, selectedReceptor).map(densifyTrajectory) : []),
    [rawTrajectories, selectedReceptor],
  );
  const sources = layers.sources?.features || [];
  const stations = useMemo(
    () =>
      rawStations.filter((feature) => {
        const lonLat = lonLatFromFeature(feature);
        return lonLat && inUrbanBbox(lonLat[0], lonLat[1], 0.02);
      }),
    [rawStations],
  );
  const { cells, scale, usingFallback } = useMemo(
    () => buildPixelGrid(rawHeatmap, stations, BASE_GRID_COLS, BASE_GRID_ROWS),
    [rawHeatmap, stations],
  );
  const stats = latestHeatmapStats(cells);
  useEffect(() => {
    onStats({
      avg: stats.avg == null ? "-" : stats.avg.toFixed(1),
      max: stats.max == null ? "-" : stats.max.toFixed(1),
      count: cells.length,
      source: usingFallback ? "fallback" : "rendered_grid",
    });
  }, [cells.length, onStats, stats.avg, stats.max, usingFallback]);
  const bbox = rectBox(width, height);
  const viewWidth = width / zoom;
  const viewHeight = height / zoom;
  const viewX = (width - viewWidth) / 2;
  const viewY = (height - viewHeight) / 2;

  function changeZoom(delta) {
    setZoom((current) => clampZoom(current + delta));
  }

  function handleWheel(event) {
    event.preventDefault();
    const direction = event.deltaY > 0 ? -0.16 : 0.16;
    setZoom((current) => clampZoom(current + direction));
  }

  function chooseReceptor(receptor) {
    setLocalSelectedReceptor(receptor);
    onReceptorChange(receptor);
  }

  return (
    <div className="windy-map-canvas">
      <svg
        viewBox={`${viewX} ${viewY} ${viewWidth} ${viewHeight}`}
        role="img"
        aria-label="Hanoi PM2.5 pixel heatmap and wind-like backward trajectories"
        onWheel={handleWheel}
      >
        <defs>
          <radialGradient id="cityGlow" cx="50%" cy="50%" r="65%">
            <stop offset="0%" stopColor="#38bdf8" stopOpacity="0.18" />
            <stop offset="52%" stopColor="#0f172a" stopOpacity="0.05" />
            <stop offset="100%" stopColor="#020617" stopOpacity="0" />
          </radialGradient>
          <filter id="softGlow" x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur stdDeviation="2.4" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <filter id="heatBlur" x="-12%" y="-12%" width="124%" height="124%">
            <feGaussianBlur stdDeviation="10" />
          </filter>
          <marker id="trajArrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L7,3 z" fill="#e0f2fe" opacity="0.92" />
          </marker>
          <linearGradient id="panelFade" x1="0" x2="1">
            <stop offset="0%" stopColor="#020617" stopOpacity="0.28" />
            <stop offset="100%" stopColor="#020617" stopOpacity="0" />
          </linearGradient>
        </defs>

        <rect width={width} height={height} className="hanoi-map-bg" />
        <rect width={width} height={height} fill="url(#cityGlow)" />
        <rect width="330" height={height} fill="url(#panelFade)" />

        <g className="hanoi-grid">
          {Array.from({ length: 9 }).map((_, index) => {
            const lon = INNER_BBOX[0] + ((INNER_BBOX[2] - INNER_BBOX[0]) / 8) * index;
            const [x1, y1] = project(lon, INNER_BBOX[1], width, height);
            const [x2, y2] = project(lon, INNER_BBOX[3], width, height);
            return <line key={`lon-${index}`} x1={x1} y1={y1} x2={x2} y2={y2} />;
          })}
          {Array.from({ length: 7 }).map((_, index) => {
            const lat = INNER_BBOX[1] + ((INNER_BBOX[3] - INNER_BBOX[1]) / 6) * index;
            const [x1, y1] = project(INNER_BBOX[0], lat, width, height);
            const [x2, y2] = project(INNER_BBOX[2], lat, width, height);
            return <line key={`lat-${index}`} x1={x1} y1={y1} x2={x2} y2={y2} />;
          })}
        </g>

        <rect className="hanoi-bbox-fill" x={bbox.x} y={bbox.y} width={bbox.width} height={bbox.height} />
        <clipPath id="hanoiClip">
          <rect x={bbox.x} y={bbox.y} width={bbox.width} height={bbox.height} />
        </clipPath>

        <g clipPath="url(#hanoiClip)">
          <g className="basemap-water">
            {BASEMAP_WATER_PATHS.map((item) => (
              <path key={item.key} d={item.d} className={item.className} />
            ))}
          </g>

          <g className="basemap-roads">
            {BASEMAP_ROAD_PATHS.map((item) => (
              <path key={item.key} d={item.d} className={item.className} />
            ))}
          </g>

          <g className="basemap-boundaries">
            {BASEMAP_BOUNDARY_PATHS.map((item) => (
              <path key={item.key} d={item.d} />
            ))}
          </g>

          {enabled.heatmap && (
            <g className="heatmap-gradient" filter="url(#heatBlur)">
              {cells.map((cell) => (
                <circle
                  key={`heat-${cell.row}-${cell.col}`}
                  cx={project(cell.lon, cell.lat, width, height)[0]}
                  cy={project(cell.lon, cell.lat, width, height)[1]}
                  r={cellGradientRadius(cell, width, height)}
                  fill={cell.color}
                  className="heatmap-gradient-spot"
                  style={{ opacity: 0.1 + cell.normalizedValue * 0.18 }}
                />
              ))}
            </g>
          )}

          {enabled.heatmap && (
            <g className="heatmap-hitgrid">
              {cells.map((cell) => (
                <path
                  key={`pixel-hit-${cell.row}-${cell.col}`}
                  d={rectPath(cell, width, height)}
                  className="heatmap-hitcell"
                  onClick={() =>
                    onSelect({
                      type: "Feature",
                      geometry: { type: "Point", coordinates: [cell.lon, cell.lat] },
                      properties: {
                        layer_name: "PM2.5 pixel",
                        pm25_value: Number(cell.pm25.toFixed(1)),
                        normalized_value: Number(cell.normalizedValue.toFixed(3)),
                        description: `${cell.pm25.toFixed(1)} µg/m³ · ${riskLabel(cell.pm25)}`,
                      },
                    })
                  }
                />
              ))}
            </g>
          )}

          {enabled.plume && (
            <g className="plume-layer">
              {plume.map((feature, index) => (
                <path key={`plume-${index}`} d={polygonPath(feature.geometry, width, height) || linePath(feature.geometry, width, height)} />
              ))}
            </g>
          )}

          {enabled.trajectories && (
            <g className="trajectory-layer">
              {trajectories.map((feature, index) => {
                const d = linePath(feature.geometry, width, height);
                const coords = feature.geometry?.coordinates || [];
                const end = coords.length ? coords[coords.length - 1] : null;
                const endPoint = end ? project(end[0], end[1], width, height) : null;
                if (!d) return null;
                const color = feature.properties?.style_color || feature.properties?.color || "#e0f2fe";
                return (
                  <g key={`traj-${index}`} onClick={() => onSelect({ ...feature, properties: { ...(feature.properties || {}), layer_name: "Backward trajectory" } })}>
                    <path className="trajectory-click-target" d={d} />
                    <path className="trajectory-line" d={d} style={{ stroke: color }} markerEnd="url(#trajArrow)" />
                    {endPoint && <circle className="trajectory-endpoint" cx={endPoint[0]} cy={endPoint[1]} r="5.5" />}
                  </g>
                );
              })}
            </g>
          )}
        </g>

        <rect className="hanoi-bbox-outline" x={bbox.x} y={bbox.y} width={bbox.width} height={bbox.height} />

        <g className="receptor-layer">
          {DISTRICTS.map(([name, lon, lat]) => {
            const [x, y] = project(lon, lat, width, height);
            const active = selectedReceptor?.name === name;
            return (
              <g
                key={name}
                className={active ? "receptor-point active" : "receptor-point"}
                transform={`translate(${x} ${y})`}
                onClick={() => {
                  const receptor = { name, lon, lat };
                  const currentValue = nearestPm25(layers.heatmap, lon, lat);
                  chooseReceptor(receptor);
                  onSelect({
                    type: "Feature",
                    geometry: { type: "Point", coordinates: [lon, lat] },
                    properties: {
                      layer_name: "Backward receptor",
                      location_name: name,
                      pm25_value: currentValue == null ? null : Number(currentValue.toFixed(1)),
                      description: currentValue == null
                        ? `Showing backward trajectory ensemble for ${name}`
                        : `${currentValue.toFixed(1)} µg/m³ on the visible heatmap`,
                    },
                  });
                }}
              >
                <circle r={active ? 8 : 6} />
                <text x="12" y="5">{name}</text>
              </g>
            );
          })}
        </g>

        {enabled.sources && (
          <g className="source-layer">
            {sources.map((feature, index) => {
              const [x, y] = pointFromGeometry(feature.geometry, width, height);
              const [lon, lat] = lonLatFromFeature(feature) || [null, null];
              const score = Number(feature.properties?.contribution_score || feature.properties?.score || 0.5);
              const currentValue = lon == null ? null : nearestPm25(layers.heatmap, lon, lat);
              return (
                <g
                  key={`source-${index}`}
                  transform={`translate(${x} ${y})`}
                  onClick={() =>
                    onSelect({
                      ...feature,
                      properties: {
                        ...(feature.properties || {}),
                        layer_name: "Source attribution",
                        pm25_value: currentValue == null ? null : Number(currentValue.toFixed(1)),
                      },
                    })
                  }
                >
                  <circle r={12 + score * 10} className="source-ring" />
                  <circle r="7" className="source-core" />
                </g>
              );
            })}
          </g>
        )}

        {enabled.stations && (
          <g className="station-layer">
            {stations.map((feature, index) => {
              const [x, y] = pointFromGeometry(feature.geometry, width, height);
              const value = valueOf(feature);
              return (
                <g
                  key={`station-${index}`}
                  transform={`translate(${x} ${y})`}
                  onClick={() =>
                    onSelect({
                      ...feature,
                      properties: {
                        ...(feature.properties || {}),
                        layer_name: "Monitoring station",
                      },
                    })
                  }
                >
                  <circle r={stationRadius(feature) + 8} className="station-heat-ring" fill={pm25Color(value, scale, 0.24)} />
                  <circle r={stationRadius(feature)} fill={pm25Color(value, scale, 0.98)} />
                  <circle r={stationRadius(feature) + 3} className="station-ring" />
                </g>
              );
            })}
          </g>
        )}

        {!cells.length && (
          <g className="fallback-warning">
            <text x="54" y="664">No PM2.5 heatmap data available for this view</text>
          </g>
        )}

        <g className="hanoi-map-title">
          <text x="54" y="716">Hà Nội PM2.5</text>
          <text x="54" y="744">Urban pixel grid · Avg {stats.avg == null ? "-" : stats.avg.toFixed(1)} µg/m³ · Max {stats.max == null ? "-" : stats.max.toFixed(1)} µg/m³</text>
          <text x="54" y="770">
            {selectedReceptor ? `Backward receptor: ${selectedReceptor.name}` : "Click a location dot to show its backward trajectory"}
          </text>
        </g>
      </svg>
      <div className="map-zoom-control" aria-label="Map zoom controls">
        <button type="button" onClick={() => changeZoom(0.25)} aria-label="Zoom in">+</button>
        <button type="button" onClick={() => changeZoom(-0.25)} aria-label="Zoom out">-</button>
        <button type="button" onClick={() => setZoom(1)} aria-label="Reset zoom">{Math.round(zoom * 100)}%</button>
      </div>
      <div className="map-attribution">© OpenStreetMap contributors</div>
    </div>
  );
}
