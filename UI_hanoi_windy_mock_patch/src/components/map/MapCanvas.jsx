import { geoBounds, geoCentroid, geoMercator, geoPath } from "d3";

const HANOI_BBOX = [105.25, 20.55, 106.1, 21.4];
const HANOI_CENTER = [105.8542, 21.0285];

const HANOI_REGION = {
  type: "Feature",
  geometry: {
    type: "Polygon",
    coordinates: [
      [
        [105.28, 21.18],
        [105.36, 21.33],
        [105.58, 21.39],
        [105.82, 21.34],
        [106.03, 21.21],
        [106.08, 21.03],
        [106.0, 20.82],
        [105.82, 20.65],
        [105.58, 20.57],
        [105.38, 20.66],
        [105.26, 20.88],
        [105.28, 21.18],
      ],
    ],
  },
  properties: { name: "Hà Nội" },
};

const DISTRICTS = [
  ["Ba Đình", 105.8342, 21.0359],
  ["Hoàn Kiếm", 105.8542, 21.0285],
  ["Cầu Giấy", 105.7903, 21.0362],
  ["Đống Đa", 105.8296, 21.0181],
  ["Long Biên", 105.8998, 21.0397],
  ["Thanh Xuân", 105.8057, 20.9956],
  ["Hà Đông", 105.7625, 20.9714],
  ["Sóc Sơn", 105.8492, 21.2578],
  ["Sơn Tây", 105.5069, 21.1406],
  ["Gia Lâm", 105.9604, 21.0194],
];

const RING_ROADS = [
  [
    [105.736, 21.02],
    [105.766, 21.075],
    [105.84, 21.103],
    [105.915, 21.075],
    [105.94, 21.012],
    [105.91, 20.95],
    [105.83, 20.926],
    [105.76, 20.955],
    [105.736, 21.02],
  ],
  [
    [105.67, 21.03],
    [105.72, 21.14],
    [105.84, 21.18],
    [105.99, 21.12],
    [106.02, 20.98],
    [105.95, 20.84],
    [105.78, 20.82],
    [105.66, 20.91],
    [105.67, 21.03],
  ],
];

function projection(width, height) {
  return geoMercator().fitExtent(
    [
      [48, 48],
      [width - 48, height - 54],
    ],
    HANOI_REGION,
  );
}

function project(lon, lat, width, height) {
  return projection(width, height)([Number(lon), Number(lat)]);
}

function valueOf(feature) {
  const props = feature?.properties || {};
  const candidates = [props.pm25_value, props.pm25, props.value, props.forecast_pm25, props.pm25_mean];
  const value = candidates.find((item) => item !== undefined && item !== null && item !== "");
  return value == null ? null : Number(value);
}

function pm25Color(value, alpha = 0.78) {
  if (value == null || Number.isNaN(value)) return `rgba(148,163,184,${alpha * 0.35})`;
  if (value <= 12) return `rgba(56,189,248,${alpha})`;
  if (value <= 35) return `rgba(34,197,94,${alpha})`;
  if (value <= 55) return `rgba(250,204,21,${alpha})`;
  if (value <= 75) return `rgba(249,115,22,${alpha})`;
  if (value <= 150) return `rgba(239,68,68,${alpha})`;
  return `rgba(126,34,206,${alpha})`;
}

function riskLabel(value) {
  if (value == null || Number.isNaN(value)) return "Unknown";
  if (value <= 12) return "Good";
  if (value <= 35) return "Moderate";
  if (value <= 55) return "Sensitive";
  if (value <= 75) return "Unhealthy";
  if (value <= 150) return "Very unhealthy";
  return "Hazardous";
}

function geometryPath(geometry, width, height) {
  const path = geoPath(projection(width, height));
  const projected = path({ type: "Feature", geometry });
  if (projected) return projected;
  return "";
}

function linePath(geometry, width, height) {
  const coords = geometry?.coordinates || [];
  return coords
    .map(([lon, lat], idx) => {
      const [x, y] = project(lon, lat, width, height);
      return `${idx === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function pointFromGeometry(geometry, width, height) {
  if (!geometry) return project(HANOI_CENTER[0], HANOI_CENTER[1], width, height);
  if (geometry.type === "Point") return project(geometry.coordinates[0], geometry.coordinates[1], width, height);
  const center = geoCentroid({ type: "Feature", geometry });
  return project(center[0], center[1], width, height);
}

function sizeFromGeometry(geometry, width, height) {
  if (!geometry || geometry.type === "Point") return 54;
  const [[minLon, minLat], [maxLon, maxLat]] = geoBounds({ type: "Feature", geometry });
  const [x1, y1] = project(minLon, minLat, width, height);
  const [x2, y2] = project(maxLon, maxLat, width, height);
  return Math.max(34, Math.min(130, Math.hypot(x2 - x1, y2 - y1) * 0.58));
}

function stationRadius(feature) {
  const value = valueOf(feature);
  if (value == null || Number.isNaN(value)) return 7;
  return Math.max(6, Math.min(16, 5 + value / 8));
}

function latestHeatmapStats(features) {
  const values = features.map(valueOf).filter((v) => v != null && !Number.isNaN(v));
  if (!values.length) return { avg: null, max: null };
  return {
    avg: values.reduce((sum, value) => sum + value, 0) / values.length,
    max: Math.max(...values),
  };
}

export default function MapCanvas({ layers = {}, enabled = {}, onSelect = () => {} }) {
  const width = 1280;
  const height = 820;
  const mapPath = geoPath(projection(width, height));
  const heatmap = layers.heatmap?.features || [];
  const plume = layers.plume?.features || [];
  const trajectories = layers.trajectories?.features || [];
  const sources = layers.sources?.features || [];
  const stations = layers.stations?.features || [];
  const stats = latestHeatmapStats(heatmap);

  return (
    <div className="windy-map-canvas">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Hanoi PM2.5 heatmap and wind-like backward trajectories">
        <defs>
          <radialGradient id="cityGlow" cx="50%" cy="50%" r="65%">
            <stop offset="0%" stopColor="#38bdf8" stopOpacity="0.26" />
            <stop offset="48%" stopColor="#0f172a" stopOpacity="0.08" />
            <stop offset="100%" stopColor="#020617" stopOpacity="0" />
          </radialGradient>
          <filter id="heatBlur" x="-80%" y="-80%" width="260%" height="260%">
            <feGaussianBlur stdDeviation="23" />
          </filter>
          <filter id="softGlow" x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur stdDeviation="3" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <marker id="trajArrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L8,3 z" fill="#dff9ff" opacity="0.92" />
          </marker>
          <linearGradient id="panelFade" x1="0" x2="1">
            <stop offset="0%" stopColor="#020617" stopOpacity="0.36" />
            <stop offset="100%" stopColor="#020617" stopOpacity="0" />
          </linearGradient>
        </defs>

        <rect width={width} height={height} className="hanoi-map-bg" />
        <rect width={width} height={height} fill="url(#cityGlow)" />
        <rect width="420" height={height} fill="url(#panelFade)" />

        <g className="hanoi-grid">
          {Array.from({ length: 9 }).map((_, index) => {
            const lon = HANOI_BBOX[0] + ((HANOI_BBOX[2] - HANOI_BBOX[0]) / 8) * index;
            const [x1, y1] = project(lon, HANOI_BBOX[1], width, height);
            const [x2, y2] = project(lon, HANOI_BBOX[3], width, height);
            return <line key={`lon-${index}`} x1={x1} y1={y1} x2={x2} y2={y2} />;
          })}
          {Array.from({ length: 8 }).map((_, index) => {
            const lat = HANOI_BBOX[1] + ((HANOI_BBOX[3] - HANOI_BBOX[1]) / 7) * index;
            const [x1, y1] = project(HANOI_BBOX[0], lat, width, height);
            const [x2, y2] = project(HANOI_BBOX[2], lat, width, height);
            return <line key={`lat-${index}`} x1={x1} y1={y1} x2={x2} y2={y2} />;
          })}
        </g>

        <path d={mapPath(HANOI_REGION)} className="hanoi-region-fill" />
        <clipPath id="hanoiClip"><path d={mapPath(HANOI_REGION)} /></clipPath>

        <g clipPath="url(#hanoiClip)">
          <g className="hanoi-roads">
            {RING_ROADS.map((coords, index) => (
              <path key={`road-${index}`} d={linePath({ type: "LineString", coordinates: coords }, width, height)} />
            ))}
          </g>

          {enabled.heatmap && (
            <g className="heatmap-glow" filter="url(#heatBlur)">
              {heatmap.map((feature, index) => {
                const value = valueOf(feature);
                const [x, y] = pointFromGeometry(feature.geometry, width, height);
                return (
                  <circle
                    key={`heat-glow-${index}`}
                    cx={x}
                    cy={y}
                    r={sizeFromGeometry(feature.geometry, width, height)}
                    fill={pm25Color(value, 0.62)}
                  />
                );
              })}
            </g>
          )}

          {enabled.heatmap && (
            <g className="heatmap-cells">
              {heatmap.map((feature, index) => {
                const value = valueOf(feature);
                const isPoint = feature.geometry?.type === "Point";
                const [x, y] = pointFromGeometry(feature.geometry, width, height);
                const title = `${Number.isFinite(value) ? value.toFixed(1) : "-"} µg/m³ · ${riskLabel(value)}`;
                return isPoint ? (
                  <circle
                    key={`heat-point-${index}`}
                    cx={x}
                    cy={y}
                    r={Math.max(18, sizeFromGeometry(feature.geometry, width, height) * 0.42)}
                    fill={pm25Color(value, 0.5)}
                    onClick={() => onSelect({ ...feature, properties: { ...(feature.properties || {}), layer_name: "PM2.5 heatmap", description: title } })}
                  />
                ) : (
                  <path
                    key={`heat-cell-${index}`}
                    d={geometryPath(feature.geometry, width, height)}
                    fill={pm25Color(value, 0.34)}
                    className="heatmap-cell"
                    onClick={() => onSelect({ ...feature, properties: { ...(feature.properties || {}), layer_name: "PM2.5 heatmap", description: title } })}
                  />
                );
              })}
            </g>
          )}

          {enabled.plume && (
            <g className="plume-layer">
              {plume.map((feature, index) => (
                <path
                  key={`plume-${index}`}
                  d={geometryPath(feature.geometry, width, height)}
                  onClick={() => onSelect({ ...feature, properties: { ...(feature.properties || {}), layer_name: "Forward plume" } })}
                />
              ))}
            </g>
          )}

          {enabled.trajectories && (
            <g className="trajectory-layer" filter="url(#softGlow)">
              {trajectories.map((feature, index) => {
                const d = linePath(feature.geometry, width, height);
                const color = feature.properties?.style_color || feature.properties?.color || "#7dd3fc";
                return (
                  <g key={`traj-${index}`} onClick={() => onSelect({ ...feature, properties: { ...(feature.properties || {}), layer_name: "Backward trajectory" } })}>
                    <path className="trajectory-halo" d={d} />
                    <path className="trajectory-line" d={d} style={{ stroke: color }} markerEnd="url(#trajArrow)" />
                    <path className="trajectory-pulse" d={d} style={{ stroke: color, animationDelay: `${index * -0.6}s` }} />
                  </g>
                );
              })}
            </g>
          )}
        </g>

        <path d={mapPath(HANOI_REGION)} className="hanoi-region-outline" />

        <g className="district-labels">
          {DISTRICTS.map(([name, lon, lat]) => {
            const [x, y] = project(lon, lat, width, height);
            return (
              <text key={name} x={x} y={y}>
                {name}
              </text>
            );
          })}
        </g>

        {enabled.sources && (
          <g className="source-layer">
            {sources.map((feature, index) => {
              const [x, y] = pointFromGeometry(feature.geometry, width, height);
              const score = Number(feature.properties?.contribution_score || feature.properties?.score || 0.5);
              return (
                <g key={`source-${index}`} transform={`translate(${x} ${y})`} onClick={() => onSelect({ ...feature, properties: { ...(feature.properties || {}), layer_name: "Source attribution" } })}>
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
                <g key={`station-${index}`} transform={`translate(${x} ${y})`} onClick={() => onSelect({ ...feature, properties: { ...(feature.properties || {}), layer_name: "Monitoring station" } })}>
                  <circle r={stationRadius(feature)} fill={pm25Color(value, 0.96)} />
                  <circle r={stationRadius(feature) + 3} className="station-ring" />
                </g>
              );
            })}
          </g>
        )}

        <g className="hanoi-map-title">
          <text x="54" y="716">Hà Nội PM2.5</text>
          <text x="54" y="744">Avg {stats.avg == null ? "-" : stats.avg.toFixed(1)} µg/m³ · Max {stats.max == null ? "-" : stats.max.toFixed(1)} µg/m³</text>
        </g>
      </svg>
    </div>
  );
}
