import { geoMercator, geoPath } from "d3";

const VIETNAM = {
  type: "Feature",
  geometry: {
    type: "Polygon",
    coordinates: [
      [
        [102.15, 22.42],
        [103.1, 22.85],
        [104.55, 23.35],
        [105.85, 22.92],
        [106.85, 22.75],
        [107.35, 21.7],
        [108.05, 21.1],
        [107.55, 20.35],
        [106.72, 20.05],
        [106.75, 19.1],
        [105.92, 18.35],
        [105.7, 17.35],
        [106.45, 16.2],
        [107.25, 15.4],
        [108.05, 14.45],
        [109.1, 13.2],
        [109.32, 12.05],
        [108.95, 11.1],
        [107.55, 10.35],
        [106.82, 9.45],
        [105.55, 8.55],
        [104.85, 8.62],
        [104.98, 9.85],
        [105.55, 10.72],
        [105.2, 11.75],
        [106.05, 12.55],
        [107.25, 13.65],
        [107.55, 14.72],
        [107.0, 15.55],
        [106.25, 16.25],
        [105.05, 17.2],
        [104.65, 18.35],
        [104.2, 19.15],
        [103.55, 19.85],
        [103.85, 20.65],
        [103.35, 21.35],
        [102.15, 22.42],
      ],
    ],
  },
  properties: { name: "Vietnam" },
};

const NEIGHBORS = [
  { name: "Laos", coordinates: [[101.0, 22.5], [103.2, 22.2], [104.0, 19.3], [105.0, 17.7], [104.0, 15.3], [105.2, 13.8], [103.1, 13.7], [101.2, 17.0], [100.5, 20.0], [101.0, 22.5]] },
  { name: "China", coordinates: [[102.1, 24.4], [108.5, 24.4], [107.4, 22.7], [105.8, 22.9], [104.5, 23.3], [103.1, 22.8], [102.1, 24.4]] },
  { name: "Cambodia", coordinates: [[103.0, 13.8], [105.2, 13.8], [106.0, 12.6], [105.2, 11.8], [105.5, 10.7], [104.9, 9.8], [102.8, 10.5], [102.5, 12.5], [103.0, 13.8]] },
].map((item) => ({
  type: "Feature",
  geometry: { type: "Polygon", coordinates: [item.coordinates] },
  properties: { name: item.name },
}));

function projection(width, height) {
  return geoMercator().fitExtent(
    [
      [42, 28],
      [width - 42, height - 32],
    ],
    { type: "FeatureCollection", features: [VIETNAM] },
  );
}

function project(lon, lat, width, height) {
  return projection(width, height)([lon, lat]);
}

function pm25Color(value) {
  if (value == null) return "rgba(148,163,184,0.18)";
  if (value < 35) return "rgba(34,197,94,0.58)";
  if (value < 75) return "rgba(245,158,11,0.62)";
  if (value < 150) return "rgba(239,68,68,0.62)";
  return "rgba(127,29,29,0.72)";
}

function polygonPath(geometry, width, height) {
  const path = geoPath(projection(width, height));
  const projected = path({ type: "Feature", geometry });
  if (projected) return projected;
  const ring = geometry?.coordinates?.[0] || [];
  return ring
    .map(([lon, lat], idx) => {
      const [x, y] = project(lon, lat, width, height);
      return `${idx === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");
}

function linePath(geometry, width, height) {
  const path = geoPath(projection(width, height));
  const projected = path({ type: "Feature", geometry });
  if (projected) return projected;
  const coords = geometry?.coordinates || [];
  return coords
    .map(([lon, lat], idx) => {
      const [x, y] = project(lon, lat, width, height);
      return `${idx === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");
}

function point(geometry, width, height) {
  const coords = geometry?.coordinates || [];
  return project(Number(coords[0]), Number(coords[1]), width, height);
}

export default function MapCanvas({ layers, enabled, onSelect }) {
  const width = 1000;
  const height = 720;
  const path = geoPath(projection(width, height));
  const heatmap = layers.heatmap?.features || [];
  const plume = layers.plume?.features || [];
  const trajectories = layers.trajectories?.features || [];
  const sources = layers.sources?.features || [];
  const stations = layers.stations?.features || [];

  return (
    <div className="map-canvas">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Northern Vietnam air quality map">
        <defs>
          <linearGradient id="water" x1="0" x2="1">
            <stop offset="0%" stopColor="#dbeafe" />
            <stop offset="100%" stopColor="#bfdbfe" />
          </linearGradient>
        </defs>
        <rect width={width} height={height} fill="#d7eef8" />
        <path d="M 650 0 C 760 125 710 245 790 360 C 860 460 930 535 1000 720 L 1000 0 Z" fill="url(#water)" opacity="0.9" />
        <g className="neighbor-layer">
          {NEIGHBORS.map((feature) => (
            <path key={feature.properties.name} d={path(feature)} />
          ))}
        </g>
        <path d={path(VIETNAM)} className="vietnam-outline" />
        <g className="map-grid">
          {[0, 1, 2, 3, 4, 5, 6].map((i) => (
            <line key={`h-${i}`} x1="0" x2={width} y1={(height / 6) * i} y2={(height / 6) * i} />
          ))}
          {[0, 1, 2, 3, 4, 5, 6, 7, 8].map((i) => (
            <line key={`v-${i}`} y1="0" y2={height} x1={(width / 8) * i} x2={(width / 8) * i} />
          ))}
        </g>

        {enabled.heatmap &&
          heatmap.map((feature, idx) => (
            <path
              key={`heat-${idx}`}
              d={polygonPath(feature.geometry, width, height)}
              fill={pm25Color(feature.properties?.pm25_value)}
              stroke="rgba(255,255,255,0.08)"
              onClick={() => onSelect(feature)}
            />
          ))}

        {enabled.plume &&
          plume.map((feature, idx) => (
            <path
              key={`plume-${idx}`}
              d={polygonPath(feature.geometry, width, height)}
              fill="rgba(14,165,233,0.26)"
              stroke="rgba(14,165,233,0.25)"
              onClick={() => onSelect(feature)}
            />
          ))}

        {enabled.trajectories &&
          trajectories.map((feature, idx) => (
            <path
              key={`traj-${idx}`}
              d={linePath(feature.geometry, width, height)}
              fill="none"
              stroke={feature.properties?.style_color || "#2563eb"}
              strokeWidth="3"
              strokeOpacity="0.72"
              onClick={() => onSelect(feature)}
            />
          ))}

        {enabled.sources &&
          sources.map((feature, idx) => {
            const [x, y] = point(feature.geometry, width, height);
            return <circle key={`source-${idx}`} cx={x} cy={y} r="11" className="source-dot" onClick={() => onSelect(feature)} />;
          })}

        {enabled.stations &&
          stations.map((feature, idx) => {
            const [x, y] = point(feature.geometry, width, height);
            return <circle key={`station-${idx}`} cx={x} cy={y} r="7" className="station-dot" onClick={() => onSelect(feature)} />;
          })}

        {[
          ["Ha Noi", 105.8542, 21.0285],
          ["Hai Phong", 106.6881, 20.8449],
          ["Da Nang", 108.2022, 16.0544],
          ["Ho Chi Minh City", 106.6297, 10.8231],
          ["Can Tho", 105.7469, 10.0452],
        ].map(([label, lon, lat]) => {
          const [x, y] = project(lon, lat, width, height);
          return (
            <text x={x + 8} y={y - 8} className="city-label" key={label}>
              {label}
            </text>
          );
        })}
      </svg>
    </div>
  );
}
