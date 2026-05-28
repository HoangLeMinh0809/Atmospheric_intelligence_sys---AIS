import { useEffect, useMemo, useState } from "react";
import {
  getBackwardTrajectoriesLatest,
  getForwardPlumeLatest,
  getManifestLatest,
  getStationsLatest,
} from "../services/visualizationApi";

const BBOX = { west: 100.0, east: 108.8, south: 18.0, north: 24.5 };
const WIDTH = 980;
const HEIGHT = 680;

function project(lon, lat) {
  const x = ((lon - BBOX.west) / (BBOX.east - BBOX.west)) * WIDTH;
  const y = HEIGHT - ((lat - BBOX.south) / (BBOX.north - BBOX.south)) * HEIGHT;
  return [x, y];
}

function coordinatesOf(feature) {
  return feature?.geometry?.coordinates || [];
}

function linePath(feature) {
  return coordinatesOf(feature)
    .map(([lon, lat], index) => {
      const [x, y] = project(lon, lat);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function polygonPoints(feature) {
  const ring = coordinatesOf(feature)?.[0] || [];
  return ring
    .map(([lon, lat]) => {
      const [x, y] = project(lon, lat);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

function plumeColor(probability) {
  const alpha = Math.max(0.15, Math.min(0.75, Number(probability || 0) * 8));
  return `rgba(239, 68, 68, ${alpha})`;
}

function Popup({ selected }) {
  if (!selected) return <div className="map-popup empty">Select a feature</div>;
  const p = selected.properties || {};
  if (selected.type === "station") {
    return (
      <div className="map-popup">
        <h3>{p.station_name || p.station_id}</h3>
        <p>PM2.5: {p.pm25 ?? "n/a"} {p.unit || "µg/m³"}</p>
        <p>Observation: {p.observation_time || "n/a"}</p>
        <p>Coverage: {p.coverage_pct ?? "n/a"}%</p>
        <p>Risk: {p.risk || "unknown"}</p>
      </div>
    );
  }
  if (selected.type === "trajectory") {
    return (
      <div className="map-popup">
        <h3>{p.traj_id}</h3>
        <p>Cluster: {p.cluster_id ?? "n/a"} / {p.source_label || "unknown"}</p>
        <p>Source: {p.source_lat ?? "n/a"}, {p.source_lon ?? "n/a"}</p>
        <p>NO2 mean: {p.path_no2_mean ?? "n/a"}</p>
        <p>AER mean: {p.path_aer_mean ?? "n/a"}</p>
      </div>
    );
  }
  return (
    <div className="map-popup">
      <h3>Forward plume</h3>
      <p>Horizon: {p.horizon_h}h</p>
      <p>Probability: {p.probability ?? "n/a"}</p>
      <p>Particles: {p.particle_count ?? 0} / {p.total_particle_count ?? 0}</p>
    </div>
  );
}

export default function AirQualityMapDashboard() {
  const [layers, setLayers] = useState({ stations: true, trajectories: true, plume: true });
  const [horizon, setHorizon] = useState(6);
  const [manifest, setManifest] = useState(null);
  const [stations, setStations] = useState(null);
  const [trajectories, setTrajectories] = useState(null);
  const [plume, setPlume] = useState(null);
  const [selected, setSelected] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;
    Promise.all([
      getManifestLatest(),
      getStationsLatest(),
      getBackwardTrajectoriesLatest(),
      getForwardPlumeLatest(horizon),
    ])
      .then(([manifestData, stationData, trajectoryData, plumeData]) => {
        if (!active) return;
        setManifest(manifestData);
        setStations(stationData);
        setTrajectories(trajectoryData);
        setPlume(plumeData);
        setError("");
      })
      .catch((err) => {
        if (active) setError(err.message);
      });
    return () => {
      active = false;
    };
  }, [horizon]);

  const plumeUnavailable = plume && plume.available === false;
  const manifestSummary = useMemo(() => manifest?.layers || [], [manifest]);

  return (
    <div className="map-dashboard">
      <div className="map-toolbar">
        <div>
          <h1>AIS Air Quality Map</h1>
          <p>Station observations, backward trajectories, and forward plume</p>
        </div>
        <div className="toolbar-controls">
          <label>
            Plume horizon
            <select value={horizon} onChange={(event) => setHorizon(Number(event.target.value))}>
              <option value={6}>+6h</option>
              <option value={12}>+12h</option>
              <option value={24}>+24h</option>
            </select>
          </label>
          {["stations", "trajectories", "plume"].map((key) => (
            <label className="layer-toggle" key={key}>
              <input
                type="checkbox"
                checked={layers[key]}
                onChange={(event) => setLayers((prev) => ({ ...prev, [key]: event.target.checked }))}
              />
              {key}
            </label>
          ))}
        </div>
      </div>

      {error && <div className="map-error">{error}</div>}
      {plumeUnavailable && <div className="map-warning">Forward plume unavailable: {plume.reason}</div>}

      <div className="map-layout">
        <svg className="air-map" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img">
          <rect width={WIDTH} height={HEIGHT} fill="#dbeafe" />
          <g opacity="0.22" stroke="#2563eb" strokeWidth="1">
            {Array.from({ length: 9 }).map((_, i) => <line key={`v-${i}`} x1={(i * WIDTH) / 8} x2={(i * WIDTH) / 8} y1="0" y2={HEIGHT} />)}
            {Array.from({ length: 7 }).map((_, i) => <line key={`h-${i}`} x1="0" x2={WIDTH} y1={(i * HEIGHT) / 6} y2={(i * HEIGHT) / 6} />)}
          </g>
          <text x="610" y="320" className="map-city">Hà Nội</text>
          <text x="760" y="360" className="map-city small">Hải Phòng</text>
          <text x="250" y="170" className="map-city small">Lào Cai</text>

          {layers.plume && plume?.features?.map((feature, index) => (
            <polygon
              key={`plume-${index}`}
              points={polygonPoints(feature)}
              fill={plumeColor(feature.properties?.probability)}
              stroke="rgba(127,29,29,0.25)"
              onClick={() => setSelected({ type: "plume", properties: feature.properties })}
            />
          ))}

          {layers.trajectories && trajectories?.features?.map((feature, index) => (
            <path
              key={`traj-${index}`}
              d={linePath(feature)}
              fill="none"
              stroke={feature.properties?.style_color || "#2563eb"}
              strokeWidth="2.4"
              opacity="0.82"
              onClick={() => setSelected({ type: "trajectory", properties: feature.properties })}
            />
          ))}

          {layers.stations && stations?.features?.map((feature, index) => {
            const [lon, lat] = coordinatesOf(feature);
            const [x, y] = project(lon, lat);
            return (
              <circle
                key={`station-${index}`}
                cx={x}
                cy={y}
                r="7"
                fill="#0f766e"
                stroke="#ffffff"
                strokeWidth="2"
                onClick={() => setSelected({ type: "station", properties: feature.properties })}
              />
            );
          })}
        </svg>

        <aside className="map-side-panel">
          <Popup selected={selected} />
          <div className="manifest-panel">
            <h3>Layer freshness</h3>
            {manifestSummary.map((layer) => (
              <div className="manifest-row" key={`${layer.layer_name}-${layer.horizon_h ?? "latest"}`}>
                <span>{layer.layer_name}{layer.horizon_h ? ` +${layer.horizon_h}h` : ""}</span>
                <strong>{layer.available ? `${layer.record_count} rows` : layer.unavailable_reason}</strong>
              </div>
            ))}
          </div>
        </aside>
      </div>
    </div>
  );
}
