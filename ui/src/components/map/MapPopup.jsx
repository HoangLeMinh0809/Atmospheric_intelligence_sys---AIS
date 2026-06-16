// File nay: component ban do hien thi layer PM2.5, station, trajectory va plume.
// Render component MapPopup va gan state/props cho UI.
export default function MapPopup({ feature, onClose }) {
  if (!feature) return null;
  const props = feature.properties || {};
  const pointForecast = props.point_forecast || null;
  const entries = Object.entries(props)
    .filter(([key, value]) => key !== "point_forecast" && value !== null && value !== undefined && value !== "" && typeof value !== "object")
    .slice(0, 10);

  return (
    <div className="map-popup">
      <button type="button" onClick={onClose} aria-label="Close popup">x</button>
      <h3>{props.layer_name || props.source_label || props.station_name || props.traj_id || "Map feature"}</h3>
      {pointForecast && (
        <div className="popup-point-forecast">
          {[
            ["now", "Now"],
            ["6h", "+6h"],
            ["12h", "+12h"],
            ["24h", "+24h"],
          ].map(([key, label]) => {
            const item = pointForecast[key] || {};
            return (
              <div className={`popup-forecast-card risk-${item.risk || "unknown"}`} key={key}>
                <span>{label}</span>
                <strong>{item.pm25 == null ? "-" : Number(item.pm25).toFixed(1)}</strong>
                <em>µg/m³</em>
              </div>
            );
          })}
        </div>
      )}
      <dl>
        {entries.map(([key, value]) => (
          <div key={key}>
            <dt>{key}</dt>
            <dd>{typeof value === "number" ? Number(value).toFixed(3) : String(value)}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}
