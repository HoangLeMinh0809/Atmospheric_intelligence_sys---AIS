export default function MapPopup({ feature, onClose }) {
  if (!feature) return null;
  const props = feature.properties || {};
  const entries = Object.entries(props)
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .slice(0, 10);

  return (
    <div className="map-popup">
      <button type="button" onClick={onClose} aria-label="Close popup">x</button>
      <h3>{props.layer_name || props.source_label || props.station_name || props.traj_id || "Map feature"}</h3>
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
