const LAYERS = [
  ["heatmap", "PM2.5 heatmap"],
  ["trajectories", "Backward trajectories"],
  ["plume", "Forward plume"],
  ["sources", "Source markers"],
  ["stations", "Stations"],
];

export default function LayerControl({ enabled, onToggle }) {
  return (
    <div className="map-panel layer-control">
      <h3>Layers</h3>
      {LAYERS.map(([key, label]) => (
        <label className="layer-toggle" key={key}>
          <input type="checkbox" checked={Boolean(enabled[key])} onChange={() => onToggle(key)} />
          <span>{label}</span>
        </label>
      ))}
    </div>
  );
}
