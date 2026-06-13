const CARDS = [
  ["now", "Now"],
  ["6h", "+6h"],
  ["12h", "+12h"],
  ["24h", "+24h"],
];

export default function ForecastPanel({ forecast }) {
  const data = forecast?.forecast || {};
  return (
    <section className="map-panel forecast-panel">
      <div className="panel-title-row">
        <h3>Model City Forecast</h3>
        <span>{forecast?.model?.model_version || forecast?.model?.model_version_6h || "model pending"}</span>
      </div>
      <div className="forecast-grid">
        {CARDS.map(([key, label]) => {
          const item = data[key] || {};
          return (
            <div className={`forecast-card risk-${item.risk || "unknown"}`} key={key}>
              <span>{label}</span>
              <strong>{item.pm25 == null ? "-" : Number(item.pm25).toFixed(1)}</strong>
              <em>{item.risk || "unknown"}</em>
            </div>
          );
        })}
      </div>
      <div className="forecast-meta">
        <span>Base {forecast?.base_hour || "-"}</span>
        <span>Generated {forecast?.generated_at || "-"}</span>
      </div>
    </section>
  );
}
