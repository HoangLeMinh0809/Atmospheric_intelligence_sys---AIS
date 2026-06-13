function valueRange(points) {
  const values = points.map((point) => Number(point.pm25_value)).filter(Number.isFinite);
  return [0, Math.max(120, ...values)];
}

export default function PM25ForecastChart({ timeseries }) {
  const points = timeseries?.points || [];
  const [minValue, maxValue] = valueRange(points);
  const width = 520;
  const height = 150;
  const pad = 22;
  const sorted = [...points].sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));

  const x = (idx) => pad + (idx / Math.max(sorted.length - 1, 1)) * (width - pad * 2);
  const y = (value) => height - pad - ((Number(value) - minValue) / (maxValue - minValue || 1)) * (height - pad * 2);
  const path = sorted
    .filter((point) => point.pm25_value != null)
    .map((point, idx) => `${idx === 0 ? "M" : "L"} ${x(idx)} ${y(point.pm25_value)}`)
    .join(" ");

  return (
    <section className="map-panel chart-card">
      <h3>Observed and Forecast</h3>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="PM2.5 timeseries chart">
        <line x1={pad} y1={height - pad} x2={width - pad} y2={height - pad} className="chart-axis" />
        <line x1={pad} y1={pad} x2={pad} y2={height - pad} className="chart-axis" />
        <path d={path} className="chart-line" fill="none" />
        {sorted.map((point, idx) => (
          <circle
            key={`${point.timestamp}-${idx}`}
            cx={x(idx)}
            cy={point.pm25_value == null ? height - pad : y(point.pm25_value)}
            r="4"
            className={point.series_type === "forecast" ? "chart-point forecast" : "chart-point"}
          />
        ))}
      </svg>
    </section>
  );
}
