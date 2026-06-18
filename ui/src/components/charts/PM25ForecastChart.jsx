// File nay: component bieu do hien thi lich su PM2.5 gan nhat cho dashboard.
function pointValue(point) {
  const value = point?.pm25_value ?? point?.pm25 ?? point?.value ?? point?.pm25_mean;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function pointTime(point) {
  return point?.timestamp || point?.base_hour || point?.time || point?.generated_at || "";
}

function chartPoints(timeseries) {
  return [...(timeseries?.points || [])]
    .map((point) => ({ ...point, pm25_value: pointValue(point), timestamp: pointTime(point) }))
    .filter((point) => point.pm25_value != null && point.timestamp)
    .sort((left, right) => String(left.timestamp).localeCompare(String(right.timestamp)))
    .slice(-48);
}

function valueRange(points) {
  const values = points.map((point) => point.pm25_value).filter(Number.isFinite);
  if (!values.length) return [0, 80];
  return [0, Math.max(80, Math.ceil(Math.max(...values) / 10) * 10)];
}

function fmtValue(value) {
  return Number.isFinite(value) ? value.toFixed(1) : "-";
}

function fmtHour(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString("vi-VN", { day: "2-digit", hour: "2-digit" });
}

// Render component PM25ForecastChart va gan state/props cho UI.
export default function PM25ForecastChart({ timeseries, locationName = "" }) {
  const sorted = chartPoints(timeseries);
  const [minValue, maxValue] = valueRange(sorted);
  const values = sorted.map((point) => point.pm25_value);
  const latest = values.at(-1);
  const avg = values.length ? values.reduce((sum, item) => sum + item, 0) / values.length : null;
  const min = values.length ? Math.min(...values) : null;
  const max = values.length ? Math.max(...values) : null;
  const width = 520;
  const height = 210;
  const pad = { left: 38, top: 18, right: 16, bottom: 34 };
  const chartWidth = width - pad.left - pad.right;
  const chartHeight = height - pad.top - pad.bottom;
  const x = (idx) => pad.left + (idx / Math.max(sorted.length - 1, 1)) * chartWidth;
  const y = (value) => pad.top + (1 - (Number(value) - minValue) / (maxValue - minValue || 1)) * chartHeight;
  const linePath = sorted.map((point, idx) => `${idx === 0 ? "M" : "L"} ${x(idx).toFixed(1)} ${y(point.pm25_value).toFixed(1)}`).join(" ");
  const areaPath = linePath ? `${linePath} L ${x(sorted.length - 1).toFixed(1)} ${height - pad.bottom} L ${pad.left} ${height - pad.bottom} Z` : "";
  const ticks = [0, 35, 55, maxValue].filter((value, index, arr) => value <= maxValue && arr.indexOf(value) === index);
  const firstLabel = sorted[0]?.timestamp;
  const lastLabel = sorted.at(-1)?.timestamp;

  return (
    <section className="map-panel chart-card pm25-history-card">
      <div className="chart-title-row">
        <div>
          <h3>PM2.5 history</h3>
          <span>{sorted.length ? `${locationName ? `${locationName} · ` : ""}${sorted.length} latest hourly points` : "No history points"}</span>
        </div>
        <strong>{fmtValue(latest)}</strong>
      </div>
      <div className="history-stat-row">
        <span>Avg <b>{fmtValue(avg)}</b></span>
        <span>Min <b>{fmtValue(min)}</b></span>
        <span>Max <b>{fmtValue(max)}</b></span>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="PM2.5 48 hour history chart">
        <rect x={pad.left} y={y(55)} width={chartWidth} height={Math.max(0, y(35) - y(55))} className="chart-band high" />
        <rect x={pad.left} y={y(35)} width={chartWidth} height={Math.max(0, height - pad.bottom - y(35))} className="chart-band moderate" />
        {ticks.map((tick) => (
          <g key={tick}>
            <line x1={pad.left} y1={y(tick)} x2={width - pad.right} y2={y(tick)} className="chart-grid-line" />
            <text x={8} y={y(tick) + 4} className="chart-tick-label">{tick}</text>
          </g>
        ))}
        <line x1={pad.left} y1={height - pad.bottom} x2={width - pad.right} y2={height - pad.bottom} className="chart-axis" />
        <line x1={pad.left} y1={pad.top} x2={pad.left} y2={height - pad.bottom} className="chart-axis" />
        {areaPath && <path d={areaPath} className="chart-area" />}
        {linePath && <path d={linePath} className="chart-line" fill="none" />}
        {sorted.map((point, idx) => (
          <circle
            key={`${point.timestamp}-${idx}`}
            cx={x(idx)}
            cy={y(point.pm25_value)}
            r={idx === sorted.length - 1 ? "4.8" : "3.1"}
            className={point.series_type === "forecast" ? "chart-point forecast" : "chart-point"}
          />
        ))}
        <text x={pad.left} y={height - 8} className="chart-time-label">{fmtHour(firstLabel)}</text>
        <text x={width - pad.right} y={height - 8} textAnchor="end" className="chart-time-label">{fmtHour(lastLabel)}</text>
      </svg>
    </section>
  );
}
