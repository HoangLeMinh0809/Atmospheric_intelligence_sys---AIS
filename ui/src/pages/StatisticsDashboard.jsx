// File nay: page React gom API calls, state va layout man hinh.
import { useEffect, useMemo, useState } from "react";
import StatisticsCharts from "../components/charts/StatisticsCharts";
import {
  getForecastLatest,
  getLiveHeatmapLatest,
  getPM25TimeseriesLatest,
  getStationsLatest,
} from "../services/visualizationApi";

// Khai bao class numeric de gom state, cau hinh hoac hanh vi lien quan.
function numeric(values) {
  return values.map(Number).filter(Number.isFinite);
}

// Khai bao class numberOrNull de gom state, cau hinh hoac hanh vi lien quan.
function numberOrNull(value) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

// Khai bao class percentile de gom state, cau hinh hoac hanh vi lien quan.
function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, (sorted.length - 1) * p));
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  const frac = idx - lo;
  return sorted[lo] * (1 - frac) + sorted[hi] * frac;
}

// Khai bao class countWhere de gom state, cau hinh hoac hanh vi lien quan.
function countWhere(values, test) {
  return values.reduce((count, value) => count + (test(value) ? 1 : 0), 0);
}

function fmt(value, digits = 1) {
  return Number.isFinite(value) ? value.toFixed(digits) : "--";
}

function trendPoints(points) {
  return [...(points || [])]
    .map((row) => ({
      timestamp: row.timestamp || row.base_hour,
      value: numberOrNull(row.pm25_value ?? row.pm25 ?? row.value ?? row.pm25_mean),
    }))
    .filter((row) => row.timestamp && row.value != null)
    .sort((left, right) => String(left.timestamp).localeCompare(String(right.timestamp)))
    .slice(-48);
}

function sparkPath(points, width, height) {
  if (!points.length) return "";
  const values = points.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  return points
    .map((point, index) => {
      const x = 10 + (index / Math.max(points.length - 1, 1)) * (width - 20);
      const y = height - 14 - ((point.value - min) / span) * (height - 26);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

// Tai du lieu realtime va render man hinh thong ke PM2.5.
export default function StatisticsDashboard() {
  const [data, setData] = useState({ forecast: null, heatmap: null, timeseries: null, stations: null });
  const [status, setStatus] = useState({ loading: true, error: "" });

  useEffect(() => {
    // Tai song song nhieu payload de giam do tre cho man hinh.
    Promise.all([
      getForecastLatest("hanoi"),
      getLiveHeatmapLatest("hanoi"),
      getPM25TimeseriesLatest("hanoi"),
      getStationsLatest(),
    ])
      .then(([forecast, heatmap, timeseries, stations]) => {
        setData({ forecast, heatmap, timeseries, stations });
        setStatus({ loading: false, error: "" });
      })
      .catch((error) => setStatus({ loading: false, error: error.message }));
  }, []);

  const points = useMemo(() => data.timeseries?.points || [], [data.timeseries]);
  const distribution = useMemo(() => {
    const liveStationValues = numeric((data.stations?.features || []).map((feature) => feature?.properties?.pm25_value ?? feature?.properties?.pm25));
    const liveHeatmapValues = numeric((data.heatmap?.features || []).map((feature) => feature?.properties?.pm25_value));
    return [...liveStationValues, ...liveHeatmapValues];
  }, [data.heatmap, data.stations]);
  const forecastBars = useMemo(() => {
    const forecast = data.forecast?.forecast || {};
    return [
      { label: "Hiện tại", value: numberOrNull(data.forecast?.pm25_now ?? forecast.now?.pm25 ?? data.heatmap?.summary?.pm25_mean) },
      { label: "+6h", value: numberOrNull(forecast["6h"]?.pm25) },
      { label: "+12h", value: numberOrNull(forecast["12h"]?.pm25) },
      { label: "+24h", value: numberOrNull(forecast["24h"]?.pm25) },
    ];
  }, [data.forecast, data.heatmap]);
  const avg = distribution.length ? distribution.reduce((sum, value) => sum + value, 0) / distribution.length : null;
  const sorted = [...distribution].sort((a, b) => a - b);
  const median = sorted.length ? sorted[Math.floor(sorted.length / 2)] : null;
  const min = sorted.length ? sorted[0] : null;
  const max = sorted.length ? sorted[sorted.length - 1] : null;
  const p90 = percentile(sorted, 0.9);
  const p95 = percentile(sorted, 0.95);
  const liveGridCount = data.heatmap?.features?.length || 0;
  const stationCount = data.stations?.features?.length || 0;
  const lowCount = countWhere(distribution, (value) => value < 25);
  const midCount = countWhere(distribution, (value) => value >= 25 && value < 40);
  const highCount = countWhere(distribution, (value) => value >= 40);
  const trend = useMemo(() => trendPoints(points), [points]);
  const latestTrend = trend.at(-1)?.value ?? null;
  const previousTrend = trend.length > 1 ? trend[trend.length - 2].value : null;
  const delta = latestTrend != null && previousTrend != null ? latestTrend - previousTrend : null;
  const coverage = liveGridCount + stationCount;
  const hotShare = distribution.length ? (highCount / distribution.length) * 100 : null;
  const riskBuckets = [
    { label: "Low", value: lowCount, className: "low" },
    { label: "Mid", value: midCount, className: "mid" },
    { label: "High", value: highCount, className: "high" },
  ];
  const sourceSummary = [
    { label: "Live grid", value: liveGridCount },
    { label: "Stations", value: stationCount },
    { label: "Timeseries", value: points.length },
  ];

  return (
    <main className="statistics-page">
      <header className="statistics-header">
        <div>
          <span className="statistics-kicker">AIS Data Statistics</span>
          <h1>Thống kê dữ liệu khí quyển</h1>
          <p>Thống kê trực tiếp từ realtime serving API và Cassandra latest state.</p>
        </div>
        <a className="dashboard-link" href="#/">Quay lại bản đồ</a>
      </header>
      {status.error ? <div className="status-toast error">{status.error}</div> : null}
      <section className="statistics-overview">
        <div className="overview-primary">
          <span>Live PM2.5 average</span>
          <strong>{fmt(avg)}</strong>
          <em>µg/m³ · P95 {fmt(p95)} · max {fmt(max)}</em>
        </div>
        <div className="overview-trend">
          <div className="overview-panel-heading">
            <span>48h history</span>
            <strong>{delta == null ? "stable" : `${delta >= 0 ? "+" : ""}${delta.toFixed(1)}`}</strong>
          </div>
          <svg viewBox="0 0 420 126" role="img" aria-label="PM2.5 48 hour trend">
            <line x1="10" x2="410" y1="90" y2="90" className="overview-threshold high" />
            <line x1="10" x2="410" y1="64" y2="64" className="overview-threshold mid" />
            <path d={sparkPath(trend, 420, 126)} className="overview-sparkline" />
          </svg>
        </div>
        <div className="overview-risk">
          <div className="overview-panel-heading">
            <span>Current risk mix</span>
            <strong>{hotShare == null ? "--" : `${hotShare.toFixed(0)}% hot`}</strong>
          </div>
          <div className="risk-stack">
            {riskBuckets.map((bucket) => (
              <i
                key={bucket.label}
                className={bucket.className}
                style={{ flexGrow: Math.max(bucket.value, distribution.length ? 1 : 0) }}
                title={`${bucket.label}: ${bucket.value}`}
              />
            ))}
          </div>
          <div className="risk-count-row">
            {riskBuckets.map((bucket) => (
              <span key={bucket.label}>{bucket.label} <b>{bucket.value}</b></span>
            ))}
          </div>
        </div>
        <div className="overview-forecast">
          {forecastBars.map((item) => (
            <span key={item.label}>
              <em>{item.label}</em>
              <strong>{fmt(item.value)}</strong>
            </span>
          ))}
        </div>
        <div className="overview-coverage">
          <span>Coverage</span>
          <strong>{coverage || "--"}</strong>
          <em>{liveGridCount} grid · {stationCount} stations · {trend.length} history</em>
        </div>
        <div className="overview-quantiles">
          <span>Distribution</span>
          <strong>{fmt(min)} / {fmt(median)} / {fmt(p90)}</strong>
          <em>min / median / p90</em>
        </div>
      </section>
      {status.loading ? (
        <div className="panel">Đang tải dữ liệu thống kê...</div>
      ) : (
        <StatisticsCharts points={points} distribution={distribution} forecastBars={forecastBars} sourceSummary={sourceSummary} />
      )}
    </main>
  );
}
