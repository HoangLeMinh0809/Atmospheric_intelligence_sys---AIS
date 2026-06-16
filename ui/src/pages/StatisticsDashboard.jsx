// File nay: page React gom API calls, state va layout man hinh.
import { useEffect, useMemo, useState } from "react";
import StatCard from "../components/cards/StatCard";
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

  const points = data.timeseries?.points || [];
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
  const forecast = data.forecast?.forecast || {};
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
      <section className="card-grid">
        <StatCard title="PM2.5 trung bình" value={avg == null ? "--" : avg.toFixed(1)} unit=" µg/m³" />
        <StatCard title="PM2.5 trung vị" value={median == null ? "--" : median.toFixed(1)} unit=" µg/m³" />
        <StatCard title="PM2.5 thấp nhất" value={min == null ? "--" : min.toFixed(1)} unit=" µg/m³" />
        <StatCard title="PM2.5 cao nhất" value={max == null ? "--" : max.toFixed(1)} unit=" µg/m³" />
        <StatCard title="P90 PM2.5" value={p90 == null ? "--" : p90.toFixed(1)} unit=" µg/m³" />
        <StatCard title="P95 PM2.5" value={p95 == null ? "--" : p95.toFixed(1)} unit=" µg/m³" />
        <StatCard title="Điểm quan sát" value={distribution.length || "--"} />
        <StatCard title="Điểm live grid" value={liveGridCount || "--"} />
        <StatCard title="Trạm quan trắc" value={stationCount || "--"} />
        <StatCard title="Chuỗi thời gian" value={points.length || "--"} />
        <StatCard title="Mức thấp" value={lowCount || "--"} />
        <StatCard title="Mức trung bình" value={midCount || "--"} />
        <StatCard title="Mức cao" value={highCount || "--"} />
        <StatCard title="Dự báo +6h" value={numberOrNull(forecast["6h"]?.pm25)?.toFixed(1) || "--"} unit=" µg/m³" />
        <StatCard title="Dự báo +12h" value={numberOrNull(forecast["12h"]?.pm25)?.toFixed(1) || "--"} unit=" µg/m³" />
        <StatCard title="Dự báo +24h" value={numberOrNull(forecast["24h"]?.pm25)?.toFixed(1) || "--"} unit=" µg/m³" />
      </section>
      {status.loading ? (
        <div className="panel">Đang tải dữ liệu thống kê...</div>
      ) : (
        <StatisticsCharts points={points} distribution={distribution} forecastBars={forecastBars} sourceSummary={sourceSummary} />
      )}
    </main>
  );
}
