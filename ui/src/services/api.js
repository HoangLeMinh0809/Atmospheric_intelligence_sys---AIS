import {
  getForecastLatest,
  getLiveHeatmapLatest,
  getPM25TimeseriesLatest,
  getSourceAttributionLatest,
  getStationsLatest,
} from "./visualizationApi";

const HANOI = "ha_noi";

function pm25ToAqi(value) {
  if (value == null || value === "") return null;
  const pm25 = Number(value);
  if (!Number.isFinite(pm25)) return null;
  if (pm25 <= 12) return Math.round((50 / 12) * pm25);
  if (pm25 <= 35.4) return Math.round(51 + ((100 - 51) / (35.4 - 12.1)) * (pm25 - 12.1));
  if (pm25 <= 55.4) return Math.round(101 + ((150 - 101) / (55.4 - 35.5)) * (pm25 - 35.5));
  return Math.min(500, Math.round(151 + (pm25 - 55.5) * 2));
}

function timeseriesRows(payload) {
  return (payload?.points || []).map((row) => {
    const rawPm25 = row.pm25 ?? row.pm25_value ?? row.value;
    const pm25 = rawPm25 == null || rawPm25 === "" ? null : Number(rawPm25);
    return {
      timestamp: row.timestamp || row.event_time || row.base_time,
      province: HANOI,
      pm25: Number.isFinite(pm25) ? pm25 : null,
      pm10: row.pm10 ?? null,
      no2: row.no2 ?? null,
      aqi: pm25ToAqi(pm25),
      series_type: row.series_type || (row.is_forecast ? "forecast" : "observed"),
    };
  }).filter((row) => row.timestamp);
}

export async function getRealtimeOpenAQ() {
  const [forecast, timeseries, stations] = await Promise.all([
    getForecastLatest("hanoi"),
    getPM25TimeseriesLatest("hanoi"),
    getStationsLatest(),
  ]);
  const rows = timeseriesRows(timeseries);
  const latest = rows.filter((row) => row.series_type !== "forecast").at(-1) || rows.at(-1) || {};
  const stationValues = (stations?.features || [])
    .map((feature) => Number(feature?.properties?.pm25_value ?? feature?.properties?.pm25))
    .filter(Number.isFinite);
  const rawPm25 = forecast?.pm25_now ?? forecast?.forecast?.now?.pm25 ?? latest.pm25;
  const pm25 = rawPm25 == null || rawPm25 === "" ? null : Number(rawPm25);
  const resolved = Number.isFinite(pm25) ? pm25 : null;
  return {
    timestamp: forecast?.generated_at || timeseries?.generated_at,
    summary: {
      avg_pm25: resolved,
      avg_aqi: pm25ToAqi(resolved),
      worst_region: HANOI,
      alert_regions: resolved >= 35 ? 1 : 0,
      station_count: stationValues.length,
    },
    provinces: [{ province: HANOI, pm25: resolved, pm10: null, no2: null, aqi: pm25ToAqi(resolved) }],
    series: { [HANOI]: rows },
  };
}

export async function getRealtimeWeather() {
  const heatmap = await getLiveHeatmapLatest("hanoi");
  const summary = heatmap?.summary || {};
  const row = {
    timestamp: heatmap?.base_hour || heatmap?.generated_at,
    province: HANOI,
    temp: summary.temperature_2m_c ?? null,
    humidity: summary.humidity ?? null,
    wind: summary.wind_speed ?? null,
    pressure: summary.surface_pressure ?? null,
    precip: summary.total_precipitation_mm ?? null,
  };
  return {
    timestamp: heatmap?.generated_at,
    provinces: [row],
    series: { [HANOI]: [row] },
  };
}

export async function getHistoricalOpenAQ() {
  return timeseriesRows(await getPM25TimeseriesLatest("hanoi"));
}

export async function getHistoricalWeather() {
  const [timeseries, heatmap] = await Promise.all([
    getPM25TimeseriesLatest("hanoi"),
    getLiveHeatmapLatest("hanoi"),
  ]);
  const summary = heatmap?.summary || {};
  return timeseriesRows(timeseries).map((row) => ({
    timestamp: row.timestamp,
    province: HANOI,
    temp: summary.temperature_2m_c ?? null,
    humidity: summary.humidity ?? null,
    wind: summary.wind_speed ?? null,
    pressure: summary.surface_pressure ?? null,
    precip: summary.total_precipitation_mm ?? null,
  }));
}

export async function getHistoricalSentinel() {
  const payload = await getSourceAttributionLatest("hanoi");
  return (payload?.features || []).map((feature, index) => ({
    id: feature.id || `source-${index}`,
    product_type: feature?.properties?.source_label || "Trajectory + satellite attribution",
    start_time_utc: payload?.base_time || payload?.generated_at || "",
    publication_date: payload?.generated_at || "",
  }));
}
