// File nay: component bieu do hien thi timeseries, forecast hoac thong ke.
import { useEffect, useRef } from "react";
import * as d3 from "d3";

const SIZE = { width: 860, height: 340, margin: { top: 28, right: 28, bottom: 52, left: 62 } };

// Tao khung SVG chung va tieu de cho tung bieu do thong ke.
function frame(ref, title) {
  const { width, height, margin } = SIZE;
  const root = d3.select(ref.current);
  root.selectAll("*").remove();
  const svg = root.append("svg").attr("viewBox", `0 0 ${width} ${height}`).attr("role", "img");
  svg.append("text").attr("x", margin.left).attr("y", 17).attr("class", "statistics-chart-title").text(title);
  return svg;
}

// Ve truc x/y dung chung cho cac chart D3 trong man thong ke.
function axes(svg, x, y) {
  const { height, margin } = SIZE;
  svg.append("g").attr("class", "statistics-axis").attr("transform", `translate(0,${height - margin.bottom})`).call(d3.axisBottom(x).ticks(6));
  svg.append("g").attr("class", "statistics-axis").attr("transform", `translate(${margin.left},0)`).call(d3.axisLeft(y).ticks(6));
}

// Loai bo gia tri khong hop le truoc khi tinh phan bo hoac quantile.
function finiteValues(values) {
  return values.map(Number).filter(Number.isFinite);
}

// Tinh phan vi cho box plot va CDF.
function quantile(values, p) {
  if (!values.length) return null;
  return d3.quantile([...values].sort((a, b) => a - b), p);
}

// Chia PM2.5 thanh cac nhom muc do de ve chart ty trong rui ro.
function riskBuckets(values) {
  return [
    { label: "Thấp", value: values.filter((item) => item < 25).length, color: "#5eead4" },
    { label: "Trung bình", value: values.filter((item) => item >= 25 && item < 40).length, color: "#bef264" },
    { label: "Cao", value: values.filter((item) => item >= 40 && item < 55).length, color: "#fdba74" },
    { label: "Rất cao", value: values.filter((item) => item >= 55).length, color: "#f87171" },
  ];
}

// Render component StatisticsCharts va gan state/props cho UI.
export default function StatisticsCharts({ points, distribution, forecastBars, sourceSummary = [] }) {
  const lineRef = useRef();
  const histogramRef = useRef();
  const forecastRef = useRef();
  const riskPieRef = useRef();
  const cdfRef = useRef();
  const boxRef = useRef();
  const sourceRef = useRef();

  useEffect(() => {
    const parsed = points
      .map((row) => ({ date: new Date(row.timestamp), value: Number(row.pm25_value ?? row.pm25) }))
      .filter((row) => !Number.isNaN(row.date.valueOf()) && Number.isFinite(row.value));
    const svg = frame(lineRef, "Xu hướng PM2.5 theo thời gian");
    if (!parsed.length) return;
    const { width, height, margin } = SIZE;
    const x = d3.scaleTime().domain(d3.extent(parsed, (row) => row.date)).range([margin.left, width - margin.right]);
    const y = d3.scaleLinear().domain([0, d3.max(parsed, (row) => row.value) || 1]).nice().range([height - margin.bottom, margin.top]);
    axes(svg, x, y);
    svg.append("path").datum(parsed).attr("class", "statistics-line").attr("d", d3.line().x((row) => x(row.date)).y((row) => y(row.value)));
  }, [points]);

  useEffect(() => {
    const values = distribution.map(Number).filter(Number.isFinite);
    const svg = frame(histogramRef, "Phân bố PM2.5 tại các trạm/grid hiện tại");
    if (!values.length) return;
    const { width, height, margin } = SIZE;
    const x = d3.scaleLinear().domain([0, d3.max(values) || 1]).nice().range([margin.left, width - margin.right]);
    const bins = d3.bin().domain(x.domain()).thresholds(12)(values);
    const y = d3.scaleLinear().domain([0, d3.max(bins, (bin) => bin.length) || 1]).nice().range([height - margin.bottom, margin.top]);
    axes(svg, x, y);
    svg.selectAll("rect").data(bins).join("rect")
      .attr("class", "statistics-bar")
      .attr("x", (bin) => x(bin.x0) + 1)
      .attr("width", (bin) => Math.max(0, x(bin.x1) - x(bin.x0) - 2))
      .attr("y", (bin) => y(bin.length))
      .attr("height", (bin) => y(0) - y(bin.length));
  }, [distribution]);

  useEffect(() => {
    const values = forecastBars.filter((row) => Number.isFinite(row.value));
    const svg = frame(forecastRef, "PM2.5 hiện tại và các mốc dự báo");
    if (!values.length) return;
    const { width, height, margin } = SIZE;
    const x = d3.scaleBand().domain(values.map((row) => row.label)).range([margin.left, width - margin.right]).padding(0.3);
    const y = d3.scaleLinear().domain([0, d3.max(values, (row) => row.value) || 1]).nice().range([height - margin.bottom, margin.top]);
    axes(svg, x, y);
    svg.selectAll("rect").data(values).join("rect")
      .attr("class", "statistics-bar forecast")
      .attr("x", (row) => x(row.label))
      .attr("width", x.bandwidth())
      .attr("y", (row) => y(row.value))
      .attr("height", (row) => y(0) - y(row.value));
    svg.selectAll(".statistics-value").data(values).join("text")
      .attr("class", "statistics-value")
      .attr("x", (row) => x(row.label) + x.bandwidth() / 2)
      .attr("y", (row) => y(row.value) - 7)
      .text((row) => row.value.toFixed(1));
  }, [forecastBars]);

  useEffect(() => {
    const values = finiteValues(distribution);
    const buckets = riskBuckets(values).filter((row) => row.value > 0);
    const svg = frame(riskPieRef, "Tỷ trọng mức PM2.5");
    if (!buckets.length) return;
    const { width, height } = SIZE;
    const radius = Math.min(width, height) * 0.32;
    const center = svg.append("g").attr("transform", `translate(${width / 2 - 80},${height / 2 + 8})`);
    const arc = d3.arc().innerRadius(radius * 0.52).outerRadius(radius);
    const pie = d3.pie().value((row) => row.value).sort(null);
    center.selectAll("path").data(pie(buckets)).join("path")
      .attr("d", arc)
      .attr("fill", (row) => row.data.color)
      .attr("stroke", "rgba(15, 23, 42, 0.92)")
      .attr("stroke-width", 2);
    center.append("text").attr("class", "statistics-value").attr("y", -4).text(values.length);
    center.append("text").attr("class", "statistics-axis").attr("y", 16).attr("text-anchor", "middle").text("samples");
    const legend = svg.append("g").attr("transform", `translate(${width - 260},74)`);
    legend.selectAll("g").data(buckets).join("g")
      .attr("transform", (_, index) => `translate(0,${index * 30})`)
      .each(function draw(row) {
        const item = d3.select(this);
        item.append("rect").attr("width", 14).attr("height", 14).attr("rx", 3).attr("fill", row.color);
        item.append("text").attr("x", 22).attr("y", 12).attr("class", "statistics-axis").text(`${row.label}: ${row.value}`);
      });
  }, [distribution]);

  useEffect(() => {
    const values = finiteValues(distribution).sort((a, b) => a - b);
    const svg = frame(cdfRef, "Đường tích lũy PM2.5");
    if (values.length < 2) return;
    const { width, height, margin } = SIZE;
    const points = values.map((value, index) => ({ value, percentile: index / (values.length - 1) }));
    const x = d3.scaleLinear().domain([0, d3.max(values) || 1]).nice().range([margin.left, width - margin.right]);
    const y = d3.scaleLinear().domain([0, 1]).range([height - margin.bottom, margin.top]);
    axes(svg, x, y);
    svg.append("path")
      .datum(points)
      .attr("class", "statistics-line cdf")
      .attr("d", d3.line().x((row) => x(row.value)).y((row) => y(row.percentile)));
  }, [distribution]);

  useEffect(() => {
    const values = finiteValues(distribution);
    const svg = frame(boxRef, "Box plot PM2.5");
    if (values.length < 2) return;
    const { width, height, margin } = SIZE;
    const min = d3.min(values);
    const max = d3.max(values);
    const q1 = quantile(values, 0.25);
    const med = quantile(values, 0.5);
    const q3 = quantile(values, 0.75);
    const x = d3.scaleLinear().domain([0, max || 1]).nice().range([margin.left, width - margin.right]);
    const y = height / 2 + 12;
    axes(svg, x, d3.scaleLinear().domain([0, 1]).range([height - margin.bottom, margin.top]));
    svg.selectAll(".tick").filter((_, index, nodes) => d3.select(nodes[index].parentNode).attr("transform")?.startsWith("translate(58")).remove();
    svg.append("line").attr("class", "statistics-box-line").attr("x1", x(min)).attr("x2", x(max)).attr("y1", y).attr("y2", y);
    svg.append("rect").attr("class", "statistics-box").attr("x", x(q1)).attr("y", y - 34).attr("width", Math.max(2, x(q3) - x(q1))).attr("height", 68);
    svg.append("line").attr("class", "statistics-box-median").attr("x1", x(med)).attr("x2", x(med)).attr("y1", y - 42).attr("y2", y + 42);
    [
      ["min", min],
      ["p25", q1],
      ["p50", med],
      ["p75", q3],
      ["max", max],
    ].forEach(([label, value], index) => {
      svg.append("text").attr("class", "statistics-value").attr("x", x(value)).attr("y", y + 72 + (index % 2) * 18).text(`${label} ${value.toFixed(1)}`);
    });
  }, [distribution]);

  useEffect(() => {
    const values = sourceSummary.filter((row) => Number.isFinite(Number(row.value)) && Number(row.value) > 0);
    const svg = frame(sourceRef, "Khối lượng dữ liệu theo nguồn");
    if (!values.length) return;
    const { width, height, margin } = SIZE;
    const x = d3.scaleBand().domain(values.map((row) => row.label)).range([margin.left, width - margin.right]).padding(0.22);
    const y = d3.scaleLinear().domain([0, d3.max(values, (row) => Number(row.value)) || 1]).nice().range([height - margin.bottom, margin.top]);
    axes(svg, x, y);
    svg.selectAll("rect").data(values).join("rect")
      .attr("class", "statistics-bar source")
      .attr("x", (row) => x(row.label))
      .attr("width", x.bandwidth())
      .attr("y", (row) => y(Number(row.value)))
      .attr("height", (row) => y(0) - y(Number(row.value)));
  }, [sourceSummary]);

  return (
    <div className="statistics-chart-grid">
      <div className="statistics-chart-card wide trend" ref={lineRef} />
      <div className="statistics-chart-card" ref={histogramRef} />
      <div className="statistics-chart-card" ref={riskPieRef} />
      <div className="statistics-chart-card wide" ref={forecastRef} />
      <div className="statistics-chart-card" ref={cdfRef} />
      <div className="statistics-chart-card" ref={boxRef} />
      <div className="statistics-chart-card" ref={sourceRef} />
    </div>
  );
}
