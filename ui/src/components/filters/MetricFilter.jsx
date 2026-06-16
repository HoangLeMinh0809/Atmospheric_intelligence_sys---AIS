// File nay: component UI dung lai trong dashboard.
// Render component MetricFilter va gan state/props cho UI.
function MetricFilter({ value, onChange, options }) {
  return (
    <select value={value} onChange={(e) => onChange(e.target.value)}>
      {options.map((item) => (
        <option key={item.value} value={item.value}>
          {item.label}
        </option>
      ))}
    </select>
  );
}

export default MetricFilter;
