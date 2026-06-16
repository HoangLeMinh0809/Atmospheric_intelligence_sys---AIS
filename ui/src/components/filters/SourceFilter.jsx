// File nay: component UI dung lai trong dashboard.
// Render component SourceFilter va gan state/props cho UI.
function SourceFilter({ value, onChange, options }) {
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

export default SourceFilter;
