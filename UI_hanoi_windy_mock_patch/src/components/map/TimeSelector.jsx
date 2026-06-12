const HORIZONS = [
  { value: 0, label: "Latest" },
  { value: 6, label: "+6h" },
  { value: 12, label: "+12h" },
  { value: 24, label: "+24h" },
];

export default function TimeSelector({ horizon, onChange }) {
  return (
    <div className="time-selector" aria-label="Forecast horizon">
      {HORIZONS.map((item) => (
        <button
          key={item.value}
          className={horizon === item.value ? "segmented-btn active" : "segmented-btn"}
          type="button"
          onClick={() => onChange(item.value)}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}
