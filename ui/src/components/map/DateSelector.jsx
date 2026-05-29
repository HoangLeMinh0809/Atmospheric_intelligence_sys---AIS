export default function DateSelector({ value, availableDates, onChange }) {
  const dates = availableDates || [];
  return (
    <div className="date-selector">
      <input
        aria-label="Visualization date"
        type="date"
        value={value || ""}
        list="available-visualization-dates"
        onChange={(event) => onChange(event.target.value)}
      />
      <datalist id="available-visualization-dates">
        {dates.map((date) => (
          <option value={date} key={date} />
        ))}
      </datalist>
    </div>
  );
}
