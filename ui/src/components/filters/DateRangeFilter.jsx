// File nay: component UI dung lai trong dashboard.
// Render component DateRangeFilter va gan state/props cho UI.
function DateRangeFilter({ startDate, endDate, onStartChange, onEndChange }) {
  return (
    <>
      <input
        type="date"
        value={startDate}
        onChange={(e) => onStartChange(e.target.value)}
      />
      <input
        type="date"
        value={endDate}
        onChange={(e) => onEndChange(e.target.value)}
      />
    </>
  );
}

export default DateRangeFilter;
