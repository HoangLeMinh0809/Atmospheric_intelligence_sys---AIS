// File nay: component UI dung lai trong dashboard.
// Render component StatCard va gan state/props cho UI.
function StatCard({ title, value, unit = "" }) {
  return (
    <div className="stat-card">
      <h4>{title}</h4>
      <p>
        {value}
        {unit}
      </p>
    </div>
  );
}

export default StatCard;
