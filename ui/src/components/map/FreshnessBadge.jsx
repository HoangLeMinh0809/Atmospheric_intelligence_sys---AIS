// File nay: component ban do hien thi layer PM2.5, station, trajectory va plume.
// Render component FreshnessBadge va gan state/props cho UI.
export default function FreshnessBadge({ forecast }) {
  const freshness = forecast?.freshness || {};
  const prediction = freshness.prediction_freshness_minutes;
  const observation = freshness.observation_freshness_minutes;
  const stale = Math.max(prediction || 0, observation || 0) > 180;

  return (
    <div className={stale ? "freshness-badge stale" : "freshness-badge"}>
      <span>{stale ? "Stale" : "Fresh"}</span>
      <strong>{prediction ?? "-"}m pred</strong>
      <strong>{observation ?? "-"}m obs</strong>
    </div>
  );
}
