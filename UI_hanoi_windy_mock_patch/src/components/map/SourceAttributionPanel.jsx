export default function SourceAttributionPanel({ sourceAttribution, plume }) {
  const features = sourceAttribution?.features || [];
  const top = features[0]?.properties || {};
  return (
    <section className="map-panel source-panel">
      <h3>Source Attribution</h3>
      <div className="source-main">
        <strong>{top.source_label || "No source cluster yet"}</strong>
        <span>Score {top.contribution_score == null ? "-" : Number(top.contribution_score).toFixed(2)}</span>
        <span>Confidence {top.confidence == null ? "-" : Number(top.confidence).toFixed(2)}</span>
      </div>
      <p>{top.explanation_vi || "Waiting for trajectory and satellite evidence in the visualization cache."}</p>
      {plume?.available === false && <div className="unavailable-note">Forward plume unavailable: {plume.reason}</div>}
    </section>
  );
}
