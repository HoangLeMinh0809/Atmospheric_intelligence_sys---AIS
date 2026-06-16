// File nay: component UI dung lai trong dashboard.
// Render component PageContainer va gan state/props cho UI.
function PageContainer({ title, subtitle, children }) {
  return (
    <div className="page-container">
      <div>
        <h1 className="page-title">{title}</h1>
        {subtitle ? <p className="page-subtitle">{subtitle}</p> : null}
      </div>
      {children}
    </div>
  );
}

export default PageContainer;
