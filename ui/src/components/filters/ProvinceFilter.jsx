// File nay: component UI dung lai trong dashboard.
import { getProvinceName } from "../../utils/provinceMap";

// Render component ProvinceFilter va gan state/props cho UI.
function ProvinceFilter({ value, onChange, options }) {
  return (
    <select value={value} onChange={(e) => onChange(e.target.value)}>
      {options.map((province) => (
        <option key={province} value={province}>
          {province === "ALL" ? "Tất cả tỉnh/thành" : getProvinceName(province)}
        </option>
      ))}
    </select>
  );
}

export default ProvinceFilter;
