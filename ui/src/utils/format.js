// File nay: helper frontend cho format, mapping hoac adapter du lieu.
// Dinh dang gia tri de hien thi gon va nhat quan tren UI.
export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return Number(value).toFixed(1);
}

// Dinh dang gia tri de hien thi gon va nhat quan tren UI.
export function formatDateTime(value) {
  return new Date(value).toLocaleString("vi-VN");
}
