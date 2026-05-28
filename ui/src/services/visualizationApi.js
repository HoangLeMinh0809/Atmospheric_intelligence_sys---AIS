const API_BASE = import.meta.env.VITE_VIS_API_BASE || "/api/v1/visualization";

async function getJson(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Visualization API ${response.status}: ${detail}`);
  }
  return response.json();
}

export function getManifestLatest() {
  return getJson("/manifest/latest");
}

export function getStationsLatest() {
  return getJson("/stations/latest");
}

export function getBackwardTrajectoriesLatest() {
  return getJson("/trajectories/backward/latest");
}

export function getForwardPlumeLatest(horizonH) {
  return getJson(`/plume/forward/latest?horizon_h=${horizonH}`);
}
