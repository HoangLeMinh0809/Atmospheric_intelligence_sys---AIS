import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const BBOX = { south: 20.95, west: 105.75, north: 21.1, east: 105.95 };
const OUT_DIR = path.resolve("src/assets/maps");
const OVERPASS_URLS = process.env.OVERPASS_URL
  ? [process.env.OVERPASS_URL]
  : [
      "https://overpass-api.de/api/interpreter",
      "https://overpass.kumi.systems/api/interpreter",
    ];

const query = `
[out:json][timeout:90];
(
  way["natural"="water"](${BBOX.south},${BBOX.west},${BBOX.north},${BBOX.east});
  relation["natural"="water"](${BBOX.south},${BBOX.west},${BBOX.north},${BBOX.east});
  way["waterway"~"river|canal|stream"](${BBOX.south},${BBOX.west},${BBOX.north},${BBOX.east});

  way["highway"~"motorway|trunk|primary|secondary|tertiary|primary_link|secondary_link|tertiary_link"](${BBOX.south},${BBOX.west},${BBOX.north},${BBOX.east});

  relation["boundary"="administrative"](${BBOX.south},${BBOX.west},${BBOX.north},${BBOX.east});
);
out body geom;
`;

function tagsOf(element) {
  return element.tags || {};
}

function propsOf(element) {
  return {
    osm_id: element.id,
    osm_type: element.type,
    ...tagsOf(element),
  };
}

function coordsFromGeometry(geometry = []) {
  return geometry
    .map((point) => [Number(point.lon), Number(point.lat)])
    .filter(([lon, lat]) => Number.isFinite(lon) && Number.isFinite(lat));
}

function insideBbox([lon, lat]) {
  return lon >= BBOX.west - 0.01 && lon <= BBOX.east + 0.01 && lat >= BBOX.south - 0.01 && lat <= BBOX.north + 0.01;
}

function filterToBbox(coords) {
  return coords.filter(insideBbox);
}

function isClosed(coords) {
  if (coords.length < 4) return false;
  const first = coords[0];
  const last = coords[coords.length - 1];
  return Math.abs(first[0] - last[0]) < 1e-9 && Math.abs(first[1] - last[1]) < 1e-9;
}

function simplifyCoords(coords, tolerance = 0.00018) {
  if (coords.length <= 2) return coords;
  const output = [coords[0]];
  let last = coords[0];
  for (let i = 1; i < coords.length - 1; i += 1) {
    const current = coords[i];
    if (Math.hypot(current[0] - last[0], current[1] - last[1]) >= tolerance) {
      output.push(current);
      last = current;
    }
  }
  output.push(coords[coords.length - 1]);
  return output;
}

function geometryCoords(geometry) {
  if (!geometry) return [];
  if (geometry.type === "LineString") return geometry.coordinates;
  if (geometry.type === "Polygon") return geometry.coordinates.flat();
  return [];
}

function geometrySpan(feature) {
  const coords = geometryCoords(feature.geometry);
  if (coords.length < 2) return 0;
  let span = 0;
  for (let i = 1; i < coords.length; i += 1) {
    span += Math.hypot(coords[i][0] - coords[i - 1][0], coords[i][1] - coords[i - 1][1]);
  }
  return span;
}

function compactLayer(features, layer) {
  const limit = layer === "roads" ? 1300 : layer === "water" ? 360 : 220;
  return features
    .filter((feature) => geometryCoords(feature.geometry).length >= 2)
    .filter((feature) => {
      const span = geometrySpan(feature);
      if (layer === "roads") return span >= 0.0025;
      if (layer === "water") return span >= 0.0012 || feature.properties?.name;
      return span >= 0.002;
    })
    .sort((a, b) => {
      const named = Number(Boolean(b.properties?.name)) - Number(Boolean(a.properties?.name));
      if (named) return named;
      return geometrySpan(b) - geometrySpan(a);
    })
    .slice(0, limit);
}

function wayToFeature(element) {
  const tags = tagsOf(element);
  const coords = simplifyCoords(filterToBbox(coordsFromGeometry(element.geometry)), tags.highway ? 0.00035 : 0.0004);
  if (coords.length < 2) return null;
  const polygon = isClosed(coords) && (tags.natural === "water" || tags.boundary === "administrative");
  return {
    type: "Feature",
    properties: propsOf(element),
    geometry: polygon
      ? { type: "Polygon", coordinates: [coords] }
      : { type: "LineString", coordinates: coords },
  };
}

function relationToFeatures(element) {
  const tags = tagsOf(element);
  const members = element.members || [];
  const features = [];
  for (const member of members) {
    const coords = simplifyCoords(filterToBbox(coordsFromGeometry(member.geometry)), 0.00045);
    if (coords.length < 2) continue;
    const polygon = isClosed(coords) && (tags.natural === "water" || tags.boundary === "administrative");
    features.push({
      type: "Feature",
      properties: { ...propsOf(element), role: member.role, member_type: member.type, member_ref: member.ref },
      geometry: polygon ? { type: "Polygon", coordinates: [coords] } : { type: "LineString", coordinates: coords },
    });
  }
  return features;
}

function toFeatureCollection(features) {
  return { type: "FeatureCollection", features };
}

function layerOf(feature) {
  const props = feature.properties || {};
  if (props.highway) return "roads";
  if (props.boundary === "administrative") return "boundaries";
  if (props.natural === "water" || props.water || props.waterway) return "water";
  return null;
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  let raw = null;
  let lastError = null;
  for (const overpassUrl of OVERPASS_URLS) {
    console.log(`Fetching Hanoi basemap from ${overpassUrl}`);
    const response = await fetch(overpassUrl, {
      method: "POST",
      headers: {
        "content-type": "text/plain;charset=UTF-8",
        "user-agent": "AIS local basemap fetch script",
      },
      body: query,
    });
    if (response.ok) {
      raw = await response.json();
      break;
    }
    const text = await response.text();
    lastError = `Overpass request failed at ${overpassUrl}: ${response.status} ${response.statusText}\n${text.slice(0, 1200)}`;
    console.warn(lastError);
  }
  if (!raw) {
    throw new Error(lastError || "Overpass request failed");
  }
  if (process.env.SAVE_RAW_OVERPASS === "1") {
    await writeFile(path.join(OUT_DIR, "raw_overpass_hanoi.json"), JSON.stringify(raw));
  }

  const layers = { water: [], roads: [], boundaries: [] };
  for (const element of raw.elements || []) {
    const features = element.type === "relation" ? relationToFeatures(element) : [wayToFeature(element)];
    for (const feature of features.filter(Boolean)) {
      const layer = layerOf(feature);
      if (layer) layers[layer].push(feature);
    }
  }

  for (const [layer, features] of Object.entries(layers)) {
    const compacted = compactLayer(features, layer);
    const file = path.join(OUT_DIR, `hanoi_${layer}.geojson`);
    await writeFile(file, `${JSON.stringify(toFeatureCollection(compacted))}\n`);
    console.log(`${layer}: ${compacted.length}/${features.length} features -> ${file}`);
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
