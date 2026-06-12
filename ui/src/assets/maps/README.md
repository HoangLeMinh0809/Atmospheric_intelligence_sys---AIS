# Hanoi Local Basemap

The UI uses local vector map assets and does not call Overpass, OSM, or online map tiles at runtime.

Source: OpenStreetMap data via Overpass API, with fallback hand-curated local GeoJSON for development.

BBox used for inner Hanoi:

```text
south=20.95
west=105.75
north=21.10
east=105.95
```

Update the local assets manually:

```bash
cd ui
npm run fetch:hanoi-map
```

Output files:

- `hanoi_water.geojson`
- `hanoi_roads.geojson`
- `hanoi_boundaries.geojson`
- `hanoi_labels.json`

The fetch script is intentionally not part of `npm run build` or `npm run dev`.

Attribution: © OpenStreetMap contributors.
