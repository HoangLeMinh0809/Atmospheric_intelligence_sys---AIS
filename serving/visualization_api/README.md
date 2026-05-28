# AIS Visualization API

Serves TODO4 visualization cache for station observations, backward trajectories, and forward plume.

This API is intentionally lightweight: it reads JSON/GeoJSON cache files and never imports PySpark, runs HYSPLIT, or loads ML models in request handlers.

Required cache layout under `VIS_CACHE_BASE_URI`:

- `manifest/latest.json`
- `stations/latest.geojson`
- `trajectories/backward/latest.geojson`
- `plume/forward/latest/horizon=6/grid.geojson`
- `plume/forward/latest/horizon=12/grid.geojson`
- `plume/forward/latest/horizon=24/grid.geojson`

Run locally:

```bash
VIS_CACHE_BASE_URI=/tmp/ais_visualization_cache uvicorn main:app --host 0.0.0.0 --port 8080
```
