# AIS Visualization API

FastAPI serving layer for TODO4 product data.

This service is request-time lightweight:

- reads `VIS_CACHE_BASE_URI/manifest/latest.json`;
- reads exported JSON/GeoJSON cache files referenced by that manifest;
- does not import PySpark, create `SparkSession`, submit Spark jobs, run HYSPLIT, train models, or run inference.

Required cache is produced by:

```bash
bash scripts/submit_spark_k8s.sh visualization-export-cache
```

Build and run locally:

```bash
docker build -t ais-visualization-api:local -f serving/visualization_api/Dockerfile .
docker run --rm -p 8082:8080 --env-file .env ais-visualization-api:local
curl -i http://localhost:8082/healthz
curl -i http://localhost:8082/readyz
```

Core endpoints:

- `GET /api/v1/visualization/manifest/latest`
- `GET /api/v1/visualization/pm25/heatmap/latest?horizon_h=0`
- `GET /api/v1/visualization/trajectories/backward/latest`
- `GET /api/v1/visualization/plume/forward/latest?horizon_h=6`
- `GET /api/v1/visualization/forecast/latest?location_id=hanoi`
- `GET /api/v1/visualization/timeseries/latest?location_id=hanoi`
- `GET /api/v1/visualization/source-attribution/latest?location_id=hanoi`
- `GET /api/v1/visualization/stations/latest`
