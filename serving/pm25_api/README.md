# PM2.5 API (Kubernetes)

## Purpose

This API is a **serving-only** layer:
- Reads **only** the materialized prediction table (`PREDICTION_TABLE`, default: `ais.predictions.hanoi_pm25_forecast_gold`).
- Must **not** submit Spark jobs.
- Must **not** train models.
- Must **not** run inference inside request handlers.

## Endpoints

- `GET /healthz`: process liveness
- `GET /readyz`: config + catalog/table connectivity
- `GET /api/v1/hanoi/pm25/forecast/latest`: latest production forecast row

## Local build/run

```bash
docker build -t ais-pm25-api:local -f serving/pm25_api/Dockerfile .
docker run --rm -p 8081:8080 --env-file .env ais-pm25-api:local
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
curl -i http://localhost:8081/api/v1/hanoi/pm25/forecast/latest
```

## Kubernetes deploy

```bash
kubectl -n ais apply -f deploy/k8s/api/pm25-api-deployment.yaml
kubectl -n ais apply -f deploy/k8s/api/pm25-api-service.yaml
kubectl -n ais port-forward svc/pm25-api 8081:80
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
curl -i http://localhost:8081/api/v1/hanoi/pm25/forecast/latest
```

## Notes

- `PREDICTION_TABLE`, `ICEBERG_CATALOG`, `ICEBERG_WAREHOUSE`, and `HDFS_NAMENODE` must be provided via ConfigMap/Secret.
- If no prediction exists yet, the forecast endpoint must return a clear 404 JSON error.
