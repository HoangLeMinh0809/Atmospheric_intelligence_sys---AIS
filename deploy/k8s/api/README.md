# PM2.5 API

Build and smoke-test locally:

```bash
docker build -t ais-pm25-api:local -f serving/pm25_api/Dockerfile .
docker run --rm -p 8081:8080 --env-file .env ais-pm25-api:local
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
curl -i http://localhost:8081/api/v1/hanoi/pm25/forecast/latest
```

Deploy to Kubernetes:

```bash
kubectl -n ais apply -f deploy/k8s/api/pm25-api-deployment.yaml
kubectl -n ais apply -f deploy/k8s/api/pm25-api-service.yaml
kubectl -n ais port-forward svc/pm25-api 8081:80
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
curl -i http://localhost:8081/api/v1/hanoi/pm25/forecast/latest
```

Semantics:
- `/healthz` only checks that the process is alive.
- `/readyz` validates required config (`PREDICTION_TABLE`, Iceberg catalog/warehouse) and that the prediction table is reachable.
- The forecast endpoint reads **only** `PREDICTION_TABLE` and returns the latest row where `model_status='production'`.
- If no prediction is available, the forecast endpoint returns HTTP 404 with:

```json
{"error":"prediction_not_found","location":"hanoi"}
```

The API process is a serving layer only. It must not submit Spark jobs, run HYSPLIT, train models, or run inference inside request handlers.
