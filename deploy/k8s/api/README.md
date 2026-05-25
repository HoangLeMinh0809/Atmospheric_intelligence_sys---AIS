# PM2.5 API

Build and smoke-test locally:

```bash
docker build -t ais-pm25-api:local -f serving/pm25_api/Dockerfile .
docker run --rm -p 8081:8080 --env API_PORT=8080 ais-pm25-api:local
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
```

Deploy to Kubernetes:

```bash
kubectl -n ais apply -f deploy/k8s/api/pm25-api-deployment.yaml
kubectl -n ais apply -f deploy/k8s/api/pm25-api-service.yaml
kubectl -n ais port-forward svc/pm25-api 8081:80
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
```

The API process is a serving layer only. It must read materialized rows from `PREDICTION_TABLE`; it must not submit Spark jobs, run HYSPLIT, train models, or run inference inside request handlers.
