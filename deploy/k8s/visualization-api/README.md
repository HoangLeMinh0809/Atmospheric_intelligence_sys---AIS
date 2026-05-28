# Visualization API

Deploy the TODO4 visualization API:

```bash
docker build -t ais-visualization-api:local -f serving/visualization_api/Dockerfile .
kubectl -n ais apply -f deploy/k8s/visualization-api
kubectl -n ais wait --for=condition=available --timeout=120s deployment/visualization-api
```

Smoke:

```bash
kubectl -n ais port-forward svc/visualization-api 8082:80
curl -i http://localhost:8082/healthz
curl -i http://localhost:8082/readyz
curl -i http://localhost:8082/api/v1/visualization/manifest/latest
```
