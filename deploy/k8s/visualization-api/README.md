# Visualization API on Kubernetes

Build the image:

```bash
docker build -t ais-visualization-api:local -f serving/visualization_api/Dockerfile .
```

Deploy:

```bash
kubectl -n ais apply -f deploy/k8s/visualization-api
kubectl -n ais wait --for=condition=available --timeout=120s deployment/visualization-api
kubectl -n ais port-forward svc/visualization-api 8082:80
```

The API reads only exported cache under `VIS_CACHE_BASE_URI`.
