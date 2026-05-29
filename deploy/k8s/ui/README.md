# AIS UI on Kubernetes

Build:

```bash
docker build -t ais-ui:local -f ui/Dockerfile .
```

Deploy:

```bash
kubectl -n ais apply -f deploy/k8s/ui
kubectl -n ais wait --for=condition=available --timeout=120s deployment/ais-ui
kubectl -n ais port-forward svc/ais-ui 3000:80
```

Production builds use `VITE_USE_MOCK_DATA=false` and call the visualization API through `/api/v1/visualization`.
