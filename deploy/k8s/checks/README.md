# PM2.5 Serving Checks

Run the serving check Job:

```bash
kubectl -n ais apply -f deploy/k8s/checks/pm25-serving-check-job.yaml
kubectl -n ais logs -f job/pm25-serving-check
```

Run the visualization serving check Job:

```bash
kubectl -n ais apply -f deploy/k8s/checks/visualization-serving-check-job.yaml
kubectl -n ais logs -f job/visualization-serving-check
```

The check image reads `SERVING_FEATURE_TABLE`, `PREDICTION_TABLE`, `MODEL_REGISTRY_TABLE`, freshness limits, and `PM25_API_BASE_URL` from `ais-runtime-config`. A failing check exits non-zero so Airflow or Kubernetes can surface the failure.
