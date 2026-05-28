# PM2.5 Serving Checks

Run the serving check Job:

```bash
kubectl -n ais apply -f deploy/k8s/checks/pm25-serving-check-job.yaml
kubectl -n ais logs -f job/pm25-serving-check
```

The check image reads `SERVING_FEATURE_TABLE`, `PREDICTION_TABLE`, `MODEL_REGISTRY_TABLE`, freshness limits, and `PM25_API_BASE_URL` from `ais-runtime-config`. A failing check exits non-zero so Airflow or Kubernetes can surface the failure.

## Visualization serving checks

Run the TODO4 visualization check Job:

```bash
kubectl -n ais apply -f deploy/k8s/checks/visualization-serving-check-job.yaml
kubectl -n ais logs -f job/visualization-serving-check
```

The check validates the visualization cache manifest, required station/trajectory layers, forward plume availability or explicit unavailable reason, and Visualization API readiness via `VIS_API_BASE_URL`.
