# PM2.5 ML Jobs

Build the ML image from the repository root:

```bash
docker build -t ais-ml-runtime:local -f ml/Dockerfile .
docker run --rm ais-ml-runtime:local python ml/train_hanoi_pm25.py --help
docker run --rm ais-ml-runtime:local python ml/predict_hanoi_pm25.py --help
```

Run inference once in Kubernetes:

```bash
kubectl -n ais apply -f deploy/k8s/ml/pm25-predict-job.yaml
kubectl -n ais logs -f job/pm25-predict
```

Create the scheduled inference CronJob:

```bash
kubectl -n ais apply -f deploy/k8s/ml/pm25-predict-cronjob.yaml
kubectl -n ais create job pm25-predict-manual --from=cronjob/pm25-predict
```

The CronJob uses `concurrencyPolicy: Forbid`, small retry limits, and explicit history limits. Replace `MODEL_ARTIFACT_BASE_URI` and storage endpoints in `deploy/k8s/configmap.yaml` for non-local clusters.

`MODEL_ARTIFACT_BASE_URI` is read from the ConfigMap and must point to durable model storage for production. The default local value is for development smoke tests only; use an external volume or object/HDFS-backed path before running production training.
