# PM2.5 ML Jobs

Build the ML image from the repository root:

```bash
docker build -t ais-ml-runtime:local -f ml/Dockerfile .
docker run --rm ais-ml-runtime:local python ml/train_hanoi_pm25.py --help
docker run --rm ais-ml-runtime:local python ml/predict_hanoi_pm25.py --help
```

## Training (manual)

Run the training job in Kubernetes:

```bash
kubectl -n ais apply -f deploy/k8s/ml/pm25-train-job.yaml
kubectl -n ais logs -f job/pm25-train
```

The training job:

- reads the Iceberg training dataset (`ais.features.hanoi_pm25_training_dataset_gold` by default)
- writes model artifacts under `${MODEL_ARTIFACT_BASE_URI}/hanoi_pm25`
- appends run metadata to `ais.models.hanoi_pm25_model_runs_gold`

Promotion into the production model registry is **explicit and separate** (see `ml/promote_hanoi_pm25_model.py`). The training job does **not** write to the registry by default.

## Inference

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

`MODEL_ARTIFACT_BASE_URI` is read from the ConfigMap and must point to durable model storage for production. The default local value is for development smoke tests only; use an external volume or remote path before running production training.
