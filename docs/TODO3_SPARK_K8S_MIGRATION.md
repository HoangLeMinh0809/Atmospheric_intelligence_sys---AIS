# TODO3 Spark-on-Kubernetes Migration

This document covers TODO3 section 8. Docker Compose Spark remains a local/dev fallback through `scripts/submit_spark.sh`; the target runtime is Spark-on-Kubernetes through `scripts/submit_spark_k8s.sh`.

## Runtime Contract

Required production/K8s environment:

```text
SPARK_K8S_MASTER
SPARK_K8S_NAMESPACE
SPARK_DRIVER_SERVICE_ACCOUNT
SPARK_IMAGE
HDFS_NAMENODE
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
KAFKA_BOOTSTRAP_SERVERS
CASSANDRA_HOST
```

Base command:

```bash
bash scripts/submit_spark_k8s.sh <job-type> [job args...]
```

The script submits with `--master "${SPARK_K8S_MASTER}"`, `--deploy-mode cluster`, and `local:///opt/spark-jobs/...` paths from the Spark runtime image. Airflow can switch from Compose to this path with:

```text
AIS_SPARK_RUNTIME=k8s
```

## Job Migration Matrix

| Category | Current Compose command | Target K8s command | Input | Output | Idempotency/rerun |
|---|---|---|---|---|---|
| Ensure Iceberg | `bash scripts/submit_spark.sh ensure-iceberg` | `bash scripts/submit_spark_k8s.sh ensure-iceberg` | catalog/warehouse config | namespaces/tables | `CREATE IF NOT EXISTS`, additive schema |
| Weather bronze | `bash scripts/submit_spark.sh weather` | `bash scripts/submit_spark_k8s.sh weather` | Kafka `weather_history` | `ais.weather.weather_history_bronze` | checkpoint + existing job dedupe |
| OpenAQ bronze | `bash scripts/submit_spark.sh openaq` | `bash scripts/submit_spark_k8s.sh openaq` | Kafka `openaq-hourly` | `ais.air_quality.openaq_hourly_bronze` | checkpoint + existing job dedupe |
| Sentinel-5P bronze | `bash scripts/submit_spark.sh sentinel5p` | `bash scripts/submit_spark_k8s.sh sentinel5p` | Kafka `sentinel5p-summary` | `ais.satellite.sentinel5p_summary_bronze` | checkpoint + existing job dedupe |
| MAIAC bronze | `bash scripts/submit_spark.sh maiac` | `bash scripts/submit_spark_k8s.sh maiac` | Kafka `maiac-summary` | `ais.satellite.maiac_summary_bronze` | checkpoint + existing job dedupe |
| ERA5 files bronze | `bash scripts/submit_spark.sh era5-files` | `bash scripts/submit_spark_k8s.sh era5-files` | Kafka `era5-files` | `ais.weather.era5_files_bronze` | checkpoint + existing job dedupe |
| Hanoi OpenAQ silver | `bash scripts/submit_spark.sh hanoi-openaq-silver` | `bash scripts/submit_spark_k8s.sh hanoi-openaq-silver` | OpenAQ bronze | `ais.air_quality.openaq_hanoi_hourly_silver` | `--full-refresh`/date range behavior from job |
| Hanoi weather silver | `bash scripts/submit_spark.sh hanoi-weather-silver` | `bash scripts/submit_spark_k8s.sh hanoi-weather-silver` | weather bronze | `ais.weather.weather_hanoi_surface_proxy_silver` | `--full-refresh`/date range behavior from job |
| ERA5 surface silver | `bash scripts/submit_spark.sh era5-surface-hanoi-silver` | `bash scripts/submit_spark_k8s.sh era5-surface-hanoi-silver` | ERA5 bronze/raw | `ais.weather.era5_surface_hanoi_hourly_silver` | `--full-refresh`/date range behavior from job |
| ERA5 pressure ARL | `bash scripts/submit_spark.sh era5-pressure-arl` | `bash scripts/submit_spark_k8s.sh era5-pressure-arl` | ERA5 pressure files | `ais.weather.era5_arl_files_bronze` | keyed by source/time path |
| HYSPLIT run | `bash scripts/submit_spark.sh hysplit-run` | `bash scripts/submit_spark_k8s.sh hysplit-run` | ARL files | `ais.trajectory.hysplit_runs_bronze` | keyed by run dimensions; requires HYSPLIT binary in image |
| HYSPLIT parse | `bash scripts/submit_spark.sh hysplit-parse` | `bash scripts/submit_spark_k8s.sh hysplit-parse` | trajectory output | `ais.trajectory.hysplit_trajectories_silver` | keyed by trajectory/time |
| HYSPLIT cluster | `bash scripts/submit_spark.sh hysplit-cluster` | `bash scripts/submit_spark_k8s.sh hysplit-cluster` | trajectory silver | `ais.trajectory.hysplit_trajectories_clustered_silver` | keyed by trajectory/cluster |
| Sentinel-5P Hanoi silver | `bash scripts/submit_spark.sh sentinel5p-hanoi-silver` | `bash scripts/submit_spark_k8s.sh sentinel5p-hanoi-silver` | S5P bronze | `ais.satellite.sentinel5p_hanoi_daily_silver` | `--full-refresh`/date range behavior from job |
| OpenAQ gradient | `bash scripts/submit_spark.sh openaq-gradient` | `bash scripts/submit_spark_k8s.sh openaq-gradient` | OpenAQ silver | `ais.features.openaq_spatial_gradient_silver` | partition overwrite/merge behavior from job |
| S5P grid silver | `bash scripts/submit_spark.sh s5p-grid-silver` | `bash scripts/submit_spark_k8s.sh s5p-grid-silver` | S5P silver/raw | `ais.satellite.sentinel5p_grid_silver` | partition overwrite/merge behavior from job |
| Trajectory path sampling | `bash scripts/submit_spark.sh traj-path-sampling` | `bash scripts/submit_spark_k8s.sh traj-path-sampling` | trajectory + satellite | `ais.features.trajectory_path_satellite_silver` | keyed by trajectory/path window |
| Trajectory hourly features | `bash scripts/submit_spark.sh traj-hourly-features` | `bash scripts/submit_spark_k8s.sh traj-hourly-features` | trajectory/path features | `ais.features.trajectory_hourly_features_silver` | keyed by hour |
| MAIAC Hanoi silver | `bash scripts/submit_spark.sh maiac-hanoi-silver` | `bash scripts/submit_spark_k8s.sh maiac-hanoi-silver` | MAIAC bronze/raw | `ais.satellite.maiac_hanoi_daily_silver` | partition overwrite/merge behavior from job |
| PM2.5 master gold | `bash scripts/submit_spark.sh hanoi-master-features-gold` | `bash scripts/submit_spark_k8s.sh hanoi-master-features-gold` | silver/Tier-2 features | `ais.features.hanoi_pm25_master_hourly_gold` | keyed by hour/partition |
| Training dataset gold | `bash scripts/submit_spark.sh hanoi-training-dataset-gold` | `bash scripts/submit_spark_k8s.sh hanoi-training-dataset-gold` | master gold | `ais.features.hanoi_pm25_training_dataset_gold` | keyed by hour/version/split |
| Model training fallback | `bash scripts/submit_spark.sh hanoi-train-baseline` | `bash scripts/submit_spark_k8s.sh hanoi-train-baseline` | training dataset gold | `ais.models.hanoi_pm25_model_runs_gold` | append run metadata |
| Iceberg maintenance | `bash scripts/submit_spark.sh maintenance-iceberg` | `bash scripts/submit_spark_k8s.sh maintenance-iceberg` | Iceberg tables | optimized tables | Iceberg maintenance procedures |
| Cassandra weather serving | `bash scripts/submit_spark.sh cassandra-weather` | `bash scripts/submit_spark_k8s.sh cassandra-weather` | Iceberg weather | Cassandra weather table | Cassandra primary keys |
| Cassandra OpenAQ serving | `bash scripts/submit_spark.sh cassandra-openaq` | `bash scripts/submit_spark_k8s.sh cassandra-openaq` | Iceberg OpenAQ | Cassandra OpenAQ table | Cassandra primary keys |
| Serving reconciliation | `bash scripts/submit_spark.sh reconcile-serving` | `bash scripts/submit_spark_k8s.sh reconcile-serving` | Iceberg/Cassandra | check logs | non-zero exit on failed tolerance |

## Smoke Tests

```bash
bash -n scripts/submit_spark_k8s.sh

SPARK_K8S_MASTER=k8s://https://kubernetes.default.svc \
SPARK_IMAGE=ais-spark-runtime:local \
HDFS_NAMENODE=hdfs://host.docker.internal:9000 \
ICEBERG_WAREHOUSE=hdfs://host.docker.internal:9000/warehouse/iceberg \
bash scripts/submit_spark_k8s.sh ensure-iceberg

kubectl -n ais get pods
kubectl -n ais logs <spark-driver-pod>
```

Expected log fields include `job_type`, `spark_master`, `namespace`, `image`, `job_file`, and `checkpoint`. A migrated checkpoint is accepted only when the driver and executor pods are visible in Kubernetes and the job can read/write the external storage endpoints.
