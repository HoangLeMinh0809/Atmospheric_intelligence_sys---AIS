# AIS System Overview (2026-05-27)

Tai lieu nay mo ta chi tiet he thong AIS hien co trong repo, gom cac lop storage, compute, dataflow, va trang thai van hanh.

## 1) Tong quan muc tieu

AIS la pipeline du lieu khi quyen end-to-end:
- Thu thap du lieu nhieu nguon (Weather, OpenAQ, Sentinel-5P, MAIAC, ERA5).
- Dong bo vao Kafka, xu ly bang Spark (streaming + batch).
- Luu lich su vao Iceberg tren HDFS, dong bo mot phan serving sang Cassandra.
- Orchestration va giam sat bang Airflow + Monitoring UI.
- Mo rong PM2.5 training/inference va PM2.5 API.

Tai lieu tham khao tong quan: [README.md](README.md), [PROJECT_FULL_NOTES.md](PROJECT_FULL_NOTES.md).

## 2) Kien truc runtime (storage vs compute)

### 2.1 Storage / Data infrastructure (ngoai K8s)

Dang chay bang Docker Compose (hoac external endpoint khi production):
- Kafka / Zookeeper
- HDFS (namenode/datanode)
- Iceberg warehouse
- Cassandra (serving)
- Airflow metadata DB
- Model artifact storage

File/cau hinh:
- [docker-compose.yml](docker-compose.yml)
- [deploy/k8s/configmap.yaml](deploy/k8s/configmap.yaml)
- [deploy/k8s/README.md](deploy/k8s/README.md)

### 2.2 Compute layer (K8s)

Compute layer chay tren Kubernetes:
- Spark driver/executor pods (Spark-on-K8s)
- ML training/inference Jobs/CronJobs
- PM2.5 API Deployment
- Check jobs

Tools/manifest lien quan:
- [scripts/submit_spark_k8s.sh](scripts/submit_spark_k8s.sh)
- [deploy/k8s/spark/README.md](deploy/k8s/spark/README.md)
- [deploy/k8s/ml/README.md](deploy/k8s/ml/README.md)
- [deploy/k8s/api/README.md](deploy/k8s/api/README.md)
- [deploy/k8s/checks/README.md](deploy/k8s/checks/README.md)

Luu y: Spark Compose (spark-master/worker) chi la dev fallback, target runtime la Spark-on-K8s.

## 3) Dataflow tong the

Luong du lieu chinh:
1) Nguon du lieu -> Python ingest adapters
2) Ingest -> Kafka topics
3) Spark streaming -> Iceberg bronze tables
4) Spark batch -> silver/gold feature tables
5) Cassandra serving (weather/openaq)
6) ML training -> model registry
7) ML inference -> prediction table
8) PM2.5 API -> doc prediction table (khong chay Spark trong request)

## 4) Data sources va ingest

Nguon du lieu:
- WeatherAPI history (hoac local JSON)
- OpenAQ hourly
- Sentinel-5P metadata API
- MAIAC metadata (NASA CMR)
- ERA5 surface va pressure-level

Ingest layer:
- [ingest/ingest_weather.py](ingest/ingest_weather.py)
- [ingest/openaq_ingest.py](ingest/openaq_ingest.py)
- [ingest/sentinel5p_ingest.py](ingest/sentinel5p_ingest.py)
- [ingest/maiac_ingest.py](ingest/maiac_ingest.py)
- [ingest/era5_ingest.py](ingest/era5_ingest.py)
- Shared helper: [ingest/kafka_utils.py](ingest/kafka_utils.py), [ingest/window_utils.py](ingest/window_utils.py)

## 5) Kafka topics

Topics chinh:
- `weather_history`
- `openaq-hourly`
- `sentinel5p-summary`
- `maiac-summary`
- `era5-files`

Script tao topics: [scripts/create_topics.sh](scripts/create_topics.sh)

## 6) Spark jobs (streaming + batch)

### 6.1 Streaming -> Iceberg bronze
- Weather: [spark_jobs/weather_streaming.py](spark_jobs/weather_streaming.py)
- OpenAQ: [spark_jobs/openaq_hourly_streaming.py](spark_jobs/openaq_hourly_streaming.py)
- Sentinel-5P summary: [spark_jobs/sentinel5p_summary_streaming.py](spark_jobs/sentinel5p_summary_streaming.py)
- MAIAC summary: [spark_jobs/maiac_summary_streaming.py](spark_jobs/maiac_summary_streaming.py)
- ERA5 files metadata: [spark_jobs/era5_files_streaming.py](spark_jobs/era5_files_streaming.py)

### 6.2 Silver/Gold (Hanoi PM2.5)
- Config + table names: [spark_jobs/hanoi_config.py](spark_jobs/hanoi_config.py)
- Silver jobs: OpenAQ, Weather proxy, ERA5 surface, Sentinel-5P daily, MAIAC daily
  - [spark_jobs/hanoi_openaq_silver.py](spark_jobs/hanoi_openaq_silver.py)
  - [spark_jobs/hanoi_weather_surface_proxy_silver.py](spark_jobs/hanoi_weather_surface_proxy_silver.py)
  - [spark_jobs/era5_surface_hanoi_silver.py](spark_jobs/era5_surface_hanoi_silver.py)
  - [spark_jobs/sentinel5p_hanoi_silver.py](spark_jobs/sentinel5p_hanoi_silver.py)
  - [spark_jobs/maiac_hanoi_silver.py](spark_jobs/maiac_hanoi_silver.py)
- Gold jobs:
  - [spark_jobs/hanoi_pm25_master_features_gold.py](spark_jobs/hanoi_pm25_master_features_gold.py)
  - [spark_jobs/hanoi_pm25_training_dataset_gold.py](spark_jobs/hanoi_pm25_training_dataset_gold.py)
  - [spark_jobs/hanoi_pm25_serving_features_gold.py](spark_jobs/hanoi_pm25_serving_features_gold.py)

### 6.3 Tier-2 trajectory (HYSPLIT)
- ERA5 pressure-level -> ARL: [spark_jobs/era5_pressure_levels_to_arl.py](spark_jobs/era5_pressure_levels_to_arl.py)
- Run/parse/cluster: [spark_jobs/hysplit_trajectory_run.py](spark_jobs/hysplit_trajectory_run.py),
  [spark_jobs/hysplit_trajectory_parse_silver.py](spark_jobs/hysplit_trajectory_parse_silver.py),
  [spark_jobs/hysplit_trajectory_cluster_silver.py](spark_jobs/hysplit_trajectory_cluster_silver.py)
- Trajectory features: [spark_jobs/trajectory_path_sampling_silver.py](spark_jobs/trajectory_path_sampling_silver.py),
  [spark_jobs/trajectory_hourly_features_silver.py](spark_jobs/trajectory_hourly_features_silver.py)

## 7) Iceberg tables va namespaces

Bootstrap tables do:
- [spark_jobs/ensure_iceberg_tables.py](spark_jobs/ensure_iceberg_tables.py)

Namespaces chinh:
- `ais.weather`, `ais.air_quality`, `ais.satellite`, `ais.features`, `ais.models`, `ais.trajectory`, `ais.predictions`

## 8) Cassandra serving

Cassandra hien phuc vu 2 bang serving:
- `ais_serving.weather_hourly_by_province_day`
- `ais_serving.openaq_hourly_by_city_parameter_day`

Job dong bo:
- [spark_jobs/iceberg_to_cassandra.py](spark_jobs/iceberg_to_cassandra.py)

## 9) Airflow orchestration

DAG chinh:
- Bootstrap: [airflow/dags/ais_pipeline_dag.py](airflow/dags/ais_pipeline_dag.py)
- Streaming supervision: [airflow/dags/ais_streaming_supervision_dag.py](airflow/dags/ais_streaming_supervision_dag.py)
- MAIAC backfill: [airflow/dags/ais_maiac_backfill_dag.py](airflow/dags/ais_maiac_backfill_dag.py)
- Maintenance: [airflow/dags/ais_maintenance_dag.py](airflow/dags/ais_maintenance_dag.py)
- Hanoi silver/gold: [airflow/dags/ais_hanoi_silver_gold_dag.py](airflow/dags/ais_hanoi_silver_gold_dag.py)
- Tier-2 trajectory: [airflow/dags/ais_trajectory_tier2_dag.py](airflow/dags/ais_trajectory_tier2_dag.py)
- ERA5 ingestion: [airflow/dags/ais_era5_ingestion_dag.py](airflow/dags/ais_era5_ingestion_dag.py)
- K8s compute chain (PM2.5): [airflow/dags/ais_pm25_k8s_compute_dag.py](airflow/dags/ais_pm25_k8s_compute_dag.py)

Helper: [airflow/dags/ais_dag_utils.py](airflow/dags/ais_dag_utils.py)

## 10) ML training, inference, va PM2.5 API

Training/Inference scripts:
- [ml/train_hanoi_pm25.py](ml/train_hanoi_pm25.py)
- [ml/predict_hanoi_pm25.py](ml/predict_hanoi_pm25.py)
- [ml/promote_hanoi_pm25_model.py](ml/promote_hanoi_pm25_model.py)

PM2.5 API (read-only serving):
- [serving/pm25_api/main.py](serving/pm25_api/main.py)

K8s manifest:
- [deploy/k8s/ml](deploy/k8s/ml)
- [deploy/k8s/api](deploy/k8s/api)

Prediction table:
- `ais.predictions.hanoi_pm25_forecast_gold`

Model registry table:
- `ais.models.hanoi_pm25_model_registry_gold`

## 11) K8s runtime details (where it runs)

- K8s workload chay trong namespace `ais`.
- Spark submit tao Kubernetes Job chay `spark-submit` trong image `ais-spark-runtime`, job nay tao driver/executor pods.
- ConfigMap `ais-runtime-config` chua endpoints Kafka/HDFS/Iceberg/Cassandra va tham so runtime.
- Docker Desktop local dung `host.docker.internal` de K8s pods truy cap Compose services.
- HDFS datanode can publish port `9866` va advertise host phu hop de pods doc block thanh cong.

Tai lieu lien quan:
- [deploy/k8s/README.md](deploy/k8s/README.md)
- [deploy/k8s/spark/README.md](deploy/k8s/spark/README.md)
- [deploy/k8s/compose-bridge-services.yaml](deploy/k8s/compose-bridge-services.yaml)

## 12) Monitoring va UI

Monitoring UI:
- Backend: [monitoring/app.py](monitoring/app.py)
- Dockerfile: [monitoring/Dockerfile](monitoring/Dockerfile)

UI frontend:
- Demo voi mock data trong [ui](ui)

## 13) Cach chay nhanh (tham khao)

- Quick start: [QUICKSTART.md](QUICKSTART.md)
- Full notes: [PROJECT_FULL_NOTES.md](PROJECT_FULL_NOTES.md)
- TODO1 E2E: [scripts/run_todo1_end_to_end.sh](scripts/run_todo1_end_to_end.sh)
- TODO3 E2E: [scripts/run_todo3_end_to_end.ps1](scripts/run_todo3_end_to_end.ps1)

## 14) Trang thai hien tai (rut gon)

- Core pipeline (ingest -> Kafka -> Spark -> Iceberg -> Cassandra): da co.
- TODO1 Hanoi PM2.5 silver/gold: da co.
- TODO2 Tier-2 trajectory: da co.
- TODO3 K8s compute layer: da co nhung can hardening cho production (monitoring, quota, secrets, stable endpoints).
- UI: demo mock data, chua noi data production.
