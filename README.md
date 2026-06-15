# Atmospheric Intelligence System (AIS)

AIS là hệ thống dữ liệu khí quyển và dự báo PM2.5, tập trung vào Hà Nội. Hệ thống thu thập dữ liệu từ nhiều nguồn, xử lý bằng Spark, lưu trữ lịch sử trên HDFS/Iceberg, phục vụ dữ liệu nhanh qua Cassandra/API và cung cấp các lớp dữ liệu cho dashboard/visualization.

## 1. Kiến trúc tổng quan

```text
Data sources
  -> Ingest services
  -> Kafka
  -> Spark processing
  -> HDFS / Iceberg
  -> Cassandra / cache
  -> API / UI
```

Các thành phần chính:

- **Ingest services**: lấy dữ liệu khí tượng, chất lượng không khí, vệ tinh và ERA5.
- **Kafka**: nhận và đệm các event dữ liệu mới.
- **Spark**: xử lý near-realtime, batch backfill, feature engineering, trajectory, ML inference và visualization products.
- **HDFS/Iceberg**: lưu trữ dữ liệu lịch sử và các bảng Bronze/Silver/Gold.
- **Cassandra**: phục vụ truy vấn nhanh cho API.
- **Airflow**: điều phối backfill, pipeline batch, supervision và maintenance.
- **FastAPI services**: cung cấp PM2.5 forecast và visualization API.
- **UI**: dashboard hiển thị dữ liệu realtime/historical/forecast.

## 2. Hai nhánh dữ liệu

AIS có hai nhánh vận hành chính.

### 2.1 Nhánh near-realtime

Nhánh near-realtime xử lý dữ liệu mới nhất vừa đến từ các nguồn.

```text
Latest source data
  -> realtime/windowed ingest
  -> Kafka
  -> Spark near-realtime processing
  -> latest features / forecast / visualization products
  -> Cassandra or cache
  -> API / UI
```

Các bước chính:

1. Ingest services lấy dữ liệu mới nhất theo window nhỏ hoặc polling interval.
2. Dữ liệu được chuẩn hóa thành event và đẩy vào Kafka.
3. Spark load dữ liệu mới nhất để xử lý near-realtime.
4. Hệ thống cập nhật feature PM2.5, forecast, heatmap, station observations và source attribution.
5. Kết quả mới nhất được ghi sang Cassandra hoặc cache để API/UI đọc nhanh.

Nhánh này dùng cho:

- dữ liệu PM2.5/latest observations
- thời tiết mới nhất
- feature state phục vụ dự báo
- forecast PM2.5 6h/12h/24h
- live heatmap và dashboard latest

### 2.2 Nhánh batch backfill

Nhánh batch backfill dùng để nạp lại dữ liệu lịch sử, bootstrap hệ thống hoặc rebuild dữ liệu theo khoảng ngày.

```text
Historical windows
  -> batch ingest
  -> Kafka / batch jobs
  -> Spark batch processing
  -> HDFS / Iceberg historical tables
  -> Silver/Gold rebuild
  -> Cassandra/cache refresh
```

Các bước chính:

1. Airflow hoặc script nhận khoảng thời gian cần backfill.
2. Ingest services lấy dữ liệu lịch sử theo window.
3. Spark xử lý dữ liệu batch và ghi vào HDFS/Iceberg.
4. Các bảng Silver/Gold được rebuild khi cần.
5. Cassandra/cache được refresh để API có dữ liệu mới nhất sau backfill.

Nhánh này dùng cho:

- historical bootstrap
- nạp lại Weather/OpenAQ/Sentinel-5P/MAIAC/ERA5
- rebuild Hanoi PM2.5 features
- rebuild training dataset
- rebuild visualization products
- reconciliation giữa Iceberg và Cassandra

## 3. Nguồn dữ liệu

Các nguồn dữ liệu đang được hệ thống hỗ trợ:

- **Weather**: dữ liệu thời tiết theo tỉnh/thành và theo giờ.
- **OpenAQ**: đo chất lượng không khí, đặc biệt PM2.5.
- **Sentinel-5P**: dữ liệu vệ tinh cho NO2, CO, SO2, O3, aerosol.
- **MAIAC/MODIS**: dữ liệu aerosol optical depth.
- **ERA5**: dữ liệu khí tượng bề mặt và pressure-level.
- **HYSPLIT trajectory**: đường lan truyền/nguồn đóng góp phục vụ phân tích PM2.5.

## 4. Storage model

Hệ thống sử dụng HDFS/Iceberg làm nơi lưu trữ lịch sử chính.

Các lớp dữ liệu:

- **Bronze**: dữ liệu đã ingest và chuẩn hóa cơ bản.
- **Silver**: dữ liệu đã lọc, làm sạch, gom theo vùng/thời gian.
- **Gold**: feature tables, training datasets, forecast products và visualization products.

Namespaces chính:

- `ais.weather`
- `ais.air_quality`
- `ais.satellite`
- `ais.features`
- `ais.trajectory`
- `ais.models`
- `ais.predictions`
- `ais.visualization`

## 5. Hanoi PM2.5 pipeline

Pipeline PM2.5 cho Hà Nội gồm:

```text
Weather + OpenAQ + Satellite + ERA5 + Trajectory
  -> Silver processing
  -> PM2.5 master features
  -> training dataset / serving features
  -> model training and promotion
  -> forecast inference
  -> API / visualization
```

Các sản phẩm chính:

- PM2.5 hourly observations
- Weather and ERA5 context
- Satellite pollution/aerosol signals
- HYSPLIT trajectory features
- PM2.5 master feature table
- PM2.5 training dataset
- PM2.5 forecast 6h/12h/24h
- heatmap, timeseries, source attribution và station layers

## 6. Airflow DAGs

Các DAG chính:

- `ais_batch_orchestration`: bootstrap/backfill dữ liệu nguồn chính.
- `ais_streaming_supervision`: giám sát các job near-realtime.
- `ais_maiac_backfill`: backfill MAIAC theo độ trễ nguồn.
- `ais_era5_ingestion`: ingest ERA5 và tạo Silver cho Hà Nội.
- `ais_hanoi_silver_gold`: build Silver/Gold PM2.5.
- `ais_trajectory_tier2`: ERA5 pressure-level, HYSPLIT và trajectory features.
- `ais_pm25_k8s_compute`: compute PM2.5 serving features/training trên Kubernetes.
- `ais_visualization_product`: build visualization Gold tables và export cache.
- `ais_maintenance`: Iceberg maintenance và reconciliation.

## 7. Serving layer

Serving được tách khỏi xử lý nặng.

```text
Materialized results
  -> Cassandra / HDFS cache
  -> FastAPI
  -> UI
```

API chính:

- PM2.5 API: `serving/pm25_api`
- Visualization API: `serving/visualization_api`

Request API chỉ đọc kết quả đã được materialize, không chạy Spark, không train model và không chạy HYSPLIT trong request handler.

## 8. Lệnh thường dùng

Khởi động infrastructure:

```bash
bash scripts/run_infrastructure_only.sh
```

Tạo Kafka topics:

```bash
bash scripts/create_topics.sh
```

Tạo/cập nhật Iceberg tables:

```bash
bash scripts/submit_spark.sh ensure-iceberg
```

Chạy full historical + near-realtime stack:

```bash
bash scripts/run_full_historical_realtime.sh
```

Chạy online PM2.5 cycle:

```bash
bash scripts/run_online_pm25_cycle.sh
```

Chạy hourly context update:

```bash
bash scripts/run_hourly_context_update.sh
```

## 9. UI và monitoring

Các endpoint thường dùng khi chạy local:

- Airflow UI: `http://localhost:8088`
- Spark UI: `http://localhost:8080`
- HDFS UI: `http://localhost:9870`
- Monitoring UI: `http://localhost:8501`

Frontend nằm trong thư mục `ui/`.

## 10. Tài liệu liên quan

- `SYSTEM_OVERVIEW.md`
- `README_DATASETS.md`
- `README_HDFS_ICEBERG.md`
- `docs/architecture/refactored_pipeline.md`
- `description/hanoi_data_processing_plan.md`
- `description/hanoi_trajectory_pipeline.md`
- `deploy/k8s/README.md`
