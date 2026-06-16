# System Code File Role Table

Tài liệu này chỉ tập trung vào **file code thực thi/chỉnh sửa logic**.  
Không liệt kê chi tiết file data mẫu, artifact sinh ra, `__pycache__`, asset tĩnh, hay manifest hạ tầng ít logic.

Mục tiêu của bảng này là trả lời rõ 3 câu hỏi:
1. File này **làm công việc gì cụ thể**?
2. Nó **đọc đầu vào nào**?
3. Nó **ghi ra bảng/API/UI/bước nào**?

## 1. Core logic và config

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `ais_architecture_logic.py` | Chứa logic chia cửa sổ `historical` và `realtime`, chọn `as-of context`, loại bỏ target leakage, và mô tả thứ tự vận hành TODO4. | `start_date`, `end_date`, danh sách record có timestamp | Helper logic dùng cho orchestration, test, và realtime flow validation |
| `config/hanoi_pipeline.yaml` | Cấu hình nghiệp vụ trung tâm: bbox Hà Nội, danh sách product Sentinel-5P, MAIAC path, horizon/lag, HYSPLIT, visualization. | Giá trị cấu hình tay | Được `spark_jobs/hanoi_config.py` nạp và phân phối cho toàn pipeline |
| `spark_jobs/hanoi_config.py` | Wrapper nạp config mặc định + YAML + env override, và expose helper `get_*()` cho mỗi job. | `config/hanoi_pipeline.yaml`, env vars | Config runtime chuẩn hóa cho ingest, Spark, ML, visualization |
| `spark_jobs/hdfs_utils.py` | Helper thao tác HDFS: normalize URI, list file, copy HDFS -> local, fallback qua Hadoop API/CLI/WebHDFS. | HDFS URI, Spark session | Local temp file hoặc HDFS file index cho các job đọc NetCDF/HDF |
| `spark_jobs/runtime_utils.py` | Parse runtime cho streaming jobs (`availableNow` hay `processingTime`). | CLI args, env vars | Trigger mode thống nhất cho job streaming |
| `spark_jobs/streaming_bronze_utils.py` | Dùng chung cho bronze streaming: thêm contract columns, audit invalid/late events, dedupe `event_id`, merge vào Iceberg. | DataFrame Kafka đã parse JSON | Iceberg bronze table + audit tables |
| `spark_jobs/ensure_iceberg_tables.py` | Bootstrap namespace và bảng Iceberg cho toàn hệ thống. | Catalog/warehouse config | Tạo schema nền cho bronze/silver/gold/models/visualization |
| `spark_jobs/iceberg_maintenance.py` | Chạy `rewrite_data_files`, `expire_snapshots`, `remove_orphan_files` cho bảng Iceberg lớn. | Retention hours | Dọn dẹp / tối ưu metadata và file layout |
| `spark_jobs/reconcile_iceberg_cassandra.py` | So sánh phần trăm dòng gần đây giữa Iceberg và Cassandra để phát hiện serving bị lệch. | Feature/forecast rows từ Iceberg và Cassandra | Báo cáo integrity và cảnh báo drift đồng bộ |

## 2. Ingest producers

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `ingest/openaq_ingest.py` | Gọi OpenAQ, chuẩn hóa PM2.5 theo event contract, đẩy lên Kafka. | OpenAQ API | Topic OpenAQ realtime/backfill |
| `ingest/ingest_weather.py` | Gọi weather source, chuẩn hóa lịch sử/giờ hiện tại, đẩy lên Kafka. | Weather API / weather JSON | Topic weather |
| `ingest/era5_ingest.py` | Download metadata/file ERA5 và phát sự kiện file-sẵn-sàng. | CDS/ERA5 source | Topic `era5-files` |
| `ingest/sentinel5p_ingest.py` | Thu thập metadata granule Sentinel-5P, kèm path raw/download status. | Sentinel-5P metadata source | Topic Sentinel-5P summary |
| `ingest/maiac_ingest.py` | Thu thập metadata MAIAC granule/tile để phục vụ silver AOD. | MAIAC metadata source / local fallback | Topic MAIAC summary |
| `ingest/kafka_utils.py` | Helper producer, serializer, retry, topic publishing. | Record đã chuẩn hóa | Kafka message contract |
| `ingest/window_utils.py` | Tính cửa sổ backfill/realtime cho ingest jobs. | `lookback`, `start/end`, current time | Mốc thời gian feed vào ingest |
| `ingest/demo_realtime_feed.py` | Phát lại dữ liệu lịch sử thành nhiều batch nhỏ để giả lập realtime local. | Iceberg/history sample | Kafka feed mô phỏng realtime |

## 3. Spark bronze streaming

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `spark_jobs/openaq_hourly_streaming.py` | Đọc topic OpenAQ từ Kafka, parse event contract, merge vào bronze Iceberg. | Kafka topic OpenAQ | `air_quality.openaq_hourly_bronze` |
| `spark_jobs/weather_streaming.py` | Đọc topic weather từ Kafka, parse event contract, merge vào bronze Iceberg. | Kafka topic weather | `weather.weather_history_bronze` |
| `spark_jobs/era5_files_streaming.py` | Đọc topic metadata file ERA5 và lưu thông tin file raw/surface vào bronze. | Kafka topic `era5-files` | `weather.era5_files_bronze` |
| `spark_jobs/sentinel5p_summary_streaming.py` | Đọc metadata Sentinel-5P và lưu bronze summary granule. | Kafka topic Sentinel-5P | `satellite.sentinel5p_summary_bronze` |
| `spark_jobs/maiac_summary_streaming.py` | Đọc metadata MAIAC và lưu bronze summary granule. | Kafka topic MAIAC | `satellite.maiac_summary_bronze` |
| `spark_jobs/sentinel5p_streaming.py` | Biến thể streaming/loader cho Sentinel-5P bronze trong một số flow cũ. | Kafka topic Sentinel-5P | Bronze summary hoặc checkpoint liên quan |

## 4. Spark silver: air quality và weather

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `spark_jobs/hanoi_openaq_silver.py` | Làm sạch PM2.5 OpenAQ, lọc bbox Hà Nội, tạo bảng trạm theo giờ và bảng tổng hợp theo giờ. | `openaq_hourly_bronze` | `openaq_hanoi_station_hourly_silver`, `openaq_hanoi_hourly_silver` |
| `spark_jobs/hanoi_weather_surface_proxy_silver.py` | Lọc bản ghi weather thuộc Hà Nội theo tên vị trí/bbox và rút gọn về 1 dòng mỗi giờ. | `weather_history_bronze` | `weather_hanoi_surface_proxy_silver` |
| `spark_jobs/era5_surface_hanoi_silver.py` | Đọc file NetCDF ERA5 surface, cắt grid về Hà Nội, đổi đơn vị, tạo hourly weather context. | `era5_files_bronze`, file NetCDF raw | `era5_surface_hanoi_hourly_silver` |
| `spark_jobs/era5_pressure_levels_to_arl.py` | Chuyển file pressure-level ERA5 thành file ARL để HYSPLIT đọc được, và lưu metadata file ARL. | `era5_files_bronze`, raw pressure-level NetCDF | `weather.era5_arl_files_bronze` + file `.arl` trên HDFS |

## 5. Spark silver: satellite

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `spark_jobs/sentinel5p_grid_silver.py` | Resolve granule raw Sentinel-5P, cắt bbox Hà Nội, áp QA theo từng product, xuất **từng pixel hợp lệ**. | `sentinel5p_summary_bronze`, file `.nc` raw | `satellite.sentinel5p_grid_silver` |
| `spark_jobs/sentinel5p_hanoi_silver.py` | Tổng hợp nhiều granule Sentinel-5P cùng ngày thành **daily product summary** cho Hà Nội. | `sentinel5p_summary_bronze`, file `.nc` raw | `satellite.sentinel5p_hanoi_daily_silver` |
| `spark_jobs/maiac_hanoi_silver.py` | Đọc HDF MAIAC, áp QA, cắt bbox, tổng hợp tile-level rồi gom theo ngày. | `maiac_summary_bronze`, file `.hdf` local/raw | `satellite.maiac_hanoi_daily_silver` |

## 6. Spark silver: trajectory / Tier-2 attribution

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `spark_jobs/hysplit_trajectory_run.py` | Lập lịch backward/forward trajectory từ ARL, chạy `hyts_std`, upload `tdump`, lưu metadata từng run. | `era5_arl_files_bronze`, config HYSPLIT, OpenAQ trigger hours | `trajectory.hysplit_runs_bronze` + file `tdump` trên HDFS |
| `spark_jobs/hysplit_trajectory_parse_silver.py` | Parse file `tdump` thành từng điểm trajectory có `age_h`, `lat/lon/alt`, `timestamp`. | `hysplit_runs_bronze`, file `tdump` | `trajectory.hysplit_trajectories_silver` |
| `spark_jobs/hysplit_trajectory_cluster_silver.py` | Rút feature anchor trên trajectory backward và gán `cluster_id` để nhận diện hành lang vận chuyển. | `hysplit_trajectories_silver` | `trajectory.hysplit_trajectories_clustered_silver` |
| `spark_jobs/trajectory_path_sampling_silver.py` | Lấy pixel NO2/AER_AI gần nhất dọc đường trajectory backward và tổng hợp feature theo `traj_id`. | `hysplit_trajectories_silver`, `sentinel5p_grid_silver` | `features.trajectory_path_satellite_silver` |
| `spark_jobs/trajectory_hourly_features_silver.py` | Gom thông tin cluster + path sampling về cấp **giờ khởi tạo trajectory** để làm feature model. | `hysplit_cluster_silver`, `trajectory_path_silver` | `features.trajectory_hourly_features_silver` |
| `spark_jobs/openaq_spatial_gradient_silver.py` | Nội suy IDW PM2.5 quanh tâm Hà Nội theo 4 hướng N/S/E/W và tính gradient magnitude. | `openaq_hanoi_station_hourly_silver` | `features.openaq_spatial_gradient_silver` |

## 7. Spark gold: feature engineering / serving state

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `spark_jobs/hanoi_pm25_master_features_gold.py` | Join OpenAQ + weather proxy + ERA5 + Sentinel-5P + MAIAC + gradient + trajectory về cùng hourly grid, tạo full feature master. | Tất cả silver tables chính | `features.hanoi_pm25_master_hourly_gold` |
| `spark_jobs/hanoi_pm25_training_dataset_gold.py` | Lấy master features, loại row thiếu target, chia `train/validation/test` theo trục thời gian. | `master_hourly_gold` | `features.hanoi_pm25_training_dataset_gold` |
| `spark_jobs/hanoi_pm25_serving_features_gold.py` | Lấy master features và cắt bỏ mọi cột target tương lai, tạo feature state an toàn cho infer. | `master_hourly_gold` | `features.hanoi_pm25_serving_features_gold` |
| `spark_jobs/online_pm25_feature_builder.py` | Lấy **as-of context mới nhất** từ Iceberg/Kafka fallback, gộp OpenAQ + weather + Tier-2 + metadata thành 1 row online cho 1 location. | Serving feature gold + bronze/silver context | Cassandra `pm25_feature_state_by_location_hour` |
| `spark_jobs/pm25_serving_features_to_cassandra.py` | Đồng bộ batch serving features từ Iceberg sang Cassandra. | `hanoi_pm25_serving_features_gold` | Cassandra serving state |
| `spark_jobs/iceberg_to_cassandra.py` | Loader tổng quát Iceberg -> Cassandra cho một số bảng serving cũ/hỗ trợ. | Iceberg table | Cassandra table đích |

## 8. Visualization product builders

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `spark_jobs/visualization_common.py` | Helper chung cho visualization jobs: build Spark, args, table lookup, cache metadata. | Config + env | Dùng lại trong các visualization gold jobs |
| `spark_jobs/visualization_pm25_heatmap_grid_gold.py` | Tạo lớp heatmap PM2.5 theo horizon cho bản đồ. | Forecast/feature tables, config visualization | `visualization.pm25_heatmap_grid_gold` |
| `spark_jobs/visualization_backward_trajectory_paths_gold.py` | Tạo payload line/path trajectory để UI vẽ attribution backward. | `hysplit_trajectories_silver`, clustering, config | `visualization.backward_trajectory_paths_gold` |
| `spark_jobs/visualization_forward_plume_probability_gold.py` | Tạo lớp plume/probability cho hướng lan truyền phía trước. | Forecast/weather/trajectory context | `visualization.forward_plume_probability_gold` |
| `spark_jobs/visualization_forecast_dashboard_gold.py` | Tạo payload card/summary forecast 6h/12h/24h cho dashboard map. | Forecast state | `visualization.pm25_forecast_dashboard_gold` |
| `spark_jobs/visualization_pm25_timeseries_gold.py` | Tạo timeseries observed + forecast để UI vẽ line chart. | OpenAQ hourly + forecast | `visualization.pm25_timeseries_gold` |
| `spark_jobs/visualization_source_attribution_gold.py` | Tạo payload source attribution từ gradient + trajectory + satellite signal. | `trajectory_hourly_silver`, `openaq_gradient_silver`, satellite features | `visualization.source_attribution_gold` |
| `spark_jobs/visualization_station_observations_gold.py` | Tạo payload station marker / live points trên bản đồ. | `openaq_hanoi_station_hourly_silver` hoặc live state | `visualization.station_observations_gold` |
| `spark_jobs/export_visualization_cache.py` | Xuất các visualization gold tables thành JSON/GeoJSON cache và manifest để API/UI đọc nhanh. | Các visualization gold tables | File cache trên HDFS/local + manifest |
| `spark_jobs/visualization_quality_checks.py` | Kiểm tra các visualization table có đủ horizon/layer trước khi publish. | Visualization gold tables | Fail/ok signal cho pipeline |

## 9. Spark pipeline wrappers

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `spark_jobs/pipelines/_pipeline_shared.py` | Chia sẻ SparkSession và invoke nhiều module con trong cùng process. | Module name, argv, env patch | Pipeline helper |
| `spark_jobs/pipelines/bronze_ingest_to_iceberg_pipeline.py` | Chạy chuỗi bronze loaders theo thứ tự bootstrap/catch-up. | Kafka topics / runtime args | Bronze Iceberg đã đủ dữ liệu |
| `spark_jobs/pipelines/pm25_feature_pipeline.py` | Chạy liên tiếp các bước feature PM2.5 từ silver đến serving state. | Silver/gold dependencies | Feature gold + serving outputs |
| `spark_jobs/pipelines/trajectory_post_pipeline.py` | Chạy parse -> cluster -> path sampling -> hourly trajectory features. | HYSPLIT outputs + satellite grid | Tier-2 trajectory feature chain |
| `spark_jobs/pipelines/visualization_pipeline.py` | Chạy chuỗi build các visualization gold product + export cache. | Forecast/features/trajectory tables | Visualization tables + cache manifest |

## 10. ML

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `ml/train_hanoi_pm25.py` | Train model theo horizon 6/12/24h, tính metric, lưu artifact, đăng ký model run vào Iceberg. | `hanoi_pm25_training_dataset_gold` | Model artifact + `models.hanoi_pm25_model_runs_gold` |
| `ml/promote_hanoi_pm25_model.py` | Chọn model run và promote/demote vào model registry (`production/staging/archived`). | `model_runs_gold`, CLI promote args | `models.hanoi_pm25_model_registry_gold` |
| `ml/predict_hanoi_pm25.py` | Nạp model production, đọc serving features (Iceberg/Cassandra), predict 6/12/24h, ghi audit và có thể ghi Cassandra forecast latest. | Model registry + serving features | `predictions.hanoi_pm25_forecast_gold` và/hoặc Cassandra forecast |

## 11. Serving APIs

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `serving/pm25_api/main.py` | FastAPI cho forecast latest, readiness/health, fallback cache/Cassandra, và payload model forecast cho dashboard. | Cassandra forecast latest, Iceberg audit/cache | Endpoint PM2.5 serving |
| `serving/visualization_api/main.py` | FastAPI cho map UI: heatmap, trajectory, source attribution, station observations, timeseries; ưu tiên Cassandra cho live và cache cho historical. | Cassandra feature/forecast state + visualization cache | Endpoint visualization cho frontend |

## 12. UI frontend

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `ui/src/App.jsx` | Khai báo route/page chính của UI. | Browser route | Mount dashboard pages |
| `ui/src/pages/AirQualityMapDashboard.jsx` | Page trung tâm cho bản đồ realtime/historical, điều phối layer state, forecast, trajectory, source attribution. | Visualization API responses | Màn hình map chính |
| `ui/src/pages/RealtimeDashboard.jsx` | Page tổng hợp card/chart realtime từ API PM2.5 và API visualization. | Serving APIs | Dashboard realtime |
| `ui/src/pages/HistoricalDashboard.jsx` | Page xem chuỗi lịch sử theo bộ lọc. | Historical API | Dashboard lịch sử |
| `ui/src/pages/StatisticsDashboard.jsx` | Page thống kê/phân bố/risk chart. | Historical/statistics data | Dashboard thống kê |
| `ui/src/components/map/MapCanvas.jsx` | Render bản đồ nền, heatmap cells, station points, trajectory paths, plume polygons, popup interaction. | GeoJSON/payload visualization | Canvas map UI |
| `ui/src/components/map/ForecastPanel.jsx` | Hiện PM2.5 now + 6h/12h/24h forecast. | Forecast payload | Panel bên map |
| `ui/src/components/map/SourceAttributionPanel.jsx` | Hiện thông tin nguồn đóng góp/chủ đạo và confidence. | Source attribution payload | Panel bên map |
| `ui/src/components/charts/StatisticsCharts.jsx` | Tự vẽ histogram/quantile/risk bucket chart cho thống kê. | Aggregated stats | Chart thống kê |
| `ui/src/services/api.js` | Adapter API tổng quát cho OpenAQ/weather/historical/realtime charts. | HTTP backend responses | Data shape cho page/chart components |
| `ui/src/services/visualizationApi.js` | Client riêng cho endpoint map visualization. | HTTP visualization responses | Data shape cho `AirQualityMapDashboard` |
| `ui/src/utils/dataAdapters.js` | Chuẩn hóa payload backend về format component UI dễ dùng. | Raw API payload | UI-friendly structures |

## 13. Airflow DAGs

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `airflow/dags/ais_dag_utils.py` | Helper tạo Bash command cho ingest, Spark submit, Cassandra schema, Kafka topic bootstrap. | Airflow context/env | Command strings cho DAGs |
| `airflow/dags/ais_pipeline_dag.py` | DAG bootstrap/backfill lịch sử: ingest -> bronze -> refresh Cassandra. | Weekly/manual trigger | Historical bootstrap flow |
| `airflow/dags/ais_era5_ingestion_dag.py` | DAG dành riêng cho ingest/file processing ERA5. | Schedule Airflow | Bronze ERA5 + silver liên quan |
| `airflow/dags/ais_hanoi_silver_gold_dag.py` | DAG chạy chuỗi silver/gold cho Hà Nội. | Bronze tables | Silver/gold tables |
| `airflow/dags/ais_pm25_k8s_compute_dag.py` | DAG submit batch compute PM2.5 trên K8s/Spark. | Runtime params | Train/predict/feature jobs |
| `airflow/dags/ais_streaming_supervision_dag.py` | DAG giám sát stream job, Kafka lag, restart nếu cần. | Monitoring scripts | Operational continuity |
| `airflow/dags/ais_trajectory_tier2_dag.py` | DAG chạy chain trajectory Tier-2 attribution. | ERA5 ARL + HYSPLIT outputs | Trajectory feature tables |
| `airflow/dags/ais_visualization_product_dag.py` | DAG build visualization products và export cache. | Forecast/feature tables | Visualization gold + cache |
| `airflow/dags/ais_maiac_backfill_dag.py` | DAG backfill riêng MAIAC. | MAIAC source/backfill params | MAIAC bronze/silver |
| `airflow/dags/ais_maintenance_dag.py` | DAG bảo trì Iceberg/Kafka/schema health. | Schedule Airflow | Cleanup/maintenance |

## 14. Scripts vận hành quan trọng

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `scripts/run_todo4_stack.ps1` | Script local to nhất cho TODO4: dừng/làm sạch runtime cũ, bật infra, backfill, stream, hourly updater, online features, predict, UI/API. | Tham số ngày, runtime flags, Docker/K8s | Full stack local/K8s workflow |
| `scripts/run_ui_stack.ps1` | Bật nhanh API/UI phục vụ visualization và dashboard. | Docker/local env | UI + APIs onl |
| `scripts/run_full_historical_realtime.ps1` | Chạy kết hợp backfill lịch sử và phần realtime liên tục. | Date range + runtime flags | End-to-end historical + near realtime |
| `scripts/run_online_pm25_cycle.sh` | Chạy một chu kỳ online: build feature state -> predict -> publish. | Runtime env | Refresh serving state và forecast |
| `scripts/run_hourly_context_update.sh` | Chạy bộ cập nhật ERA5/HYSPLIT/context theo giờ. | Hourly scheduler env | Context/Tier-2 data mới |
| `scripts/ensure_cassandra_online_schema.sh` | Tạo/bắt buộc schema Cassandra cho serving features và forecast latest. | Cassandra host/keyspace | Serving tables có schema đúng |
| `scripts/check_pm25_serving.py` | Kiểm readiness/chính xác cơ bản của PM2.5 serving API. | Serving endpoint | Health result / CI gate |
| `scripts/check_visualization_serving.py` | Kiểm layer visualization API, manifest, và payload map. | Visualization endpoint/cache | Health result / CI gate |
| `scripts/check_operational_health.py` | Tổng hợp một loạt operational checks cho stack đang chạy. | Services + tables + endpoints | Báo cáo sức khỏe hệ thống |
| `scripts/submit_spark.sh` | Wrapper submit Spark local/standalone. | App name + script + args | Job Spark chạy |
| `scripts/submit_spark_k8s.sh` | Wrapper submit Spark lên Kubernetes. | K8s + Spark params | SparkApplication/job trên K8s |
| `scripts/create_topics.sh` | Tạo topic Kafka cần cho pipeline. | Kafka bootstrap | Topic sẵn sàng |
| `scripts/init_hdfs_layout.sh` | Tạo cây thư mục HDFS nền cho raw/checkpoint/cache. | HDFS access | Layout HDFS chuẩn |

## 15. Monitoring và tests

| File | Công việc cụ thể | Đầu vào chính | Đầu ra / ảnh hưởng |
| --- | --- | --- | --- |
| `monitoring/app.py` | App/endpoint theo dõi operational status của stack. | Health checks / metrics | Monitoring surface |
| `tests/test_online_feature_builder_asof_context.py` | Bảo vệ logic chọn context as-of trong online feature builder. | Test fixtures | Ngừa leak dữ liệu tương lai |
| `tests/test_online_feature_builder_no_target_leakage.py` | Bảo vệ việc serving/online features không mang cột target tương lai. | Feature schema | Safety test cho infer |
| `tests/test_api_latest_vs_historical_sources.py` | Kiểm tra API latest dùng Cassandra và API historical dùng cache/Iceberg. | API contract assumptions | Ngừa đọc nhầm source |
| `tests/test_event_contract_and_statistics_ui.py` | Kiểm tra event contract backend và cách UI xử lý payload thống kê. | API/data adapters | Regression test UI/data contract |
| `tests/test_realtime_dual_flow_orchestration.py` | Kiểm tra historical và realtime flow được phân tách đúng. | Orchestration helpers | Ngừa vỡ logic TODO4 |

## Ghi chú cuối

- Nếu cần tra chi tiết **hàm nào làm gì bên trong file**, ưu tiên đọc comment inline ngay trong source code; file `.md` này chỉ đóng vai trò **bản đồ định vị file**.
- Khi thêm file code mới, nên cập nhật bảng theo mẫu: **Công việc cụ thể / Đầu vào / Đầu ra**.
