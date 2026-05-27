# TODO 4 - Windy-like Visualization Layer and Final Product Dashboard

Mục tiêu của TODO4 là xây dựng lớp sản phẩm cuối cùng của AIS: bản đồ chất lượng không khí kiểu Windy cho Bắc Bộ và Hà Nội, dashboard dự báo PM2.5, hiển thị quỹ đạo khối khí, plume forward nếu có, và giải thích nguồn ô nhiễm theo cụm/region.

TODO4 không viết lại pipeline dữ liệu lõi. TODO1, TODO2, TODO3 được xem là đã chạy được và là upstream contract cho lớp visualization.

Quy tắc request-time bắt buộc kế thừa từ TODO3:

```text
Frontend request
-> Visualization API / tile endpoint
-> đọc bảng gold/visualization đã materialize hoặc cache/tile đã export
-> JSON/GeoJSON/tile response
```

API không được chạy Spark, HYSPLIT, ML inference, training, feature engineering, hoặc raw extraction trong request handler.

## 0. Architecture decision

TODO4 là final product layer nằm trên TODO1/TODO2/TODO3.

Trạng thái upstream:

- TODO1 tạo Hanoi PM2.5 silver/gold pipeline.
- TODO2 tạo Tier-2 trajectory/HYSPLIT features.
- TODO3 tạo Kubernetes compute layer, ML inference, PM2.5 API, prediction table, K8s manifests, và serving checks.

Quyết định kiến trúc cho TODO4:

- [ ] TODO4 chỉ materialize sản phẩm visualization-ready và phục vụ cho UI.
- [ ] Visualization không query random silver/raw tables trực tiếp từ UI hoặc request handler.
- [ ] Visualization chỉ dùng stable gold/visualization tables và API contracts.
- [ ] Thêm namespace Iceberg `ais.visualization`.
- [ ] Thêm visualization gold tables.
- [ ] Thêm Spark jobs chạy trên K8s để build visualization tables.
- [ ] Thêm API endpoints cho forecast, heatmap grid, backward trajectories, forward plume, station observations, và source attribution.
- [ ] Tạo mới `serving/visualization_api` làm API product mặc định cho dashboard. `serving/pm25_api` có thể giữ làm forecast compatibility endpoint, nhưng không nên mở rộng thêm nếu chưa refactor request-time Spark.
- [ ] Thêm frontend map dashboard kiểu Windy, kết nối production API/tile endpoints.
- [ ] Mock data trong `ui/public/mock` chỉ được dùng khi bật dev mode rõ ràng.
- [ ] Thêm Airflow DAG để refresh visualization data.
- [ ] Thêm K8s manifests/check jobs cho visualization API/UI.
- [ ] Storage layer vẫn nằm ngoài K8s như TODO3: Kafka/HDFS/Iceberg/Cassandra/Airflow metadata/model artifact storage không migrate vào K8s trong TODO4.
- [ ] Request-time phải lightweight: đọc cache/tile/table đã materialize, không chạy compute nặng.

Decision quan trọng về API:

- Tạo mới `serving/visualization_api` để tránh nhồi thêm product contract vào PM2.5 API hiện tại.
- PM2.5 API hiện tại có forecast endpoint, nhưng implementation đang import PySpark và tạo `SparkSession` trong request/readiness path. TODO4 không được copy pattern này cho visualization API.
- Visualization API mặc định đọc precomputed JSON/GeoJSON/tile/cache manifest do Spark jobs export. Nếu cần đọc Iceberg trực tiếp, chỉ dùng lightweight reader/query service đã cấu hình sẵn, không dùng PySpark trong web process.

Tiêu chí chấp nhận của architecture decision:

- UI production gọi visualization API/tile endpoints, không gọi `/mock/*`.
- API request path không import hoặc khởi tạo Spark.
- Bảng/cache visualization được build bởi Spark-on-K8s/Airflow trước khi người dùng request.
- Có tài liệu rõ bảng nào là API contract và bảng nào chỉ là upstream source.

## 1. Current-state inventory

Inventory sau khi scan repo hiện tại:

| Item | Trạng thái hiện có | Quyết định TODO4 |
|---|---|---|
| `SYSTEM_OVERVIEW.md` | Có. Mô tả storage ngoài K8s, compute trên K8s, PM2.5 API, UI mock data. | **use as-is**, **modify optional** sau khi TODO4 hoàn thành để cập nhật product layer. |
| `TODO_1.md` | Có. Hanoi PM2.5 silver/gold pipeline. | **use as-is** làm upstream contract. |
| `TODO_2.md` | Có. Tier-2 trajectory/HYSPLIT, S5P grid, trajectory features. | **use as-is** làm upstream trajectory/source contract. |
| `TODO_3.md` | Có. K8s compute, serving features, prediction table, API/checks. | **use as-is** làm runtime và serving boundary. |
| `description/hanoi_trajectory_pipeline.md` | Có. Có blueprint cho source attribution GeoJSON và forward plume probability grid. | **use as reference**, production hóa bằng Iceberg tables/cache thay vì local `output/*.geojson` hoặc CSV. |
| `spark_jobs/hanoi_config.py` | Có table names cho TODO1/2/3 và config HYSPLIT/trajectory. | **modify** để thêm Northern Vietnam visualization config, `ais.visualization.*` table names, cache path helpers. |
| `spark_jobs/ensure_iceberg_tables.py` | Có namespace `weather`, `air_quality`, `satellite`, `features`, `models`, `trajectory`, `predictions`; có TODO3 tables. | **modify** để thêm namespace `ais.visualization` và schema visualization gold tables. |
| `ais.features.hanoi_pm25_serving_features_gold` | Có trong config/schema TODO3. | **use as-is** cho forecast metadata và model/source fields, không expose trực tiếp cho UI. |
| `ais.predictions.hanoi_pm25_forecast_gold` | Có schema trong `ensure_iceberg_tables.py`; PM2.5 API đọc table này. | **use as-is** làm input chính cho forecast cards và dashboard gold table. |
| `ais.models.hanoi_pm25_model_registry_gold` | Có schema trong `ensure_iceberg_tables.py`. | **use as-is** để hiển thị `model_version`, freshness, production status. |
| `ais.trajectory.hysplit_trajectories_silver` | Có schema direction/backward/forward point-level. | **modify via new jobs**: không expose trực tiếp; aggregate sang trajectory GeoJSON gold. |
| `ais.trajectory.hysplit_trajectories_clustered_silver` | Có schema cluster/source point-level. | **modify via new jobs**: aggregate sang backward trajectory paths và source markers. |
| `ais.features.trajectory_path_satellite_silver` | Có path NO2/AER features theo `traj_id`. | **use as input** cho trajectory popup và attribution evidence. |
| `ais.features.trajectory_hourly_features_silver` | Có dominant cluster/source/path features theo giờ. | **use as input** cho source attribution và forecast metadata. |
| `ais.satellite.sentinel5p_grid_silver` | Có grid S5P theo product/date/lat/lon. | **use as input** cho heatmap proxy và trajectory evidence; không query trực tiếp từ UI. |
| `ais.air_quality.openaq_hanoi_station_hourly_silver` | Có station-level PM2.5 Hà Nội. | **use as input** cho station layer và observed heatmap anchor. |
| `serving/pm25_api/main.py` | Có FastAPI `/healthz`, `/readyz`, `/api/v1/hanoi/pm25/forecast/latest`. Hiện dùng PySpark trong request/readiness. | **modify optional/hardening** để loại Spark request-time; **không dùng làm pattern** cho visualization API. |
| `serving/pm25_api/README.md` | Ghi rõ API không được train/inference/Spark trong request, nhưng implementation cần hardening. | **use as architecture intent**, update sau nếu refactor. |
| `ml/predict_hanoi_pm25.py` | Có inference script đọc serving features + model registry, ghi prediction gold. | **use as-is** upstream; TODO4 không chạy inference trong API/UI. |
| `airflow/dags/ais_pm25_k8s_compute_dag.py` | Có DAG build serving features và train job; chưa orchestration visualization. | **modify optional** nếu nối dependency, nhưng nên **create new** DAG `ais_visualization_product_dag.py`. |
| `airflow/dags/ais_trajectory_tier2_dag.py` | Có DAG Tier-2 nhưng đang gọi `scripts/submit_spark.sh` fallback Compose path. | **use as upstream**, **modify optional** để đồng bộ K8s path sau; TODO4 DAG chỉ phụ thuộc output table. |
| `deploy/k8s/api` | Có PM2.5 API deployment/service. | **use as reference**, **create new** visualization API manifests. |
| `deploy/k8s/checks` | Có PM2.5 serving check job. | **extend** bằng visualization check job. |
| `deploy/k8s/configmap.yaml` | Có env runtime TODO3. | **modify** để thêm visualization env vars, API base URL, cache URI, UI config. |
| `deploy/k8s/kustomization.yaml` | Hiện chưa include API/check resources. | **modify** để include visualization API/UI/check manifests khi sẵn sàng. |
| `ui` | Có Vite React app với Realtime/Historical dashboards. `ui/src/services/api.js` fetch trực tiếp `/mock/*.json`. Không có map library. | **modify heavily**: dashboard mới là main page, production mode gọi API thật, mock chỉ sau dev flag. |
| `monitoring/app.py` | Có Flask monitoring pipeline/storage/ingest. | **modify optional** để thêm cards freshness visualization/API/UI. |
| `crawler/geoBoundaries-VNM-ADM1_simplified.geojson`, `crawler/geoBoundaries-VNM-ADM2_simplified.geojson`, `crawler/hanoi_districts_clean.geojson` | Có boundary assets. | **optional** dùng làm map overlay/source labeling nếu license/size phù hợp; không bắt buộc cho MVP. |

Current gaps cần giải quyết trong TODO4:

- Chưa có namespace `ais.visualization`.
- Chưa có visualization-ready heatmap/trajectory/plume/source/dashboard tables.
- Chưa có tile/cache export manifest cho request-time lightweight serving.
- Chưa có visualization API.
- UI đang là demo mock data, chưa là product dashboard.
- Chưa có K8s deployment cho UI.
- Chưa có Airflow DAG refresh visualization.
- Chưa có smoke test end-to-end từ visualization jobs -> API -> UI.

## 2. Target product UX

UI cuối cùng là một dashboard bản đồ full-screen tương tự tinh thần Windy, tập trung vào chất lượng không khí Bắc Bộ và Hà Nội.

Main page:

- Full-screen map.
- Viewport mặc định bao Bắc Bộ và Hà Nội:
  - center gần Hà Nội.
  - zoom đủ thấy Hà Nội, Hải Phòng, Quảng Ninh, Thái Nguyên, Bắc Ninh, Hưng Yên, Nam Định, Ninh Bình, Thanh Hóa, Lào Cai nếu bbox config bao phủ.
- Panel trái/phải/dưới tùy layout:
  - forecast cards.
  - source attribution.
  - layer controls.
  - timeline/chart.
- Time selector:
  - `latest`
  - `+6h`
  - `+12h`
  - `+24h`
- Layer control:
  - PM2.5 heatmap.
  - backward trajectories.
  - forward plume probability.
  - source attribution markers.
  - station observations.
- Forecast cards:
  - PM2.5 now/latest observed.
  - PM2.5 +6h.
  - PM2.5 +12h.
  - PM2.5 +24h.
  - `generated_at`.
  - `model_version`.
  - data freshness.
- Chart:
  - observed PM2.5 history.
  - predicted PM2.5 future.
- Map interactions:
  - Click trajectory:
    - `traj_id`
    - `cluster_id`
    - `source_lat`
    - `source_lon`
    - path NO2/AER evidence.
    - age range.
  - Click heatmap cell:
    - `pm25_value`
    - `valid_time`
    - `horizon_h`
    - `source_method`
    - `uncertainty`
  - Click source marker:
    - source label.
    - source region.
    - contribution score.
    - confidence.
    - evidence fields.
  - Click station:
    - station name/location.
    - PM2.5 observed.
    - coverage.
    - observation time.

UX acceptance criteria:

- UI không rely vào mock data ở production mode.
- User switch được giữa latest/+6/+12/+24.
- User toggle độc lập heatmap, trajectories, plume, source markers, station observations.
- UI hiển thị rõ freshness và model version.
- UI có state rõ khi plume forward chưa available.
- UI có error/empty/stale state, không fail trắng màn hình.
- Tất cả request từ UI đi qua configured API base/tile base, không fetch trực tiếp Iceberg/HDFS/raw file.

## 3. Target data flow

Luồng target:

```text
Existing TODO1/TODO2/TODO3 outputs
-> visualization Spark jobs on K8s
-> ais.visualization.* gold tables
-> optional tile/cache export
-> visualization API / PM2.5 API compatibility endpoint
-> UI map/dashboard
```

Chi tiết:

```text
ais.predictions.hanoi_pm25_forecast_gold
ais.models.hanoi_pm25_model_registry_gold
ais.air_quality.openaq_hanoi_station_hourly_silver
ais.satellite.sentinel5p_grid_silver
ais.trajectory.hysplit_trajectories_silver
ais.trajectory.hysplit_trajectories_clustered_silver
ais.features.trajectory_path_satellite_silver
ais.features.trajectory_hourly_features_silver
    |
    v
Spark-on-K8s visualization jobs
    |
    v
ais.visualization.pm25_heatmap_grid_gold
ais.visualization.backward_trajectory_paths_gold
ais.visualization.forward_plume_probability_gold
ais.visualization.pm25_forecast_dashboard_gold
ais.visualization.pm25_timeseries_gold
ais.visualization.source_attribution_gold
ais.visualization.station_observations_gold
ais.visualization.visualization_cache_manifest_gold
    |
    v
Exported JSON / GeoJSON / tile cache
    |
    v
serving/visualization_api
    |
    v
ui Windy-like dashboard
```

Request-time flow:

```text
Browser
-> GET /api/v1/visualization/...
-> Visualization API reads cache/manifest/materialized product rows
-> response
```

Cấm trong request path:

- SparkSession.
- `spark-submit`.
- HYSPLIT binary.
- ML model loading/prediction.
- Feature engineering.
- Training.
- Raw OpenAQ/S5P/ERA5 extraction.
- Full-table scan trên silver/raw tables.

## 4. Mục tiêu

Mục tiêu functional:

- [ ] Có PM2.5 heat layer cho Bắc Bộ, với horizon `0/latest`, `+6h`, `+12h`, `+24h`.
- [ ] Có backward trajectory layer từ Hà Nội, trace source regions theo cluster.
- [ ] Có forward plume/probability layer nếu upstream HYSPLIT forward output tồn tại.
- [ ] Có forecast dashboard cho PM2.5 +6h/+12h/+24h.
- [ ] Có source attribution panel giải thích likely source cluster/region.
- [ ] Có production API endpoints cho map/dashboard data.
- [ ] UI dùng production API/tile endpoints, không dùng mock trong production.
- [ ] Airflow refresh được visualization data.
- [ ] K8s deploy được visualization API/UI/check jobs.
- [ ] Có final smoke test và acceptance criteria rõ.

Mục tiêu non-functional:

- [ ] Request-time API p95 latency phải thấp vì chỉ đọc cache/product table nhỏ.
- [ ] Các visualization jobs rerun idempotent theo `base_time + horizon_h + product_version`.
- [ ] Data freshness hiển thị trong UI và kiểm tra được bằng check job.
- [ ] Layer response có schema version để frontend không phụ thuộc vào shape không ổn định.
- [ ] Forward plume là optional layer: thiếu forward HYSPLIT không làm fail toàn dashboard.

## 5. Kiến trúc đích

| Component | Runtime | Input | Output | Notes |
|---|---|---|---|---|
| Visualization Spark jobs | Spark-on-K8s | TODO1/2/3 tables | `ais.visualization.*` | Heavy transform, geospatial binning, GeoJSON aggregation. |
| Cache/tile export job | Spark-on-K8s hoặc Kubernetes Job | `ais.visualization.*` | JSON/GeoJSON/tile files + manifest | API đọc cache để tránh Spark/table scan request-time. |
| Visualization API | K8s Deployment | cache/manifest hoặc lightweight table reader | JSON/GeoJSON/tile endpoints | Không PySpark trong request process. |
| UI dashboard | K8s Deployment hoặc static hosting | Visualization API | Browser app | Production API only; mock behind dev flag. |
| Airflow DAG | Existing Airflow control plane | Upstream table freshness | K8s/Spark jobs + checks | Orchestrates refresh and backfill. |
| K8s checks | Kubernetes Job/Airflow task | API/cache/table metadata | exit code + logs | Smoke and freshness checks. |
| Monitoring optional | Docker Compose monitoring UI | check/API status | cards/status | Not part of core serving path. |

Kiến trúc serving:

- Visualization API không trực tiếp đọc raw/silver random tables.
- Các endpoint map lớn đọc cache/tile đã export.
- Các endpoint nhỏ như `/forecast/latest` có thể đọc dashboard cache hoặc `pm25_forecast_dashboard_gold` qua lightweight adapter, nhưng không dùng Spark.
- Cache manifest là contract giữa compute layer và API layer.

## 6. Nguyên tắc thiết kế

- Materialize first, serve second.
- UI chỉ gọi API/tile endpoints.
- API chỉ đọc bảng/cache đã materialize.
- Không chạy Spark/HYSPLIT/ML/feature engineering/training trong request handler.
- Không expose raw/silver schema trực tiếp ra frontend.
- Visualization tables nằm trong `ais.visualization` và có schema ổn định.
- Mỗi product table có:
  - `product_version`
  - `schema_version`
  - `generated_at`
  - `base_time`
  - `valid_time` nếu có horizon.
  - `source_method`
  - freshness metadata.
- Rerun phải idempotent.
- Forward plume optional:
  - nếu có `direction='forward'` rows thì build probability grid.
  - nếu không có, manifest ghi `available=false` và API trả response rỗng có lý do.
- Heatmap Bắc Bộ phải ghi rõ `source_method` và `uncertainty`; không giả vờ có model PM2.5 cấp tỉnh nếu upstream chỉ có Hanoi model.
- Mock data trong UI:
  - chỉ dùng khi `VITE_USE_MOCK_DATA=true`.
  - production build mặc định `false`.
  - nếu production mode gọi `/mock/*` thì fail lint/build/check.
- Time semantics:
  - `base_time`: thời điểm model/visualization run dùng làm gốc.
  - `valid_time`: thời điểm layer có hiệu lực.
  - `horizon_h`: `0`, `6`, `12`, `24`.
- Bounds:
  - Northern Vietnam bbox config nằm trong `config/hanoi_pipeline.yaml`.
  - UI không hardcode bbox ngoài config/API manifest.

## 7. Files cần tạo/sửa

### 7.1 Config và table bootstrap

Files cần sửa:

- `config/hanoi_pipeline.yaml` - **modify**
  - Thêm `visualization.region_bbox`.
  - Thêm grid resolution.
  - Thêm horizon list.
  - Thêm source cluster labels.
  - Thêm cache/export config.
- `spark_jobs/hanoi_config.py` - **modify**
  - Thêm `get_visualization_config()`.
  - Thêm `get_visualization_region_bbox()`.
  - Thêm table names `ais.visualization.*`.
  - Thêm cache path helper.
- `spark_jobs/ensure_iceberg_tables.py` - **modify**
  - Thêm namespace `ais.visualization`.
  - Tạo visualization gold tables.
- `deploy/k8s/configmap.yaml` - **modify**
  - Thêm env vars cho visualization jobs/API/UI.
- `.env.example` - **modify optional**
  - Thêm env mẫu không chứa secret.

Table names cần thêm vào `spark_jobs/hanoi_config.py`:

```python
TABLES.update({
    "visualization_heatmap_grid_gold": f"{ICEBERG_CATALOG}.visualization.pm25_heatmap_grid_gold",
    "visualization_backward_trajectory_paths_gold": f"{ICEBERG_CATALOG}.visualization.backward_trajectory_paths_gold",
    "visualization_forward_plume_probability_gold": f"{ICEBERG_CATALOG}.visualization.forward_plume_probability_gold",
    "visualization_forecast_dashboard_gold": f"{ICEBERG_CATALOG}.visualization.pm25_forecast_dashboard_gold",
    "visualization_pm25_timeseries_gold": f"{ICEBERG_CATALOG}.visualization.pm25_timeseries_gold",
    "visualization_source_attribution_gold": f"{ICEBERG_CATALOG}.visualization.source_attribution_gold",
    "visualization_station_observations_gold": f"{ICEBERG_CATALOG}.visualization.station_observations_gold",
    "visualization_cache_manifest_gold": f"{ICEBERG_CATALOG}.visualization.visualization_cache_manifest_gold",
})
```

### 7.2 Spark jobs

Files cần tạo:

- `spark_jobs/visualization_pm25_heatmap_grid_gold.py` - **create**
- `spark_jobs/visualization_backward_trajectory_paths_gold.py` - **create**
- `spark_jobs/visualization_forward_plume_probability_gold.py` - **create**
- `spark_jobs/visualization_forecast_dashboard_gold.py` - **create**
- `spark_jobs/visualization_pm25_timeseries_gold.py` - **create**
- `spark_jobs/visualization_source_attribution_gold.py` - **create**
- `spark_jobs/visualization_station_observations_gold.py` - **create**
- `spark_jobs/export_visualization_cache.py` - **create**
- `spark_jobs/visualization_quality_checks.py` - **create optional** nếu check cần Spark/table scan.

Files cần sửa:

- `scripts/submit_spark_k8s.sh` - **modify**
  - Thêm job types:
    - `visualization-heatmap-grid`
    - `visualization-backward-trajectories`
    - `visualization-forward-plume`
    - `visualization-forecast-dashboard`
    - `visualization-pm25-timeseries`
    - `visualization-source-attribution`
    - `visualization-station-observations`
    - `visualization-export-cache`
    - `visualization-quality-checks`
- `scripts/submit_spark.sh` - **modify optional**
  - Chỉ làm dev fallback nếu cần; target runtime vẫn là K8s.

### 7.3 API

Files cần tạo:

- `serving/visualization_api/main.py` - **create**
- `serving/visualization_api/requirements.txt` - **create**
- `serving/visualization_api/Dockerfile` - **create**
- `serving/visualization_api/README.md` - **create**

Files cần sửa optional:

- `serving/pm25_api/main.py` - **modify optional**
  - Refactor để không dùng PySpark trong request/readiness.
  - Hoặc giữ như compatibility service, nhưng UI TODO4 không phụ thuộc vào implementation này.

### 7.4 UI

Files cần sửa:

- `ui/package.json` - **modify**
  - Thêm map dependencies, ví dụ `maplibre-gl` hoặc `leaflet`.
  - Giữ `d3` nếu dùng chart/scale.
- `ui/src/App.jsx` - **modify**
  - Dashboard map là default page.
- `ui/src/services/api.js` - **modify**
  - Không fetch `/mock/*` mặc định.
  - Tách production API client và dev mock client.
- `ui/src/index.css` - **modify**
  - Layout full-screen map/dashboard.

Files cần tạo:

- `ui/src/pages/AirQualityMapDashboard.jsx` - **create**
- `ui/src/services/visualizationApi.js` - **create**
- `ui/src/components/map/MapCanvas.jsx` - **create**
- `ui/src/components/map/LayerControl.jsx` - **create**
- `ui/src/components/map/TimeSelector.jsx` - **create**
- `ui/src/components/map/ForecastPanel.jsx` - **create**
- `ui/src/components/map/SourceAttributionPanel.jsx` - **create**
- `ui/src/components/map/FreshnessBadge.jsx` - **create**
- `ui/src/components/map/MapPopup.jsx` - **create**
- `ui/src/components/charts/PM25ForecastChart.jsx` - **create**
- `ui/Dockerfile` - **create**
- `ui/nginx.conf` - **create optional** nếu serve static bằng Nginx.

Mock data handling:

- Giữ `ui/public/mock` chỉ cho dev/demo.
- Thêm env `VITE_USE_MOCK_DATA=false`.
- Production check fail nếu `VITE_USE_MOCK_DATA=true`.

### 7.5 Airflow

Files cần tạo:

- `airflow/dags/ais_visualization_product_dag.py` - **create**

Files cần sửa:

- `airflow/dags/ais_dag_utils.py` - **modify**
  - Thêm helper submit visualization Spark jobs trên K8s.
  - Thêm helper check API/cache nếu cần.
- `airflow/dags/ais_pm25_k8s_compute_dag.py` - **modify optional**
  - Có thể trigger visualization DAG sau khi prediction freshness pass.
- `airflow/dags/ais_trajectory_tier2_dag.py` - **modify optional**
  - Có thể expose dependency marker hoặc K8s path cho upstream trajectory.

### 7.6 K8s manifests và checks

Files cần tạo:

- `deploy/k8s/visualization-api/visualization-api-deployment.yaml` - **create**
- `deploy/k8s/visualization-api/visualization-api-service.yaml` - **create**
- `deploy/k8s/visualization-api/README.md` - **create**
- `deploy/k8s/ui/ais-ui-deployment.yaml` - **create**
- `deploy/k8s/ui/ais-ui-service.yaml` - **create**
- `deploy/k8s/ui/README.md` - **create**
- `deploy/k8s/checks/visualization-serving-check-job.yaml` - **create**

Files cần sửa:

- `deploy/k8s/kustomization.yaml` - **modify**
  - Include visualization API/UI/check manifests.
- `deploy/k8s/rbac.yaml` - **modify optional**
  - Nếu check job cần read pods/jobs.

### 7.7 Monitoring và docs

Files cần sửa optional:

- `monitoring/app.py` - **modify optional**
  - Thêm cards:
    - latest visualization run.
    - heatmap freshness.
    - plume availability.
    - API readiness.
    - UI readiness.
- `SYSTEM_OVERVIEW.md` - **modify optional after implementation**
  - Thêm TODO4 product layer.
- `README.md` - **modify optional after implementation**
  - Thêm cách chạy dashboard.

## 8. Input/Output

### 8.1 PM2.5 heatmap product

Input:

- `ais.predictions.hanoi_pm25_forecast_gold`
- `ais.air_quality.openaq_hanoi_station_hourly_silver`
- `ais.satellite.sentinel5p_grid_silver`
- `ais.features.trajectory_hourly_features_silver`
- Optional: `ais.visualization.forward_plume_probability_gold`

Output:

- `ais.visualization.pm25_heatmap_grid_gold`
- Cache:
  - `VIS_CACHE_BASE_URI/pm25_heatmap/latest/horizon=0/grid.geojson`
  - `VIS_CACHE_BASE_URI/pm25_heatmap/latest/horizon=6/grid.geojson`
  - `VIS_CACHE_BASE_URI/pm25_heatmap/latest/horizon=12/grid.geojson`
  - `VIS_CACHE_BASE_URI/pm25_heatmap/latest/horizon=24/grid.geojson`

Notes:

- `horizon_h=0` là latest observed/now layer.
- `horizon_h=6/12/24` dùng forecast table làm anchor và các proxy spatial để tạo visualization grid.
- Vì upstream model hiện là Hanoi PM2.5 forecast, heatmap Bắc Bộ phải có `source_method` và `uncertainty` rõ.

### 8.2 Backward trajectory product

Input:

- `ais.trajectory.hysplit_trajectories_clustered_silver`
- `ais.features.trajectory_path_satellite_silver`
- `ais.features.trajectory_hourly_features_silver`

Output:

- `ais.visualization.backward_trajectory_paths_gold`
- Cache:
  - `VIS_CACHE_BASE_URI/trajectories/backward/latest.geojson`

Notes:

- Một `traj_id` thành một LineString GeoJSON.
- Path points order theo `age_h`.
- Popup properties giữ source/evidence fields.

### 8.3 Forward plume product

Input:

- `ais.trajectory.hysplit_trajectories_silver` với `direction='forward'`

Output:

- `ais.visualization.forward_plume_probability_gold`
- Cache:
  - `VIS_CACHE_BASE_URI/plume/forward/latest/horizon=6/grid.geojson`
  - `VIS_CACHE_BASE_URI/plume/forward/latest/horizon=12/grid.geojson`
  - `VIS_CACHE_BASE_URI/plume/forward/latest/horizon=24/grid.geojson`

Notes:

- Nếu không có forward rows, job vẫn ghi cache manifest `available=false`.
- Dashboard không fail toàn page khi plume thiếu.

### 8.4 Forecast dashboard product

Input:

- `ais.predictions.hanoi_pm25_forecast_gold`
- `ais.models.hanoi_pm25_model_registry_gold`
- `ais.air_quality.openaq_hanoi_station_hourly_silver`

Output:

- `ais.visualization.pm25_forecast_dashboard_gold`
- `ais.visualization.pm25_timeseries_gold`
- Cache:
  - `VIS_CACHE_BASE_URI/dashboard/latest.json`
  - `VIS_CACHE_BASE_URI/timeseries/hanoi/latest.json`

### 8.5 Source attribution product

Input:

- `ais.features.trajectory_hourly_features_silver`
- `ais.trajectory.hysplit_trajectories_clustered_silver`
- `ais.features.trajectory_path_satellite_silver`
- `ais.predictions.hanoi_pm25_forecast_gold`

Output:

- `ais.visualization.source_attribution_gold`
- Cache:
  - `VIS_CACHE_BASE_URI/source_attribution/latest.geojson`

### 8.6 Station observations product

Input:

- `ais.air_quality.openaq_hanoi_station_hourly_silver`

Output:

- `ais.visualization.station_observations_gold`
- Cache:
  - `VIS_CACHE_BASE_URI/stations/latest.geojson`

### 8.7 Cache manifest

Input:

- All visualization product tables.
- Exported cache files.

Output:

- `ais.visualization.visualization_cache_manifest_gold`

Purpose:

- API biết file nào là latest.
- API trả readiness dựa trên manifest/freshness.
- Check job detect missing/stale/bad checksum.

## 9. Schema đề xuất

### 9.1 Namespace

Thêm namespace:

```sql
CREATE NAMESPACE IF NOT EXISTS ais.visualization;
```

### 9.2 `ais.visualization.pm25_heatmap_grid_gold`

Purpose:

- Canonical PM2.5 heatmap grid cho map layer.

Schema đề xuất:

```sql
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
base_time TIMESTAMP,
valid_time TIMESTAMP,
horizon_h INT,
cell_id STRING,
lat DOUBLE,
lon DOUBLE,
lat_min DOUBLE,
lat_max DOUBLE,
lon_min DOUBLE,
lon_max DOUBLE,
pm25_value DOUBLE,
risk STRING,
uncertainty DOUBLE,
source_method STRING,
observation_count INT,
satellite_product_count INT,
prediction_id STRING,
model_version STRING,
feature_version STRING,
source_cluster_id INT,
source_label STRING,
generated_at TIMESTAMP,
data_freshness_minutes INT,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (horizon_h, year, month, day)
```

Idempotency key:

```text
base_time + horizon_h + cell_id + product_version
```

Validation:

- `horizon_h` thuộc `0|6|12|24`.
- `risk` thuộc `low|medium|high|very_high|unknown`.
- `pm25_value` không âm.
- `source_method` không null.
- `uncertainty` nằm trong `[0, 1]` nếu dùng normalized uncertainty.

### 9.3 `ais.visualization.backward_trajectory_paths_gold`

Purpose:

- Backward trajectory layer sẵn sàng trả GeoJSON.

Schema đề xuất:

```sql
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
base_time TIMESTAMP,
init_time TIMESTAMP,
direction STRING,
traj_id STRING,
traj_no INT,
cluster_id INT,
source_label STRING,
source_lat DOUBLE,
source_lon DOUBLE,
source_alt_m DOUBLE,
start_lat DOUBLE,
start_lon DOUBLE,
end_lat DOUBLE,
end_lon DOUBLE,
age_start_h INT,
age_end_h INT,
point_count INT,
path_no2_mean DOUBLE,
path_aer_mean DOUBLE,
path_no2_aer_ratio DOUBLE,
geometry_geojson STRING,
properties_json STRING,
style_color STRING,
generated_at TIMESTAMP,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (direction, year, month, day)
```

Idempotency key:

```text
base_time + direction + traj_id + product_version
```

Validation:

- `direction='backward'`.
- `geometry_geojson` là GeoJSON LineString hợp lệ.
- `point_count >= 2`.
- `age_start_h <= age_end_h` sau khi normalize ordering.

### 9.4 `ais.visualization.forward_plume_probability_gold`

Purpose:

- Probability grid từ forward HYSPLIT ensemble.

Schema đề xuất:

```sql
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
base_time TIMESTAMP,
valid_time TIMESTAMP,
horizon_h INT,
cell_id STRING,
lat DOUBLE,
lon DOUBLE,
lat_min DOUBLE,
lat_max DOUBLE,
lon_min DOUBLE,
lon_max DOUBLE,
particle_count BIGINT,
total_particle_count BIGINT,
probability DOUBLE,
available BOOLEAN,
unavailable_reason STRING,
source_run_count INT,
source_method STRING,
geometry_geojson STRING,
generated_at TIMESTAMP,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (horizon_h, year, month, day)
```

Idempotency key:

```text
base_time + horizon_h + cell_id + product_version
```

Validation:

- `horizon_h` thuộc `6|12|24`.
- Nếu `available=true`, tổng `probability` theo `base_time+horizon_h` xấp xỉ `1.0`.
- Nếu không có forward HYSPLIT, không fail dashboard; manifest ghi `available=false`.

### 9.5 `ais.visualization.pm25_forecast_dashboard_gold`

Purpose:

- One-row dashboard summary cho latest Hanoi forecast.

Schema đề xuất:

```sql
dashboard_id STRING,
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
base_hour TIMESTAMP,
location_id STRING,
location_name STRING,
latest_observed_time TIMESTAMP,
pm25_latest_observed DOUBLE,
pm25_now DOUBLE,
pm25_6h DOUBLE,
risk_6h STRING,
pm25_12h DOUBLE,
risk_12h STRING,
pm25_24h DOUBLE,
risk_24h STRING,
dominant_cluster INT,
source_lat DOUBLE,
source_lon DOUBLE,
source_label STRING,
path_no2_mean DOUBLE,
path_aer_mean DOUBLE,
pm25_grad_mag DOUBLE,
model_version STRING,
model_version_6h STRING,
model_version_12h STRING,
model_version_24h STRING,
model_status STRING,
feature_version STRING,
feature_schema_hash STRING,
prediction_id STRING,
prediction_created_at TIMESTAMP,
generated_at TIMESTAMP,
prediction_freshness_minutes INT,
observation_freshness_minutes INT,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (location_id, year, month, day)
```

Idempotency key:

```text
base_hour + location_id + product_version
```

### 9.6 `ais.visualization.pm25_timeseries_gold`

Purpose:

- Chart observed history + predicted future.

Schema đề xuất:

```sql
series_id STRING,
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
location_id STRING,
location_name STRING,
base_time TIMESTAMP,
timestamp TIMESTAMP,
series_type STRING,
horizon_h INT,
pm25_value DOUBLE,
risk STRING,
source_table STRING,
source_id STRING,
model_version STRING,
generated_at TIMESTAMP,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (location_id, series_type, year, month, day)
```

Validation:

- `series_type` thuộc `observed|forecast`.
- Forecast rows có `horizon_h` thuộc `6|12|24`.
- Observed rows có `horizon_h=0`.

### 9.7 `ais.visualization.source_attribution_gold`

Purpose:

- Source attribution markers/panel cho UI.

Schema đề xuất:

```sql
attribution_id STRING,
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
base_time TIMESTAMP,
valid_time TIMESTAMP,
location_id STRING,
cluster_id INT,
source_label STRING,
source_region_type STRING,
source_lat DOUBLE,
source_lon DOUBLE,
contribution_score DOUBLE,
confidence DOUBLE,
traj_count INT,
age_window_start_h INT,
age_window_end_h INT,
evidence_no2_mean DOUBLE,
evidence_aer_mean DOUBLE,
evidence_no2_aer_ratio DOUBLE,
evidence_pm25_grad_mag DOUBLE,
explanation_vi STRING,
geometry_geojson STRING,
generated_at TIMESTAMP,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (location_id, year, month, day)
```

Validation:

- `contribution_score` trong `[0, 1]`.
- `confidence` trong `[0, 1]`.
- Source marker không được thiếu `source_lat/source_lon` nếu `cluster_id` tồn tại.

### 9.8 `ais.visualization.station_observations_gold`

Purpose:

- Station observation point layer.

Schema đề xuất:

```sql
observation_id STRING,
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
observation_time TIMESTAMP,
station_id STRING,
station_name STRING,
location_id STRING,
city STRING,
lat DOUBLE,
lon DOUBLE,
pm25 DOUBLE,
risk STRING,
coverage_pct DOUBLE,
unit STRING,
provider STRING,
source STRING,
geometry_geojson STRING,
generated_at TIMESTAMP,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (year, month, day)
```

### 9.9 `ais.visualization.visualization_cache_manifest_gold`

Purpose:

- Contract giữa export job và API.

Schema đề xuất:

```sql
manifest_id STRING,
visualization_run_id STRING,
product_version STRING,
schema_version STRING,
layer_name STRING,
base_time TIMESTAMP,
valid_time TIMESTAMP,
horizon_h INT,
location_id STRING,
format STRING,
content_type STRING,
cache_uri STRING,
tile_template STRING,
bbox_west DOUBLE,
bbox_south DOUBLE,
bbox_east DOUBLE,
bbox_north DOUBLE,
row_count BIGINT,
byte_size BIGINT,
checksum STRING,
available BOOLEAN,
unavailable_reason STRING,
generated_at TIMESTAMP,
expires_at TIMESTAMP,
year INT,
month INT,
day INT
```

Partition:

```sql
PARTITIONED BY (layer_name, year, month, day)
```

Validation:

- Mỗi required layer có latest manifest.
- `cache_uri` tồn tại nếu `available=true`.
- `checksum` match file content nếu cache backend hỗ trợ read.

## 10. Transform

### 10.1 `visualization_pm25_heatmap_grid_gold.py`

Mục đích:

- Tạo grid PM2.5 Bắc Bộ cho `horizon_h=0/6/12/24`.

CLI args:

```text
--base-time YYYY-MM-DDTHH:00:00Z
--start-date YYYY-MM-DD
--end-date YYYY-MM-DD
--horizons 0,6,12,24
--grid-resolution-deg 0.1
--product-version windy_v1
--full-refresh 0|1
--dry-run 0|1
```

Transform logic:

- Load bbox Northern Vietnam từ config.
- Generate grid cells theo resolution config.
- For `horizon_h=0`:
  - lấy latest OpenAQ station observations quanh `base_time`.
  - dùng IDW hoặc nearest-station interpolation trong phạm vi dữ liệu có coverage.
  - blend satellite proxy nếu configured, nhưng output phải ghi `source_method`.
- For `horizon_h=6/12/24`:
  - đọc `ais.predictions.hanoi_pm25_forecast_gold` latest production row.
  - map forecast value vào Hanoi anchor.
  - dùng station/satellite/plume/trajectory proxy để tạo heat surface nếu có.
  - không claim province-level model; set `uncertainty` cao hơn ở xa Hanoi/source data.
- Risk bands theo PM2.5 threshold dùng cùng API/TODO3.
- Ghi `ais.visualization.pm25_heatmap_grid_gold`.

Expected logs:

```text
job=visualization_pm25_heatmap_grid
base_time
horizons
grid_cell_count
input_prediction_count
input_station_count
input_satellite_count
output_count
min_pm25
max_pm25
source_method_counts
dry_run
status
```

Acceptance criteria:

- Rerun cùng `base_time+horizon_h+cell_id+product_version` không duplicate.
- Output có đủ 4 horizons khi upstream forecast có 6/12/24.
- `source_method` và `uncertainty` không null.
- Không đọc raw paths.

### 10.2 `visualization_backward_trajectory_paths_gold.py`

Mục đích:

- Convert point-level clustered backward trajectories thành LineString GeoJSON product.

Transform logic:

- Đọc `hysplit_trajectories_clustered_silver` với `direction='backward'`.
- Filter date/base_time window.
- Group by `traj_id`.
- Sort points by `timestamp` hoặc `age_h`.
- Build GeoJSON LineString `[lon, lat, alt_m]`.
- Join `trajectory_path_satellite_silver` theo `traj_id`.
- Join cluster labels từ config.
- Ghi `backward_trajectory_paths_gold`.

Expected logs:

```text
input_point_count
trajectory_count
cluster_count
missing_cluster_label_count
invalid_geometry_count
output_count
status
```

Acceptance criteria:

- Mỗi trajectory có `point_count >= 2`.
- GeoJSON hợp lệ.
- Popup fields có `traj_id`, `cluster_id`, `source_lat/lon`, NO2/AER evidence.

### 10.3 `visualization_forward_plume_probability_gold.py`

Mục đích:

- Tạo probability grid từ forward HYSPLIT output nếu tồn tại.

Transform logic:

- Đọc `hysplit_trajectories_silver` với `direction='forward'`.
- For `horizon_h in 6,12,24`:
  - filter `age_h = horizon_h`.
  - bin lat/lon vào grid resolution.
  - `probability = particle_count / total_particle_count`.
  - build cell GeoJSON polygon.
- Nếu không có forward rows:
  - không throw fatal nếu `FORWARD_PLUME_REQUIRED=false`.
  - ghi manifest `available=false`, `unavailable_reason='forward_hysplit_missing'`.
- Ghi `forward_plume_probability_gold`.

Acceptance criteria:

- Nếu forward rows tồn tại, mỗi horizon có probability grid.
- Tổng probability theo horizon xấp xỉ 1.
- Nếu thiếu forward rows, API/UI vẫn hoạt động và layer hiển thị unavailable.

### 10.4 `visualization_forecast_dashboard_gold.py`

Mục đích:

- Build dashboard summary one-row/latest cho forecast cards.

Transform logic:

- Đọc latest production row từ `hanoi_pm25_forecast_gold`.
- Đọc latest station observed PM2.5.
- Join model metadata fields.
- Compute freshness:
  - prediction age.
  - observation age.
  - visualization age.
- Add source label từ cluster config.
- Ghi `pm25_forecast_dashboard_gold`.

Acceptance criteria:

- Dashboard row có PM2.5 +6/+12/+24 và model version.
- Missing forecast trả failure rõ trong job/check, không tạo row giả production.

### 10.5 `visualization_pm25_timeseries_gold.py`

Mục đích:

- Build series cho chart observed history + predicted future.

Transform logic:

- Observed:
  - lấy OpenAQ station hourly, aggregate median/mean theo hour.
  - window mặc định 48h hoặc config.
- Forecast:
  - explode latest prediction row thành 3 rows `base_time+6h`, `+12h`, `+24h`.
- Union observed + forecast theo schema.
- Ghi `pm25_timeseries_gold`.

Acceptance criteria:

- Chart API có ít nhất observed history khi data tồn tại.
- Forecast points có horizon và model version.

### 10.6 `visualization_source_attribution_gold.py`

Mục đích:

- Tạo source markers và text explanation cho attribution panel.

Transform logic:

- Đọc `trajectory_hourly_features_silver` quanh latest `base_time`.
- Map `dominant_cluster` sang source label theo config.
- Compute `contribution_score` từ:
  - trajectory count.
  - path NO2/AER evidence.
  - PM2.5 gradient magnitude.
  - recency.
- Compute `confidence` từ:
  - trajectory coverage.
  - satellite evidence availability.
  - station freshness.
- Build `explanation_vi` ngắn, factual.
- Ghi `source_attribution_gold`.

Acceptance criteria:

- Panel giải thích được cluster/region và evidence.
- Không đưa khẳng định quá chắc nếu confidence thấp.

### 10.7 `visualization_station_observations_gold.py`

Mục đích:

- Tạo point layer station observations.

Transform logic:

- Đọc OpenAQ Hanoi station hourly silver.
- Filter latest/window.
- Normalize station ids.
- Compute risk.
- Build GeoJSON Point.
- Ghi station observations gold.

Acceptance criteria:

- Mỗi station point có PM2.5, observation time, coverage.
- Missing coordinates bị drop và log count.

### 10.8 `export_visualization_cache.py`

Mục đích:

- Export API-ready JSON/GeoJSON/tile cache từ visualization gold tables.

Transform logic:

- Read latest rows per product/layer/horizon.
- Write compact files:
  - heatmap grid GeoJSON or JSON grid.
  - trajectories GeoJSON.
  - plume GeoJSON.
  - dashboard JSON.
  - timeseries JSON.
  - source attribution GeoJSON.
  - stations GeoJSON.
- Compute checksum/row_count/byte_size.
- Write/update `visualization_cache_manifest_gold`.

Acceptance criteria:

- API có thể serve dashboard chỉ từ cache manifest + cache files.
- Missing required cache file làm check fail.
- Cache export rerun idempotent.

## 11. Production API endpoints

API mặc định:

- `serving/visualization_api`.

Required endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /api/v1/visualization/manifest/latest`
- `GET /api/v1/visualization/pm25/heatmap/latest?horizon_h=0|6|12|24`
- `GET /api/v1/visualization/pm25/heatmap/tiles/{z}/{x}/{y}?horizon_h=0|6|12|24` optional nếu export tile.
- `GET /api/v1/visualization/trajectories/backward/latest`
- `GET /api/v1/visualization/plume/forward/latest?horizon_h=6|12|24`
- `GET /api/v1/visualization/forecast/latest?location_id=hanoi`
- `GET /api/v1/visualization/timeseries/latest?location_id=hanoi`
- `GET /api/v1/visualization/source-attribution/latest?location_id=hanoi`
- `GET /api/v1/visualization/stations/latest`

API rules:

- Không import `pyspark`.
- Không tạo `SparkSession`.
- Không gọi `spark-submit`.
- Không gọi HYSPLIT.
- Không load ML model.
- Không query raw/silver tables trong request path.
- `/healthz` chỉ check process alive.
- `/readyz` check:
  - required env.
  - cache manifest readable.
  - required layers available/fresh.
- Logs:
  - path.
  - status_code.
  - latency_ms.
  - layer_name.
  - horizon_h.
  - cache_hit.
  - error_code.

Response shape forecast:

```json
{
  "location_id": "hanoi",
  "base_hour": "2026-05-27T12:00:00Z",
  "generated_at": "2026-05-27T12:10:00Z",
  "freshness": {
    "prediction_freshness_minutes": 10,
    "observation_freshness_minutes": 20
  },
  "forecast": {
    "now": {"pm25": 42.1, "risk": "medium"},
    "6h": {"pm25": 55.2, "risk": "medium"},
    "12h": {"pm25": 78.0, "risk": "high"},
    "24h": {"pm25": 63.5, "risk": "medium"}
  },
  "model": {
    "model_version": "example",
    "model_version_6h": "example",
    "model_version_12h": "example",
    "model_version_24h": "example",
    "feature_version": "hanoi_pm25_core_v1"
  }
}
```

Response shape plume unavailable:

```json
{
  "available": false,
  "layer_name": "forward_plume_probability",
  "horizon_h": 6,
  "reason": "forward_hysplit_missing",
  "generated_at": "2026-05-27T12:10:00Z"
}
```

Acceptance criteria:

- API returns 200 for health.
- API readiness fails with clear JSON if cache/manifest missing.
- API layer endpoints return production cache data when available.
- API returns stable empty/unavailable response for optional plume.
- No request handler uses Spark/HYSPLIT/ML.

## 12. Frontend UI implementation plan

Existing UI:

- Vite + React.
- `ui/src/services/api.js` currently fetches `/mock/*.json`.
- No map library currently in dependencies.

Target UI:

- Main page: `AirQualityMapDashboard`.
- Map library:
  - Recommended: `maplibre-gl` for vector map style and high-performance layers.
  - Alternative: `leaflet` if implementation should be simpler.
- D3 can remain for forecast chart and color scales.

Required frontend env:

```text
VITE_VIS_API_BASE=/api/v1/visualization
VITE_USE_MOCK_DATA=false
VITE_DEFAULT_HORIZON_H=0
VITE_DEFAULT_MAP_CENTER_LAT=21.0285
VITE_DEFAULT_MAP_CENTER_LON=105.8542
VITE_DEFAULT_MAP_ZOOM=7
```

Implementation tasks:

- Replace default page with map dashboard.
- Add API client:
  - `getManifestLatest()`
  - `getHeatmapLatest(horizon_h)`
  - `getBackwardTrajectoriesLatest()`
  - `getForwardPlumeLatest(horizon_h)`
  - `getForecastLatest(location_id)`
  - `getPM25TimeseriesLatest(location_id)`
  - `getSourceAttributionLatest(location_id)`
  - `getStationsLatest()`
- Add state:
  - selected horizon.
  - enabled layers.
  - selected feature popup.
  - API readiness/error state.
  - data freshness.
- Add map layers:
  - heatmap fill/canvas layer.
  - trajectory line layer.
  - plume probability fill layer.
  - source marker layer.
  - station marker layer.
- Add panels:
  - Forecast cards.
  - Source attribution panel.
  - PM2.5 chart.
  - Layer control.
  - Freshness badge.
- Mock isolation:
  - `VITE_USE_MOCK_DATA=true` can use `ui/public/mock`.
  - production mode must call API.
  - add check script or lint rule to detect `/mock/` in production API path.

UI acceptance criteria:

- `npm run build` succeeds.
- Production build does not fetch `/mock/*`.
- User can toggle each layer independently.
- User can switch horizon without full page reload.
- Popups show requested fields.
- Stale data is visible to user.
- Optional plume unavailable state is visible but not fatal.

## 13. Airflow orchestration

File cần tạo:

- `airflow/dags/ais_visualization_product_dag.py`

DAG role:

- Airflow là control plane.
- Kubernetes/Spark-on-K8s là runtime compute.
- DAG refreshes visualization products after upstream prediction/trajectory outputs exist.

Suggested DAG flow:

```text
ensure_iceberg_tables
-> check_upstream_predictions
-> check_upstream_trajectory_optional
-> visualization_forecast_dashboard
-> visualization_pm25_timeseries
-> visualization_station_observations
-> visualization_backward_trajectories
-> visualization_source_attribution
-> visualization_forward_plume_optional
-> visualization_pm25_heatmap_grid
-> export_visualization_cache
-> visualization_quality_checks
-> visualization_api_ready_check
```

Scheduling:

- Manual trigger first.
- Later schedule every 1h or after prediction CronJob.
- `catchup=False` for near-real-time dashboard.
- Backfill support by `dag_run.conf`:

```json
{
  "base_time": "2026-05-27T12:00:00Z",
  "start_date": "2026-05-27",
  "end_date": "2026-05-27",
  "horizons": "0,6,12,24",
  "product_version": "windy_v1",
  "dry_run": 0
}
```

Acceptance criteria:

- DAG can run visualization refresh end-to-end.
- DAG fails early if required prediction table/cache is missing.
- DAG does not fail if forward plume is missing and `FORWARD_PLUME_REQUIRED=false`.
- DAG has manual backfill knobs.
- Logs include cache URIs and manifest ids.

## 14. K8s deployment/checks

### 14.1 Visualization API

Files:

- `deploy/k8s/visualization-api/visualization-api-deployment.yaml`
- `deploy/k8s/visualization-api/visualization-api-service.yaml`

Deployment requirements:

- Image: `ais-visualization-api:local`.
- Env from `ais-runtime-config`.
- Liveness: `/healthz`.
- Readiness: `/readyz`.
- Resource starting point:
  - requests: `100m` CPU, `256Mi` memory.
  - limits: `500m` CPU, `512Mi` memory.
- No Spark jars/packages needed in API image.

### 14.2 UI

Files:

- `deploy/k8s/ui/ais-ui-deployment.yaml`
- `deploy/k8s/ui/ais-ui-service.yaml`

Deployment requirements:

- Build static assets.
- Serve through Nginx or simple static server.
- Configure API base URL via build args or runtime env injection.
- Service can be ClusterIP first; Ingress optional.

### 14.3 Checks

File:

- `deploy/k8s/checks/visualization-serving-check-job.yaml`

Checks:

| Check | Expected | Failure |
|---|---|---|
| API health | `/healthz` returns 200 | non-200 |
| API readiness | `/readyz` returns 200 when cache fresh | non-200 or stale reason |
| Manifest latest | required layers found | missing manifest |
| Heatmap horizons | 0/6/12/24 available | missing required horizon |
| Forecast dashboard | latest row/cache available | missing forecast |
| Backward trajectories | latest GeoJSON available if upstream has rows | missing required layer |
| Forward plume optional | available or explicit unavailable reason | silent missing |
| UI service | returns HTML/index | non-200 |
| Production mock check | no `/mock/` fetch in production bundle | mock usage found |

Acceptance criteria:

- `kubectl -n ais apply` deploys API/UI.
- `kubectl -n ais wait` passes for deployments.
- Check job exits non-zero on stale/missing required data.

## 15. Env vars

Existing env vars reused:

```text
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
HDFS_NAMENODE
HDFS_WEBHDFS_BASE
PREDICTION_TABLE
MODEL_REGISTRY_TABLE
SERVING_FEATURE_TABLE
LOCATION_ID
LOCATION_NAME
FEATURE_VERSION
SPARK_K8S_MASTER
SPARK_K8S_NAMESPACE
SPARK_IMAGE
```

New visualization job env vars:

```text
VIS_PRODUCT_VERSION=windy_v1
VIS_SCHEMA_VERSION=1
VIS_REGION_BBOX_WEST=100.0
VIS_REGION_BBOX_EAST=108.8
VIS_REGION_BBOX_SOUTH=18.0
VIS_REGION_BBOX_NORTH=24.5
VIS_GRID_RESOLUTION_DEG=0.1
VIS_HORIZONS=0,6,12,24
VIS_OBS_HISTORY_HOURS=48
VIS_FORWARD_PLUME_REQUIRED=false
VIS_CACHE_BASE_URI=hdfs://host.docker.internal:9000/visualization_cache
VIS_CACHE_FORMAT=geojson
VIS_HEATMAP_TABLE=ais.visualization.pm25_heatmap_grid_gold
VIS_TRAJECTORY_TABLE=ais.visualization.backward_trajectory_paths_gold
VIS_PLUME_TABLE=ais.visualization.forward_plume_probability_gold
VIS_DASHBOARD_TABLE=ais.visualization.pm25_forecast_dashboard_gold
VIS_TIMESERIES_TABLE=ais.visualization.pm25_timeseries_gold
VIS_SOURCE_ATTRIBUTION_TABLE=ais.visualization.source_attribution_gold
VIS_STATION_TABLE=ais.visualization.station_observations_gold
VIS_CACHE_MANIFEST_TABLE=ais.visualization.visualization_cache_manifest_gold
```

New visualization API env vars:

```text
VIS_API_PORT=8080
VIS_CACHE_BASE_URI=hdfs://host.docker.internal:9000/visualization_cache
VIS_CACHE_MANIFEST_TABLE=ais.visualization.visualization_cache_manifest_gold
VIS_REQUIRED_LAYERS=pm25_heatmap,forecast_dashboard,pm25_timeseries,backward_trajectories,source_attribution,station_observations
VIS_OPTIONAL_LAYERS=forward_plume
VIS_FRESHNESS_MAX_MINUTES=180
VIS_READY_TIMEOUT_SECONDS=5
VIS_ENABLE_TABLE_FALLBACK=false
```

New UI env vars:

```text
VITE_VIS_API_BASE=/api/v1/visualization
VITE_USE_MOCK_DATA=false
VITE_DEFAULT_HORIZON_H=0
VITE_DEFAULT_MAP_CENTER_LAT=21.0285
VITE_DEFAULT_MAP_CENTER_LON=105.8542
VITE_DEFAULT_MAP_ZOOM=7
```

Acceptance criteria:

- `.env.example`/ConfigMap document env without real secret.
- API fails readiness clearly if required env missing.
- Production UI build has `VITE_USE_MOCK_DATA=false`.

## 16. Smoke test

Smoke tests bắt buộc:

1. Bootstrap table/schema:

```bash
bash scripts/submit_spark_k8s.sh ensure-iceberg
```

Pass khi:

- `ais.visualization` namespace tồn tại.
- Tất cả visualization tables tồn tại.

2. Build forecast dashboard:

```bash
bash scripts/submit_spark_k8s.sh visualization-forecast-dashboard --dry-run 1
bash scripts/submit_spark_k8s.sh visualization-forecast-dashboard --dry-run 0
```

Pass khi:

- Có row trong `ais.visualization.pm25_forecast_dashboard_gold`.
- Row có `pm25_6h`, `pm25_12h`, `pm25_24h`, `model_version`.

3. Build timeseries:

```bash
bash scripts/submit_spark_k8s.sh visualization-pm25-timeseries --dry-run 0
```

Pass khi:

- Có observed rows.
- Có forecast rows nếu prediction tồn tại.

4. Build backward trajectories:

```bash
bash scripts/submit_spark_k8s.sh visualization-backward-trajectories --dry-run 0
```

Pass khi:

- GeoJSON LineString rows hợp lệ nếu upstream trajectory tồn tại.

5. Build forward plume:

```bash
bash scripts/submit_spark_k8s.sh visualization-forward-plume --dry-run 0
```

Pass khi:

- Nếu forward rows tồn tại: có probability grid 6/12/24.
- Nếu không tồn tại: manifest/layer state ghi `available=false`, không fail toàn DAG khi optional.

6. Build heatmap grid:

```bash
bash scripts/submit_spark_k8s.sh visualization-heatmap-grid --dry-run 0
```

Pass khi:

- Có heatmap rows cho horizons 0/6/12/24.
- `source_method`, `uncertainty`, `generated_at` không null.

7. Export cache:

```bash
bash scripts/submit_spark_k8s.sh visualization-export-cache
```

Pass khi:

- Cache files tồn tại.
- `visualization_cache_manifest_gold` có latest manifest cho required layers.

8. Build and run visualization API locally:

```bash
docker build -t ais-visualization-api:local -f serving/visualization_api/Dockerfile .
docker run --rm -p 8082:8080 --env-file .env ais-visualization-api:local
curl -i http://localhost:8082/healthz
curl -i http://localhost:8082/readyz
curl -i "http://localhost:8082/api/v1/visualization/forecast/latest?location_id=hanoi"
curl -i "http://localhost:8082/api/v1/visualization/pm25/heatmap/latest?horizon_h=6"
```

Pass khi:

- `/healthz` 200.
- `/readyz` 200 nếu cache fresh.
- Forecast/heatmap endpoints trả JSON/GeoJSON.

9. Deploy API/UI on K8s:

```bash
kubectl -n ais apply -f deploy/k8s/visualization-api
kubectl -n ais apply -f deploy/k8s/ui
kubectl -n ais wait --for=condition=available --timeout=120s deployment/visualization-api
kubectl -n ais wait --for=condition=available --timeout=120s deployment/ais-ui
```

Pass khi:

- Pods running.
- Services route được.

10. K8s API/UI smoke:

```bash
kubectl -n ais port-forward svc/visualization-api 8082:80
curl -i http://localhost:8082/healthz
curl -i http://localhost:8082/readyz
curl -i "http://localhost:8082/api/v1/visualization/manifest/latest"
```

```bash
kubectl -n ais port-forward svc/ais-ui 3000:80
curl -i http://localhost:3000/
```

Pass khi:

- API ready.
- UI returns index HTML.

11. UI production build:

```bash
cd ui
npm ci
npm run build
```

Pass khi:

- Build succeeds.
- Production bundle không chứa hardcoded `/mock/` API path.

12. Airflow DAG:

```text
Trigger ais_visualization_product_dag with dry_run=0
```

Pass khi:

- DAG runs visualization jobs in order.
- Export cache task succeeds.
- API readiness check succeeds.

## 17. Acceptance criteria

TODO4 chỉ hoàn thành khi:

- [ ] `TODO_4.md` implementation plan đã được follow bằng implementation tương ứng.
- [ ] `ais.visualization` namespace tồn tại.
- [ ] Tất cả visualization gold tables được tạo idempotent.
- [ ] Heatmap grid có data cho `latest/0h`, `+6h`, `+12h`, `+24h`.
- [ ] Backward trajectory layer hiển thị path từ Hà Nội và popup có `traj_id`, `cluster_id`, `source_lat/lon`, NO2/AER evidence.
- [ ] Forward plume layer hiển thị probability grid nếu forward HYSPLIT tồn tại, hoặc unavailable state rõ nếu không tồn tại.
- [ ] Forecast dashboard hiển thị PM2.5 now/latest observed, +6h, +12h, +24h, `generated_at`, `model_version`, freshness.
- [ ] Source attribution panel hiển thị likely source cluster/region, score, confidence, evidence.
- [ ] Production API endpoints phục vụ map/dashboard data.
- [ ] API request handlers không dùng Spark/HYSPLIT/ML/feature engineering/raw extraction.
- [ ] UI production không fetch `/mock/*`.
- [ ] UI switch được horizon và toggle layers độc lập.
- [ ] Airflow orchestration refresh được visualization tables/cache.
- [ ] K8s manifests deploy được visualization API và UI.
- [ ] Check job phát hiện stale/missing visualization data.
- [ ] Final smoke tests section 16 pass hoặc có gap được document rõ với nguyên nhân.
- [ ] TODO1/TODO2/TODO3 table contracts không bị phá.

## 18. Out of scope

- Không train model PM2.5 mới cho toàn bộ Bắc Bộ theo từng tỉnh/trạm.
- Không chạy HYSPLIT on-demand khi user click map.
- Không chạy ML inference on-demand trong API.
- Không migrate Kafka/HDFS/Iceberg/Cassandra/Airflow metadata DB vào Kubernetes.
- Không xây user auth, billing, public multi-tenant access.
- Không làm mobile app native.
- Không làm alerting/push notification.
- Không tối ưu tile server quy mô internet lớn; TODO4 chỉ cần cache/tile serving đủ cho product smoke và local/K8s deployment.
- Không thay đổi business logic TODO1/TODO2/TODO3 ngoài additions cần thiết cho visualization outputs.
