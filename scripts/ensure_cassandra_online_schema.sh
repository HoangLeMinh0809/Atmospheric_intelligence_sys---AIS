#!/usr/bin/env bash
set -euo pipefail

CASSANDRA_CONTAINER="${CASSANDRA_CONTAINER:-cassandra}"
CASSANDRA_KEYSPACE="${CASSANDRA_KEYSPACE:-ais_serving}"
CASSANDRA_K8S_NAMESPACE="${CASSANDRA_K8S_NAMESPACE:-ais}"
CASSANDRA_K8S_POD="${CASSANDRA_K8S_POD:-cassandra-0}"
CASSANDRA_SCHEMA_TARGET="${CASSANDRA_SCHEMA_TARGET:-docker}"

use_k8s_cassandra() {
  case "$CASSANDRA_SCHEMA_TARGET" in
    k8s) return 0 ;;
    docker) return 1 ;;
    auto)
      kubectl -n "$CASSANDRA_K8S_NAMESPACE" get pod "$CASSANDRA_K8S_POD" >/dev/null 2>&1
      return $?
      ;;
    *)
      echo "[ERROR] Invalid CASSANDRA_SCHEMA_TARGET=$CASSANDRA_SCHEMA_TARGET (expected auto|k8s|docker)" >&2
      exit 2
      ;;
  esac
}

cqlsh_stdin() {
  if use_k8s_cassandra; then
    kubectl -n "$CASSANDRA_K8S_NAMESPACE" exec -i "$CASSANDRA_K8S_POD" -- cqlsh
  else
    docker exec -i "$CASSANDRA_CONTAINER" cqlsh
  fi
}

cqlsh_exec() {
  local cql="$1"
  if use_k8s_cassandra; then
    kubectl -n "$CASSANDRA_K8S_NAMESPACE" exec "$CASSANDRA_K8S_POD" -- cqlsh -e "$cql"
  else
    docker exec "$CASSANDRA_CONTAINER" cqlsh -e "$cql"
  fi
}

cqlsh_stdin <<CQL
CREATE KEYSPACE IF NOT EXISTS ${CASSANDRA_KEYSPACE}
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS ${CASSANDRA_KEYSPACE}.pm25_feature_state_by_location_hour (
  location_id text,
  feature_version text,
  base_hour timestamp,
  location_name text,
  feature_set_name text,
  dataset_version text,
  schema_hash text,
  feature_schema_hash text,
  base_time timestamp,
  created_at timestamp,
  loaded_at timestamp,
  data_watermark timestamp,
  pm25_now double,
  openaq_time timestamp,
  weather_time timestamp,
  era5_time timestamp,
  hysplit_time timestamp,
  satellite_date timestamp,
  s5p_staleness_days int,
  maiac_staleness_days int,
  era5_staleness_hours int,
  hysplit_staleness_hours int,
  updated_at timestamp,
  pm25_median double,
  pm25_mean double,
  station_count int,
  coverage_avg double,
  vis_km double,
  uv double,
  condition_code int,
  is_day int,
  will_it_rain int,
  chance_of_rain int,
  wind_u10 double,
  wind_v10 double,
  wind_speed double,
  wind_dir double,
  pbl_height_m double,
  low_pbl boolean,
  surface_pressure double,
  temperature_2m_c double,
  dewpoint_2m_c double,
  total_precipitation_mm double,
  s5p_no2_mean double,
  s5p_co_mean double,
  s5p_so2_mean double,
  s5p_o3_mean double,
  s5p_aer_ai_mean double,
  s5p_no2_valid_pct double,
  s5p_aer_ai_valid_pct double,
  aod_047_mean double,
  aod_055_mean double,
  aod_mean double,
  aod_max double,
  aod_valid_pct double,
  pm25_grad_n double,
  pm25_grad_s double,
  pm25_grad_e double,
  pm25_grad_w double,
  pm25_spatial_std double,
  pm25_grad_mag double,
  dominant_cluster int,
  n_traj int,
  traj_source_lat double,
  traj_source_lon double,
  traj_path_no2_mean double,
  traj_path_aer_mean double,
  traj_path_no2_aer_ratio double,
  hour_of_day int,
  day_of_week int,
  month int,
  season text,
  is_weekend boolean,
  hour_sin double,
  hour_cos double,
  dow_sin double,
  dow_cos double,
  month_sin double,
  month_cos double,
  is_rush_hour boolean,
  pm25_lag_1h double,
  pm25_lag_3h double,
  pm25_lag_6h double,
  pm25_lag_12h double,
  pm25_lag_24h double,
  pm25_roll_mean_3h double,
  pm25_roll_mean_6h double,
  pm25_roll_mean_24h double,
  pm25_roll_max_24h double,
  pm25_roll_std_24h double,
  PRIMARY KEY ((location_id, feature_version), base_hour)
) WITH CLUSTERING ORDER BY (base_hour DESC);

CREATE TABLE IF NOT EXISTS ${CASSANDRA_KEYSPACE}.pm25_forecast_latest_by_location (
  location_id text PRIMARY KEY,
  base_hour timestamp,
  prediction_id text,
  location_name text,
  pm25_now double,
  pm25_6h double,
  risk_6h text,
  pm25_12h double,
  risk_12h text,
  pm25_24h double,
  risk_24h text,
  dominant_cluster int,
  source_lat double,
  source_lon double,
  path_no2_mean double,
  path_aer_mean double,
  pm25_grad_mag double,
  model_version text,
  model_version_6h text,
  model_version_12h text,
  model_version_24h text,
  model_status text,
  feature_version text,
  feature_source text,
  feature_schema_hash text,
  data_watermark timestamp,
  updated_at timestamp,
  inference_run_id text,
  created_at timestamp
);
CQL

ensure_column() {
  local table="$1"
  local column="$2"
  local type="$3"
  if ! cqlsh_exec "SELECT column_name FROM system_schema.columns WHERE keyspace_name='${CASSANDRA_KEYSPACE}' AND table_name='${table}' AND column_name='${column}';" | grep -q "$column"; then
    cqlsh_exec "ALTER TABLE ${CASSANDRA_KEYSPACE}.${table} ADD ${column} ${type};"
  fi
}

ensure_column pm25_feature_state_by_location_hour feature_schema_hash text
ensure_column pm25_feature_state_by_location_hour base_time timestamp
ensure_column pm25_feature_state_by_location_hour data_watermark timestamp
ensure_column pm25_feature_state_by_location_hour pm25_now double
ensure_column pm25_feature_state_by_location_hour openaq_time timestamp
ensure_column pm25_feature_state_by_location_hour weather_time timestamp
ensure_column pm25_feature_state_by_location_hour era5_time timestamp
ensure_column pm25_feature_state_by_location_hour hysplit_time timestamp
ensure_column pm25_feature_state_by_location_hour satellite_date timestamp
ensure_column pm25_feature_state_by_location_hour s5p_staleness_days int
ensure_column pm25_feature_state_by_location_hour maiac_staleness_days int
ensure_column pm25_feature_state_by_location_hour era5_staleness_hours int
ensure_column pm25_feature_state_by_location_hour hysplit_staleness_hours int
ensure_column pm25_feature_state_by_location_hour updated_at timestamp

ensure_column pm25_forecast_latest_by_location feature_source text
ensure_column pm25_forecast_latest_by_location data_watermark timestamp
ensure_column pm25_forecast_latest_by_location updated_at timestamp

if use_k8s_cassandra; then
  echo "Ensured Cassandra online serving schema in keyspace ${CASSANDRA_KEYSPACE} via k8s pod ${CASSANDRA_K8S_NAMESPACE}/${CASSANDRA_K8S_POD}"
else
  echo "Ensured Cassandra online serving schema in keyspace ${CASSANDRA_KEYSPACE} via docker container ${CASSANDRA_CONTAINER}"
fi
