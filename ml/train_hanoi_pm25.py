from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for candidate in [
    Path(__file__).resolve().parents[1] / "spark_jobs",
    Path("/opt/spark-jobs"),
]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from hanoi_config import HDFS_NAMENODE, ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_table_names  # noqa: E402


TARGETS = {
    6: "pm25_next_6h",
    12: "pm25_next_12h",
    24: "pm25_next_24h",
}

FEATURE_COLUMNS = [
    "pm25_median",
    "pm25_mean",
    "station_count",
    "coverage_avg",
    "vis_km",
    "uv",
    "condition_code",
    "is_day",
    "will_it_rain",
    "chance_of_rain",
    "wind_u10",
    "wind_v10",
    "wind_speed",
    "wind_dir",
    "pbl_height_m",
    "low_pbl",
    "surface_pressure",
    "temperature_2m_c",
    "dewpoint_2m_c",
    "total_precipitation_mm",
    "s5p_no2_mean",
    "s5p_co_mean",
    "s5p_so2_mean",
    "s5p_o3_mean",
    "s5p_aer_ai_mean",
    "s5p_no2_valid_pct",
    "s5p_aer_ai_valid_pct",
    "aod_047_mean",
    "aod_055_mean",
    "aod_mean",
    "aod_max",
    "aod_valid_pct",
    # Tier-2: gradient
    "pm25_grad_n",
    "pm25_grad_s",
    "pm25_grad_e",
    "pm25_grad_w",
    "pm25_spatial_std",
    "pm25_grad_mag",
    # Tier-2: trajectory
    "dominant_cluster",
    "n_traj",
    "traj_source_lat",
    "traj_source_lon",
    "traj_path_no2_mean",
    "traj_path_aer_mean",
    "traj_path_no2_aer_ratio",
    # time
    "hour_of_day",
    "day_of_week",
    "month",
    "season",
    "is_weekend",
    # Tier-2: time_sincos
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
    "is_rush_hour",
    # lags/rolling
    "pm25_lag_1h",
    "pm25_lag_3h",
    "pm25_lag_6h",
    "pm25_lag_12h",
    "pm25_lag_24h",
    "pm25_roll_mean_3h",
    "pm25_roll_mean_6h",
    "pm25_roll_mean_24h",
    "pm25_roll_max_24h",
    "pm25_roll_std_24h",
]


FEATURE_SCHEMA_HASH = hashlib.sha256(
    json.dumps({"features": FEATURE_COLUMNS}, separators=(",", ":"), sort_keys=True).encode("utf-8")
).hexdigest()

BOOLEAN_FEATURES = {"low_pbl", "is_weekend", "is_rush_hour"}
INTEGER_FEATURES = {
    "station_count",
    "condition_code",
    "is_day",
    "will_it_rain",
    "dominant_cluster",
    "n_traj",
    "hour_of_day",
    "day_of_week",
    "month",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train baseline Hanoi PM2.5 models from gold training dataset")
    parser.add_argument("--dataset-version", default=os.getenv("DATASET_VERSION", "hanoi_pm25_v1"))
    parser.add_argument("--feature-set-name", default=os.getenv("FEATURE_SET_NAME", "hanoi_pm25_core_v1"))
    parser.add_argument("--model-type", default=os.getenv("MODEL_TYPE", "lightgbm"), choices=["lightgbm", "xgboost"])
    default_output_dir = "/opt/models/hanoi_pm25" if Path("/opt/models").exists() else "models/hanoi_pm25"
    parser.add_argument("--output-dir", default=os.getenv("MODEL_OUTPUT_DIR", default_output_dir))

    # Promotion/registry related identifiers.
    parser.add_argument("--feature-version", default=os.getenv("FEATURE_VERSION", "hanoi_pm25_core_v1"))
    # Model version is an explicit label (e.g. v1, v2, 20260525_01). Default uses UTC timestamp.
    parser.add_argument("--model-version", default=os.getenv("MODEL_VERSION", ""))
    return parser.parse_args()


def build_spark() -> SparkSession:
    packages = os.getenv(
        "SPARK_JARS_PACKAGES",
        "org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    )
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")
    return (
        SparkSession.builder
        .appName("TrainHanoiPM25Baseline")
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_NAMENODE", HDFS_NAMENODE))
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"),
        )
        .getOrCreate()
    )


def ensure_model_runs_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.models")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            model_run_id STRING,
            dataset_version STRING,
            feature_set_name STRING,
            horizon_hour INT,
            model_type STRING,
            model_path STRING,
            train_start TIMESTAMP,
            train_end TIMESTAMP,
            validation_start TIMESTAMP,
            validation_end TIMESTAMP,
            test_start TIMESTAMP,
            test_end TIMESTAMP,
            mae DOUBLE,
            rmse DOUBLE,
            mape DOUBLE,
            feature_importance_path STRING,
            created_at TIMESTAMP,
            feature_version STRING,
            model_version STRING,
            artifact_uri STRING,
            feature_schema_hash STRING
        )
        USING ICEBERG
        TBLPROPERTIES ('format-version'='2')
        """
    )
    existing = set(spark.table(table_name).columns)
    for column, dtype in {
        "feature_version": "STRING",
        "model_version": "STRING",
        "artifact_uri": "STRING",
        "feature_schema_hash": "STRING",
    }.items():
        if column not in existing:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {column} {dtype}")


def prepare_frame(pdf: pd.DataFrame, target_col: str):
    pdf = pdf.dropna(subset=[target_col]).copy()
    pdf["low_pbl"] = pdf["low_pbl"].fillna(False).astype(int)
    pdf["is_weekend"] = pdf["is_weekend"].fillna(False).astype(int)
    features = pd.get_dummies(pdf[FEATURE_COLUMNS], columns=["season"], dummy_na=True)
    labels = pdf[target_col].astype(float)
    return pdf, features, labels


def default_feature_expr(name: str):
    if name == "season":
        return F.lit("unknown")
    if name in BOOLEAN_FEATURES:
        return F.lit(False)
    if name in INTEGER_FEATURES:
        return F.lit(0)
    return F.lit(0.0)


def ensure_training_feature_columns(df):
    existing = set(df.columns)
    missing = [name for name in FEATURE_COLUMNS if name not in existing]
    for name in missing:
        df = df.withColumn(name, default_feature_expr(name))
    if missing:
        print(f"Filled missing training feature columns with defaults: {missing}")
    return df


def fit_model(model_type: str, x_train: pd.DataFrame, y_train: pd.Series):
    if model_type == "lightgbm":
        from lightgbm import LGBMRegressor

        model = LGBMRegressor(
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=31,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=42,
        )
    else:
        from xgboost import XGBRegressor

        model = XGBRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.9,
            colsample_bytree=0.9,
            objective="reg:squarederror",
            random_state=42,
        )
    model.fit(x_train, y_train)
    return model


def metrics(model, x: pd.DataFrame, y: pd.Series) -> tuple[float | None, float | None, float | None]:
    if x.empty or y.empty:
        return None, None, None
    pred = model.predict(x)
    err = pred - y.to_numpy()
    mae = float(abs(err).mean())
    rmse = float(math.sqrt((err ** 2).mean()))
    non_zero = y.to_numpy() != 0
    mape = float((abs(err[non_zero] / y.to_numpy()[non_zero])).mean() * 100.0) if non_zero.any() else None
    return mae, rmse, mape


def save_model(model, model_type: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if model_type == "lightgbm":
        model.booster_.save_model(str(path))
    else:
        model.save_model(str(path))


def is_hdfs_uri(uri: str) -> bool:
    return urlparse(uri).scheme == "hdfs"


def hdfs_join(*parts: str) -> str:
    first = parts[0].rstrip("/")
    rest = [part.strip("/") for part in parts[1:] if part]
    return "/".join([first, *rest])


def copy_local_to_hdfs(spark: SparkSession, local_path: Path, hdfs_uri: str) -> None:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(hdfs_uri), conf)
    target = jvm.org.apache.hadoop.fs.Path(hdfs_uri)
    fs.mkdirs(target.getParent())
    fs.copyFromLocalFile(False, True, jvm.org.apache.hadoop.fs.Path(str(local_path)), target)


def materialize_artifact(spark: SparkSession, local_path: Path, artifact_uri: str) -> str:
    if is_hdfs_uri(artifact_uri):
        copy_local_to_hdfs(spark, local_path, artifact_uri)
        return artifact_uri

    if artifact_uri.startswith("file://"):
        artifact_uri = artifact_uri.removeprefix("file://")

    target = Path(artifact_uri)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(local_path.read_bytes())
    return str(target)


def write_importance(model, features: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    importance = getattr(model, "feature_importances_", None)
    if importance is None:
        return
    pd.DataFrame({"feature": features, "importance": importance}).sort_values(
        "importance", ascending=False
    ).to_csv(path, index=False)


def split_bounds(pdf: pd.DataFrame, split: str) -> tuple[datetime | None, datetime | None]:
    split_pdf = pdf[pdf["split"] == split]
    if split_pdf.empty:
        return None, None
    return split_pdf["hour"].min().to_pydatetime(), split_pdf["hour"].max().to_pydatetime()


def train_one_horizon(
    spark: SparkSession,
    pdf: pd.DataFrame,
    args: argparse.Namespace,
    horizon: int,
    target_col: str,
    output_dir: Path,
) -> dict:
    prepared, features, labels = prepare_frame(pdf, target_col)
    train_mask = prepared["split"] == "train"
    val_mask = prepared["split"] == "validation"
    test_mask = prepared["split"] == "test"

    if train_mask.sum() == 0:
        raise ValueError(f"No train rows for horizon={horizon}")

    x_train = features[train_mask]
    y_train = labels[train_mask]
    model = fit_model(args.model_type, x_train, y_train)

    aligned_features = list(x_train.columns)
    x_val = features[val_mask].reindex(columns=aligned_features, fill_value=0)
    y_val = labels[val_mask]
    x_test = features[test_mask].reindex(columns=aligned_features, fill_value=0)
    y_test = labels[test_mask]

    val_mae, val_rmse, val_mape = metrics(model, x_val, y_val)
    test_mae, test_rmse, test_mape = metrics(model, x_test, y_test)

    suffix = f"{args.model_type}_pm25_{horizon}h"
    local_output_dir = output_dir
    if is_hdfs_uri(os.getenv("MODEL_ARTIFACT_BASE_URI", "")):
        local_output_dir = Path(tempfile.mkdtemp(prefix="hanoi-pm25-model-"))

    model_path = local_output_dir / f"{suffix}.txt"
    importance_path = local_output_dir / f"{suffix}_feature_importance.csv"
    save_model(model, args.model_type, model_path)
    write_importance(model, aligned_features, importance_path)

    train_start, train_end = split_bounds(prepared, "train")
    validation_start, validation_end = split_bounds(prepared, "validation")
    test_start, test_end = split_bounds(prepared, "test")
    created_at = datetime.utcnow()

    artifact_base = os.getenv("MODEL_ARTIFACT_BASE_URI", "/opt/models").rstrip("/")
    model_version = args.model_version
    ext = "txt"  # train_hanoi_pm25.py writes .txt
    artifact_uri = hdfs_join(artifact_base, "hanoi_pm25", args.feature_version, model_version, f"horizon={horizon}", f"model.{ext}")
    importance_uri = hdfs_join(
        artifact_base,
        "hanoi_pm25",
        args.feature_version,
        model_version,
        f"horizon={horizon}",
        "feature_importance.csv",
    )
    materialized_model_uri = materialize_artifact(spark, model_path, artifact_uri)
    materialized_importance_uri = materialize_artifact(spark, importance_path, importance_uri)

    return {
        "model_run_id": f"{args.dataset_version}_{args.feature_set_name}_{args.model_type}_{horizon}h_{created_at.strftime('%Y%m%d%H%M%S')}",
        "dataset_version": args.dataset_version,
        "feature_set_name": args.feature_set_name,
        "horizon_hour": int(horizon),
        "model_type": args.model_type,
        "model_path": materialized_model_uri,
        "train_start": train_start,
        "train_end": train_end,
        "validation_start": validation_start,
        "validation_end": validation_end,
        "test_start": test_start,
        "test_end": test_end,
        "mae": test_mae if test_mae is not None else val_mae,
        "rmse": test_rmse if test_rmse is not None else val_rmse,
        "mape": test_mape if test_mape is not None else val_mape,
        "feature_importance_path": materialized_importance_uri,
        "created_at": created_at,
        "feature_version": args.feature_version,
        "model_version": model_version,
        "artifact_uri": materialized_model_uri,
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
    }


def main() -> None:
    args = parse_args()
    if not args.model_version:
        args.model_version = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_model_runs_table(spark, tables["model_runs_gold"])

    source = (
        spark.table(tables["training_gold"])
        .filter(f"dataset_version = '{args.dataset_version}'")
        .filter(f"feature_set_name = '{args.feature_set_name}'")
        .orderBy("hour")
    )
    source = ensure_training_feature_columns(source).select("split", "hour", *FEATURE_COLUMNS, *TARGETS.values())
    pdf = source.toPandas()
    if pdf.empty:
        raise SystemExit("No rows found in gold training dataset for requested dataset_version/feature_set_name")

    output_dir = Path(args.output_dir)
    rows = [train_one_horizon(spark, pdf, args, horizon, target, output_dir) for horizon, target in TARGETS.items()]
    spark.createDataFrame(rows).writeTo(tables["model_runs_gold"]).append()
    print(f"Wrote model metadata rows: {len(rows)} -> {tables['model_runs_gold']}")
    spark.stop()


if __name__ == "__main__":
    main()
