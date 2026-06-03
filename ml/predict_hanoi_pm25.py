from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

for candidate in [
    Path(__file__).resolve().parents[1] / "spark_jobs",
    Path("/opt/ais/spark_jobs"),
    Path("/opt/spark-jobs"),
]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from hanoi_config import HDFS_NAMENODE, ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_table_names  # noqa: E402
from train_hanoi_pm25 import FEATURE_COLUMNS, FEATURE_SCHEMA_HASH  # noqa: E402


HORIZONS = [6, 12, 24]
RISK_BANDS = [
    (35.0, "low"),
    (75.0, "medium"),
    (150.0, "high"),
]


def parse_bool(raw: Any) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Hanoi PM2.5 forecast inference")
    parser.add_argument("--base-hour", default=os.getenv("BASE_HOUR", ""))
    parser.add_argument("--location", "--location-id", dest="location_id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--model-status", default=os.getenv("MODEL_STATUS", "production"))
    parser.add_argument("--feature-version", default=os.getenv("FEATURE_VERSION", "hanoi_pm25_core_v1"))
    parser.add_argument("--feature-set-name", default=os.getenv("FEATURE_SET_NAME", "hanoi_pm25_core_v1"))
    parser.add_argument(
        "--dry-run",
        nargs="?",
        const="1",
        default=os.getenv("DRY_RUN", "0"),
        help="Use 1/true to validate without writing. Bare --dry-run is treated as true.",
    )
    parser.add_argument(
        "--max-feature-age-minutes",
        type=int,
        default=int(os.getenv("MAX_FEATURE_AGE_MINUTES", "180")),
    )
    parser.add_argument(
        "--enforce-feature-freshness",
        nargs="?",
        const="1",
        default=os.getenv("ENFORCE_FEATURE_FRESHNESS", "0"),
        help="Use 1/true to fail when serving feature is older than max-feature-age-minutes.",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    catalog = os.getenv("ICEBERG_CATALOG", ICEBERG_CATALOG)
    warehouse = os.getenv("ICEBERG_WAREHOUSE", ICEBERG_WAREHOUSE)
    hdfs_namenode = os.getenv("HDFS_NAMENODE", HDFS_NAMENODE)
    packages = os.getenv(
        "SPARK_JARS_PACKAGES",
        "org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    )
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")

    return (
        SparkSession.builder.appName("PredictHanoiPM25")
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.hadoop.fs.defaultFS", hdfs_namenode)
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"),
        )
        .getOrCreate()
    )


def risk_level(pm25: float | None) -> str | None:
    if pm25 is None:
        return None
    for threshold, label in RISK_BANDS:
        if pm25 < threshold:
            return label
    return "very_high"


def prediction_id(location_id: str, base_hour: Any, feature_version: str, model_versions: dict[int, str]) -> str:
    if hasattr(base_hour, "isoformat"):
        base_hour_value = base_hour.isoformat()
    else:
        base_hour_value = str(base_hour)
    payload = "|".join(
        [
            location_id,
            base_hour_value,
            feature_version,
            model_versions[6],
            model_versions[12],
            model_versions[24],
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resolve_model_path(spark: SparkSession, raw_path: str) -> str:
    path = (raw_path or "").strip()
    if not path:
        raise ValueError("Model registry row has no model_path/artifact_uri")

    if path.startswith("file://"):
        path = path.removeprefix("file://")

    local = Path(path)
    if local.exists():
        return str(local)

    if path.startswith("hdfs://"):
        tmp_dir = Path(tempfile.mkdtemp(prefix="pm25-model-"))
        local_target = tmp_dir / Path(path).name
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), conf)
        fs.copyToLocalFile(False, jvm.org.apache.hadoop.fs.Path(path), jvm.org.apache.hadoop.fs.Path(str(local_target)))
        if not local_target.exists():
            raise FileNotFoundError(f"Failed to copy HDFS model artifact to {local_target}: {path}")
        return str(local_target)

    raise FileNotFoundError(
        f"Model artifact is not available in this container: {raw_path}. "
        "Mount MODEL_ARTIFACT_BASE_URI or promote a reachable hdfs:// artifact URI."
    )


def load_production_models(spark: SparkSession, table: str, args: argparse.Namespace) -> dict[int, dict[str, Any]]:
    now = datetime.utcnow()
    rows = (
        spark.table(table)
        .filter(F.col("location_id") == F.lit(args.location_id))
        .filter(F.col("feature_version") == F.lit(args.feature_version))
        .filter(F.col("status") == F.lit(args.model_status))
        .filter(F.col("horizon_hour").isin(HORIZONS))
        .filter((F.col("effective_from").isNull()) | (F.col("effective_from") <= F.lit(now)))
        .filter((F.col("effective_to").isNull()) | (F.col("effective_to") > F.lit(now)))
        .orderBy(F.col("promoted_at").desc_nulls_last(), F.col("created_at").desc_nulls_last())
        .collect()
    )

    models: dict[int, dict[str, Any]] = {}
    for row in rows:
        data = row.asDict()
        horizon = int(data["horizon_hour"])
        if horizon not in models:
            models[horizon] = data

    missing = [h for h in HORIZONS if h not in models]
    if missing:
        raise RuntimeError(
            f"Missing {args.model_status} model(s) for location={args.location_id} "
            f"feature_version={args.feature_version} horizons={missing}"
        )
    return models


def load_feature_row(spark: SparkSession, table: str, args: argparse.Namespace) -> dict[str, Any]:
    base_df = spark.table(table)
    candidates = []
    candidates.append(
        base_df.filter(F.col("location_id") == F.lit(args.location_id)).filter(F.col("feature_version") == F.lit(args.feature_version))
    )
    candidates.append(base_df.filter(F.col("location_id") == F.lit(args.location_id)))
    candidates.append(base_df)

    row = None
    for index, df in enumerate(candidates):
        scoped = df
        if args.feature_set_name and "feature_set_name" in scoped.columns:
            scoped = scoped.filter(F.col("feature_set_name") == F.lit(args.feature_set_name))
        if args.base_hour:
            scoped = scoped.filter(F.col("base_hour") == F.to_timestamp(F.lit(args.base_hour)))
        else:
            scoped = scoped.orderBy(F.col("base_hour").desc())
        rows = scoped.limit(1).collect()
        if rows:
            row = rows[0].asDict()
            if index > 0:
                print(f"pm25_predict feature_row_fallback_level={index}")
            break

    if row is None:
        hint = f"base_hour={args.base_hour}" if args.base_hour else "latest"
        raise RuntimeError(f"No serving feature row found for {args.location_id} {hint}")

    base_hour = row.get("base_hour")
    if base_hour is not None and not args.base_hour:
        age_minutes = (datetime.utcnow() - base_hour.replace(tzinfo=None)).total_seconds() / 60.0
        if age_minutes > args.max_feature_age_minutes:
            message = (
                f"Latest serving feature is stale: base_hour={base_hour} "
                f"age_minutes={age_minutes:.1f} threshold={args.max_feature_age_minutes}"
            )
            if parse_bool(args.enforce_feature_freshness):
                raise RuntimeError(message)
            print(f"pm25_predict warning=stale_feature_row {message}")

    missing = [name for name in FEATURE_COLUMNS if name not in row]
    if missing:
        raise RuntimeError(f"Serving feature row is missing required feature columns: {missing}")
    return row


def prepare_features(row: dict[str, Any], model_feature_names: list[str]) -> pd.DataFrame:
    pdf = pd.DataFrame([{name: row.get(name) for name in FEATURE_COLUMNS}])
    pdf["low_pbl"] = pdf["low_pbl"].fillna(False).astype(int)
    pdf["is_weekend"] = pdf["is_weekend"].fillna(False).astype(int)
    if "is_rush_hour" in pdf:
        pdf["is_rush_hour"] = pdf["is_rush_hour"].fillna(False).astype(int)
    numeric_columns = [name for name in FEATURE_COLUMNS if name != "season"]
    for name in numeric_columns:
        pdf[name] = pd.to_numeric(pdf[name], errors="coerce")
    features = pd.get_dummies(pdf[FEATURE_COLUMNS], columns=["season"], dummy_na=True)
    features = features.reindex(columns=model_feature_names, fill_value=0)
    return features.apply(pd.to_numeric, errors="coerce").astype("float64")


def predict_one(spark: SparkSession, model_meta: dict[str, Any], feature_row: dict[str, Any]) -> float:
    model_type = str(model_meta.get("model_type") or "").lower()
    raw_path = model_meta.get("artifact_uri") or model_meta.get("model_path")
    model_path = resolve_model_path(spark, str(raw_path))

    if model_type == "lightgbm":
        import lightgbm as lgb

        model = lgb.Booster(model_file=model_path)
        model_features = list(model.feature_name())
        x = prepare_features(feature_row, model_features)
        return float(model.predict(x)[0])

    if model_type == "xgboost":
        import xgboost as xgb

        model = xgb.Booster()
        model.load_model(model_path)
        model_features = list(model.feature_names or [])
        if not model_features:
            raise RuntimeError("XGBoost model artifact has no feature names; cannot safely align inference features")
        x = prepare_features(feature_row, model_features)
        return float(model.predict(xgb.DMatrix(x, feature_names=model_features))[0])

    raise RuntimeError(f"Unsupported model_type in registry: {model_type!r}")


def validate_schema(feature_row: dict[str, Any], models: dict[int, dict[str, Any]]) -> str:
    feature_schema_hash = feature_row.get("schema_hash")
    if not feature_schema_hash:
        raise RuntimeError("Serving feature row has no schema_hash")
    if feature_schema_hash != FEATURE_SCHEMA_HASH:
        raise RuntimeError(
            f"Serving feature schema_hash mismatch: expected={FEATURE_SCHEMA_HASH} actual={feature_schema_hash}"
        )

    mismatches = []
    missing = []
    for horizon, meta in models.items():
        model_hash = meta.get("feature_schema_hash")
        if not model_hash:
            missing.append(horizon)
        elif model_hash != feature_schema_hash:
            mismatches.append((horizon, model_hash))
    if missing:
        raise RuntimeError(f"Production registry row(s) missing feature_schema_hash for horizons={missing}")
    if mismatches:
        raise RuntimeError(f"Model feature_schema_hash mismatch: expected={feature_schema_hash} actual={mismatches}")
    return str(feature_schema_hash)


def ensure_prediction_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.predictions")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            prediction_id STRING,
            base_hour TIMESTAMP,
            location_id STRING,
            location_name STRING,
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
            inference_run_id STRING,
            created_at TIMESTAMP,
            year INT,
            month_partition INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month_partition)
        TBLPROPERTIES (
            'format-version'='2',
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read'
        )
        """
    )


def build_prediction_row(
    args: argparse.Namespace,
    feature_row: dict[str, Any],
    models: dict[int, dict[str, Any]],
    predictions: dict[int, float],
    feature_schema_hash: str,
) -> dict[str, Any]:
    base_hour = feature_row["base_hour"]
    model_versions = {h: str(models[h]["model_version"]) for h in HORIZONS}
    created_at = datetime.utcnow()
    inference_run_id = hashlib.sha256(f"{created_at.isoformat()}|{base_hour}|{args.location_id}".encode()).hexdigest()

    return {
        "prediction_id": prediction_id(args.location_id, base_hour, args.feature_version, model_versions),
        "base_hour": base_hour,
        "location_id": args.location_id,
        "location_name": feature_row.get("location_name") or "Hanoi",
        "pm25_now": as_float(feature_row.get("pm25_mean")),
        "pm25_6h": predictions[6],
        "risk_6h": risk_level(predictions[6]),
        "pm25_12h": predictions[12],
        "risk_12h": risk_level(predictions[12]),
        "pm25_24h": predictions[24],
        "risk_24h": risk_level(predictions[24]),
        "dominant_cluster": as_int(feature_row.get("dominant_cluster")),
        "source_lat": as_float(feature_row.get("traj_source_lat")),
        "source_lon": as_float(feature_row.get("traj_source_lon")),
        "path_no2_mean": as_float(feature_row.get("traj_path_no2_mean")),
        "path_aer_mean": as_float(feature_row.get("traj_path_aer_mean")),
        "pm25_grad_mag": as_float(feature_row.get("pm25_grad_mag")),
        "model_version": "|".join(model_versions[h] for h in HORIZONS),
        "model_version_6h": model_versions[6],
        "model_version_12h": model_versions[12],
        "model_version_24h": model_versions[24],
        "model_status": args.model_status,
        "feature_version": args.feature_version,
        "feature_schema_hash": feature_schema_hash,
        "inference_run_id": inference_run_id,
        "created_at": created_at,
        "year": int(base_hour.year),
        "month_partition": int(base_hour.month),
    }


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def write_prediction(spark: SparkSession, table: str, row: dict[str, Any]) -> None:
    ensure_prediction_table(spark, table)
    schema = StructType(
        [
            StructField("prediction_id", StringType(), False),
            StructField("base_hour", TimestampType(), False),
            StructField("location_id", StringType(), False),
            StructField("location_name", StringType(), True),
            StructField("pm25_now", DoubleType(), True),
            StructField("pm25_6h", DoubleType(), True),
            StructField("risk_6h", StringType(), True),
            StructField("pm25_12h", DoubleType(), True),
            StructField("risk_12h", StringType(), True),
            StructField("pm25_24h", DoubleType(), True),
            StructField("risk_24h", StringType(), True),
            StructField("dominant_cluster", IntegerType(), True),
            StructField("source_lat", DoubleType(), True),
            StructField("source_lon", DoubleType(), True),
            StructField("path_no2_mean", DoubleType(), True),
            StructField("path_aer_mean", DoubleType(), True),
            StructField("pm25_grad_mag", DoubleType(), True),
            StructField("model_version", StringType(), False),
            StructField("model_version_6h", StringType(), False),
            StructField("model_version_12h", StringType(), False),
            StructField("model_version_24h", StringType(), False),
            StructField("model_status", StringType(), False),
            StructField("feature_version", StringType(), False),
            StructField("feature_schema_hash", StringType(), False),
            StructField("inference_run_id", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("year", IntegerType(), False),
            StructField("month_partition", IntegerType(), False),
        ]
    )
    spark.createDataFrame([row], schema=schema).createOrReplaceTempView("prediction_src")
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING prediction_src s
        ON t.prediction_id = s.prediction_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    args = parse_args()
    dry_run = parse_bool(args.dry_run)
    tables = get_table_names()

    serving_feature_table = os.getenv("SERVING_FEATURE_TABLE", tables["serving_features_gold"])
    prediction_table = os.getenv("PREDICTION_TABLE", tables["prediction_gold"])
    model_registry_table = os.getenv("MODEL_REGISTRY_TABLE", tables["model_registry_gold"])

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        models = load_production_models(spark, model_registry_table, args)
        feature_row = load_feature_row(spark, serving_feature_table, args)
        feature_schema_hash = validate_schema(feature_row, models)
        predictions = {horizon: predict_one(spark, models[horizon], feature_row) for horizon in HORIZONS}
        row = build_prediction_row(args, feature_row, models, predictions, feature_schema_hash)

        print(
            "pm25_predict "
            f"input_count=1 output_count=1 "
            f"model_version_6h={row['model_version_6h']} "
            f"model_version_12h={row['model_version_12h']} "
            f"model_version_24h={row['model_version_24h']} "
            f"feature_version={args.feature_version} "
            f"base_hour={row['base_hour']} "
            f"location_id={args.location_id} "
            f"dry_run={int(dry_run)}"
        )

        if dry_run:
            print("pm25_predict status=dry_run_success")
            return

        write_prediction(spark, prediction_table, row)
        print(f"pm25_predict status=success prediction_id={row['prediction_id']} table={prediction_table}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
