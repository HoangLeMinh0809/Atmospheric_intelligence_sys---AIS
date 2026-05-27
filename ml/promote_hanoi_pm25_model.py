from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for candidate in [
    Path(__file__).resolve().parents[1] / "spark_jobs",
    Path("/opt/ais/spark_jobs"),
    Path("/opt/spark-jobs"),
]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from hanoi_config import MODEL_ARTIFACT_BASE_URI, get_table_names  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote/rollback Hanoi PM2.5 model into Iceberg registry")
    parser.add_argument("--model-run-id", required=True)
    parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))
    parser.add_argument("--horizon-hour", type=int, required=True, choices=[6, 12, 24])
    parser.add_argument("--feature-version", default=os.getenv("FEATURE_VERSION", "hanoi_pm25_core_v1"))
    parser.add_argument("--model-version", default=os.getenv("MODEL_VERSION", ""))
    parser.add_argument("--status", default="production", choices=["production", "staging", "archived"])
    parser.add_argument("--promoted-by", default=os.getenv("PROMOTED_BY", "unknown"))
    parser.add_argument("--effective-from", default=os.getenv("EFFECTIVE_FROM", ""))
    parser.add_argument("--effective-to", default=os.getenv("EFFECTIVE_TO", ""))
    parser.add_argument("--dry-run", default=os.getenv("DRY_RUN", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    catalog = os.getenv("ICEBERG_CATALOG", "ais")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    packages = os.getenv(
        "SPARK_JARS_PACKAGES",
        "org.apache.hadoop:hadoop-client:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    )
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")

    builder = (
        SparkSession.builder.appName("PromoteHanoiPM25Model")
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config("spark.hadoop.fs.defaultFS", hdfs_namenode)
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"),
        )
    )
    if warehouse:
        builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    return builder.getOrCreate()


def main() -> None:
    args = parse_args()
    dry_run = as_bool(args.dry_run)

    tables = get_table_names()
    runs_table = os.getenv("MODEL_RUNS_TABLE", tables["model_runs_gold"])
    registry_table = os.getenv("MODEL_REGISTRY_TABLE", tables["model_registry_gold"])

    model_artifact_base = os.getenv("MODEL_ARTIFACT_BASE_URI", MODEL_ARTIFACT_BASE_URI).rstrip("/")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        run_df = spark.table(runs_table).filter(F.col("model_run_id") == F.lit(args.model_run_id))
        run_df = run_df.filter(F.col("horizon_hour") == F.lit(int(args.horizon_hour)))
        run_row = run_df.limit(1).collect()
        if not run_row:
            raise SystemExit(f"No model run found for model_run_id={args.model_run_id} horizon={args.horizon_hour}")
        run = run_row[0].asDict()

        # Prefer immutable metadata written by training; fall back to the documented convention for old runs.
        model_version = (
            (args.model_version or "").strip()
            or str(run.get("model_version") or "").strip()
            or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        )
        artifact_uri = str(run.get("artifact_uri") or "").strip()
        if not artifact_uri:
            ext = "txt"
            artifact_uri = (
                f"{model_artifact_base}/hanoi_pm25/{args.feature_version}/{model_version}"
                f"/horizon={int(args.horizon_hour)}/model.{ext}"
            )

        promoted_at = datetime.now(timezone.utc)

        # Some older runs may not have these fields; fall back safely.
        feature_schema_hash = run.get("feature_schema_hash") or run.get("schema_hash") or run.get("feature_schema")

        new_row = {
            "location_id": args.location_id,
            "horizon_hour": int(args.horizon_hour),
            "feature_version": args.feature_version,
            "model_version": model_version,
            "model_run_id": args.model_run_id,
            "model_type": run.get("model_type"),
            "model_path": run.get("model_path") or artifact_uri,
            "artifact_uri": artifact_uri,
            "feature_set_name": run.get("feature_set_name"),
            "training_dataset_version": run.get("dataset_version"),
            "feature_schema_hash": feature_schema_hash,
            "status": args.status,
            "mae": run.get("mae"),
            "rmse": run.get("rmse"),
            "mape": run.get("mape"),
            "promoted_at": promoted_at,
            "promoted_by": args.promoted_by,
            "created_at": promoted_at,
            "effective_from": args.effective_from or None,
            "effective_to": args.effective_to or None,
        }

        print(
            "promotion_request "
            f"model_run_id={args.model_run_id} location_id={args.location_id} horizon={args.horizon_hour} "
            f"feature_version={args.feature_version} model_version={model_version} status={args.status} dry_run={int(dry_run)}"
        )

        out = spark.createDataFrame([new_row], schema=spark.table(registry_table).schema)
        out.createOrReplaceTempView("src")

        # Demotion behavior: when promoting to production, explicitly demote current production
        # for the same (location_id, horizon_hour). This keeps exactly one active production
        # unless time-windowed models are used.
        if args.status == "production":
            spark.sql(
                f"""
                UPDATE {registry_table}
                SET status='archived', effective_to=current_timestamp()
                WHERE status='production'
                  AND location_id = '{args.location_id}'
                  AND horizon_hour = {int(args.horizon_hour)}
                  AND (effective_to IS NULL OR effective_to > current_timestamp())
                """
            )

        if dry_run:
            print("promotion_status status=dry_run_success")
            return

        # Promotion idempotency via MERGE on pointer dimensions.
        spark.sql(
            f"""
            MERGE INTO {registry_table} t
            USING src s
            ON t.location_id = s.location_id
              AND t.horizon_hour = s.horizon_hour
              AND t.feature_version = s.feature_version
              AND t.model_version = s.model_version
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

        print("promotion_status status=success")

        # Acceptance rule: production inference needs production model for all horizons.
        if args.status == "production":
            required = [6, 12, 24]
            prod = (
                spark.table(registry_table)
                .filter(F.col("status") == F.lit("production"))
                .filter(F.col("location_id") == F.lit(args.location_id))
                .filter(F.col("horizon_hour").isin(required))
                .select("horizon_hour")
                .distinct()
            )
            present = {int(r[0]) for r in prod.collect()}
            missing = [h for h in required if h not in present]
            if missing:
                print(f"promotion_warning missing_production_horizons={missing}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
