# File nay: orchestrate cac Spark job theo dung thu tu bronze/silver/gold.
from __future__ import annotations

import contextlib
import importlib
import os
import sys
from pathlib import Path
from typing import Iterator

from pyspark.sql import SparkSession


ROOT_DIR = Path(__file__).resolve().parents[2]
SPARK_JOBS_DIR = ROOT_DIR / "spark_jobs"

for path in (ROOT_DIR, SPARK_JOBS_DIR):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)


# Tao mot SparkSession dung chung cho ca pipeline de cac module con co the tai su dung.
def build_pipeline_spark(app_name: str) -> SparkSession:
    from hanoi_config import ICEBERG_CATALOG, ICEBERG_WAREHOUSE, SPARK_SQL_SESSION_TIMEZONE

    packages = os.getenv("SPARK_JARS_PACKAGES", "").strip()
    ivy_dir = os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2")
    builder = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder.appName(app_name)
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.session.timeZone", SPARK_SQL_SESSION_TIMEZONE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", hdfs_base_uri())
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"),
        )
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()


# Tam thoi patch bien moi truong trong pham vi chay module dich.
@contextlib.contextmanager
def patched_environ(updates: dict[str, str | None]) -> Iterator[None]:
    original: dict[str, str | None] = {}
    for key, value in updates.items():
        original[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# Ep PySpark nhan session chia se nay la active/default session cua process hien tai.
def _mark_active_session(shared_spark: SparkSession) -> None:
    try:
        SparkSession._instantiatedSession = shared_spark
        SparkSession._activeSession = shared_spark
    except Exception:
        pass


# Goi `main()` cua mot module pipeline con trong cung process va cung SparkSession.
def invoke_module_main(
    module_name: str,
    argv: list[str],
    shared_spark: SparkSession,
    *,
    env: dict[str, str | None] | None = None,
) -> None:
    env_updates = env or {}
    print(f"pipeline_step module={module_name} argv={argv}")
    with patched_environ(env_updates):
        original_argv = sys.argv
        original_stop = SparkSession.stop
        sys.argv = [module_name, *argv]
        try:
            _mark_active_session(shared_spark)
            SparkSession.stop = lambda self: None
            module = importlib.import_module(module_name)
            module = importlib.reload(module)
            if hasattr(module, "build_spark"):
                module.build_spark = lambda *args, **kwargs: shared_spark
            _mark_active_session(shared_spark)
            module.main()
        finally:
            SparkSession.stop = original_stop
            sys.argv = original_argv
            _mark_active_session(shared_spark)


# Tra ve HDFS endpoint goc cho pipeline orchestration.
def hdfs_base_uri() -> str:
    return (
        os.getenv("HDFS_NAMENODE")
        or os.getenv("HDFS_DEFAULT_FS")
        or os.getenv("HADOOP_DEFAULT_FS")
        or "hdfs://namenode:9000"
    ).rstrip("/")
