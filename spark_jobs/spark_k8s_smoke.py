# File nay: xu ly du lieu lakehouse hoac tac vu Spark tien ich.
from __future__ import annotations

import os

from pyspark.sql import SparkSession


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    catalog = os.getenv("ICEBERG_CATALOG", "ais")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "")
    check_iceberg = os.getenv("SPARK_SMOKE_CHECK_ICEBERG", "1").strip().lower() not in {"0", "false", "no"}

    spark = (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder.appName("AIS_SparkK8sSmoke")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        rows = spark.range(0, 10).where("id >= 0").count()
        print(f"status=running spark_action_count={rows}")
        print(f"status=config catalog={catalog} warehouse={warehouse}")

        if check_iceberg:
            namespaces = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
            namespace_names = [row[0] for row in namespaces]
            print(f"status=iceberg namespace_count={len(namespace_names)} namespaces={namespace_names[:10]}")
        else:
            print("status=iceberg skipped")

        print("status=success job=spark_k8s_smoke")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
