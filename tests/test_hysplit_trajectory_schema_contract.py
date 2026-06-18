from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_hysplit_parse_outputs_location_id_for_existing_iceberg_schema():
    job = (ROOT / "spark_jobs" / "hysplit_trajectory_parse_silver.py").read_text(encoding="utf-8")

    assert 'parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))' in job
    assert ".withColumn(\"location_id\", F.lit(args.location_id))" in job
    assert "ALTER TABLE {table_name} ADD COLUMN location_id STRING" in job
    assert "def align_to_target_schema" in job
    assert "df = align_to_target_schema(spark, df, table_name)" in job


def test_hysplit_trajectory_table_schema_includes_location_id():
    ensure = (ROOT / "spark_jobs" / "ensure_iceberg_tables.py").read_text(encoding="utf-8")
    parser = (ROOT / "spark_jobs" / "hysplit_trajectory_parse_silver.py").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS {TABLES[\"hysplit_traj_silver\"]}" in ensure
    assert "location_id STRING" in ensure
    assert "CREATE TABLE IF NOT EXISTS {table_name}" in parser
    assert "location_id STRING" in parser


def test_hysplit_cluster_stage_preserves_location_id_and_aligns_schema():
    cluster = (ROOT / "spark_jobs" / "hysplit_trajectory_cluster_silver.py").read_text(encoding="utf-8")
    ensure = (ROOT / "spark_jobs" / "ensure_iceberg_tables.py").read_text(encoding="utf-8")

    assert 'parser.add_argument("--location-id", default=os.getenv("LOCATION_ID", "hanoi"))' in cluster
    assert "points = points.withColumn(\"location_id\", F.lit(args.location_id))" in cluster
    assert "\"location_id\"," in cluster
    assert "def align_to_target_schema" in cluster
    assert "output = align_to_target_schema(spark, output, target_table)" in cluster
    assert "CREATE TABLE IF NOT EXISTS {TABLES[\"hysplit_cluster_silver\"]}" in ensure
    assert "location_id STRING" in ensure
