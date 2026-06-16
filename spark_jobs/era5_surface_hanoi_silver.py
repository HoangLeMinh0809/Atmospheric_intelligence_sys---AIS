# File nay: xu ly ERA5 thanh bang khi tuong hoac dau vao ARL cho HYSPLIT.
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import tempfile
import traceback
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hanoi_config import (
    HDFS_NAMENODE,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    SPARK_SQL_SESSION_TIMEZONE,
    get_hanoi_bbox,
    get_hanoi_center,
    get_table_names,
    parse_asof_time,
)

try:
    import netCDF4 as nc  # type: ignore
    import numpy as np  # type: ignore
except Exception as exc:  # pragma: no cover - checked at runtime.
    nc = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]
    NETCDF_IMPORT_ERROR = exc
else:
    NETCDF_IMPORT_ERROR = None


OUTPUT_COLUMNS = [
    "hour",
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
    "mean_sea_level_pressure",
    "grid_point_count",
    "source_file",
    "year",
    "month",
    "day",
    "spark_processed_at",
]

OUTPUT_SCHEMA = StructType(
    [
        StructField("hour", TimestampType(), False),
        StructField("wind_u10", DoubleType(), True),
        StructField("wind_v10", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_dir", DoubleType(), True),
        StructField("pbl_height_m", DoubleType(), True),
        StructField("low_pbl", BooleanType(), True),
        StructField("surface_pressure", DoubleType(), True),
        StructField("temperature_2m_c", DoubleType(), True),
        StructField("dewpoint_2m_c", DoubleType(), True),
        StructField("total_precipitation_mm", DoubleType(), True),
        StructField("mean_sea_level_pressure", DoubleType(), True),
        StructField("grid_point_count", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("spark_processed_at", TimestampType(), False),
    ]
)

VAR_ALIASES = {
    "wind_u10": ["u10", "10u", "10m_u_component_of_wind"],
    "wind_v10": ["v10", "10v", "10m_v_component_of_wind"],
    "pbl_height_m": ["blh", "boundary_layer_height"],
    "surface_pressure": ["sp", "surface_pressure"],
    "temperature_2m_c": ["t2m", "2t", "2m_temperature"],
    "dewpoint_2m_c": ["d2m", "2d", "2m_dewpoint_temperature"],
    "total_precipitation_mm": ["tp", "total_precipitation"],
    "mean_sea_level_pressure": ["msl", "msl_pressure", "mean_sea_level_pressure"],
}

# Doc tham so CLI va bien moi truong de cau hinh job.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi ERA5 surface hourly silver table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--asof-time", default=os.getenv("ASOF_TIME", os.getenv("SIMULATED_NOW", os.getenv("BASE_TIME", ""))))
    parser.add_argument("--asof-lookback-days", default=os.getenv("ERA5_ASOF_LOOKBACK_DAYS", "14"))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


# Chuyen flag dang chuoi nhu 1/true/yes thanh boolean.
def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# Parse va chuan hoa input cho du lieu ERA5.
def parse_date(raw: str) -> date | None:
    return datetime.strptime(raw, "%Y-%m-%d").date() if raw else None




# Doc phan cuoi file/log cho du lieu ERA5.
def _tail(value: str | None, limit: int = 2000) -> str:
    return (value or "")[-limit:]


# Chay mot lan xu ly cho du lieu ERA5.
def run_external_command(command: list[str], timeout_sec: int) -> None:
    try:
        proc = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=max(1, int(timeout_sec)),
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        raise RuntimeError(
            f"External command timed out after {timeout_sec}s: {' '.join(command)}\n"
            f"stdout_tail={_tail(stdout)}\nstderr_tail={_tail(stderr)}"
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(
            f"External command failed with code {proc.returncode}: {' '.join(command)}\n"
            f"stdout_tail={_tail(proc.stdout)}\nstderr_tail={_tail(proc.stderr)}"
        )

# Kiem tra ho tro NetCDF cho du lieu ERA5.
def require_netcdf() -> None:
    if nc is None or np is None:
        raise RuntimeError("ERA5 surface silver requires netCDF4 and numpy in the Spark Python environment") from NETCDF_IMPORT_ERROR


# Khoi tao SparkSession voi Iceberg catalog, warehouse va HDFS config.
def build_spark() -> SparkSession:
    default_fs = hdfs_default_fs()
    return (
        # Khoi tao SparkSession voi cac config cua job hien tai.
        SparkSession.builder
        .appName("ERA5SurfaceHanoiSilver")
        .config("spark.sql.session.timeZone", SPARK_SQL_SESSION_TIMEZONE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", default_fs)
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            os.getenv("HDFS_CLIENT_USE_DATANODE_HOSTNAME", "true"),
        )
        .getOrCreate()
    )


# Tao bang hourly ERA5 surface da cat ve Ha Noi.
def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.weather")
    # Bang silver nay la hourly weather backbone cho feature builder va trajectory attribution.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            hour TIMESTAMP,
            wind_u10 DOUBLE,
            wind_v10 DOUBLE,
            wind_speed DOUBLE,
            wind_dir DOUBLE,
            pbl_height_m DOUBLE,
            low_pbl BOOLEAN,
            surface_pressure DOUBLE,
            temperature_2m_c DOUBLE,
            dewpoint_2m_c DOUBLE,
            total_precipitation_mm DOUBLE,
            mean_sea_level_pressure DOUBLE,
            grid_point_count INT,
            source_file STRING,
            year INT,
            month INT,
            day INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )


# Chuan hoa va loc moc thoi gian cho du lieu ERA5.
def collect_candidate_files(spark: SparkSession, source_table: str, start_date: date | None, end_date: date | None) -> list[dict[str, Any]]:
    df = spark.table(source_table).filter(F.col("dataset_type") == F.lit("surface"))
    if start_date:
        df = df.filter(F.to_date("end_time") >= F.lit(start_date.isoformat()))
    if end_date:
        df = df.filter(F.to_date("start_time") <= F.lit(end_date.isoformat()))
    rows = (
        df.select("event_id", "file_path", "start_time", "end_time", "checksum")
        .dropDuplicates(["event_id"])
        .collect()
    )
    return [row.asDict(recursive=True) for row in rows if row["file_path"]]


# Xu ly path va storage HDFS cho du lieu ERA5.
def hdfs_default_fs() -> str:
    return (
        os.getenv("HDFS_NAMENODE")
        or os.getenv("HDFS_DEFAULT_FS")
        or os.getenv("HADOOP_DEFAULT_FS")
        or HDFS_NAMENODE
        or "hdfs://namenode:9000"
    ).rstrip("/")


# Chuan hoa record cho du lieu ERA5.
def normalize_hdfs_path(path: str, spark: SparkSession | None = None) -> str:
    raw = str(path or "").strip()
    if not raw:
        raise ValueError("Empty HDFS path")
    if not raw.startswith("hdfs://"):
        remote = raw if raw.startswith("/") else f"/{raw}"
        return f"{hdfs_default_fs()}{remote}"

    parsed = urlparse.urlparse(raw)
    configured = hdfs_default_fs()
    configured_parsed = urlparse.urlparse(configured)
    if parsed.netloc == "namenode:9000" and configured_parsed.scheme == "hdfs" and configured_parsed.netloc:
        return urlparse.urlunparse((parsed.scheme, configured_parsed.netloc, parsed.path, "", "", ""))
    return raw


# Xu ly path va storage HDFS cho du lieu ERA5.
def _hdfs_remote_path(path: str) -> str:
    parsed = urlparse.urlparse(path)
    return parsed.path if parsed.scheme == "hdfs" else path


# Lay kich thuoc file local cho du lieu ERA5.
def _local_file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return -1


# Xac nhan file local copy tu HDFS ton tai, khong rong, va san sang de doc NetCDF.
def _ensure_valid_local_copy(local_path: Path, source: str, diagnostics: list[str]) -> Path:
    if not local_path.exists():
        raise RuntimeError(f"copy completed but local file is missing: {local_path}")
    size = local_path.stat().st_size
    if size <= 0:
        raise RuntimeError(f"copy completed but local file is empty: {local_path}")
    diagnostics.append(f"local_exists=true local_size={size}")
    print(f"copied_hdfs_to_local source={source} target={local_path} size={size}")
    return local_path


# Thu thap thong tin chan doan Hadoop cho du lieu ERA5.
def _hadoop_diagnostics(spark: SparkSession | None, source: str, local_path: Path) -> list[str]:
    diagnostics = [
        f"source={source}",
        f"target={local_path}",
        f"env.HDFS_NAMENODE={os.getenv('HDFS_NAMENODE', '')}",
        f"env.HDFS_DEFAULT_FS={os.getenv('HDFS_DEFAULT_FS', '')}",
        f"env.HADOOP_DEFAULT_FS={os.getenv('HADOOP_DEFAULT_FS', '')}",
    ]
    if spark is None:
        diagnostics.append("spark_available=false")
        return diagnostics

    try:
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        diagnostics.append(f"fs.defaultFS={conf.get('fs.defaultFS')}")
        diagnostics.append(f"dfs.client.use.datanode.hostname={conf.get('dfs.client.use.datanode.hostname')}")
        jvm = spark._jvm
        uri = jvm.java.net.URI.create(source)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        src = jvm.org.apache.hadoop.fs.Path(source)
        exists = bool(fs.exists(src))
        diagnostics.append(f"exists={str(exists).lower()}")
        if exists:
            diagnostics.append(f"size={int(fs.getFileStatus(src).getLen())}")
    except Exception:
        diagnostics.append(f"diagnostic_exception={traceback.format_exc()}")
    return diagnostics


# Xu ly path va storage HDFS cho du lieu ERA5.
def copy_hdfs_to_local(path: str, spark: SparkSession | None = None) -> Path:
    original = str(path or "").strip()
    source = normalize_hdfs_path(path, spark=spark)
    print(f"hdfs_copy_prepare original={original} normalized={source}")
    if not source.startswith("hdfs://"):
        local = Path(source)
        if local.exists() and local.stat().st_size > 0:
            return local
        raise RuntimeError(f"Local ERA5 file does not exist or is empty: {local}")

    base_tmp = Path(os.getenv("ERA5_LOCAL_TMP_DIR", "/tmp/ais_era5"))
    base_tmp.mkdir(parents=True, exist_ok=True)
    local_dir = Path(tempfile.mkdtemp(prefix="surface_", dir=str(base_tmp)))
    local_path = local_dir / Path(_hdfs_remote_path(source)).name
    if local_path.exists():
        local_path.unlink()

    diagnostics = _hadoop_diagnostics(spark, source, local_path)
    diagnostics.insert(0, f"original={original}")
    errors: list[str] = []

    if spark is not None:
        try:
            # Thu copy bang Hadoop API truoc vi day la cach on dinh nhat khi job dang chay trong cluster.
            jvm = spark._jvm
            conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(source), conf)
            src = jvm.org.apache.hadoop.fs.Path(source)
            dst = jvm.org.apache.hadoop.fs.Path(str(local_path))
            if not fs.exists(src):
                raise FileNotFoundError(f"HDFS source does not exist: {source}")
            fs.copyToLocalFile(False, src, dst, True)
            return _ensure_valid_local_copy(local_path, source, diagnostics)
        except Exception:
            errors.append(f"hadoop_api_exception={traceback.format_exc()}")

    timeout = int(os.getenv("HDFS_CMD_TIMEOUT_SEC", "300") or 300)
    commands = [
        ["hdfs", "dfs", "-copyToLocal", "-f", source, str(local_path)],
        ["/opt/hadoop/bin/hdfs", "dfs", "-copyToLocal", "-f", source, str(local_path)],
        ["/opt/hadoop-3.2.1/bin/hdfs", "dfs", "-copyToLocal", "-f", source, str(local_path)],
    ]
    for command in commands:
        try:
            if local_path.exists():
                local_path.unlink()
            # Fallback sang hdfs cli de giai quyet cac case JVM API bi loi classpath/quyen.
            run_external_command(command, timeout)
            return _ensure_valid_local_copy(local_path, source, diagnostics)
        except Exception:
            errors.append(f"command_exception command={' '.join(command)}\n{traceback.format_exc()}")

    webhdfs_base = os.getenv("HDFS_WEBHDFS_BASE", "").rstrip("/")
    if webhdfs_base:
        try:
            if local_path.exists():
                local_path.unlink()
            # Fallback cuoi cung qua WebHDFS de van keo duoc file tu container khong co hdfs cli.
            quoted_remote = urlparse.quote(_hdfs_remote_path(source), safe="/")
            metadata_url = f"{webhdfs_base}{quoted_remote}?op=OPEN&noredirect=true"
            with urlrequest.urlopen(metadata_url, timeout=120) as response:  # nosec B310
                # Parse JSON tra ve thanh cau truc dict/list de xu ly tiep.
                payload = json.loads(response.read().decode("utf-8"))
            data_url = payload.get("Location", "")
            if not data_url:
                raise RuntimeError(f"WebHDFS OPEN did not return Location for {source}")
            with urlrequest.urlopen(data_url, timeout=300) as file_response:  # nosec B310
                local_path.write_bytes(file_response.read())
            return _ensure_valid_local_copy(local_path, source, diagnostics)
        except Exception:
            errors.append(f"webhdfs_exception={traceback.format_exc()}")

    details = "\n".join([*diagnostics, *errors, f"local_exists={local_path.exists()} local_size={_local_file_size(local_path)}"])
    raise RuntimeError(f"Unable to copy HDFS file to local path\n{details}")


# Xac dinh duong dan NetCDF can doc cho du lieu ERA5.
def resolve_netcdf_path(path: Path) -> Path:
    try:
        header = path.read_bytes()[:4]
    except OSError:
        return path

    # ZIP signature: PK\x03\x04
    if header != b"PK\x03\x04":
        return path

    extract_dir = path.parent / f"{path.stem}_unzipped"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "r") as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".nc")]
        if not members:
            raise RuntimeError(f"ZIP file does not contain any .nc member: {path}")
        target_member = members[0]
        zf.extract(target_member, path=extract_dir)
    return extract_dir / target_member


# Tim bien can doc trong file cho du lieu ERA5.
def _find_variable(dataset, aliases: list[str]):
    lowered = {name.lower(): name for name in dataset.variables}
    for alias in aliases:
        match = lowered.get(alias.lower())
        if match:
            return dataset.variables[match]
    return None


# Chuan hoa va loc moc thoi gian cho du lieu ERA5.
def _time_values(dataset) -> list[datetime]:
    time_var = dataset.variables.get("valid_time") or dataset.variables.get("time")
    if time_var is None:
        raise KeyError("ERA5 NetCDF missing valid_time/time variable")
    raw = np.asarray(time_var[:])
    units = getattr(time_var, "units", "")
    calendar = getattr(time_var, "calendar", "standard")
    if units:
        values = nc.num2date(raw, units=units, calendar=calendar, only_use_cftime_datetimes=False)
        return [datetime(v.year, v.month, v.day, v.hour, v.minute, v.second, tzinfo=timezone.utc) for v in values]
    return [datetime.fromtimestamp(float(v), tz=timezone.utc) for v in raw]


# Rut luoi lat/lon cho du lieu ERA5.
def _lat_lon(dataset):
    lat_var = dataset.variables.get("latitude") or dataset.variables.get("lat")
    lon_var = dataset.variables.get("longitude") or dataset.variables.get("lon")
    if lat_var is None or lon_var is None:
        raise KeyError("ERA5 NetCDF missing latitude/longitude variables")
    lat = np.asarray(lat_var[:], dtype=float)
    lon = np.asarray(lon_var[:], dtype=float)
    if lat.ndim == 1 and lon.ndim == 1:
        lon_grid, lat_grid = np.meshgrid(lon, lat)
    else:
        lat_grid = lat
        lon_grid = lon
    return lat, lon, lat_grid, lon_grid


# Chuan hoa va loc moc thoi gian cho du lieu ERA5.
def _to_time_lat_lon(variable, arr, time_len: int, lat_len: int, lon_len: int):
    dims = list(getattr(variable, "dimensions", []))
    data = np.ma.filled(np.ma.asarray(arr), np.nan).astype(float)
    data = np.where(np.isfinite(data), data, np.nan)

    time_axis = next((i for i, d in enumerate(dims) if d in {"time", "valid_time"}), None)
    lat_axis = next((i for i, d in enumerate(dims) if d in {"latitude", "lat"}), None)
    lon_axis = next((i for i, d in enumerate(dims) if d in {"longitude", "lon"}), None)

    if time_axis is None or lat_axis is None or lon_axis is None:
        shape = data.shape
        try:
            time_axis = shape.index(time_len)
            lat_axis = shape.index(lat_len)
            lon_axis = len(shape) - 1 if shape[-1] == lon_len else shape.index(lon_len)
        except ValueError as exc:
            raise ValueError(f"Cannot align ERA5 variable dimensions: dims={dims} shape={shape}") from exc

    keep_axes = [time_axis, lat_axis, lon_axis]
    extra_axes = [axis for axis in range(data.ndim) if axis not in keep_axes]
    for axis in sorted(extra_axes, reverse=True):
        data = np.nanmean(data, axis=axis)
        keep_axes = [a - 1 if a > axis else a for a in keep_axes]

    data = np.moveaxis(data, keep_axes, [0, 1, 2])
    return data


# Tao mask cho grid hop le cho du lieu ERA5.
def _grid_mask(lat_grid, lon_grid, bbox: dict[str, float], center: dict[str, float]):
    mask = (
        (lat_grid >= bbox["south"])
        & (lat_grid <= bbox["north"])
        & (lon_grid >= bbox["west"])
        & (lon_grid <= bbox["east"])
    )
    if int(mask.sum()) > 0:
        return mask, int(mask.sum())
    distance = (lat_grid - center["lat"]) ** 2 + (lon_grid - center["lon"]) ** 2
    nearest = np.unravel_index(np.nanargmin(distance), distance.shape)
    mask = np.zeros_like(lat_grid, dtype=bool)
    mask[nearest] = True
    return mask, 1


# Chuan hoa va loc moc thoi gian cho du lieu ERA5.
def _mean_at_time(cube, time_index: int, mask):
    if cube is None:
        return None
    values = cube[time_index]
    selected = values[mask]
    selected = selected[np.isfinite(selected)]
    if selected.size == 0:
        return None
    return float(np.nanmean(selected))


# Doc mot file ERA5 surface, cat theo bbox/grid gan Ha Noi, va xuat chuoi ban ghi hourly.
def read_era5_file(
    path: Path,
    source_file: str,
    start_date: date | None,
    end_date: date | None,
    asof_time: datetime | None = None,
) -> list[dict[str, Any]]:
    require_netcdf()
    bbox = get_hanoi_bbox()
    center = get_hanoi_center()
    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    resolved_path = resolve_netcdf_path(path)
    with nc.Dataset(str(resolved_path)) as dataset:
        times = _time_values(dataset)
        lat, lon, lat_grid, lon_grid = _lat_lon(dataset)
        # Cat grid theo bbox Ha Noi, neu bbox khong an diem nao thi roi ve diem gan tam nhat.
        mask, grid_count = _grid_mask(lat_grid, lon_grid, bbox, center)

        cubes = {}
        for output_name, aliases in VAR_ALIASES.items():
            variable = _find_variable(dataset, aliases)
            if variable is None:
                cubes[output_name] = None
                print(f"warning=era5_missing_variable output={output_name} aliases={aliases}")
                continue
            # Chuan hoa moi bien ve chung shape [time, lat, lon] de tinh mean theo grid de dang hon.
            cubes[output_name] = _to_time_lat_lon(variable, variable[:], len(times), len(lat), len(lon))

        for idx, hour in enumerate(times):
            hour = hour.replace(minute=0, second=0, microsecond=0)
            hour_utc_naive = hour.astimezone(timezone.utc).replace(tzinfo=None)
            if start_date and hour_utc_naive.date() < start_date:
                continue
            if end_date and hour_utc_naive.date() > end_date:
                continue
            if asof_time and hour_utc_naive > asof_time:
                continue

            wind_u10 = _mean_at_time(cubes["wind_u10"], idx, mask)
            wind_v10 = _mean_at_time(cubes["wind_v10"], idx, mask)
            wind_speed = None
            wind_dir = None
            if wind_u10 is not None and wind_v10 is not None:
                # Doi vector gio u/v thanh toc do va huong gio khi dua sang feature/visual layer.
                wind_speed = float(math.sqrt(wind_u10 ** 2 + wind_v10 ** 2))
                wind_dir = float((270.0 - math.degrees(math.atan2(wind_v10, wind_u10))) % 360.0)

            pbl = _mean_at_time(cubes["pbl_height_m"], idx, mask)
            t2m = _mean_at_time(cubes["temperature_2m_c"], idx, mask)
            d2m = _mean_at_time(cubes["dewpoint_2m_c"], idx, mask)
            tp = _mean_at_time(cubes["total_precipitation_mm"], idx, mask)

            rows.append(
                {
                    "hour": hour_utc_naive,
                    "wind_u10": wind_u10,
                    "wind_v10": wind_v10,
                    "wind_speed": wind_speed,
                    "wind_dir": wind_dir,
                    "pbl_height_m": pbl,
                    "low_pbl": bool(pbl < 300.0) if pbl is not None else None,
                    "surface_pressure": _mean_at_time(cubes["surface_pressure"], idx, mask),
                    # ERA5 thuong luu nhiet do theo Kelvin va mua theo met, doi ve don vi UI/model dang dung.
                    "temperature_2m_c": float(t2m - 273.15) if t2m is not None and t2m > 150 else t2m,
                    "dewpoint_2m_c": float(d2m - 273.15) if d2m is not None and d2m > 150 else d2m,
                    "total_precipitation_mm": float(tp * 1000.0) if tp is not None and abs(tp) < 10 else tp,
                    "mean_sea_level_pressure": _mean_at_time(cubes["mean_sea_level_pressure"], idx, mask),
                    "grid_point_count": grid_count,
                    "source_file": source_file,
                    "year": hour_utc_naive.year,
                    "month": hour_utc_naive.month,
                    "day": hour_utc_naive.day,
                    "spark_processed_at": now.replace(tzinfo=None),
                }
            )
    return rows


# Dedupe cac ban ghi hourly trung nhau tu nhieu file nguon va dong bo ve schema output cuoi.
def build_output_df(spark: SparkSession, rows: list[dict[str, Any]]):
    if rows:
        raw_df = spark.createDataFrame(rows, OUTPUT_SCHEMA)
    else:
        raw_df = spark.createDataFrame([], OUTPUT_SCHEMA)
    return (
        raw_df
        .withColumn(
            "rn",
            # Dung row_number de giu lai ban ghi uu tien nhat trong moi nhom.
            F.row_number().over(Window.partitionBy("hour").orderBy(F.col("source_file").desc_nulls_last())),
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(*OUTPUT_COLUMNS)
    )


# In metric kiem tra row count, thoi gian, duplicate va null ratio.
def log_metrics(file_count: int, rows: list[dict[str, Any]], df) -> None:
    output_count = df.count()
    duplicate_count = max(0, len(rows) - output_count)
    bounds = df.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()
    checks = df.agg(
        F.min("wind_speed").alias("wind_speed_min"),
        F.max("wind_speed").alias("wind_speed_max"),
        F.min("wind_dir").alias("wind_dir_min"),
        F.max("wind_dir").alias("wind_dir_max"),
        F.avg(F.when(F.col("pbl_height_m").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("pbl_height_null_ratio"),
    ).first().asDict() if output_count else {}
    print(f"input_count={file_count}")
    print(f"raw_hour_count={len(rows)}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"era5_checks={checks}")


# Xoa cua so ngay cu truoc khi full refresh ghi lai du lieu.
def delete_date_window(spark: SparkSession, table_name: str, time_col: str, start_date: date | None, end_date: date | None) -> None:
    predicates = []
    if start_date:
        predicates.append(f"to_date({time_col}) >= DATE '{start_date.isoformat()}'")
    if end_date:
        predicates.append(f"to_date({time_col}) <= DATE '{end_date.isoformat()}'")
    if predicates:
        spark.sql(f"DELETE FROM {table_name} WHERE {' AND '.join(predicates)}")
    else:
        spark.sql(f"DELETE FROM {table_name}")


# Ghi output cho du lieu ERA5.
def write_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool, start_date: date | None, end_date: date | None) -> None:
    if full_refresh:
        delete_date_window(spark, table_name, "hour", start_date, end_date)
    # Dang ky DataFrame tam de co the dung SQL o cac buoc sau.
    df.createOrReplaceTempView("era5_surface_hanoi_silver_updates")
    assignments = ", ".join([f"t.{c} = s.{c}" for c in OUTPUT_COLUMNS])
    insert_cols = ", ".join(OUTPUT_COLUMNS)
    insert_vals = ", ".join([f"s.{c}" for c in OUTPUT_COLUMNS])
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING era5_surface_hanoi_silver_updates s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


# Entrypoint noi cac buoc cau hinh, xu ly, ghi ket qua va cleanup.
def main() -> None:
    args = parse_args()
    tables = get_table_names()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    asof_time = parse_asof_time(args.asof_time)
    if asof_time is not None:
        lookback_days = max(0, int(args.asof_lookback_days or 0))
        lookback_start = (asof_time - timedelta(days=lookback_days)).date()
        if start_date is None or lookback_start < start_date:
            start_date = lookback_start

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["era5_files_bronze"])
    target_table = os.getenv("ICEBERG_TABLE", tables["era5_surface_silver"])
    ensure_table(spark, target_table)

    files = collect_candidate_files(spark, source_table, start_date, end_date)
    rows: list[dict[str, Any]] = []
    for item in files:
        source_file = item["file_path"]
        local_path = copy_hdfs_to_local(source_file, spark=spark)
        rows.extend(read_era5_file(local_path, source_file, start_date, end_date, asof_time))

    df = build_output_df(spark, rows)
    log_metrics(len(files), rows, df)
    write_iceberg(spark, df, target_table, full_refresh=as_bool(args.full_refresh), start_date=start_date, end_date=end_date)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
