from __future__ import annotations

import json
import os
import subprocess
import tempfile
import traceback
from pathlib import Path
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest


def hdfs_default_fs() -> str:
    return (
        os.getenv("HDFS_NAMENODE")
        or os.getenv("HDFS_DEFAULT_FS")
        or os.getenv("HADOOP_DEFAULT_FS")
        or "hdfs://namenode:9000"
    ).rstrip("/")


def normalize_hdfs_path(path: str) -> str:
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


def hdfs_remote_path(path: str) -> str:
    parsed = urlparse.urlparse(path)
    return parsed.path if parsed.scheme == "hdfs" else path


def _tail(value: str | None, limit: int = 2000) -> str:
    return (value or "")[-limit:]


def _run_external_command(command: list[str], timeout_sec: int) -> None:
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


def _local_file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return -1


def _ensure_valid_local_copy(local_path: Path, source: str) -> Path:
    if not local_path.exists():
        raise RuntimeError(f"copy completed but local file is missing: {local_path}")
    size = local_path.stat().st_size
    if size <= 0:
        raise RuntimeError(f"copy completed but local file is empty: {local_path}")
    print(f"copied_hdfs_to_local source={source} target={local_path} size={size}")
    return local_path


def _hadoop_diagnostics(spark, source: str, local_path: Path) -> list[str]:
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


def list_hdfs_files(root_path: str, spark) -> dict[str, str]:
    source = normalize_hdfs_path(root_path)
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(source), conf)
    root = jvm.org.apache.hadoop.fs.Path(source)
    try:
        if not fs.exists(root):
            return {}
    except Exception:
        return {}

    index: dict[str, str] = {}
    iterator = fs.listFiles(root, True)
    while iterator.hasNext():
        status = iterator.next()
        path_text = status.getPath().toString()
        basename = status.getPath().getName()
        index.setdefault(basename, path_text)
    return index


def copy_hdfs_to_local(path: str, spark, *, prefix: str = "hdfs_", temp_base: str = "/tmp/ais_hdfs") -> Path:
    original = str(path or "").strip()
    source = normalize_hdfs_path(path)
    print(f"hdfs_copy_prepare original={original} normalized={source}")
    if not source.startswith("hdfs://"):
        local = Path(source)
        if local.exists() and local.stat().st_size > 0:
            return local
        raise RuntimeError(f"Local file does not exist or is empty: {local}")

    base_tmp = Path(temp_base)
    base_tmp.mkdir(parents=True, exist_ok=True)
    local_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=str(base_tmp)))
    local_path = local_dir / Path(hdfs_remote_path(source)).name
    if local_path.exists():
        local_path.unlink()

    diagnostics = _hadoop_diagnostics(spark, source, local_path)
    diagnostics.insert(0, f"original={original}")
    errors: list[str] = []

    if spark is not None:
        try:
            jvm = spark._jvm
            conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(source), conf)
            src = jvm.org.apache.hadoop.fs.Path(source)
            dst = jvm.org.apache.hadoop.fs.Path(str(local_path))
            if not fs.exists(src):
                raise FileNotFoundError(f"HDFS source does not exist: {source}")
            fs.copyToLocalFile(False, src, dst, True)
            return _ensure_valid_local_copy(local_path, source)
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
            _run_external_command(command, timeout)
            return _ensure_valid_local_copy(local_path, source)
        except Exception:
            errors.append(f"command_exception command={' '.join(command)}\n{traceback.format_exc()}")

    webhdfs_base = os.getenv("HDFS_WEBHDFS_BASE", "").rstrip("/")
    if webhdfs_base:
        try:
            if local_path.exists():
                local_path.unlink()
            quoted_remote = urlparse.quote(hdfs_remote_path(source), safe="/")
            metadata_url = f"{webhdfs_base}{quoted_remote}?op=OPEN&noredirect=true"
            with urlrequest.urlopen(metadata_url, timeout=120) as response:  # nosec B310
                payload = json.loads(response.read().decode("utf-8"))
            data_url = payload.get("Location", "")
            if not data_url:
                raise RuntimeError(f"WebHDFS OPEN did not return Location for {source}")
            with urlrequest.urlopen(data_url, timeout=300) as file_response:  # nosec B310
                local_path.write_bytes(file_response.read())
            return _ensure_valid_local_copy(local_path, source)
        except (urlerror.URLError, TimeoutError, OSError, ValueError, RuntimeError):
            errors.append(f"webhdfs_exception={traceback.format_exc()}")

    details = "\n".join([*diagnostics, *errors, f"local_exists={local_path.exists()} local_size={_local_file_size(local_path)}"])
    raise RuntimeError(f"Unable to copy HDFS file to local path\n{details}")
