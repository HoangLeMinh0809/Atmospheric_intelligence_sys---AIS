# File nay: xu ly du lieu lakehouse hoac tac vu Spark tien ich.
from __future__ import annotations

import argparse
import os


# Chuyen flag dang chuoi nhu 1/true/yes thanh boolean.
def _as_bool(raw_value: str) -> bool:
    value = (raw_value or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


# Parse hai tham so runtime dung chung cho cac Spark streaming jobs: stop-after-batch va processing-time.
def parse_streaming_runtime(default_processing_time: str = "30 seconds") -> tuple[bool, str]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--stop-after-batch", nargs="?", const="1", default="0")
    parser.add_argument(
        "--processing-time",
        default=os.getenv("SPARK_STREAMING_TRIGGER", default_processing_time),
    )

    args, _ = parser.parse_known_args()
    stop_after_batch = _as_bool(str(args.stop_after_batch))
    processing_time = str(args.processing_time or default_processing_time).strip() or default_processing_time
    return stop_after_batch, processing_time


# Ap trigger mode phu hop cho writer: `availableNow` khi catch-up, `processingTime` khi stream lien tuc.
def apply_stream_trigger(writer, stop_after_batch: bool, processing_time: str):
    if stop_after_batch:
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=processing_time)
