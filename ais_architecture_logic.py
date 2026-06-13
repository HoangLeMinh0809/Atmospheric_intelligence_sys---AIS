from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Iterable, Mapping, Sequence


FUTURE_TARGET_COLUMNS = {"pm25_next_6h", "pm25_next_12h", "pm25_next_24h"}


@dataclass(frozen=True)
class DateSplit:
    start_date: str
    historical_end_date: str
    realtime_current_date: str
    historical_window_start_utc: str
    historical_window_end_utc: str
    realtime_base_date: str


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def compute_historical_and_realtime_dates(start_date: str, end_date: str) -> DateSplit:
    start = _parse_date(start_date)
    realtime = _parse_date(end_date)
    historical_end = realtime.fromordinal(realtime.toordinal() - 1)
    if start > historical_end:
        raise ValueError(
            f"Invalid AIS date split: start_date={start_date} must be <= "
            f"historical_end_date={historical_end.isoformat()} (end_date - 1 day)"
        )
    return DateSplit(
        start_date=start.isoformat(),
        historical_end_date=historical_end.isoformat(),
        realtime_current_date=realtime.isoformat(),
        historical_window_start_utc=f"{start.isoformat()}T00:00:00Z",
        historical_window_end_utc=f"{historical_end.isoformat()}T23:59:59Z",
        realtime_base_date=realtime.isoformat(),
    )


def build_historical_backfill_env(start_date: str, end_date: str) -> dict[str, str]:
    split = compute_historical_and_realtime_dates(start_date, end_date)
    return {
        "WINDOW_START_UTC": split.historical_window_start_utc,
        "WINDOW_END_UTC": split.historical_window_end_utc,
        "START_DATE": split.start_date,
        "END_DATE": split.historical_end_date,
        "HISTORICAL_BACKFILL_END_DATE": split.historical_end_date,
        "REALTIME_SIMULATION_DATE": split.realtime_current_date,
    }


def filter_records_asof(records: Iterable[Mapping[str, Any]], time_key: str, base_time: Any) -> list[dict[str, Any]]:
    base = _parse_datetime(base_time)
    result: list[dict[str, Any]] = []
    for record in records:
        value = record.get(time_key)
        if value is None:
            continue
        if _parse_datetime(value) <= base:
            result.append(dict(record))
    return result


def select_asof_context(records: Iterable[Mapping[str, Any]], time_key: str, base_time: Any) -> dict[str, Any] | None:
    scoped = filter_records_asof(records, time_key, base_time)
    if not scoped:
        return None
    return max(scoped, key=lambda row: _parse_datetime(row[time_key]))


def drop_future_target_columns(columns: Sequence[str]) -> list[str]:
    return [column for column in columns if column not in FUTURE_TARGET_COLUMNS]


def assert_no_future_target_columns(columns: Sequence[str]) -> None:
    leaked = sorted(FUTURE_TARGET_COLUMNS.intersection(columns))
    if leaked:
        raise ValueError(f"Future target columns are not allowed in online inference features: {leaked}")


def staleness_hours(base_time: Any, context_time: Any) -> float:
    return (_parse_datetime(base_time) - _parse_datetime(context_time)).total_seconds() / 3600.0


def staleness_days(base_date: Any, context_date: Any) -> int:
    return (_parse_datetime(base_date).date() - _parse_datetime(context_date).date()).days


def expected_todo4_online_order() -> list[str]:
    return [
        "historical backfill",
        "historical bronze",
        "historical silver/gold",
        "train model",
        "promote model",
        "start realtime ingest",
        "start streaming kafka to bronze",
        "start hourly era5/hysplit updater",
        "build online feature state",
        "run realtime prediction",
        "api/ui latest reads cassandra",
    ]


def resolve_latest_source(*, date: str | None, artifact: str) -> str:
    if date:
        return "historical_cache"
    if artifact in {"forecast", "heatmap"}:
        return "cassandra"
    return "historical_cache"
