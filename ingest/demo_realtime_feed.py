import json
import logging
import math
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

LOGGER = logging.getLogger("demo_realtime_feed")

WEATHER_LOCATIONS = [
    {
        "province": "Hanoi",
        "country": "Vietnam",
        "region": "Ha Noi",
        "location_name": "Hanoi",
        "lat": 21.0285,
        "lon": 105.8542,
        "temp_base": 29.2,
        "humidity_base": 72,
        "pm_proxy": 34.0,
    },
    {
        "province": "Bac Ninh",
        "country": "Vietnam",
        "region": "Bac Ninh",
        "location_name": "Bac Ninh",
        "lat": 21.1861,
        "lon": 106.0763,
        "temp_base": 28.8,
        "humidity_base": 74,
        "pm_proxy": 38.0,
    },
    {
        "province": "Hung Yen",
        "country": "Vietnam",
        "region": "Hung Yen",
        "location_name": "Hung Yen",
        "lat": 20.6464,
        "lon": 106.0511,
        "temp_base": 29.4,
        "humidity_base": 73,
        "pm_proxy": 36.5,
    },
]

OPENAQ_STATIONS = [
    {
        "location_id": 10001,
        "location_name": "Demo Hanoi Hoan Kiem",
        "city": "Hanoi",
        "latitude": 21.0285,
        "longitude": 105.8542,
        "provider": "demo-prepared-feed",
        "sensor_id": 50001,
        "base_pm25": 35.0,
    },
    {
        "location_id": 10002,
        "location_name": "Demo Hanoi Tay Ho",
        "city": "Hanoi",
        "latitude": 21.0681,
        "longitude": 105.8146,
        "provider": "demo-prepared-feed",
        "sensor_id": 50002,
        "base_pm25": 31.0,
    },
    {
        "location_id": 10003,
        "location_name": "Demo Hanoi Long Bien",
        "city": "Hanoi",
        "latitude": 21.0381,
        "longitude": 105.8897,
        "provider": "demo-prepared-feed",
        "sensor_id": 50003,
        "base_pm25": 39.0,
    },
]


def env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    return float(value) if value else default


def parse_sources() -> set[str]:
    raw = os.getenv("DEMO_FEED_SOURCES", "weather,openaq")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def parse_base_time() -> datetime:
    raw = os.getenv("DEMO_FEED_BASE_TIME", "").strip()
    if not raw:
        return datetime.now(timezone.utc).replace(microsecond=0)
    normalized = raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def round1(value: float) -> float:
    return round(value, 1)


def noisy(rng: random.Random, value: float, ratio: float, absolute_floor: float = 0.05) -> float:
    width = max(abs(value) * ratio, absolute_floor)
    return value + rng.uniform(-width, width)


def tick_wave(tick_index: int, phase: float = 0.0, amplitude: float = 1.0) -> float:
    return amplitude * math.sin(tick_index * 0.72 + phase)


def location_phase(*parts: object) -> float:
    token = "|".join(str(part) for part in parts)
    return (sum(ord(ch) for ch in token) % 628) / 100.0


def build_weather_events(ticks: list[datetime], noise_ratio: float, rng: random.Random, replay_id: str) -> list[dict]:
    events: list[dict] = []
    for tick_index, tick in enumerate(ticks):
        hour_angle = (tick.hour + tick.minute / 60.0) / 24.0 * 2 * math.pi
        for location in WEATHER_LOCATIONS:
            phase = location_phase(location["province"], replay_id)
            tick_pulse = tick_wave(tick_index, phase)
            gust_pulse = tick_wave(tick_index, phase + 1.3)
            temp_c = noisy(rng, location["temp_base"] + 2.8 * math.sin(hour_angle - 0.8) + tick_pulse * 0.75, noise_ratio)
            humidity = clamp(noisy(rng, location["humidity_base"] - 8.0 * math.sin(hour_angle - 0.4) - tick_pulse * 3.5, noise_ratio), 35, 98)
            wind_kph = clamp(noisy(rng, 8.0 + 3.0 * math.sin(hour_angle + 1.7) + gust_pulse * 2.2, noise_ratio), 1, 32)
            gust_kph = wind_kph + abs(noisy(rng, 5.0, noise_ratio))
            pressure_mb = noisy(rng, 1009.0 + 2.5 * math.cos(hour_angle) + tick_pulse * 1.2, min(noise_ratio, 0.006), 0.8)
            precip_mm = max(0.0, noisy(rng, (0.12 + abs(tick_pulse) * 0.04) if humidity > 82 else 0.0, noise_ratio, 0.03))
            cloud = int(clamp(noisy(rng, 58 + humidity * 0.25 + tick_pulse * 7.0, noise_ratio), 5, 100))
            uv = clamp(noisy(rng, 4.5 + 3.0 * max(math.sin(hour_angle), 0.0) - max(tick_pulse, 0.0), noise_ratio), 0, 11)
            chance_of_rain = int(clamp((humidity - 55) * 1.2 + precip_mm * 20, 0, 95))
            condition_text = "Patchy rain nearby" if chance_of_rain >= 45 else "Partly cloudy"
            condition_code = 1063 if chance_of_rain >= 45 else 1003
            event_time = tick.strftime("%Y-%m-%d %H:%M")
            ingest_time = iso_z(datetime.now(timezone.utc))
            event_id = (
                f"demo-weather-{replay_id}-{location['province'].lower().replace(' ', '-')}-"
                f"{tick.strftime('%Y%m%d%H%M')}"
            )

            events.append(
                {
                    "event_id": event_id,
                    "province": location["province"],
                    "country": location["country"],
                    "region": location["region"],
                    "location_name": location["location_name"],
                    "lat": location["lat"],
                    "lon": location["lon"],
                    "tz_id": "Asia/Bangkok",
                    "query_date": tick.strftime("%Y-%m-%d"),
                    "time": event_time,
                    "time_epoch": int(tick.timestamp()),
                    "is_day": 1 if 6 <= tick.hour < 18 else 0,
                    "temp_c": round1(temp_c),
                    "temp_f": round1(temp_c * 9 / 5 + 32),
                    "feelslike_c": round1(temp_c + clamp((humidity - 65) / 20.0, -1.5, 2.5)),
                    "feelslike_f": round1((temp_c + clamp((humidity - 65) / 20.0, -1.5, 2.5)) * 9 / 5 + 32),
                    "windchill_c": round1(temp_c - 0.4),
                    "windchill_f": round1((temp_c - 0.4) * 9 / 5 + 32),
                    "heatindex_c": round1(temp_c + clamp((humidity - 70) / 15.0, 0, 3.0)),
                    "heatindex_f": round1((temp_c + clamp((humidity - 70) / 15.0, 0, 3.0)) * 9 / 5 + 32),
                    "dewpoint_c": round1(temp_c - ((100 - humidity) / 5.0)),
                    "dewpoint_f": round1((temp_c - ((100 - humidity) / 5.0)) * 9 / 5 + 32),
                    "condition_text": condition_text,
                    "condition_code": condition_code,
                    "condition_icon": "//cdn.weatherapi.com/weather/64x64/day/116.png",
                    "wind_mph": round1(wind_kph / 1.609344),
                    "wind_kph": round1(wind_kph),
                    "wind_degree": int(clamp(noisy(rng, 110 + 40 * math.sin(hour_angle) + tick_pulse * 18.0, noise_ratio), 0, 359)),
                    "wind_dir": "ESE",
                    "gust_mph": round1(gust_kph / 1.609344),
                    "gust_kph": round1(gust_kph),
                    "pressure_mb": round1(pressure_mb),
                    "pressure_in": round(pressure_mb / 33.8639, 2),
                    "precip_mm": round1(precip_mm),
                    "precip_in": round(precip_mm / 25.4, 3),
                    "snow_cm": 0.0,
                    "humidity": int(round(humidity)),
                    "cloud": cloud,
                    "vis_km": round1(clamp(noisy(rng, 8.0 - cloud / 35.0, noise_ratio), 2, 12)),
                    "vis_miles": round1(clamp(noisy(rng, 8.0 - cloud / 35.0, noise_ratio), 2, 12) / 1.609344),
                    "uv": round1(uv),
                    "will_it_rain": 1 if chance_of_rain >= 50 else 0,
                    "chance_of_rain": chance_of_rain,
                    "will_it_snow": 0,
                    "chance_of_snow": 0,
                    "source": "demo_interpolated_weather",
                    "source_file": "demo://realtime-feed/weather",
                    "ingest_time": ingest_time,
                    "window_mode": "demo_near_realtime",
                    "window_start_utc": iso_z(ticks[0]),
                    "window_end_utc": iso_z(ticks[-1]),
                    "window_now_utc": ingest_time,
                    "is_interpolated": True,
                    "demo_replay_id": replay_id,
                    "demo_tick_index": tick_index,
                }
            )
    return events


def build_openaq_events(ticks: list[datetime], noise_ratio: float, rng: random.Random, replay_id: str) -> list[dict]:
    events: list[dict] = []
    for tick_index, tick in enumerate(ticks):
        hour_angle = (tick.hour + tick.minute / 60.0) / 24.0 * 2 * math.pi
        for station in OPENAQ_STATIONS:
            phase = location_phase(station["location_id"], replay_id)
            tick_pulse = tick_wave(tick_index, phase, 1.0)
            short_burst = max(0.0, tick_wave(tick_index, phase + 1.7, 1.0)) ** 2
            baseline = station["base_pm25"] + 5.5 * math.cos(hour_angle - 0.3) + tick_pulse * 4.2 + short_burst * 3.5
            value = clamp(noisy(rng, baseline, noise_ratio, 1.4), 3.0, 180.0)
            spread = max(1.0, abs(noisy(rng, 2.5 + abs(tick_pulse) * 1.6, noise_ratio, 0.35)))
            ingest_time = iso_z(datetime.now(timezone.utc))
            event_id = (
                f"demo-openaq-{replay_id}-{station['location_id']}-pm25-"
                f"{tick.strftime('%Y%m%d%H%M')}"
            )
            events.append(
                {
                    "location_id": station["location_id"],
                    "location_name": station["location_name"],
                    "city": station["city"],
                    "latitude": station["latitude"],
                    "longitude": station["longitude"],
                    "provider": station["provider"],
                    "sensor_id": station["sensor_id"],
                    "parameter": "pm25",
                    "unit": "ug/m3",
                    "datetime_utc": iso_z(tick),
                    "datetime_local": tick.astimezone(timezone(timedelta(hours=7))).replace(microsecond=0).isoformat(),
                    "value": round1(value),
                    "min": round1(max(0.0, value - spread)),
                    "max": round1(value + spread),
                    "sd": round1(spread / 2),
                    "expected_count": 1,
                    "observed_count": 1,
                    "coverage_pct": 100.0,
                    "source": "demo_interpolated_openaq",
                    "ingest_time": ingest_time,
                    "window_mode": "demo_near_realtime",
                    "window_start_utc": iso_z(ticks[0]),
                    "window_end_utc": iso_z(ticks[-1]),
                    "window_now_utc": ingest_time,
                    "event_id": event_id,
                    "is_interpolated": True,
                    "demo_replay_id": replay_id,
                    "demo_tick_index": tick_index,
                }
            )
    return events


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def group_by_tick(events: list[dict]) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = {}
    for event in events:
        grouped.setdefault(int(event["demo_tick_index"]), []).append(event)
    return grouped


def replay_events(weather_events: list[dict], openaq_events: list[dict]) -> None:
    from kafka_utils import create_kafka_producer, flush_producer, send_events

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    weather_topic = os.getenv("DEMO_FEED_WEATHER_TOPIC", os.getenv("KAFKA_WEATHER_TOPIC", "weather_history"))
    openaq_topic = os.getenv("DEMO_FEED_OPENAQ_TOPIC", os.getenv("KAFKA_OPENAQ_TOPIC", "openaq-hourly"))
    batch_interval_sec = env_int("DEMO_FEED_BATCH_INTERVAL_SECONDS", 30)
    batch_size = max(1, env_int("DEMO_FEED_BATCH_SIZE", 24))
    send_delay_ms = max(0, env_int("DEMO_FEED_SEND_DELAY_MS", 0))

    producer = create_kafka_producer(bootstrap, LOGGER, max_retries=120, retry_delay=5)
    weather_by_tick = group_by_tick(weather_events)
    openaq_by_tick = group_by_tick(openaq_events)
    tick_ids = sorted(set(weather_by_tick) | set(openaq_by_tick))

    try:
        for index, tick_id in enumerate(tick_ids, start=1):
            sent_total = 0
            weather_batch = weather_by_tick.get(tick_id, [])
            openaq_batch = openaq_by_tick.get(tick_id, [])

            for offset in range(0, len(weather_batch), batch_size):
                sent_total += send_events(
                    producer,
                    weather_topic,
                    weather_batch[offset : offset + batch_size],
                    LOGGER,
                    send_delay_ms=send_delay_ms,
                )
            for offset in range(0, len(openaq_batch), batch_size):
                sent_total += send_events(
                    producer,
                    openaq_topic,
                    openaq_batch[offset : offset + batch_size],
                    LOGGER,
                    send_delay_ms=send_delay_ms,
                )

            flush_producer(producer, LOGGER, timeout_sec=60)
            LOGGER.info("Published demo tick %s/%s with %s event(s)", index, len(tick_ids), sent_total)
            if index < len(tick_ids) and batch_interval_sec > 0:
                time.sleep(batch_interval_sec)
    finally:
        producer.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    mode = os.getenv("DEMO_FEED_MODE", "prepare-and-replay").strip().lower()
    sources = parse_sources()
    step_minutes = max(1, env_int("DEMO_FEED_STEP_MINUTES", 15))
    max_ticks = max(1, env_int("DEMO_FEED_MAX_BATCHES", 8))
    noise_ratio = max(0.0, env_float("DEMO_FEED_NOISE_RATIO", 0.08))
    output_dir = Path(os.getenv("DEMO_FEED_OUTPUT_DIR", "/opt/demo_realtime_feed"))
    replay_id = os.getenv("DEMO_FEED_REPLAY_ID", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
    seed_raw = os.getenv("DEMO_FEED_SEED", "").strip()
    seed = int(seed_raw) if seed_raw else int(datetime.now(timezone.utc).timestamp() * 1000) % 2_147_483_647
    rng = random.Random(seed)

    base_time = parse_base_time()
    first_tick = base_time - timedelta(minutes=step_minutes * (max_ticks - 1))
    ticks = [first_tick + timedelta(minutes=step_minutes * idx) for idx in range(max_ticks)]

    LOGGER.info(
        "Demo near-realtime feed mode=%s sources=%s ticks=%s step=%sm base_time=%s noise_ratio=%.3f replay_id=%s",
        mode,
        ",".join(sorted(sources)),
        max_ticks,
        step_minutes,
        iso_z(base_time),
        noise_ratio,
        replay_id,
    )

    weather_events = build_weather_events(ticks, noise_ratio, rng, replay_id) if "weather" in sources else []
    openaq_events = build_openaq_events(ticks, noise_ratio, rng, replay_id) if "openaq" in sources else []

    if mode in {"prepare", "prepare-and-replay"}:
        write_jsonl(output_dir / "weather_demo_feed.jsonl", weather_events)
        write_jsonl(output_dir / "openaq_demo_feed.jsonl", openaq_events)
        LOGGER.info("Prepared %s weather event(s) and %s OpenAQ event(s) in %s", len(weather_events), len(openaq_events), output_dir)

    if mode in {"replay", "prepare-and-replay"}:
        replay_events(weather_events, openaq_events)


if __name__ == "__main__":
    main()
