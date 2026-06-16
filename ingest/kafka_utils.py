import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


CONTRACT_METADATA_FIELDS = {
    "schema_version",
    "available_at",
    "quality_flags",
    "trace",
    "payload",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event_time(event: dict, ingest_time: str) -> str:
    for field in (
        "event_time",
        "datetime_utc",
        "content_start",
        "time_start",
        "start_time",
        "window_end_utc",
        "ingest_time",
    ):
        value = event.get(field)
        if value not in (None, ""):
            return str(value)
    return ingest_time


def apply_event_contract(event: dict) -> dict:
    """Attach the shared AIS event envelope without changing flat source fields."""
    enriched = dict(event)
    ingest_time = str(enriched.get("ingest_time") or _utc_now_iso())
    source = str(enriched.get("source") or "unknown")
    producer = str(enriched.get("producer") or os.getenv("AIS_PRODUCER", source))
    trace = dict(enriched.get("trace") or {})

    enriched["ingest_time"] = ingest_time
    enriched.setdefault("event_time", _event_time(enriched, ingest_time))
    enriched.setdefault("available_at", ingest_time)
    enriched.setdefault("schema_version", os.getenv("AIS_SCHEMA_VERSION", "v1"))
    enriched.setdefault("quality_flags", [])
    enriched["trace"] = {
        "request_id": trace.get("request_id") or str(uuid.uuid4()),
        "producer": trace.get("producer") or producer,
        "retry_count": int(trace.get("retry_count") or 0),
    }
    enriched.setdefault(
        "payload",
        {key: value for key, value in event.items() if key not in CONTRACT_METADATA_FIELDS},
    )
    return enriched


def create_kafka_producer(
    bootstrap_servers: str,
    logger,
    max_retries: int | None = None,
    retry_delay: int | None = None,
) -> KafkaProducer:
    """Create a Kafka producer with simple retry logic while broker is starting."""
    resolved_max_retries = int(os.getenv("KAFKA_CONNECT_MAX_RETRIES", str(max_retries or 10)))
    resolved_retry_delay = int(os.getenv("KAFKA_CONNECT_RETRY_DELAY", str(retry_delay or 5)))

    for attempt in range(1, resolved_max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_block_ms=30000,
            )
            logger.info(f"Kafka connected (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not ready (attempt {attempt}/{resolved_max_retries}), wait {resolved_retry_delay}s"
            )
            time.sleep(resolved_retry_delay)

    raise RuntimeError("Cannot connect to Kafka")


def send_event(
    producer: KafkaProducer,
    topic: str,
    event: dict,
    logger,
    key_field: str = "event_id",
    wait_for_ack: bool = False,
    ack_timeout_sec: int = 10,
) -> bool:
    """Send one event and return True when the producer accepted it."""
    try:
        contracted_event = apply_event_contract(event)
        key = contracted_event.get(key_field) if key_field else None
        future = producer.send(topic, key=key, value=contracted_event)
        if wait_for_ack:
            future.get(timeout=ack_timeout_sec)
        return True
    except Exception as exc:
        event_id = event.get(key_field) if key_field else None
        logger.error(f"Failed to send message: {exc} | event_id={event_id}")
        dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "ais-dlq").strip()
        if dlq_topic and topic != dlq_topic:
            try:
                producer.send(
                    dlq_topic,
                    key=str(event_id) if event_id else None,
                    value=apply_event_contract(
                        {
                            "event_id": str(event_id or uuid.uuid4()),
                            "source": "kafka_producer_dlq",
                            "failed_topic": topic,
                            "failure_reason": str(exc),
                            "failed_event": event,
                            "quality_flags": ["producer_send_failed"],
                        }
                    ),
                )
            except Exception as dlq_exc:
                logger.error(f"Failed to send DLQ message: {dlq_exc} | event_id={event_id}")
        return False


def send_events(
    producer: KafkaProducer,
    topic: str,
    events: list[dict],
    logger,
    key_field: str = "event_id",
    send_delay_ms: int = 0,
    wait_for_ack: bool = False,
    ack_timeout_sec: int = 10,
) -> int:
    """Send a list of events and return the number of successful sends."""
    success_count = 0

    for event in events:
        ok = send_event(
            producer=producer,
            topic=topic,
            event=event,
            logger=logger,
            key_field=key_field,
            wait_for_ack=wait_for_ack,
            ack_timeout_sec=ack_timeout_sec,
        )
        if ok:
            success_count += 1

        if send_delay_ms > 0:
            time.sleep(send_delay_ms / 1000.0)

    return success_count


def flush_producer(producer: KafkaProducer, logger, timeout_sec: int | None = None) -> None:
    """Flush Kafka producer without allowing an unbounded hang."""
    resolved_timeout = int(os.getenv("KAFKA_FLUSH_TIMEOUT_SEC", str(timeout_sec or 60)) or 60)
    remaining = producer.flush(timeout=resolved_timeout)
    if remaining:
        message = f"Kafka producer flush timed out after {resolved_timeout}s with {remaining} message(s) remaining"
        logger.error(message)
        raise TimeoutError(message)
