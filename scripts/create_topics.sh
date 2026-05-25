#!/bin/bash
# =============================================================================
# Create Kafka topics for AIS pipeline
# =============================================================================

set -euo pipefail

TOPICS=(
  "weather_history"
  "openaq-hourly"
  "sentinel5p-summary"
  "maiac-summary"
  "era5-files"
)
PARTITIONS=3

echo "=== Create AIS Kafka topics ==="
for topic in "${TOPICS[@]}"; do
  echo "- Create topic: ${topic}"
  docker exec kafka kafka-topics \
    --create \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions "${PARTITIONS}" \
    --topic "${topic}" \
    --if-not-exists

  current_partitions="$(
    docker exec kafka kafka-topics \
      --describe \
      --bootstrap-server kafka:9092 \
      --topic "${topic}" \
    | awk -F'PartitionCount: ' 'NR == 1 { split($2, fields, "\t"); print fields[1] }'
  )"

  if [ "${current_partitions:-0}" -lt "${PARTITIONS}" ]; then
    echo "  Increase partitions: ${current_partitions} -> ${PARTITIONS}"
    docker exec kafka kafka-topics \
      --alter \
      --bootstrap-server kafka:9092 \
      --topic "${topic}" \
      --partitions "${PARTITIONS}"
  fi
done

echo
echo "=== Current topics ==="
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092

echo
echo "=== Topic details ==="
for topic in "${TOPICS[@]}"; do
  echo "- ${topic}"
  docker exec kafka kafka-topics \
    --describe \
    --bootstrap-server kafka:9092 \
    --topic "${topic}"
done
