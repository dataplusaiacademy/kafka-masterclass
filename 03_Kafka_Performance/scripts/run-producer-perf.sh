#!/bin/sh
set -e

SCENARIO="${SCENARIO:-throughput}"
CFG="/client-configs/${SCENARIO}/producer.properties"
TOPIC="${TOPIC:-perf.test}"
NUM_RECORDS="${NUM_RECORDS:-5000000}"
RECORD_SIZE="${RECORD_SIZE:-1024}"
THROUGHPUT="${THROUGHPUT:--1}"

if [ ! -f "$CFG" ]; then
  echo "Missing producer config: $CFG" >&2
  exit 1
fi

echo "Producer perf: scenario=$SCENARIO topic=$TOPIC records=$NUM_RECORDS size=$RECORD_SIZE"

exec kafka-producer-perf-test \
  --topic "$TOPIC" \
  --num-records "$NUM_RECORDS" \
  --record-size "$RECORD_SIZE" \
  --throughput "$THROUGHPUT" \
  --producer.config "$CFG"
