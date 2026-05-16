#!/bin/sh
set -e

SCENARIO="${SCENARIO:-latency}"
CFG="/client-configs/${SCENARIO}/producer.properties"
BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092,kafka3:29092}"
TOPIC="${TOPIC:-perf.test}"
NUM_RECORDS="${NUM_RECORDS:-5000000}"
RECORD_SIZE="${RECORD_SIZE:-1024}"
ACKS="${ACKS:-}"

if [ -z "$ACKS" ] && [ -f "$CFG" ]; then
  ACKS=$(grep -E '^acks=' "$CFG" | cut -d= -f2 | tr -d ' ')
fi
ACKS="${ACKS:-1}"

echo "E2E latency: topic=$TOPIC records=$NUM_RECORDS acks=$ACKS"

if [ -f "$CFG" ]; then
  kafka-e2e-latency "$BOOTSTRAP" "$TOPIC" "$NUM_RECORDS" "$ACKS" "$RECORD_SIZE" "$CFG" &
else
  kafka-e2e-latency "$BOOTSTRAP" "$TOPIC" "$NUM_RECORDS" "$ACKS" "$RECORD_SIZE" &
fi
PID=$!

. /scripts/wait-for-jmx.sh
sh /scripts/push-jmx-metrics.sh "$PID" &
PUSH_PID=$!

wait "$PID"
wait "$PUSH_PID" 2>/dev/null || true
