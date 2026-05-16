#!/bin/sh
set -e

SCENARIO="${SCENARIO:-throughput}"
CFG="/client-configs/${SCENARIO}/consumer.properties"
TOPIC="${TOPIC:-perf.test}"
BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092,kafka3:29092}"
MESSAGES="${MESSAGES:-5000000}"
FETCH_SIZE="${FETCH_SIZE:-1048576}"
THREADS="${THREADS:-1}"
GROUP="${GROUP:-perf-consumer-${SCENARIO}}"

if [ ! -f "$CFG" ]; then
  echo "Missing consumer config: $CFG" >&2
  exit 1
fi

echo "Consumer perf: scenario=$SCENARIO topic=$TOPIC messages=$MESSAGES"

kafka-consumer-perf-test \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --messages "$MESSAGES" \
  --fetch-size "$FETCH_SIZE" \
  --threads "$THREADS" \
  --group "$GROUP" \
  --consumer.config "$CFG" &
PID=$!

. /scripts/wait-for-jmx.sh
sh /scripts/push-jmx-metrics.sh "$PID" &
PUSH_PID=$!

wait "$PID"
wait "$PUSH_PID" 2>/dev/null || true
sh /scripts/print-grafana-hint.sh consumer
