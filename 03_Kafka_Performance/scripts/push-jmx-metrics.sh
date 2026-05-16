#!/bin/sh
# Push JMX exporter metrics to Pushgateway while a perf client PID is running.
PID="${1:?usage: push-jmx-metrics.sh <pid>}"
INSTANCE="${METRICS_INSTANCE:-${HOSTNAME:-perf-client}}"
PUSHGATEWAY_URL="${PUSHGATEWAY_URL:-http://pushgateway:9091}"
PUSH_PATH="/metrics/job/kafka-client/instance/${INSTANCE}/env/lab/hostname/${INSTANCE}"

curl -sf -X DELETE "${PUSHGATEWAY_URL}/metrics/job/kafka-client/instance/${INSTANCE}/env/lab" >/dev/null 2>&1 || true

push_metrics() {
  metrics=$(curl -sf http://127.0.0.1:7071/metrics 2>/dev/null) || return 0
  if ! printf '%s' "$metrics" | grep -qE '^kafka_(producer|consumer)'; then
    return 0
  fi
  if printf '%s' "$metrics" | curl -sf --data-binary @- "${PUSHGATEWAY_URL}${PUSH_PATH}"; then
    return 0
  fi
  echo "Pushgateway push failed for ${INSTANCE} (url=${PUSHGATEWAY_URL}${PUSH_PATH})" >&2
  return 1
}

while kill -0 "$PID" 2>/dev/null; do
  push_metrics || true
  sleep 2
done

if push_metrics; then
  echo "Pushed client JMX metrics to Pushgateway (${INSTANCE})"
else
  echo "Warning: final push to Pushgateway failed for ${INSTANCE}" >&2
  exit 1
fi
