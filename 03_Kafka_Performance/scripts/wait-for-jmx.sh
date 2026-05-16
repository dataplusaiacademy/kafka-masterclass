#!/bin/sh
# Wait until the Kafka client JVM exposes JMX exporter metrics on :7071.
i=0
while [ "$i" -lt 60 ]; do
  if curl -sf http://127.0.0.1:7071/metrics 2>/dev/null | grep -qE '^kafka_(producer|consumer)'; then
    return 0
  fi
  sleep 1
  i=$((i + 1))
done
echo "JMX metrics not ready on :7071" >&2
return 1
