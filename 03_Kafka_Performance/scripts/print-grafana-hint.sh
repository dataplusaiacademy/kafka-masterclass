#!/bin/sh
ROLE="${1:-producer}"
case "$ROLE" in
  producer)
    echo "Grafana: Kafka → Kafka Perf (lab) or Kafka Producer — env=lab, instance=perf-producer, client_id=perf-producer-${SCENARIO:-throughput} (defaults pre-set on Producer dashboard)"
    ;;
  consumer)
    echo "Grafana: Kafka → Kafka Perf (lab) or Kafka Consumer — env=lab, instance=perf-consumer, client_id=perf-consumer-${SCENARIO:-throughput}, group=perf-consumer-${SCENARIO:-throughput}"
    ;;
  *)
    echo "Grafana: Kafka → Kafka Perf (lab) — no variables required"
    ;;
esac
