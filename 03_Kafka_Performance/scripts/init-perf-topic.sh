#!/bin/sh
set -e

BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092,kafka3:29092}"
TOPIC="${TOPIC:-perf.test}"
PARTITIONS="${PARTITIONS:-12}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

echo "Topic $TOPIC ready (partitions=$PARTITIONS replication-factor=$REPLICATION_FACTOR)."
