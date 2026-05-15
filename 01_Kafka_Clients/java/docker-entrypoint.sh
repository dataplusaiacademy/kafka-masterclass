#!/bin/sh
set -e
MAIN_CLASS="${MAIN_CLASS:-com.masterclass.kafka.OrderConsumer}"
exec java -cp "/app/lib/*" "$MAIN_CLASS" "$@"
