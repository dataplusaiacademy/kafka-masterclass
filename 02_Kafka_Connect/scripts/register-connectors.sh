#!/bin/sh
# Idempotent provisioning: Postgres Debezium → Kafka → JDBC MySQL. Unwrap (ExtractNewRecordState) stays on postgres-cdc
# (Debezium plugin classpath); InsertField$Value adds ingest_source on jdbc-mysql-sink.
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"

wait_for_connect() {
  i=0
  while true; do
    if curl -fsS "${CONNECT_URL}/" >/dev/null 2>&1; then break; fi
    i=$((i + 1))
    if [ "$i" -gt 90 ]; then
      printf '%s\n' "Timed out waiting for Kafka Connect at ${CONNECT_URL}" >&2
      exit 1
    fi
    sleep 2
  done
}

delete_connector() {
  code=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "${CONNECT_URL}/connectors/$1") || code=000
  # 404 OK (already absent).
  printf 'DELETE %s -> HTTP %s\n' "$1" "$code"
}

post_connector() {
  payload_file=$1
  name=$2
  printf 'Creating connector %s\n' "$name"
  curl -fsS -X POST -H "Content-Type: application/json" "${CONNECT_URL}/connectors" -d @"${payload_file}"
  printf '\n'
}

wait_connector_running() {
  name=$1
  secs=${2:-120}
  deadline=$(( $(date +%s) + secs ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    resp=$(curl -fsS "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || printf '{}')
    if printf '%s' "$resp" | grep -q '"state":"RUNNING"'; then
      printf 'Connector %s is RUNNING.\n' "$name"
      return 0
    fi
    if printf '%s' "$resp" | grep -q '"state":"FAILED"'; then
      printf '%s\n' "Connector $name FAILED. Latest status:"
      curl -fsS "${CONNECT_URL}/connectors/${name}/status" || true
      exit 1
    fi
    sleep 4
  done
  printf '%s\n' "Timed out waiting for $name"
  curl -fsS "${CONNECT_URL}/connectors/${name}/status" || true
  exit 1
}

wait_for_connect

delete_connector "jdbc-mysql-sink"
delete_connector "postgres-cdc"

trap 'rm -f /tmp/pg.json /tmp/jdbc.json' EXIT

cat >/tmp/pg.json <<'EOF'
{
  "name": "postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "demo",

    "topic.prefix": "demo",

    "table.include.list": "public.orders",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "disabled",
    "publication.name": "dbz_orders",

    "snapshot.mode": "initial",

    "database.history.kafka.bootstrap.servers": "kafka:29092",
    "database.history.kafka.topic": "demo.schemahistory",

    "heartbeat.interval.ms": "10000",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
EOF

cat >/tmp/jdbc.json <<'EOF'
{
  "name": "jdbc-mysql-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",

    "topics": "demo.public.orders",
    "connection.url": "jdbc:mysql://mysql:3306/sink?useSSL=false&allowPublicKeyRetrieval=true",
    "connection.user": "root",
    "connection.password": "connect_demo",

    "auto.create": "true",
    "auto.evolve": "true",

    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id",

    "delete.enabled": "true",
    "table.name.format": "orders",
    "db.timezone": "UTC",

    "transforms": "addIngestMeta",

    "transforms.addIngestMeta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.addIngestMeta.static.field": "ingest_source",
    "transforms.addIngestMeta.static.value": "kafka-connect-lab",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
EOF

post_connector /tmp/pg.json postgres-cdc
wait_connector_running postgres-cdc 300

post_connector /tmp/jdbc.json jdbc-mysql-sink
wait_connector_running jdbc-mysql-sink 180

printf '%s\n' "Connectors are registered."
