#!/bin/sh
set -e
SR_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SUBJECT="orders.events-value"
SCHEMA_FILE="/schemas/order_event_v2.avsc"

apk add --no-cache curl jq >/dev/null

echo "Registering schema v2 for ${SUBJECT} ..."
schema=$(jq -c . "${SCHEMA_FILE}")
payload=$(jq -n --arg schema "${schema}" '{schema: $schema}')

curl -sf -X POST "${SR_URL}/subjects/${SUBJECT}/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "${payload}"

echo ""
echo "Schema v2 registered."
