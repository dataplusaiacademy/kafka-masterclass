#!/bin/sh
set -e
SR_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SUBJECT="orders.events-value"

echo "Setting BACKWARD compatibility on ${SUBJECT} ..."
curl -sf -X PUT "${SR_URL}/config/${SUBJECT}" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
echo "Done."
