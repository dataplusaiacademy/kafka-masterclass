import os
from pathlib import Path

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "orders.events"
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", "/app/schemas/order_event.avsc"))
