#!/usr/bin/env python3
"""Consume order lifecycle events from Kafka."""

from __future__ import annotations

import os
import signal
import sys

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config import BOOTSTRAP_SERVERS, SCHEMA_PATH, SCHEMA_REGISTRY_URL, TOPIC

running = True


def shutdown(_signum, _frame) -> None:
    global running
    running = False


def max_messages() -> int:
    value = os.getenv("MAX_MESSAGES", "").strip()
    return int(value) if value else 0


def build_consumer() -> DeserializingConsumer:
    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(registry, schema_str)

    return DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": os.getenv("CONSUMER_GROUP_ID", "order-fulfillment-py"),
            "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
        }
    )


def main() -> int:
    limit = max_messages()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer = build_consumer()
    consumer.subscribe([TOPIC])
    suffix = f" (max {limit} messages)" if limit > 0 else " (Ctrl+C to stop)"
    print(f"Consuming from '{TOPIC}'{suffix} ...")

    consumed = 0
    try:
        while running and (limit <= 0 or consumed < limit):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue
            value = msg.value()
            line = (
                f"Consumed order_id={value['order_id']} status={value['status']} "
                f"customer_id={value['customer_id']} amount_cents={value['amount_cents']}"
            )
            if value.get("fulfillment_center"):
                line += f" fulfillment_center={value['fulfillment_center']}"
            print(f"{line} partition={msg.partition()} offset={msg.offset()}")
            consumed += 1
            if limit > 0 and consumed >= limit:
                print(f"Reached MAX_MESSAGES={limit}, exiting.")
                break
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
