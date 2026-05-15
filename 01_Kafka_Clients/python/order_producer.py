#!/usr/bin/env python3
"""Publish order lifecycle events to Kafka using Avro and Schema Registry."""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config import BOOTSTRAP_SERVERS, SCHEMA_PATH, SCHEMA_PATH_V2, SCHEMA_REGISTRY_URL, TOPIC

STATUSES = ("PLACED", "PAID", "SHIPPED")
FULFILLMENT_CENTERS = ("US-EAST", "US-WEST", "EU-CENTRAL")


def resolve_schema_path(evolved: bool) -> Path:
    if evolved:
        return Path(os.getenv("SCHEMA_PATH", str(SCHEMA_PATH_V2)))
    return SCHEMA_PATH


def build_producer(schema_path: Path) -> SerializingProducer:
    schema_str = schema_path.read_text(encoding="utf-8")
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(registry, schema_str)

    return SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)
        return
    key = msg.key().decode("utf-8") if isinstance(msg.key(), bytes) else msg.key()
    print(f"Produced order_id={key} partition={msg.partition()} offset={msg.offset()}")


def publish_orders(producer: SerializingProducer, order_count: int, evolved: bool) -> None:
    for i in range(order_count):
        order_id = f"ord-{uuid.uuid4().hex[:8]}"
        customer_id = f"cust-{1000 + i}"
        amount_cents = 4999 + (i * 1500)

        for status in STATUSES:
            event = {
                "order_id": order_id,
                "customer_id": customer_id,
                "status": status,
                "amount_cents": amount_cents,
                "currency": "USD",
                "event_time": int(time.time() * 1000),
            }
            if evolved:
                event["fulfillment_center"] = FULFILLMENT_CENTERS[i % len(FULFILLMENT_CENTERS)]

            producer.produce(
                topic=TOPIC,
                key=order_id,
                value=event,
                on_delivery=delivery_report,
            )
            label = f"Sent order_id={order_id} status={status}"
            if evolved:
                label += f" fulfillment_center={event['fulfillment_center']}"
            print(label)
            producer.poll(0)
            time.sleep(0.1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Order event producer")
    parser.add_argument("--orders", type=int, default=3, help="Number of orders to simulate")
    parser.add_argument(
        "--evolved",
        action="store_true",
        help="Use schema v2 (adds fulfillment_center field)",
    )
    args = parser.parse_args()

    schema_path = resolve_schema_path(args.evolved)
    producer = build_producer(schema_path)
    version = "v2" if args.evolved else "v1"
    print(f"Publishing {args.orders} order(s) to topic '{TOPIC}' (schema {version}) ...")
    publish_orders(producer, args.orders, args.evolved)
    producer.flush()
    print("Producer finished.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
