#!/usr/bin/env python3
"""Publish order lifecycle events to Kafka using Avro and Schema Registry."""

from __future__ import annotations

import argparse
import sys
import time
import uuid

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config import BOOTSTRAP_SERVERS, SCHEMA_PATH, SCHEMA_REGISTRY_URL, TOPIC

STATUSES = ("PLACED", "PAID", "SHIPPED")


def build_producer() -> SerializingProducer:
    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
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


def publish_orders(producer: SerializingProducer, order_count: int) -> None:
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
            producer.produce(
                topic=TOPIC,
                key=order_id,
                value=event,
                on_delivery=delivery_report,
            )
            print(f"Sent order_id={order_id} status={status}")
            producer.poll(0)
            time.sleep(0.1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Order event producer")
    parser.add_argument("--orders", type=int, default=3, help="Number of orders to simulate")
    args = parser.parse_args()

    producer = build_producer()
    print(f"Publishing {args.orders} order(s) to topic '{TOPIC}' ...")
    publish_orders(producer, args.orders)
    producer.flush()
    print("Producer finished.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
