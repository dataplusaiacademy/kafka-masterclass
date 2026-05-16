#!/usr/bin/env python3
"""Consume Debezium CDC topic values (Avro) via Schema Registry. Runs inside Compose."""
from __future__ import annotations

import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    topic = os.environ.get("SOURCE_TOPIC", "demo.public.orders")
    group = os.environ.get("CONSUMER_GROUP_ID", "python-cdc-lab")
    reset = os.environ.get("AUTO_OFFSET_RESET", "earliest").strip().lower()
    if reset not in ("earliest", "latest"):
        reset = "earliest"
    max_msgs = max(1, _env_int("CONSUME_MAX_MESSAGES", 20))
    poll_secs = max(0.2, float(os.environ.get("CONSUME_POLL_TIMEOUT_SEC", "1.5")))
    total_wait = max(poll_secs, float(os.environ.get("CONSUME_MAX_WAIT_SEC", "90")))

    schema_registry_client = SchemaRegistryClient({"url": sr_url})
    key_deserializer = AvroDeserializer(schema_registry_client)
    value_deserializer = AvroDeserializer(schema_registry_client)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": reset,
            "enable.auto.commit": True,
            "partition.assignment.strategy": "cooperative-sticky",
        }
    )

    print(
        json.dumps(
            {
                "bootstrap_servers": bootstrap,
                "schema_registry": sr_url,
                "topic": topic,
                "group.id": group,
                "auto.offset.reset": reset,
                "will_read_up_to_messages": max_msgs,
            },
            indent=2,
        ),
        flush=True,
    )

    consumer.subscribe([topic])
    seen = 0
    deadline = time.monotonic() + total_wait

    try:
        while seen < max_msgs:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(
                    json.dumps({"event": "timeout", "read": seen, "target": max_msgs}),
                    flush=True,
                )
                break
            timeout = min(poll_secs, remaining)
            msg = consumer.poll(timeout=timeout)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            raw_key = msg.key()
            raw_val = msg.value()
            parsed_key = (
                None
                if raw_key is None
                else key_deserializer(raw_key, SerializationContext(topic, MessageField.KEY))
            )
            parsed_val = (
                None
                if raw_val is None
                else value_deserializer(raw_val, SerializationContext(topic, MessageField.VALUE))
            )
            record = {
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": parsed_key,
                "value": parsed_val,
            }
            print(json.dumps(record, default=str))
            sys.stdout.flush()
            seen += 1
        print(json.dumps({"event": "done", "read": seen}), flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
