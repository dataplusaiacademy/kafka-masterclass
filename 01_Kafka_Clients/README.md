# Kafka Clients Masterclass

A hands-on example of **Kafka**, **Confluent Schema Registry**, and **Avro** with producers and consumers in **Python** and **Java**. Everything runs in **Docker Compose** — no Python, Java, or other tools required on the host.

## What you will see

- Kafka (KRaft) and Schema Registry in Docker
- Order lifecycle events on topic `orders.events` (`PLACED` → `PAID` → `SHIPPED`)
- Cross-language Avro: **Python produces**, **Java consumes** in the default demo
- **Schema evolution** (backward-compatible Avro v2 with `fulfillment_center`)
- **Observability**: Prometheus, Grafana (consumer lag), and Confluent Control Center

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| **Docker Compose V2** | `docker compose version` — Docker Desktop (Windows/macOS) or Engine + plugin (Linux) |

Works the same on **Windows, macOS, and Linux**.

## Quick start

### Clients demo (Python produce, Java consume)

```bash
make demo
```

Or manually:

```bash
docker compose up -d --wait
cid=$(docker compose --profile demo run -d -q demo-java-consumer)
docker compose --profile demo run --rm demo-python-producer
docker wait $cid && docker logs $cid && docker rm $cid
```

### Full stack with observability

```bash
make up-all
make monitoring   # print UI URLs
make demo         # generate traffic for Grafana lag panels
```

| UI | URL | Login |
|----|-----|-------|
| **Control Center** | http://localhost:9021 | (none for local dev) |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |
| **Schema Registry** | http://localhost:8081 | — |

Control Center uses the Confluent Enterprise image — suitable for local learning; production requires a Confluent license.

In Grafana, open the **Kafka Consumer Lag** dashboard (folder: Kafka). Key metrics from kafka-exporter:

- `kafka_consumergroup_lag` — consumer lag by group, topic, and partition
- `kafka_consumergroup_current_offset` — committed read position

## Schema evolution

v1 schema: [`schemas/order_event.avsc`](schemas/order_event.avsc)  
v2 schema: [`schemas/order_event_v2.avsc`](schemas/order_event_v2.avsc) — adds optional `fulfillment_center` (default `US-EAST`) for **backward** compatibility.

### Walkthrough

1. Produce v1 events and register the initial schema:

   ```bash
   make demo
   ```

2. Set BACKWARD compatibility and register v2:

   ```bash
   make evolve
   ```

   This runs `init-schema-compat`, `register-schema-v2`, and `evolve-python-producer` (Python producer with `--evolved`).

3. Consume with an existing v1 consumer (still works — new field has a default):

   ```bash
   docker compose --profile clients run --rm python-consumer
   ```

   Python consumer prints `fulfillment_center` when present. Java consumer (v1-generated `OrderEvent`) deserializes v2 payloads via Schema Registry without crashing.

4. Inspect versions in Schema Registry: http://localhost:8081/subjects/orders.events-value/versions

Or in Control Center: **Schema Registry** → `orders.events-value` → view schema versions and compatibility.

### Manual evolve commands

```bash
docker compose up -d --wait
docker compose --profile evolve up init-schema-compat register-schema-v2
docker compose --profile evolve run --rm evolve-python-producer
```

## Running individual clients

Start infrastructure first (`docker compose up -d --wait` or `make up-all`), then:

| Action | Command |
|--------|---------|
| Python producer | `docker compose --profile clients run --rm python-producer` |
| Python producer (v2) | `docker compose --profile evolve run --rm evolve-python-producer` |
| Python consumer | `docker compose --profile clients run --rm python-consumer` |
| Java producer | `docker compose --profile clients run --rm java-producer` |
| Java consumer | `docker compose --profile clients run --rm java-consumer` |

**Reverse direction (Java produce → Python consume):** run `python-consumer` in one terminal and `java-producer` in another.

**Consume a fixed number of messages:**

```bash
docker compose --profile clients run --rm -e MAX_MESSAGES=9 java-consumer
```

## Configuration

| Variable | Default |
|----------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `SCHEMA_PATH` | `/app/schemas/order_event.avsc` (override for v2) |
| `MAX_MESSAGES` | unset (consume until stopped) |

Host ports: Kafka `9092`, Schema Registry `8081`, Control Center `9021`, Grafana `3000`, Prometheus `9090`.

## Compose profiles

| Profile | Services |
|---------|----------|
| (default) | `kafka`, `schema-registry`, `init-topic` |
| `clients` | Python/Java producers and consumers |
| `demo` | Cross-language demo services |
| `observability` | `kafka-exporter`, `prometheus`, `grafana`, `control-center` |
| `evolve` | Schema compatibility, v2 registration, evolved producer |

## Troubleshooting

| Issue | What to try |
|-------|-------------|
| `network ... not found` | `make reset` or `docker compose --profile clients --profile demo --profile observability --profile evolve down --remove-orphans` then `make up` |
| Port conflicts | Stop other stacks; `make down` |
| Demo shows no consumed messages | `make up` first; re-run `make demo` |
| Grafana shows no lag | Run `make demo` or start a consumer; wait ~30s for scrape |
| Control Center slow to start | First boot creates internal topics; check `docker compose logs control-center` |
| Schema v2 registration fails | Run `make demo` first (creates subject), or check `register-schema-v2` logs |
| Evolve incompatible | Subject must be BACKWARD-compatible; run `init-schema-compat` before v2 |

Do **not** use `docker compose --profile demo up --abort-on-container-exit` without naming only demo services.

## Cleanup

```bash
make down
# or remove volumes:
docker compose down -v
```

## Project layout

```
docker-compose.yml
schemas/                    # order_event.avsc (v1), order_event_v2.avsc
monitoring/
  prometheus/prometheus.yml
  grafana/provisioning/     # datasource + Kafka lag dashboard
  scripts/                  # schema compat + v2 registration (curl)
python/
java/
Makefile
```
