# Kafka Clients Masterclass

A hands-on example of **Kafka**, **Confluent Schema Registry**, and **Avro** with producers and consumers in **Python** and **Java**. Everything runs in **Docker Compose** — no Python, Java, or other tools required on the host.

## What you will see

- Kafka (KRaft) and Schema Registry in Docker
- Order lifecycle events on topic `orders.events` (`PLACED` → `PAID` → `SHIPPED`)
- Cross-language Avro: **Python produces**, **Java consumes** in the default demo
- Same schema registered in Schema Registry for both languages

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| **Docker Compose V2** | `docker compose version` — Docker Desktop (Windows/macOS) or Engine + plugin (Linux) |

Works the same on **Windows, macOS, and Linux**.

## Quick start

From the project root (`01_Kafka_Clients`):

```bash
docker compose up -d --wait
cid=$(docker compose --profile demo run -d -q demo-java-consumer)
docker compose --profile demo run --rm demo-python-producer
docker wait $cid && docker logs $cid && docker rm $cid
```

Or simply: `make demo` (prints consumer logs when finished).

The first command starts Kafka and Schema Registry and creates the `orders.events` topic. The second starts the Java consumer in the background. The third runs the Python producer (after an 8-second delay so the consumer can join the group). You should see nine consumed events in the Java consumer logs (3 orders × 3 statuses).

`make demo` runs all of this and prints the consumer logs when finished.

With Make (optional):

```bash
make demo
```

Or step by step: `make up`, then `make demo` (which calls `up` again — safe to run).

## Running individual clients

Start infrastructure first (`docker compose up -d --wait`), then:

| Action | Command |
|--------|---------|
| Python producer | `docker compose --profile clients run --rm python-producer` |
| Python consumer | `docker compose --profile clients run --rm python-consumer` |
| Java producer | `docker compose --profile clients run --rm java-producer` |
| Java consumer | `docker compose --profile clients run --rm java-consumer` |

**Reverse direction (Java produce → Python consume):**

Terminal 1:

```bash
docker compose --profile clients run --rm python-consumer
```

Terminal 2:

```bash
docker compose --profile clients run --rm java-producer
```

Stop the consumer with `Ctrl+C`.

**Consume a fixed number of messages** (container exits automatically):

```bash
docker compose --profile clients run --rm -e MAX_MESSAGES=9 java-consumer
```

## Configuration

Set on client containers via Compose (defaults shown):

| Variable | Default |
|----------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `MAX_MESSAGES` | unset (consume until stopped) |

Host ports for optional debugging: Kafka `localhost:9092`, Schema Registry `http://localhost:8081`.

## Inspecting the stack

```bash
docker compose ps
```

Schema Registry subjects: [http://localhost:8081/subjects](http://localhost:8081/subjects) (browser or `curl` on the host).

Expected subject after producing: `orders.events-value`.

## Troubleshooting

| Issue | What to try |
|-------|-------------|
| `network ... not found` | Stale Docker state from a previous run. Run `make reset` or: `docker compose --profile clients --profile demo down --remove-orphans` then `docker compose up -d --wait` and retry the demo |
| Port 9092 or 8081 in use | Stop other Kafka stacks; `docker compose down` |
| Docker not running | Start Docker Desktop / system Docker service |
| Demo shows no consumed messages | Run `docker compose up -d --wait` first; re-run demo (increase sleep in `demo-python-producer` if consumer is slow to join) |
| Image build fails | Check network access for Gradle/Maven and pip; `docker compose --profile clients --profile demo build` |
| `init-topic` failed | Ensure Kafka is healthy: `docker compose logs kafka` |

Do **not** use `docker compose --profile demo up --abort-on-container-exit` without naming only demo services — that pattern can stop infrastructure when `init-topic` exits and leave broken network references.

## Cleanup

```bash
docker compose --profile clients --profile demo down --remove-orphans
docker compose down -v   # also remove volumes if you want a clean slate
```

## Project layout

```
docker-compose.yml     # infra, init-topic, client services, demo profile
schemas/               # shared Avro schema
python/                # Python producer & consumer + Dockerfile
java/                  # Java clients + Dockerfile (Gradle build inside image)
Makefile               # optional shortcuts (all invoke docker compose)
```
