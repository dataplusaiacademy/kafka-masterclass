# Kafka Performance masterclass

Hands-on **3-broker KRaft** cluster with **JMX Exporter → Prometheus → Grafana** (Confluent dashboards) and **Kafka perf tools** driven by scenario-specific [`client-configs/`](client-configs/). Everything runs in **Docker Compose** — no Python, Java, or other host tools required.

Host ports are chosen to avoid clashes with [`01_Kafka_Clients`](../01_Kafka_Clients/README.md) and [`02_Kafka_Connect`](../02_Kafka_Connect/README.md).

## What you will see

- Three KRaft nodes (`kafka1`–`kafka3`) on [`confluentinc/cp-kafka:7.6.1`](https://hub.docker.com/r/confluentinc/cp-kafka) with the Prometheus JMX javaagent
- Prometheus and Grafana scraping broker metrics
- Confluent **Kafka cluster (KRaft)**, **Kafka topics (KRaft)**, and client dashboards (from [jmx-monitoring-stacks](https://github.com/confluentinc/jmx-monitoring-stacks/tree/main/jmxexporter-prometheus-grafana))
- `kafka-producer-perf-test`, `kafka-consumer-perf-test`, and `kafka-e2e-latency` via Compose profile `perf`
- Reusable producer/consumer property files for **throughput**, **latency**, **durability**, and **availability**

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Docker Compose V2** | Run `docker compose version`. Bundled with **Docker Desktop** (Windows/macOS). On Linux install **Docker Engine** + Compose plugin. |
| **`make`** (optional) | [`Makefile`](Makefile) wrappers only; equivalent `docker compose` commands are listed below. |

Works on **Windows, macOS, and Linux** when Docker Compose is installed.

## Quick start

From **`03_Kafka_Performance/`**:

```bash
docker compose up -d --wait
```

Or:

```bash
make up
```

Print URLs:

```bash
make urls
```

Run the throughput scenario (producer then consumer):

```bash
make perf-throughput
```

Open Grafana at http://localhost:13000 (login **admin** / **admin**). In folder **Kafka**, open **Kafka cluster (KRaft)** and **Kafka topics (KRaft)**. After perf runs, check **Kafka producer** / **Kafka consumer** dashboards.

Allow ~2 minutes after broker start for JMX rules (`startDelaySeconds` in [`monitoring/jmx-exporter/kafka_broker.yml`](monitoring/jmx-exporter/kafka_broker.yml)) before all panels populate.

## Services and ports (host)

| Service | URL / host port |
|---------|-----------------|
| Kafka broker 1 | `localhost:39092` |
| Kafka broker 2 | `localhost:39093` |
| Kafka broker 3 | `localhost:39094` |
| Prometheus | http://localhost:19090 |
| Grafana | http://localhost:13000 (admin / admin) |
| JMX metrics (kafka1) | http://localhost:19404/metrics |

Inside the Compose network (`kafka-perf-net`), bootstrap servers are **`kafka1:29092,kafka2:29092,kafka3:29092`**. The Compose project name is **`kafka-performance`**.

## Scenarios

Each scenario has [`client-configs/<name>/producer.properties`](client-configs/) and `consumer.properties` you can reuse outside Docker.

| Goal | Config dir | Make | What to tune / observe |
|------|------------|------|------------------------|
| **Throughput** | `throughput/` | `make perf-throughput` | Batching, `lz4`, large fetch windows; broker bytes in/out on cluster dashboard |
| **Latency** | `latency/` | `make perf-latency` | `linger.ms=0`, single in-flight; **kafka-e2e-latency** prints percentile table |
| **Durability** | `durability/` | `make perf-durability` | `acks=all`, idempotence; RF=3 topic, `min.insync.replicas=2` on brokers |
| **Availability** | `availability/` | `make perf-availability` then `make perf-availability-produce` after stopping a broker | Stop `kafka2`, produce, observe errors/lag; `docker compose start kafka2` to heal |

### Docker Compose equivalents

**Throughput** (after `docker compose up -d --wait`):

```bash
docker compose --profile perf run --rm -e SCENARIO=throughput perf-producer
docker compose --profile perf run --rm -e SCENARIO=throughput perf-consumer
```

**Latency** (producer + e2e):

```bash
docker compose --profile perf run --rm -e SCENARIO=latency \
  -e NUM_RECORDS=500000 -e RECORD_SIZE=256 perf-producer
docker compose --profile perf run --rm -e SCENARIO=latency perf-e2e-latency
```

**Durability**:

```bash
docker compose --profile perf run --rm -e SCENARIO=durability perf-producer
```

**Availability** (broker failure drill):

```bash
docker compose stop kafka2
docker compose --profile perf run --rm -e SCENARIO=availability perf-producer
docker compose start kafka2
```

Override load via environment variables: `NUM_RECORDS`, `RECORD_SIZE`, `THROUGHPUT`, `MESSAGES`, `FETCH_SIZE`, `TOPIC`, `SCENARIO`.

Recreate the perf topic with different layout:

```bash
docker compose run --rm -e PARTITIONS=24 -e REPLICATION_FACTOR=3 init-perf-topic
```

## Monitoring

Metrics flow: broker JMX → JMX Exporter (`:9404`) → Prometheus → Grafana.

Prometheus targets: `kafka1:9404`, `kafka2:9404`, `kafka3:9404` ([`monitoring/prometheus/prometheus.yml`](monitoring/prometheus/prometheus.yml)).

Recommended dashboards (provisioned under folder **Kafka**):

| Dashboard | Use |
|-----------|-----|
| Kafka cluster (KRaft) | Broker CPU, network, request rates |
| Kafka topics (KRaft) | Per-topic throughput |
| Kraft | Controller / metadata view |
| Kafka producer / Kafka consumer | While perf containers run |

On each dashboard, set the **Environment** variable to **`lab`** (matches the `env: lab` label on Prometheus scrape targets). If panels show no data but the datasource is healthy, this dropdown is usually unset.

Upstream reference: [Confluent jmxexporter-prometheus-grafana](https://github.com/confluentinc/jmx-monitoring-stacks/tree/main/jmxexporter-prometheus-grafana).

## Operational notes

### Clean reset

```bash
docker compose down -v --remove-orphans
docker compose up -d --wait
```

Or: `make reset` then `make up`.

### Consumer perf shows no data

Run the matching **producer** first (or `make perf-throughput`). The consumer reads existing records on `perf.test`.

## Cross-OS usability

### Windows paths and line endings

- Compose bind mounts use forward slashes; Docker Desktop resolves them.
- [`scripts/*.sh`](scripts/) must stay **Unix (LF)**. CRLF causes `$'\r': command not found`.

### WSL versus Docker Desktop (Windows)

- Published ports (`39092`, `13000`, …) are reachable on **`localhost`** from Windows and WSL2 when using Docker Desktop.

### Linux

Install Docker CE + Compose V2. Add your user to the **`docker`** group if needed. High ports (`39092`, `19090`) avoid rootless port binding issues.

### ARM-based MacBooks

Official Confluent **linux/amd64** images usually run under Docker Desktop emulation. If pulls fail, set `platform: linux/amd64` on broker services.

## Troubleshooting

| Issue | Things to try |
|-------|----------------|
| Port already allocated | Change left side of `ports:` in [`docker-compose.yml`](docker-compose.yml) (`39092`, `13000`, `19090`, …). |
| Brokers not healthy | `docker compose logs kafka1` (and kafka2/3); ensure all three nodes share the same `CLUSTER_ID` and quorum voters. |
| Empty Grafana panels | Wait for JMX `startDelaySeconds`; confirm http://localhost:19404/metrics returns data; check Prometheus **Targets** at :19090; set **Environment** to `lab`. |
| `Datasource ${Prometheus} was not found` | `docker compose restart grafana` after pulling latest dashboard/datasource provisioning (UID `prometheus`). |
| `NOT_ENOUGH_REPLICAS` on produce | Cluster needs 3 brokers up for `acks=all` with `min.insync.replicas=2`. |
| `perf-consumer` exits quickly | Topic empty — run producer first. |

## Attribution

- JMX rule file and Grafana dashboard JSON from [confluentinc/jmx-monitoring-stacks](https://github.com/confluentinc/jmx-monitoring-stacks) (Apache 2.0).
- JMX Prometheus javaagent from [prometheus/jmx_exporter](https://github.com/prometheus/jmx_exporter).

## Project layout

```
docker-compose.yml
Makefile
README.md
brokers/Dockerfile
client-configs/
  throughput/ latency/ durability/ availability/
monitoring/
  jmx-exporter/kafka_broker.yml
  prometheus/prometheus.yml
  grafana/provisioning/
scripts/
  init-perf-topic.sh
  run-producer-perf.sh
  run-consumer-perf.sh
  run-e2e-latency.sh
```
