# Kafka Connect masterclass

End-to-end **Postgres CDC (Debezium)** → **Kafka** → **MySQL JDBC sink** with **Schema Registry** and **Avro**, wired with **Docker Compose**. You only need Compose on the host; optional **[`python/`](python/)** helpers (**Faker** seed + Avro consumer) run **inside** their own containers via the Compose **`tools`** profile.

Default host ports deliberately differ from [`01_Kafka_Clients`](../01_Kafka_Clients/README.md) so this stack can often run beside module 01 without clashes.

## What you will see

- Single-broker Kafka (KRaft) [`confluentinc/cp-kafka:7.6.1`](https://hub.docker.com/r/confluentinc/cp-kafka)
- Confluent Schema Registry `7.6.1` for Avro
- Custom Kafka Connect image: **Debezium PostgreSQL connector** + **Confluent JDBC sink** + **MySQL driver**
- One-shot **`register-connectors`** service mounts [`scripts/register-connectors.sh`](scripts/register-connectors.sh) and uses **`curl`** + POSIX **`sh`** only
- Postgres `public.orders` → topic `demo.public.orders` → MySQL `sink.orders` with synthetic column **`ingest_source`** proving an **SMT**
- Optional **bulk fake inserts** and **peek at Avro CDC records** ([`fake-seed`](#on-demand-python-tools-profile-tools) / [`topic-consumer`](#on-demand-python-tools-profile-tools)) — Python stays **inside** Docker (`tools` profile)

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Docker Compose V2** | Run `docker compose version`. Bundled with **Docker Desktop** (Windows/macOS). On Linux install **Docker Engine** + Compose plugin (`docker-ce` distro packages typically include Compose V2). |
| **`make`** (optional) | [`Makefile`](Makefile) wrappers only; equivalent `docker compose` commands appear below without `make`. On Windows install **GNU Make** via Git Bash, Chocolatey, or invoke `docker compose` directly. |

## Quick start

From **`02_Kafka_Connect/`**:

```bash
docker compose up -d --wait
```

This starts Kafka, Schema Registry, Postgres, MySQL, Kafka Connect, runs connector registration once, then exits cleanly when both connectors reach **RUNNING**.

Or with [**`make`**](Makefile) (same directory):

```bash
make up
```

Smoke test (**insert Postgres row, observe MySQL**):

```bash
make demo
```

Or without `make`:

```bash
docker compose up -d --wait
docker compose exec -T postgres psql -U postgres -d demo -c "INSERT INTO public.orders (product_name, qty) VALUES ('manual-demo-row', 3);"
sleep 3
docker compose exec -T mysql mysql -uroot -pconnect_demo -e "SELECT id, product_name, qty, ingest_source FROM sink.orders ORDER BY id;"
```

## Services and ports (host)

| Service | URL / host port |
|---------|----------------|
| Kafka (PLAINTEXT, host clients) | `localhost:29092` |
| Schema Registry | http://localhost:28081 |
| Kafka Connect REST | http://localhost:28083 ([plugins](http://localhost:28083/connector-plugins), [connectors](http://localhost:28083/connectors)) |
| Postgres | `localhost:55432` (user/pass/db `postgres` / `postgres` / `demo`) |
| MySQL | `localhost:53306` (root / `connect_demo`, database **`sink`**) |

Inside the Compose network (`kafka-connect-net`), services address each other by DNS name (**`kafka:29092`**, **`postgres`**, **`mysql`**, **`schema-registry`**, **`kafka-connect`**). The Compose top-level **`name: kafka-connect`** namespaces containers and anonymous volumes (`docker compose ls` lists it as **`kafka-connect`**).

## On-demand Python tools (`profile: tools`)

The image is built under [`python/`](python/) on first **`run`**. Infrastructure must already be up (`docker compose up -d --wait`) so Postgres and the CDC topic exist. **`requirements.txt`** pins **`fastavro`** explicitly: `AvroDeserializer` imports it at runtime (the `schema-registry` extra alone may not pull it reliably).

### Fake data into Postgres (`fake-seed`)

```bash
make seed
```

Or:

```bash
docker compose --profile tools run --rm -e FAKE_ROWS=100 fake-seed
```

Useful env vars: **`FAKE_ROWS`** (default `25`), **`FAKER_SEED`** (digits only for repeatable names/qty sampling), Postgres **`POSTGRES_*`** match the Compose service defaults.

Give Debezium a few seconds, then **`make demo`** (MySQL SELECT) or your own JDBC checks to see propagated rows.

### Consume CDC topic (`demo.public.orders`) as Avro (`topic-consumer`)

Records are **Confluent wire-format Avro** and require **Schema Registry**; a plain `kafka-console-consumer` without Avro decoding is not useful here.

```bash
make consume-topic
```

Or:

```bash
docker compose --profile tools run --rm \
  -e CONSUME_MAX_MESSAGES=50 \
  -e AUTO_OFFSET_RESET=earliest \
  topic-consumer
```

Useful env vars: **`SOURCE_TOPIC`** (default `demo.public.orders`), **`CONSUMER_GROUP_ID`**, **`AUTO_OFFSET_RESET`** (`earliest` \| `latest`), **`CONSUME_MAX_MESSAGES`**, **`CONSUME_MAX_WAIT_SEC`**, **`CONSUME_POLL_TIMEOUT_SEC`**.

## Transforms (SMT)

| Stage | Connector | Chain |
|-------|-----------|--------|
| Unwrap CDC envelope | **Debezium** `postgres-cdc` | `ExtractNewRecordState` so Kafka records are flat row payloads (not bundled `before`/`after` envelopes). Applied on the **source** because those classes ship with the Debezium connector plugin classpath. |
| Add metadata column | JDBC **sink** `jdbc-mysql-sink` | `InsertField$Value` adds **`ingest_source = kafka-connect-lab`** before the row is written to MySQL (**`transforms`** order is only `addIngestMeta` here; unwrap already ran upstream). |

**Why not chain both SMTs on the sink:** Kafka Connect isolates connector plugins into separate class loaders by default; the JDBC sink bundle does not load **`io.debezium.transforms.*`**, which returns HTTP 400 if referenced from the sink connector.

## Operational notes

### Clean reset (wipe Postgres/MySQL/Kafka ephemeral state)

Kafka and Connect storage are **anonymous volumes** tied to containers; wiping them resets topics, schemas, replication slots inside Postgres, etc.

```bash
docker compose down -v --remove-orphans
docker compose up -d --wait
```

Or: `make reset` then `make up`.

### Re-run connector registration

After editing [`scripts/register-connectors.sh`](scripts/register-connectors.sh):

```bash
docker compose run --rm register-connectors
```

(or `make connectors-rerun`)

The script **`DELETE`**s connectors first (404 is fine) then **`POST`**s fresh configs.

### `created_at` in MySQL

Debezium encodes Postgres logical times with microsecond-precision Connect types that the JDBC sink maps to **`BIGINT`** in this minimal setup. Rows still round-trip reliably; tightening MySQL temporal types needs extra Debezium/JDBC tuning beyond this lab scope.

### Topic auto creation

Kafka is started with **`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`** so Debezium/Connect topics appear on first CDC without maintaining a brittle `kafka-topics` prelude.

### ARM-based MacBooks

Official Confluent **linux/amd64** images may rely on QEMU under Docker Desktop ARM. Prefer **aarch64-compatible** Compose overrides only if needed; typical Docker Desktop setups run amd64 binaries transparently (`linux/amd64` pull). If pulls fail due to unsupported multi-arch, enforce `platform: linux/amd64` on Confluent/MySQL/postgres lines (last resort).

## Cross-OS usability

### Windows paths and line endings

- Compose binds **forward slashes**; Docker Desktop resolves them (`./scripts/...`).
- **`register-connectors.sh`** must remain **Unix (LF)** line endings. Git on Windows: `git config core.autocrlf false` **for this repo only** before checkout, or an editor LF mode; CRLF shells error with `$'\r'`.

### WSL versus Docker Desktop (Windows)

- **Docker Desktop** exposes services on **`localhost`** to Windows and WSL2 equally for published ports (`29092`, `28081`, …).
- If Docker runs **inside** WSL alone, substitute `localhost` with the WSL host IP only when querying from pure Windows tooling without forwarded ports—most users stay on Compose-published `localhost`.

### Linux

Install Docker CE + Compose V2 (`docker-compose-plugin`). Your user may need **`docker`** group membership (log out/in after `usermod -aG docker $USER`). Rootless Docker needs extra sysctl (`net.ipv4.ip_unprivileged_port_start`); exposing high ports (**29092**) reduces friction either way.

### macOS firewall / VPN

VPN or strict firewalls occasionally block **`localhost`** loopback multiplexing rare; disabling split-tunnel exclusions for Docker Desktop is the usual fix.

## Troubleshooting

| Issue | Things to try |
|-------|----------------|
| `Bind for ... port is already allocated` | Something else listens on **`29092`**, **`28081`**, **`28083`**, **`55432`**, or **`53306`**. Change the left side of **`ports:`** mappings in [`docker-compose.yml`](docker-compose.yml). |
| `register-connectors` exited non-zero | `docker compose logs register-connectors` and **`docker compose logs kafka-connect`**; fix JSON in [`scripts/register-connectors.sh`](scripts/register-connectors.sh). |
| JDBC sink FAILED / no MySQL rows | Inspect `connector.status` traces: `curl -s http://localhost:28083/connectors/jdbc-mysql-sink/status`. Common causes: MySQL JDBC URL/driver, **`delete.enabled`** with unsupported **`pk.mode`**, incompatible field types (**`TIMESTAMP WITH TIME ZONE`** in Postgres was flattened to BIGINT here). |
| Debezium cannot create slot/publication errors | Postgres must use **`wal_level=logical`** and include the table in the named publication **`dbz_orders`** (see [`postgres/init`](postgres/init)); reset with `docker compose down -v` if DDL changed. |
| `consume-topic` prints nothing / times out | Try **`AUTO_OFFSET_RESET=earliest`**, raise **`CONSUME_MAX_WAIT_SEC`**, use a fresh **`CONSUMER_GROUP_ID`**, and confirm infra is up (`make up`) so **`demo.public.orders`** exists (`make seed` + short sleep first). |

## Project layout

```
docker-compose.yml
Makefile
README.md
connect/Dockerfile
python/Dockerfile
python/requirements.txt
python/seed_orders.py
python/consume_cdc_topic.py
postgres/init/
mysql/init/
scripts/register-connectors.sh
```
