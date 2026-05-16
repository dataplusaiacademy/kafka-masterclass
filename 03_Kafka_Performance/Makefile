.PHONY: help up down reset urls metrics-check perf-throughput perf-latency perf-durability perf-availability perf-producer perf-consumer perf-e2e

DOCKER_COMPOSE := docker compose
COMPOSE_PERF := $(DOCKER_COMPOSE) --profile perf

BOOTSTRAP := kafka1:29092,kafka2:29092,kafka3:29092

help:
	@echo "Kafka Performance lab (Docker Compose only)"
	@echo ""
	@echo "  make up                  Start 3 brokers, Prometheus, Grafana, perf topic"
	@echo "  make down                Stop containers"
	@echo "  make reset               down -v (wipe broker data)"
	@echo "  make urls                Print UI and bootstrap URLs"
	@echo "  Perf runs export client JMX to Grafana Kafka producer/consumer dashboards"
	@echo "  make perf-throughput     High-throughput producer then consumer"
	@echo "  make perf-latency        Low-latency producer + e2e latency tool"
	@echo "  make perf-durability     acks=all / idempotent producer load"
	@echo "  make perf-availability   Durability producer; see README for broker stop"
	@echo "  make perf-producer       Run producer (SCENARIO=throughput)"
	@echo "  make perf-consumer       Run consumer (SCENARIO=throughput)"
	@echo "  make perf-e2e            End-to-end latency (SCENARIO=latency)"
	@echo "  make metrics-check       Verify perf client metrics reached Prometheus"

up:
	$(DOCKER_COMPOSE) up -d --wait

down:
	$(DOCKER_COMPOSE) --profile perf down --remove-orphans

reset:
	$(DOCKER_COMPOSE) --profile perf down -v --remove-orphans

urls:
	@echo "Kafka bootstrap (host):  localhost:39092,localhost:39093,localhost:39094"
	@echo "Grafana:                 http://localhost:13000  (admin / admin)"
	@echo "Prometheus:              http://localhost:19090"
	@echo "Pushgateway:             http://localhost:19091"
	@echo "Broker JMX metrics:      http://localhost:19404/metrics (kafka1; 19405/19406 for kafka2/3)"

PROM_URL ?= http://localhost:19090

metrics-check:
	@python3 scripts/metrics-check.py "$(PROM_URL)"

perf-throughput: up
	$(COMPOSE_PERF) run --rm -e SCENARIO=throughput \
		-e NUM_RECORDS=5000000 -e RECORD_SIZE=1024 -e THROUGHPUT=-1 perf-producer
	$(COMPOSE_PERF) run --rm -e SCENARIO=throughput \
		-e MESSAGES=5000000 perf-consumer

perf-latency: up
	$(COMPOSE_PERF) run --rm -e SCENARIO=latency \
		-e NUM_RECORDS=5000000 -e RECORD_SIZE=256 -e THROUGHPUT=-1 perf-producer
	$(COMPOSE_PERF) run --rm -e SCENARIO=latency perf-e2e-latency

perf-durability: up
	$(COMPOSE_PERF) run --rm -e SCENARIO=durability \
		-e NUM_RECORDS=2000000 -e RECORD_SIZE=1024 -e THROUGHPUT=50000 perf-producer

perf-availability: up
	@echo "Run: docker compose stop kafka2"
	@echo "Then re-run: make perf-availability-produce"
	@echo "Restore: docker compose start kafka2"

perf-availability-produce:
	$(COMPOSE_PERF) run --rm -e SCENARIO=availability \
		-e NUM_RECORDS=5000000 -e RECORD_SIZE=1024 -e THROUGHPUT=10000 perf-producer

perf-producer:
	$(COMPOSE_PERF) run --rm -e SCENARIO=$${SCENARIO:-throughput} perf-producer

perf-consumer:
	$(COMPOSE_PERF) run --rm -e SCENARIO=$${SCENARIO:-throughput} perf-consumer

perf-e2e:
	$(COMPOSE_PERF) run --rm -e SCENARIO=$${SCENARIO:-latency} perf-e2e-latency
