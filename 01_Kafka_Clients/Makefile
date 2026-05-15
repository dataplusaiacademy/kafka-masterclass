.PHONY: help up up-all down reset demo evolve monitoring produce-py consume-py produce-java consume-java

help:
	@echo "Kafka Clients Masterclass (Docker only)"
	@echo ""
	@echo "  make up            Start Kafka, Schema Registry, create topic"
	@echo "  make up-all        up + observability (Prometheus, Grafana, Control Center)"
	@echo "  make down          Stop all containers"
	@echo "  make reset         down + up (recover from stale Docker state)"
	@echo "  make demo          Python produce -> Java consume"
	@echo "  make evolve        Register schema v2 and produce evolved events"
	@echo "  make monitoring    Print observability UI URLs"
	@echo "  make produce-py    Run Python producer"
	@echo "  make consume-java  Run Java consumer (follow logs; Ctrl+C to stop)"
	@echo "  make produce-java  Run Java producer"
	@echo "  make consume-py    Run Python consumer"

up:
	docker compose up -d --wait

up-all:
	docker compose --profile observability up -d --wait

down:
	docker compose --profile clients --profile demo --profile observability --profile evolve down --remove-orphans

reset: down up

monitoring:
	@echo "Control Center:  http://localhost:9021"
	@echo "Grafana:         http://localhost:3000  (admin / admin)"
	@echo "Prometheus:      http://localhost:9090"
	@echo "Kafka exporter:  http://localhost:9308/metrics"
	@echo "Schema Registry: http://localhost:8081"

demo:
	docker compose up -d --wait
	@cid=$$(docker compose --profile demo run -d -q demo-java-consumer); \
	echo "Java consumer container: $$cid"; \
	docker compose --profile demo run --rm demo-python-producer; \
	docker wait $$cid; \
	echo "--- Java consumer output ---"; \
	docker logs $$cid; \
	docker rm $$cid >/dev/null

evolve:
	docker compose up -d --wait
	docker compose --profile evolve up init-schema-compat register-schema-v2
	docker compose --profile evolve run --rm evolve-python-producer

produce-py:
	docker compose --profile clients run --rm python-producer

consume-py:
	docker compose --profile clients run --rm python-consumer

produce-java:
	docker compose --profile clients run --rm java-producer

consume-java:
	docker compose --profile clients run --rm java-consumer
