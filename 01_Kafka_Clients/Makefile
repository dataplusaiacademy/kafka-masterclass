.PHONY: help up down reset demo produce-py consume-py produce-java consume-java

help:
	@echo "Kafka Clients Masterclass (Docker only)"
	@echo ""
	@echo "  make up            Start Kafka, Schema Registry, create topic"
	@echo "  make down          Stop all containers"
	@echo "  make reset         down + up (recover from stale Docker state)"
	@echo "  make demo          Python produce -> Java consume"
	@echo "  make produce-py    Run Python producer"
	@echo "  make consume-java  Run Java consumer (follow logs; Ctrl+C to stop)"
	@echo "  make produce-java  Run Java producer"
	@echo "  make consume-py    Run Python consumer"

up:
	docker compose up -d --wait

down:
	docker compose --profile clients --profile demo down --remove-orphans

reset: down up

demo:
	docker compose up -d --wait
	@cid=$$(docker compose --profile demo run -d -q demo-java-consumer); \
	echo "Java consumer container: $$cid"; \
	docker compose --profile demo run --rm demo-python-producer; \
	docker wait $$cid; \
	echo "--- Java consumer output ---"; \
	docker logs $$cid; \
	docker rm $$cid >/dev/null

produce-py:
	docker compose --profile clients run --rm python-producer

consume-py:
	docker compose --profile clients run --rm python-consumer

produce-java:
	docker compose --profile clients run --rm java-producer

consume-java:
	docker compose --profile clients run --rm java-consumer
