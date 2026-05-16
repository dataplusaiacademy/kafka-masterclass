.PHONY: help up down reset demo status logs-connect connectors-rerun urls seed consume-topic

DOCKER_COMPOSE := docker compose
COMPOSE_TOOLS := $(DOCKER_COMPOSE) --profile tools

help:
	@echo "Kafka Connect CDC lab (Docker Compose only)"
	@echo ""
	@echo "  make up                Start infra and register connectors (--wait)"
	@echo "  make down              Stop containers (keep volumes)"
	@echo "  make reset             Destroy containers and volumes (clean slate)"
	@echo "  make demo              INSERT in Postgres then show rows in MySQL"
	@echo "  make seed              Faker bulk INSERT into Postgres (profile: tools)"
	@echo "  make consume-topic     Read Avro CDC messages from demo.public.orders"
	@echo "  make urls              Print service URLs"
	@echo "  make status            Print connector statuses (curl JSON)"
	@echo "  make logs-connect      Follow Kafka Connect logs"

up:
	$(DOCKER_COMPOSE) up -d --wait

down:
	$(DOCKER_COMPOSE) down --remove-orphans

reset:
	$(DOCKER_COMPOSE) down -v --remove-orphans

urls:
	@echo "Kafka bootstrap (host):  localhost:29092"
	@echo "Schema Registry:           http://localhost:28081"
	@echo "Kafka Connect REST:        http://localhost:28083"
	@echo "Postgres (host):           localhost:55432"
	@echo "MySQL (host):              localhost:53306"

demo: up
	$(DOCKER_COMPOSE) exec -T postgres psql -U postgres -d demo -c "INSERT INTO public.orders (product_name, qty) VALUES ('manual-demo-row', 3);"
	@sleep 3
	$(DOCKER_COMPOSE) exec -T mysql mysql -uroot -pconnect_demo -e "SELECT id, product_name, qty, ingest_source FROM sink.orders ORDER BY id;"

status:
	curl -fsS http://localhost:28083/connectors/postgres-cdc/status
	@echo ""
	curl -fsS http://localhost:28083/connectors/jdbc-mysql-sink/status
	@echo ""

logs-connect:
	$(DOCKER_COMPOSE) logs -f kafka-connect

# Re-register after editing scripts/register-connectors.sh (infra must be up).
connectors-rerun:
	$(DOCKER_COMPOSE) run --rm register-connectors

seed:
	$(COMPOSE_TOOLS) run --rm fake-seed

consume-topic:
	$(COMPOSE_TOOLS) run --rm topic-consumer
