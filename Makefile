SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: up down logs clean render connector verify

render:
	./scripts/render.sh

up: render
	$(COMPOSE) up -d --build --wait
	@echo ""
	@echo "All services healthy."
	@echo "  AKHQ:            http://localhost:8080"
	@echo "  pgAdmin:         http://localhost:5050"
	@echo "  Grafana:         http://localhost:3000/d/pstream-debezium"
	@echo "  Prometheus:      http://localhost:9090"
	@echo "  Kafka Connect:   http://localhost:8083"
	@echo "  Schema Registry: http://localhost:8081"

connector: render
	$(COMPOSE) run --rm register-connector

verify:
	./scripts/verify.sh

logs:
	$(COMPOSE) logs -f

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans
