# E-Commerce Real-Time Analytics Pipeline

A production-style, learning-focused data engineering project that simulates a realtime e-commerce pipeline: synthetic event generation, Kafka (KRaft) streaming, PostgreSQL storage, basic monitoring hooks, and a simple Streamlit dashboard. Everything runs locally via Docker Compose and is covered by unit and integration tests.

Status: Initial Kafka KRaft cluster, Postgres, and test scaffolding are in place. Code for producers/consumers/dashboard is stubbed and will be filled in next.

## Why this project?

- Demonstrates a modern streaming stack with Kafka in KRaft mode (no ZooKeeper)
- Shows clean Python packaging, structured logging, environment-based settings
- Includes realistic tests: environment checks and Kafka end-to-end flows
- Serves as a portfolio-ready project for Data Engineering roles

## Architecture

- Data Generator (Python) → Kafka topics (orders, customers, products, events)
- Kafka KRaft cluster (3 combined broker/controller nodes)
- Consumer (Python) → PostgreSQL (normalized schemas + indexes)
- Transformations (dbt skeleton) → marts (planned)
- Dashboard (Streamlit) → simple realtime views (planned)
- Monitoring hooks (structlog, health checks stubs, metrics stubs)

## Tech Stack

- Python 3.11+
- Kafka (Confluent Platform images, KRaft mode)
- PostgreSQL 16
- dbt (skeleton in place)
- Streamlit
- Docker Compose
- Pytest (unit + integration)

## Repository Structure

## Project Structure

```
src/
  config/           : centralized settings and logging setup
  data_generator/   : schemas, utilities, Kafka producer
  consumer/         : Kafka consumer, DB handler, validators
  monitoring/       : metrics and health checks
  dashboard/        : Streamlit app
sql/
  init/             : database init scripts (schemas, tables, indexes)
  dbt/              : dbt project skeleton (staging, marts, tests)
tests/              : unit and integration tests
scripts/            : setup/start/stop scripts
data/               : sample data folder
logs/               : runtime logs (git-ignored)
```

## Quickstart

1. Prerequisites

- Docker & Docker Compose
- Python 3.11+ (optional for running tests locally)

1. Configure environment

- Copy .env.example → .env
- Generate a KRaft Cluster ID:
  docker run --rm confluentinc/cp-kafka:7.7.1 kafka-storage random-uuid
- Set KAFKA_CLUSTER_ID in .env

1. Start infrastructure

- docker compose up -d
- Verify:
  - Kafka UI at http://localhost:8080
  - pgAdmin at http://localhost:8081
  - Postgres health: docker ps (container healthy)

1. Run tests (local host)

- python -m venv .venv && source .venv/bin/activate
- pip install -r requirements.txt
- pytest -q

Tip: Integration tests expect Kafka at localhost:9092 (mapped to kafka1).

## Planned Milestones

- Data generator
  - Implement schemas.py and realistic event payloads
  - Producer with batching, idempotence, and backoff
- Consumer & storage
  - Implement consumer with validation (pydantic) and DB upsert patterns
  - Create initial queries and basic aggregate tables
- dbt models
  - Staging models for raw topics
  - Marts for orders, revenue, cohorts
- Dashboard
  - Streamlit KPIs: orders/min, revenue, top products, errors
  - Live view using incremental queries
- Monitoring
  - Health checks for Kafka and Postgres
  - Metrics counters (produced/consumed/errors)
- CI/CD
  - Linting, tests, and Docker build on PRs
  - Optional: pre-commit hooks

## Disclaimer

This repository is for educational purposes and local development.

**Warning:** _Do not use default credentials in production environments._

## License

MIT License

## Contributions

This is a personal learning project; external contributions are not currently accepted.

Feedback and suggestions are welcome—please open an issue to share ideas or report problems.
