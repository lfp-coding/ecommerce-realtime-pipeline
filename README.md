# E-Commerce Real-Time Analytics Pipeline

A learning-focused, production-style data engineering project that simulates an e-commerce realtime pipeline: synthetic event generation, Kafka streaming, Postgres storage, basic monitoring, and a simple dashboard.

**Status:** Configuration & Testing Setup Complete

## Goals

- Generate realistic e-commerce events (customers, products, orders, events) in realtime
- Stream data via Kafka topics
- Persist and query data in Postgres
- Prepare transformations and analytics workflows
- Add basic monitoring and a lightweight dashboard

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

## Tech Stack

- Python 3.11+
- Kafka + Zookeeper
- PostgreSQL
- dbt
- Streamlit
- Docker Compose for local orchestration

## Disclaimer

This repository is for educational purposes and local development.

**Warning:** _Do not use default credentials in production environments._

## License

MIT License

## Contributions

This is a personal learning project; external contributions are not currently accepted.

Feedback and suggestions are welcome—please open an issue to share ideas or report problems.
