# E-Commerce Real-Time Analytics Pipeline

A production-style, learning-focused data engineering project that demonstrates a complete real-time e-commerce analytics pipeline. Features synthetic event generation, Kafka streaming (KRaft mode), PostgreSQL storage, comprehensive testing, and CI/CD automation.

![Pipeline Status](https://img.shields.io/badge/status-active%20development-yellow)
![Tests](https://img.shields.io/badge/tests-passing-green)
![License](https://img.shields.io/badge/license-MIT-blue)

## Key Features

- **Synthetic Data Generation**: Generate realistic products, customers, orders, and events with configurable batch sizes and optional data corruption for robustness testing.
- **Kafka (KRaft Mode)**: A ZooKeeper-free Kafka cluster with three broker/controller nodes for high availability and exactly-once semantics.
- **PostgreSQL Storage**: Raw JSONB storage with audit tables and normalized business schemas with triggers and indexes for performance.
- **dbt Transformations**: Skeleton for staging and mart models, enabling modular, version-controlled SQL transformations.
- **Streamlit Dashboard**: Real-time KPI visualizations including orders per minute, revenue trends, top products, and data quality metrics.
- **Monitoring & Health Checks**: Metrics and health check hooks to monitor consumer offsets, processing stats, and service health.
- **CI/CD Pipeline**: GitHub Actions workflows for linting (pre-commit & Ruff), unit tests, integration tests with Docker Compose, and security scans (Bandit, Safety).

## Project Status

**Current Implementation:**

- ✅ **Data Generator**: Fully implemented with synthetic data generation for products, customers, orders, and events
- ✅ **Kafka Infrastructure**: 3-node KRaft cluster with proper topic management and performance optimization
- ✅ **Database Schema**: Complete PostgreSQL setup with normalized tables, indexes, and monitoring
- ✅ **Testing Suite**: Comprehensive unit and integration tests with 95%+ coverage
- ✅ **CI/CD Pipeline**: Automated testing, linting, and security scanning via GitHub Actions
- ✅ **Data Corruption**: Realistic data corruption simulation for robustness testing
- 🔄 **Consumer Implementation**: In progress
- 🔄 **Streamlit Dashboard**: Planned
- 🔄 **dbt Transformations**: Planned

## Project Structure

```bash
├── .github/workflows      # CI configurations
├── docs                   # Architecture and setup documentation
├── sql                    # Database initialization and dbt project
├── src                    # Application source code
│   ├── data_generator     # Synthetic batch generation and producer
│   ├── consumer           # Kafka consumers and DB handlers
│   ├── dashboard          # Streamlit app for visualization
│   ├── monitoring         # Health checks and metrics collection
│   └── config             # Settings and structured logging setup
├── tests                  # Unit and integration tests
├── docker-compose.yaml    # Infrastructure definition
├── Dockerfile             # Multi-stage build for application image
├── scripts                # Helper scripts to setup and run the pipeline
├── .env.example           # Environment variable template
└── README
```

## Why this project?

- Demonstrates a modern streaming stack with Kafka in KRaft mode
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

## Disclaimer

This repository is for educational purposes and local development.

**Warning:** _Do not use default credentials in production environments._

## License

MIT License

## Contributions

This is a personal learning project; external contributions are not currently accepted.

Feedback and suggestions are welcome—please open an issue to share ideas or report problems.
