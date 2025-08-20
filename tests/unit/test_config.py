import structlog

from src.config.logging_config import configure_logging
from src.config.settings import Settings

logger = structlog.get_logger(__name__)
configure_logging(Settings())


def test_settings_defaults():
    """Test that all settings are initialized with default values."""
    logger.info("config.defaults.start")

    settings = Settings()

    # Application
    assert settings.APP_ENV == "local"
    assert settings.SERVICE_NAME == "ecommerce-pipeline"
    assert settings.LOG_LEVEL == "INFO"
    assert settings.LOG_FORMAT == "console"
    assert settings.LOG_TO_FILE is False
    assert settings.LOG_FILE_MAX_BYTES == 10_485_760
    assert settings.LOG_FILE_BACKUP_COUNT == 5
    assert settings.BATCH_SIZE == 50

    # Kafka - Client
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"
    assert settings.KAFKA_CLIENT_ID == "ecommerce-app"
    assert settings.KAFKA_ORDER_TOPIC == "orders"
    assert settings.KAFKA_CUSTOMER_TOPIC == "customers"
    assert settings.KAFKA_PRODUCT_TOPIC == "products"
    assert settings.KAFKA_EVENT_TOPIC == "events"

    # Kafka - Cluster
    assert settings.KAFKA_CLUSTER_ID == "REPLACE_WITH_BASE64_UUID"

    # PostgreSQL
    assert settings.DB_HOST == "localhost"
    assert settings.DB_PORT == 5432
    assert settings.DB_NAME == "ecommerce_analytics"
    assert settings.DB_USER == "admin"
    assert settings.DB_PASSWORD == "your-strong-db-password"

    # Dashboard
    assert settings.DASHBOARD_PORT == 8501

    # Monitoring
    assert settings.METRICS_ENABLED is False
    assert settings.HEALTHCHECK_INTERVAL_SECONDS == 30

    # pgAdmin
    assert settings.PGADMIN_EMAIL == "admin@ecommerce.com"
    assert settings.PGADMIN_PASSWORD == "your-strong-pgadmin-password"
    logger.info("config.defaults.ok")


def test_settings_env_override(monkeypatch):
    """Test that all settings can be overridden by environment variables."""
    logger.info("config.env_override.start")

    # Application
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("SERVICE_NAME", "ecommerce-pipeline-dev")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setenv("LOG_TO_FILE", "true")
    monkeypatch.setenv("LOG_FILE_MAX_BYTES", "2097152")  # 2MB
    monkeypatch.setenv("LOG_FILE_BACKUP_COUNT", "9")
    monkeypatch.setenv("BATCH_SIZE", "100")

    # Kafka - Client
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "test:1234")
    monkeypatch.setenv("KAFKA_CLIENT_ID", "test-client")
    monkeypatch.setenv("KAFKA_ORDER_TOPIC", "test-orders")
    monkeypatch.setenv("KAFKA_CUSTOMER_TOPIC", "test-customers")
    monkeypatch.setenv("KAFKA_PRODUCT_TOPIC", "test-products")
    monkeypatch.setenv("KAFKA_EVENT_TOPIC", "test-events")

    # Kafka - Cluster
    monkeypatch.setenv("KAFKA_CLUSTER_ID", "cluster-123")

    # PostgreSQL
    monkeypatch.setenv("DB_HOST", "dbhost")
    monkeypatch.setenv("DB_PORT", "12345")
    monkeypatch.setenv("DB_NAME", "test_db")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_pw")

    # Dashboard
    monkeypatch.setenv("DASHBOARD_PORT", "9999")

    # Monitoring
    monkeypatch.setenv("METRICS_ENABLED", "true")
    monkeypatch.setenv("HEALTHCHECK_INTERVAL_SECONDS", "99")

    # pgAdmin
    monkeypatch.setenv("PGADMIN_EMAIL", "test@ecommerce.com")
    monkeypatch.setenv("PGADMIN_PASSWORD", "test-pgadmin-password")

    settings = Settings()

    # Application
    assert settings.APP_ENV == "dev"
    assert settings.SERVICE_NAME == "ecommerce-pipeline-dev"
    assert settings.LOG_LEVEL == "DEBUG"
    assert settings.LOG_FORMAT == "json"
    assert settings.LOG_TO_FILE is True
    assert settings.LOG_FILE_MAX_BYTES == 2_097_152
    assert settings.LOG_FILE_BACKUP_COUNT == 9
    assert settings.BATCH_SIZE == 100

    # Kafka - Client
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "test:1234"
    assert settings.KAFKA_CLIENT_ID == "test-client"
    assert settings.KAFKA_ORDER_TOPIC == "test-orders"
    assert settings.KAFKA_CUSTOMER_TOPIC == "test-customers"
    assert settings.KAFKA_PRODUCT_TOPIC == "test-products"
    assert settings.KAFKA_EVENT_TOPIC == "test-events"

    # Kafka - Cluster
    assert settings.KAFKA_CLUSTER_ID == "cluster-123"

    # PostgreSQL
    assert settings.DB_HOST == "dbhost"
    assert settings.DB_PORT == 12345
    assert settings.DB_NAME == "test_db"
    assert settings.DB_USER == "test_user"
    assert settings.DB_PASSWORD == "test_pw"

    # Dashboard
    assert settings.DASHBOARD_PORT == 9999

    # Monitoring
    assert settings.METRICS_ENABLED is True
    assert settings.HEALTHCHECK_INTERVAL_SECONDS == 99

    # pgAdmin
    assert settings.PGADMIN_EMAIL == "test@ecommerce.com"
    assert settings.PGADMIN_PASSWORD == "test-pgadmin-password"
    logger.info("config.env_override.ok")


def test_database_url_construction():
    """Test that the database URL is constructed correctly."""
    logger.info("config.db_url.start")

    settings = Settings()
    expected_db_url = f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    assert settings.database_url == expected_db_url
    logger.info("config.db_url.ok")
