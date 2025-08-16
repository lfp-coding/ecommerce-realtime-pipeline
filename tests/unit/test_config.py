# tests/unit/test_config.py
from src.config.settings import Settings


def test_settings_defaults():
    """Test that all settings are initialized with default values."""
    settings = Settings()
    assert settings.LOG_LEVEL == "INFO"
    assert settings.BATCH_SIZE == 50
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"
    assert settings.KAFKA_CLIENT_ID == "ecommerce-app"
    assert settings.KAFKA_ORDER_TOPIC == "orders"
    assert settings.KAFKA_CUSTOMER_TOPIC == "customers"
    assert settings.KAFKA_PRODUCT_TOPIC == "products"
    assert settings.KAFKA_EVENT_TOPIC == "events"
    assert settings.DB_HOST == "localhost"
    assert settings.DB_PORT == 5432
    assert settings.DB_NAME == "ecommerce_analytics"
    assert settings.DB_USER == "postgres"
    assert settings.DB_PASSWORD == "postgres"
    assert settings.DASHBOARD_PORT == 8501
    assert settings.METRICS_ENABLED is False
    assert settings.HEALTHCHECK_INTERVAL_SECONDS == 30


def test_settings_env_override(monkeypatch):
    """Test that all settings can be overridden by environment variables."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("BATCH_SIZE", "100")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "test:1234")
    monkeypatch.setenv("KAFKA_CLIENT_ID", "test-client")
    monkeypatch.setenv("KAFKA_ORDER_TOPIC", "test-orders")
    monkeypatch.setenv("KAFKA_CUSTOMER_TOPIC", "test-customers")
    monkeypatch.setenv("KAFKA_PRODUCT_TOPIC", "test-products")
    monkeypatch.setenv("KAFKA_EVENT_TOPIC", "test-events")
    monkeypatch.setenv("DB_HOST", "dbhost")
    monkeypatch.setenv("DB_PORT", "12345")
    monkeypatch.setenv("DB_NAME", "test_db")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_pw")
    monkeypatch.setenv("DASHBOARD_PORT", "9999")
    monkeypatch.setenv("METRICS_ENABLED", "true")
    monkeypatch.setenv("HEALTHCHECK_INTERVAL_SECONDS", "99")

    settings = Settings()
    assert settings.LOG_LEVEL == "DEBUG"
    assert settings.BATCH_SIZE == 100
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "test:1234"
    assert settings.KAFKA_CLIENT_ID == "test-client"
    assert settings.KAFKA_ORDER_TOPIC == "test-orders"
    assert settings.KAFKA_CUSTOMER_TOPIC == "test-customers"
    assert settings.KAFKA_PRODUCT_TOPIC == "test-products"
    assert settings.KAFKA_EVENT_TOPIC == "test-events"
    assert settings.DB_HOST == "dbhost"
    assert settings.DB_PORT == 12345
    assert settings.DB_NAME == "test_db"
    assert settings.DB_USER == "test_user"
    assert settings.DB_PASSWORD == "test_pw"
    assert settings.DASHBOARD_PORT == 9999
    assert settings.METRICS_ENABLED is True
    assert settings.HEALTHCHECK_INTERVAL_SECONDS == 99


def test_database_url_construction():
    """Test that the database URL is constructed correctly."""
    settings = Settings()
    expected_db_url = f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    assert settings.database_url == expected_db_url
