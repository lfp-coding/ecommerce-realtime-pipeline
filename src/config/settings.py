# src/config/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")
    """Application settings loaded from environment variables."""

    # Application
    LOG_LEVEL: str = "INFO"
    BATCH_SIZE: int = 50

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CLIENT_ID: str = "ecommerce-app"
    KAFKA_ORDER_TOPIC: str = "orders"
    KAFKA_CUSTOMER_TOPIC: str = "customers"
    KAFKA_PRODUCT_TOPIC: str = "products"
    KAFKA_EVENT_TOPIC: str = "events"

    # PostgreSQL
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "ecommerce_analytics"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"

    # Dashboard
    DASHBOARD_PORT: int = 8501

    # Monitoring
    METRICS_ENABLED: bool = False
    HEALTHCHECK_INTERVAL_SECONDS: int = 30

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


# Singleton instance
# Use this global 'settings' object to access configuration values throughout the project.
settings = Settings()
