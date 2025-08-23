from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict()

    # Application
    APP_ENV: str = "local"  # local | dev | prod
    SERVICE_NAME: str = "ecommerce-pipeline"

    LOG_LEVEL: str = "INFO"  # DEBUG | INFO | WARNING | ERROR | CRITICAL
    LOG_FORMAT: str = "console"  # console | json
    LOG_TO_FILE: bool = False  # enable file logging to logs/app.log
    LOG_FILE_MAX_BYTES: int = 10_485_760  # 10MB
    LOG_FILE_BACKUP_COUNT: int = 5  # number of rotated files to keep

    BATCH_SIZE: int = 50

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CLIENT_ID: str = "ecommerce-app"

    # Kafka topics
    KAFKA_ORDER_TOPIC: str = "orders"
    KAFKA_CUSTOMER_TOPIC: str = "customers"
    KAFKA_PRODUCT_TOPIC: str = "products"
    KAFKA_EVENT_TOPIC: str = "events"

    # Kafka - Cluster (KRaft)
    KAFKA_CLUSTER_ID: str = "REPLACE_WITH_BASE64_UUID"

    # PostgreSQL
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "ecommerce_analytics"
    DB_USER: str = "admin"
    DB_PASSWORD: str = "your-strong-db-password"
    DB_TEST_NAME: str = "ecommerce_analytics_test"

    @property
    def database_url(self) -> str:
        """
        Build a safe SQLAlchemy/psycopg2 DSN.
        Note: Do not log the full URL in plaintext.
        """
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    # Dashboard (Streamlit)
    DASHBOARD_PORT: int = 8501

    # Monitoring
    METRICS_ENABLED: bool = False
    HEALTHCHECK_INTERVAL_SECONDS: int = 30

    # pgAdmin (for local database management)
    PGADMIN_EMAIL: str = "admin@ecommerce.com"
    PGADMIN_PASSWORD: str = "your-strong-pgadmin-password"

    # Corruption Settings
    CORRUPTION_ENABLED: bool = True
    CORRUPTION_RANDOM_SEED: int | None = None
    CORRUPTION_PROBABILITY_PRODUCT: float = 0.01
    CORRUPTION_PROBABILITY_CUSTOMER: float = 0.02
    CORRUPTION_PROBABILITY_ORDER: float = 0.03
    CORRUPTION_PROBABILITY_EVENT: float = 0.04

    @classmethod
    def from_env_file(cls, env_file: str = ".env") -> "Settings":
        """
        Create Settings loading variables explicitly from a .env file.
        """

        class _EnvFileSettings(cls):
            model_config = SettingsConfigDict(env_file=env_file)

        return _EnvFileSettings()
