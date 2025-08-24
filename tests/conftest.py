import pytest
from src.config.logging_config import configure_logging
from src.config.settings import Settings


@pytest.fixture(scope="session", autouse=True)
def setup_logging():
    configure_logging(Settings())
