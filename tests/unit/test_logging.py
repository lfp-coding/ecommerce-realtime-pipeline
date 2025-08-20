import importlib
import json
import logging
from pathlib import Path

import pytest
import structlog

from src.config import logging_config as lc
from src.config.settings import Settings


@pytest.fixture
def tmp_logs_dir(tmp_path, monkeypatch):
    # Force logging into a temporary directory
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    return logs_dir


def test_configure_logging_console_format(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    # Console format, no file logging
    monkeypatch.setenv("LOG_FORMAT", "console")
    monkeypatch.setenv("LOG_TO_FILE", "false")
    settings = Settings()

    # Reset existing logging config
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    importlib.reload(lc)

    lc.configure_logging(settings)

    logger = structlog.get_logger("test.console")
    # A simple log entry – should work without exception
    logger.info("hello", foo="bar")

    # Check that no log file was created
    assert not Path("logs/app.log").exists()


def test_configure_logging_json_format_and_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    # JSON format + enable file logging and force small log size
    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setenv("LOG_TO_FILE", "true")
    monkeypatch.setenv("LOG_FILE_MAX_BYTES", "2000")
    monkeypatch.setenv("LOG_FILE_BACKUP_COUNT", "2")
    settings = Settings()

    # Reset Logging-Config
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    importlib.reload(lc)

    lc.configure_logging(settings)
    logger = structlog.get_logger("test.json")
    logger.info("event_one", alpha=1)
    logger.warning("event_two", beta=2)

    log_file = Path("logs/app.log")
    assert log_file.exists(), "Logfile sollte existieren"

    content = log_file.read_text(encoding="utf-8").strip()
    # For JSON renderer, lines should be JSON
    first_line = content.splitlines()[0]
    parsed = json.loads(first_line)
    assert parsed.get("event") in ("event_one", "event_two")
    assert "level" in parsed


def test_get_logger_binds_context(monkeypatch, tmp_path):
    # Check that get_logger binds context when settingsObj is passed
    monkeypatch.chdir(tmp_path)
    settings = Settings()
    # Reset logging config, minimal
    for h in list(logging.root.handlers):
        logging.root.removeHandler(h)
    importlib.reload(lc)
    lc.configure_logging(settings)

    logger = lc.get_logger("test.bound", settings_obj=settings)
    # Bound logger should write additional fields without error
    # For this we use structlog "testing" capture method
    from structlog.testing import capture_logs

    with capture_logs() as cap:
        logger.info("bound-test")
    assert any(
        r.get("event") == "bound-test"
        and r.get("service") == settings.SERVICE_NAME
        and r.get("env") == settings.APP_ENV
        for r in cap
    )
