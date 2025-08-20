import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog


def configure_logging(settings_obj) -> None:
    """Configure structured logging for the application with provided settings."""

    handlers = [logging.StreamHandler(sys.stdout)]
    if settings_obj.LOG_TO_FILE:
        log_file_path = Path("logs/app.log")
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_file_path,
                maxBytes=settings_obj.LOG_FILE_MAX_BYTES,
                backupCount=settings_obj.LOG_FILE_BACKUP_COUNT,
                encoding="utf-8",
            )
        )
    logging.basicConfig(
        handlers=handlers,
        level=getattr(logging, settings_obj.LOG_LEVEL),
        format="%(message)s",
    )

    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # Optional: JSON vs. Console Format
    if settings_obj.LOG_FORMAT.lower() == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, settings_obj=None) -> structlog.BoundLogger:
    """Get a structured logger with the specified name."""
    log = structlog.get_logger(name)
    if settings_obj:
        log = log.bind(service=settings_obj.SERVICE_NAME, env=settings_obj.APP_ENV)
    return log
