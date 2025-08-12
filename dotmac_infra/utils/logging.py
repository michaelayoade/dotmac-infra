"""
Internal logging utilities for dotmac_infra package
"""
import logging
import structlog
from typing import Any, Dict, Optional


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance"""
    return structlog.get_logger(name)


# Default logger for the package
logger = get_logger("dotmac_infra")


class StructuredLogger:
    """Structured logging wrapper for consistent logging across the package"""
    
    def __init__(self, name: str = "dotmac_infra"):
        self.logger = structlog.get_logger(name)
    
    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self.logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self.logger.critical(message, **kwargs)
    
    def bind(self, **kwargs) -> "StructuredLogger":
        """Bind additional context to the logger"""
        new_logger = StructuredLogger()
        new_logger.logger = self.logger.bind(**kwargs)
        return new_logger


def configure_logging(level: str = "INFO", json_logs: bool = True):
    """Configure structured logging for the package"""
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    
    if json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=None,
        level=getattr(logging, level.upper()),
    )
