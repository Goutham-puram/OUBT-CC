"""Logging configuration for the project."""

import logging
import sys
from typing import Optional
from pythonjsonlogger import jsonlogger

from ..config import config


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name. If None, uses root logger.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name or __name__)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    # Set log level from config
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    # Use JSON formatter for production, simple formatter for dev
    if config.environment == "prod":
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger
