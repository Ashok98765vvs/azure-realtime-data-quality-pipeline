"""
Centralized Logging Utility
Azure Real-Time Data Quality & Anomaly Detection Pipeline
"""

import logging
import sys
from datetime import datetime


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a configured logger with console handler.
    Includes timestamp, level, module name, and correlation ID support.

    Args:
        name: Logger name (typically __name__)
        level: Logging level (default: INFO)

    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(level)

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


class PipelineLogger:
    """
    Context-aware logger that attaches pipeline metadata
    (batch_id, stage, source_system) to every log entry.
    """

    def __init__(self, name: str, batch_id: str = None, stage: str = None):
        self._logger   = get_logger(name)
        self.batch_id  = batch_id or "N/A"
        self.stage     = stage    or "UNKNOWN"

    def _prefix(self) -> str:
        return f"[batch={self.batch_id}][stage={self.stage}]"

    def info(self, msg: str, *args):
        self._logger.info(f"{self._prefix()} {msg}", *args)

    def warning(self, msg: str, *args):
        self._logger.warning(f"{self._prefix()} {msg}", *args)

    def error(self, msg: str, *args):
        self._logger.error(f"{self._prefix()} {msg}", *args)

    def debug(self, msg: str, *args):
        self._logger.debug(f"{self._prefix()} {msg}", *args)
