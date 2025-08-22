"""
Logging Utility

Provides a simple logging interface for all data processing scripts
with standardised formatting and log levels.
"""

import logging


class Logger:
    """Simple logger wrapper with standard configuration."""

    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def info(self, message: str) -> None:
        """Log an informational message."""
        self.logger.info(message)

    def error(self, message: str) -> None:
        """Log an error message."""
        self.logger.error(message)

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.logger.debug(message)
