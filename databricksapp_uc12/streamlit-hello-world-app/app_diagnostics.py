"""GRIDWATCH AI - application diagnostics.

Logs uncaught exceptions and helps troubleshoot application startup errors.
"""

import logging
import os
import sys
import traceback
from datetime import datetime

# Store logs in a local directory inside the app.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "gridwatch_errors.log")


def setup_logger():
    """Configure application error logging."""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)

        logger = logging.getLogger("gridwatch")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s | %(levelname)s | %(message)s"
                )
            )
            logger.addHandler(handler)

        return logger

    except OSError:
        # File logging may not be available in a deployed environment.
        return logging.getLogger("gridwatch")


logger = setup_logger()


def install_diagnostics():
    """Install a handler for uncaught Python exceptions."""
    original_hook = sys.excepthook

    def exception_handler(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            original_hook(exc_type, exc_value, exc_traceback)
            return

        logger.error(
            "Uncaught application exception",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

        original_hook(exc_type, exc_value, exc_traceback)

    sys.excepthook = exception_handler

    logger.info(
        "GRIDWATCH AI diagnostics initialized at %s",
        datetime.now().isoformat(timespec="seconds"),
    )


def log_error(message, exception=None):
    """Log an application error with its traceback, if available."""
    if exception is not None:
        logger.error(
            message,
            exc_info=(
                type(exception),
                exception,
                exception.__traceback__,
            ),
        )
    else:
        logger.error(message)


def log_info(message):
    """Log an informational application event."""
    logger.info(message)