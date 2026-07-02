# -*- coding: utf-8 -*-
"""
ETL Pipeline Structured Logging Module
-------------------------------------
Provides a highly readable, context-aware logger for the ETL framework.
Formats logs to cleanly identify the Operation, Vehicle Model, Part, and Machine,
ensuring that troubleshooting, debugging, and root cause analysis are instantaneous.

Example Log Format:
[2026-07-02 17:10:00] [INFO] [MC: KTSF-0453] [MODEL: RC] [PART: HD-RH] [OP: OP010] - Message here
"""

import os
import logging
from logging.handlers import RotatingFileHandler


class StructuredLogger:
    """
    Thread-safe contextual logger wrapper that formats message payloads
    with machine, vehicle, part, and operation prefixes.
    """

    def __init__(self, log_folder: str = "logs"):
        """
        Initializes the logging directories and files.
        
        Args:
            log_folder (str): Directory where the structured logs will be saved.
        """
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, "etl_pipeline.log")

        self.logger = logging.getLogger("ETL_Structured_Logger")
        self.logger.setLevel(logging.INFO)

        # Clear existing handlers to prevent duplicate logs in case of hot reload
        if self.logger.handlers:
            self.logger.handlers.clear()

        # Format layout: Time, Level name, Process ID, Message
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [PID:%(process)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Rotate logs: Max size 10MB, keep 5 backup versions
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Stream handler for stdout console output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _format_context(self, machine: str, model: str, part: str, op: str) -> str:
        """
        Builds a structured contextual prefix for the log messages.
        If a context element is not provided, defaults to 'UNKNOWN'.
        """
        mc_prefix = f"[MC: {machine or 'N/A'}]"
        model_prefix = f"[MODEL: {model or 'N/A'}]"
        part_prefix = f"[PART: {part or 'N/A'}]"
        op_prefix = f"[OP: {op or 'N/A'}]"
        return f"{mc_prefix} {model_prefix} {part_prefix} {op_prefix}"

    def info(self, msg: str, machine: str = None, model: str = None, part: str = None, op: str = None):
        """Logs informational messages with context."""
        context = self._format_context(machine, model, part, op)
        self.logger.info(f"{context} - {msg}")

    def warning(self, msg: str, machine: str = None, model: str = None, part: str = None, op: str = None):
        """Logs warnings with context."""
        context = self._format_context(machine, model, part, op)
        self.logger.warning(f"{context} - {msg}")

    def error(self, msg: str, machine: str = None, model: str = None, part: str = None, op: str = None, exc_info=False):
        """
        Logs error messages, optionally including exception tracebacks.
        """
        context = self._format_context(machine, model, part, op)
        self.logger.error(f"{context} - {msg}", exc_info=exc_info)

    def critical(self, msg: str, machine: str = None, model: str = None, part: str = None, op: str = None, exc_info=False):
        """Logs critical issues with context, optionally including exception tracebacks."""
        context = self._format_context(machine, model, part, op)
        self.logger.critical(f"{context} - {msg}", exc_info=exc_info)


# Global instance to be imported and used by dispatcher, job_runner, steps, etc.
LOGGER = StructuredLogger()
