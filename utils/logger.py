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
from typing import Optional
from datetime import datetime


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
        log_file = os.path.join(log_folder, "etl_pipeline_master.log")

        self.logger = logging.getLogger("ETL_Structured_Logger")
        self.logger.setLevel(logging.INFO)

        self.log_folder = log_folder
        self.context_handlers = {}
        # Clear existing handlers to prevent duplicate logs in case of hot reload
        if self.logger.handlers:
            self.logger.handlers.clear()

        # Format layout: Time, Level name, Process ID, Message
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [PID:%(process)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Rotate logs: Max size 50MB, keep 10 backup versions
        file_handler = RotatingFileHandler(
            log_file, maxBytes=100 * 1024 * 1024, backupCount=10, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Stream handler for stdout console output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _format_context(
        self, machine: str | None, model: str | None, part: str | None, op: str | None
    ) -> str:
        """
        Builds a structured contextual prefix for the log messages.
        If a context element is not provided, defaults to 'UNKNOWN'.
        """
        mc_prefix = f"[MC: {machine or 'N/A'}]"
        model_prefix = f"[MODEL: {model or 'N/A'}]"
        part_prefix = f"[PART: {part or 'N/A'}]"
        op_prefix = f"[OP: {op or 'N/A'}]"
        return f"{mc_prefix} {model_prefix} {part_prefix} {op_prefix}"

    def info(
        self,
        msg: str,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
    ):
        """Logs informational messages with context."""
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        self.logger.info(message)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
        )
        if ctx_logger:
            ctx_logger.info(message)

    def warning(
        self,
        msg: str,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
    ):
        """Logs warnings with context."""
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        self.logger.warning(message)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
        )
        if ctx_logger:
            ctx_logger.warning(message)

    def error(
        self,
        msg: str,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
        exc_info=False,
    ):
        """
        Logs error messages, optionally including exception tracebacks.
        """
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        self.logger.error(message, exc_info=exc_info)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
        )
        if ctx_logger:
            ctx_logger.error(message, exc_info=exc_info)

    def critical(
        self,
        msg: str,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
        exc_info=False,
    ):
        """Logs critical issues with context, optionally including exception tracebacks."""
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        self.logger.critical(message, exc_info=exc_info)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
        )
        if ctx_logger:
            ctx_logger.critical(message, exc_info=exc_info)

    def _get_context_logger(
        self,
        machine: Optional[str],
        model: Optional[str],
        part: Optional[str],
        op: Optional[str],
    ):
        if not (machine and model and part and op):
            return None

        key = f"{machine}_{model}_{part}_{op}"

        if key in self.context_handlers:
            return self.context_handlers[key]

        logger_name = f"CTX_{key}"
        ctx_logger = logging.getLogger(logger_name)
        ctx_logger.setLevel(logging.INFO)
        ctx_logger.propagate = False

        if not ctx_logger.handlers:
            formatter = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] [PID:%(process)d] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            log_file = os.path.join(
                self.log_folder,
                f"3D_{machine}_{model}_{part}_{op}.log",
            )

            handler = RotatingFileHandler(
                log_file,
                maxBytes=50 * 1024 * 1024,
                backupCount=50,
                encoding="utf-8",
            )

            def namer(default_name):
                directory = os.path.dirname(default_name)
                base = os.path.basename(log_file)

                try:
                    suffix = int(default_name.rsplit(".", 1)[-1]) + 1
                except Exception:
                    suffix = 2

                base_name = os.path.splitext(base)[0]

                return os.path.join(
                    directory,
                    f"{base_name}_{suffix}.log",
                )

            handler.namer = namer
            handler.setFormatter(formatter)

            ctx_logger.addHandler(handler)
            ctx_logger.info(
                f"===== LOG FILE CREATED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ====="
            )
        self.context_handlers[key] = ctx_logger
        return ctx_logger


# Global instance to be imported and used by dispatcher, job_runner, steps, etc.
LOGGER = StructuredLogger()
