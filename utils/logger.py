# -*- coding: utf-8 -*-
"""
ETL Pipeline Structured Logging Module
-------------------------------------
Provides a highly readable, context-aware logger for the ETL framework.
Formats logs to cleanly identify the Operation, Vehicle Model, Part, and Machine,
ensuring strict separation between 3D, Checksheet, and Infrastructure/System logs.
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional


class StructuredLogger:
    """
    Thread-safe contextual logger wrapper that formats message payloads
    with machine, vehicle, part, and operation prefixes.
    
    Routes log records strictly to their target master loggers:
    - 3D Pipelines -> etl_3d_master.log
    - Checksheet Pipelines -> etl_checksheet_master.log
    - System / Infrastructure -> etl_system_master.log
    """

    def __init__(self, log_folder: str = "logs"):
        os.makedirs(log_folder, exist_ok=True)

        self.log_folder = log_folder
        self.context_handlers = {}

        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [PID:%(process)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Enterprise Logging Architecture Strategy:
        # Strict isolation between 3D, Checksheet, and System infrastructure logs.
        self.master_loggers = {
            "3D": self._create_master_logger(
                logger_name="ETL_3D_Master_Logger",
                file_name="etl_3d_master.log",
                formatter=formatter,
            ),
            "CHECKSHEET": self._create_master_logger(
                logger_name="ETL_Checksheet_Master_Logger",
                file_name="etl_checksheet_master.log",
                formatter=formatter,
            ),
            "SYSTEM": self._create_master_logger(
                logger_name="ETL_System_Master_Logger",
                file_name="etl_system_master.log",
                formatter=formatter,
            ),
            "UNKNOWN": self._create_master_logger(
                logger_name="ETL_Pipeline_Master_Logger",
                file_name="etl_pipeline_master.log",
                formatter=formatter,
            ),
        }

        self.logger = self.master_loggers["UNKNOWN"]

    def _create_master_logger(
        self,
        logger_name: str,
        file_name: str,
        formatter: logging.Formatter,
    ) -> logging.Logger:
        master_logger = logging.getLogger(logger_name)
        master_logger.setLevel(logging.INFO)
        master_logger.propagate = False

        if master_logger.handlers:
            master_logger.handlers.clear()

        # Existing rotation policy retained: 100MB per file, max 10 backup files
        file_handler = RotatingFileHandler(
            os.path.join(self.log_folder, file_name),
            maxBytes=100 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        master_logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        master_logger.addHandler(console_handler)

        return master_logger

    def _get_master_logger(
        self,
        pipeline_type: Optional[str] = None,
        category: Optional[str] = None,
    ) -> logging.Logger:
        """
        Resolves and returns the corresponding master logger instance.
        """
        pipeline_name = self._resolve_pipeline_type(pipeline_type, category)

        if pipeline_name == "3D":
            return self.master_loggers["3D"]

        if pipeline_name == "CHECKSHEET":
            return self.master_loggers["CHECKSHEET"]

        if pipeline_name == "SYSTEM":
            return self.master_loggers["SYSTEM"]

        if pipeline_name == "UNKNOWN":
            self.master_loggers["SYSTEM"].warning(
                "[LOGGER ROUTING WARNING] Missing pipeline_type detected. "
                "Log routed to etl_pipeline_master.log."
            )
        return self.master_loggers["UNKNOWN"]

    def _format_context(
        self,
        machine: Optional[str],
        model: Optional[str],
        part: Optional[str],
        op: Optional[str],
    ) -> str:
        mc_prefix = f"[MC: {machine or 'N/A'}]"
        model_prefix = f"[MODEL: {model or 'N/A'}]"
        part_prefix = f"[PART: {part or 'N/A'}]"
        op_prefix = f"[OP: {op or 'N/A'}]"
        return f"{mc_prefix} {model_prefix} {part_prefix} {op_prefix}"

    @staticmethod
    def _resolve_pipeline_type(
        pipeline_type: Optional[str] = None,
        category: Optional[str] = None,
    ) -> str:
        """
        Normalizes input parameters into canonical master logger categories:
        '3D', 'CHECKSHEET', or 'SYSTEM'.
        """
        value = category if category is not None else pipeline_type
        if value is None:
            return "UNKNOWN"

        normalized = str(value).strip().upper()
        if not normalized:
            return "UNKNOWN"

        # Explicit SYSTEM infrastructure resolution
        if normalized in {"SYSTEM", "SYS", "INFRA", "INFRASTRUCTURE", "DB", "STABILITY", "WATCHER"}:
            return "SYSTEM"

        # Explicit 3D pipeline resolution (3D, CMM, 3D_HD, 3D_BL, 3D_CR)
        if normalized in {"3D", "CMM", "3D_HD", "3D_BL", "3D_CR"}:
            return "3D"

        # Explicit Checksheet pipeline resolution
        if normalized == "CHECKSHEET" or normalized.startswith("CHECKSHEET_"):
            return "CHECKSHEET"

        return normalized

    def info(
        self,
        msg: str,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        category: Optional[str] = None,
        watch_id: Optional[str] = None,
        source_filename: Optional[str] = None,
        **kwargs: Any,
    ):
        """Logs informational messages with context."""
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        master_logger = self._get_master_logger(pipeline_type, category)
        master_logger.info(message)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
            pipeline_type=pipeline_type,
            category=category,
            watch_id=watch_id,
            source_filename=source_filename,
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
        pipeline_type: Optional[str] = None,
        category: Optional[str] = None,
        watch_id: Optional[str] = None,
        source_filename: Optional[str] = None,
        **kwargs: Any,
    ):
        """Logs warnings with context."""
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        master_logger = self._get_master_logger(pipeline_type, category)
        master_logger.warning(message)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
            pipeline_type=pipeline_type,
            category=category,
            watch_id=watch_id,
            source_filename=source_filename,
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
        exc_info: bool = False,
        pipeline_type: Optional[str] = None,
        category: Optional[str] = None,
        watch_id: Optional[str] = None,
        source_filename: Optional[str] = None,
        **kwargs: Any,
    ):
        """Logs error messages, optionally including exception tracebacks."""
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        master_logger = self._get_master_logger(pipeline_type, category)
        master_logger.error(message, exc_info=exc_info)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
            pipeline_type=pipeline_type,
            category=category,
            watch_id=watch_id,
            source_filename=source_filename,
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
        exc_info: bool = False,
        pipeline_type: Optional[str] = None,
        category: Optional[str] = None,
        watch_id: Optional[str] = None,
        source_filename: Optional[str] = None,
        **kwargs: Any,
    ):
        """Logs critical issues with context, optionally including tracebacks."""
        context = self._format_context(machine, model, part, op)
        message = f"{context} - {msg}"
        master_logger = self._get_master_logger(pipeline_type, category)
        master_logger.critical(message, exc_info=exc_info)
        ctx_logger = self._get_context_logger(
            machine,
            model,
            part,
            op,
            pipeline_type=pipeline_type,
            category=category,
            watch_id=watch_id,
            source_filename=source_filename,
        )
        if ctx_logger:
            ctx_logger.critical(message, exc_info=exc_info)

    def _get_context_logger(
        self,
        machine: Optional[str],
        model: Optional[str],
        part: Optional[str],
        op: Optional[str],
        pipeline_type: Optional[str] = None,
        category: Optional[str] = None,
        watch_id: Optional[str] = None,
        source_filename: Optional[str] = None,
    ):
        pipeline_name = self._resolve_pipeline_type(pipeline_type, category)

        # STRICT ARCHITECTURAL RULE:
        # System/Infrastructure logs write ONLY to etl_system_master.log
        if pipeline_name == "SYSTEM":
            return None

        if pipeline_name == "CHECKSHEET":
            # 1. If no workbook filename exists, write ONLY to etl_checksheet_master.log
            if not source_filename:
                return None

            # 2. Extract canonical workbook file stem
            source_stem = Path(source_filename).stem

            # 3. Clean watch_id for optional prefixing if not already inside source_stem
            raw_watch = watch_id or machine
            clean_watch_id = None
            if raw_watch:
                raw_str = str(raw_watch).strip()
                if raw_str.upper().startswith("CHECKSHEET_"):
                    clean_watch_id = raw_str[11:].strip()
                elif raw_str.upper() != "CHECKSHEET":
                    clean_watch_id = raw_str

            # 4. Derive filename_base cleanly without duplication
            if clean_watch_id and clean_watch_id not in source_stem:
                filename_base = f"{clean_watch_id}_{source_stem}"
            else:
                filename_base = source_stem

            # 5. Canonical context key tied strictly to the workbook file stem
            key = f"CHECKSHEET_{filename_base}"
        else:
            if not (machine and model and part and op):
                return None
            key = f"{pipeline_name}_{machine}_{model}_{part}_{op}"
            filename_base = key

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
                f"{filename_base}.log",
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
                f"===== LOG FILE CREATED AT "
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ====="
            )
        self.context_handlers[key] = ctx_logger
        return ctx_logger


# Global instance
LOGGER = StructuredLogger()

__all__ = ["LOGGER", "StructuredLogger"]