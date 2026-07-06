# -*- coding: utf-8 -*-
"""
Pipeline Status Manager
-----------------------
Professional, database-backed state machine for ETL monitoring.
"""

import os
import traceback as tb_module
from datetime import datetime
from enum import Enum
from typing import Optional, Any

from db.db_handler import DatabaseHandler
from utils.logger import LOGGER


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────


class PipelineStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class DLQCategory(str, Enum):
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    DB_TIMEOUT = "DB_TIMEOUT"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    PARSE_ERROR = "PARSE_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    TRANSFORM_ERROR = "TRANSFORM_ERROR"
    IO_ERROR = "IO_ERROR"
    UNKNOWN = "UNKNOWN"


# ─────────────────────────────────────────────────────────────────────────────
# StatusManager
# ─────────────────────────────────────────────────────────────────────────────


class StatusManager:
    TABLE_LOG = "fs3.etl_pipeline_log"
    TABLE_AUDIT = "fs3.etl_audit_log"
    TABLE_DLQ = "fs3.etl_dlq"

    def __init__(self):
        self._db = DatabaseHandler()

    def register(
        self,
        file_name: str,
        file_size: Optional[int] = None,
        machine_id: Optional[str] = None,
        vehicle_model: Optional[str] = None,
        parts_code: Optional[str] = None,
        op_no: Optional[str] = None,
        pid: Optional[int] = None,
    ) -> Optional[int]:
        sql = f"""
            INSERT INTO {self.TABLE_LOG}
                (file_name, file_size, machine_id, vehicle_model, parts_code, op_no, status, pid)
            VALUES (%s, %s, %s, %s, %s, %s, 'PENDING', %s)
            ON CONFLICT (file_name) WHERE (status IN ('PENDING', 'PROCESSING'))
            DO NOTHING
            RETURNING id;
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            file_name,
                            file_size,
                            machine_id,
                            vehicle_model,
                            parts_code,
                            op_no,
                            pid,
                        ),
                    )
                    result = cur.fetchone()

            if result is None:
                return None

            log_id = int(result[0])
            self._write_audit(
                level="INFO",
                message=f"[LOG_ID:{log_id}] File '{file_name}' registered.",
                pipeline_log_id=log_id,
                machine_id=machine_id,
                vehicle_model=vehicle_model,
                parts_code=parts_code,
                op_no=op_no,
                current_step="REGISTER",
            )
            return log_id
        except Exception as e:
            LOGGER.error(f"Failed to register file '{file_name}': {e}", exc_info=True)
            raise

    def update_file_size(self, log_id: int, file_size: int):
        sql = f"UPDATE {self.TABLE_LOG} SET file_size = %s WHERE id = %s;"
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (file_size, log_id))
        except Exception as e:
            LOGGER.warning(f"[LOG_ID:{log_id}] Size update failed: {e}")

    def reset_for_retry(
        self,
        log_id: int,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
    ):
        sql = f"UPDATE {self.TABLE_LOG} SET status='PROCESSING', current_step='DLQ_RETRY', error_message=NULL, updated_at=NOW() WHERE id=%s;"
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (log_id,))
            self._write_audit(
                "INFO",
                f"[LOG_ID:{log_id}] DLQ RETRY initiated.",
                log_id,
                machine,
                model,
                part,
                op,
                "DLQ_RETRY",
            )
        except Exception as e:
            LOGGER.warning(f"[LOG_ID:{log_id}] Reset failed: {e}")

    def set_processing(
        self,
        log_id: int,
        step_name: str,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
    ):
        self._update_log(log_id, PipelineStatus.PROCESSING, current_step=step_name)
        self._write_audit(
            "INFO",
            f"[LOG_ID:{log_id}] Step [{step_name}] started.",
            log_id,
            machine,
            model,
            part,
            op,
            step_name,
        )

    def set_completed(
        self,
        log_id: int,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
    ):
        self._update_log(
            log_id,
            PipelineStatus.COMPLETED,
            completed_at=datetime.utcnow(),
            compute_duration=True,
        )
        self._write_audit(
            "INFO",
            f"[LOG_ID:{log_id}] COMPLETED.",
            log_id,
            machine,
            model,
            part,
            op,
            "FINALIZE",
        )

    def set_failed(
        self,
        log_id: int,
        exc: Optional[Exception] = None,
        dlq_category: DLQCategory = DLQCategory.UNKNOWN,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
        failed_step: Optional[str] = None,
        file_path: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        error_message = str(exc) if exc else "Unknown failure"
        traceback_str = tb_module.format_exc() if exc else None

        self._update_log(log_id, PipelineStatus.FAILED, error_message=error_message)
        self._write_audit(
            "ERROR",
            f"[LOG_ID:{log_id}] FAILED at {failed_step}: {error_message}",
            log_id,
            machine,
            model,
            part,
            op,
            failed_step,
            traceback_str,
        )

        if file_path and file_name:
            self._push_to_dlq(
                log_id,
                file_name,
                file_path,
                machine,
                model,
                part,
                op,
                failed_step or "UNKNOWN",
                dlq_category.value,
                error_message,
                traceback_str,
            )

    def write_warning(
        self,
        message: str,
        log_id: Optional[int] = None,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
        step: Optional[str] = None,
    ):
        self._write_audit("WARNING", message, log_id, machine, model, part, op, step)

    def write_critical(
        self,
        message: str,
        log_id: Optional[int] = None,
        machine: Optional[str] = None,
        model: Optional[str] = None,
        part: Optional[str] = None,
        op: Optional[str] = None,
        step: Optional[str] = None,
        exc: Optional[Exception] = None,
    ):
        traceback_str = tb_module.format_exc() if exc else None
        self._write_audit(
            "CRITICAL", message, log_id, machine, model, part, op, step, traceback_str
        )

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _update_log(
        self,
        log_id: int,
        status: PipelineStatus,
        current_step: Optional[str] = None,
        error_message: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        compute_duration: bool = False,
    ):
        fields = ["status = %s", "updated_at = NOW()"]
        values: list[Any] = [status.value]

        if current_step:
            fields.append("current_step = %s")
            values.append(current_step)
        if error_message:
            fields.append("error_message = %s")
            values.append(error_message[:2000])
        if completed_at:
            fields.append("completed_at = %s")
            values.append(completed_at)
        if compute_duration:
            fields.append("processing_sec = EXTRACT(EPOCH FROM (NOW() - started_at))")

        values.append(log_id)
        sql = f"UPDATE {self.TABLE_LOG} SET {', '.join(fields)} WHERE id = %s;"
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, values)
        except Exception as e:
            LOGGER.error(f"[LOG_ID:{log_id}] Update failed: {e}")

    def _write_audit(
        self,
        level: str,
        message: str,
        pipeline_log_id: Optional[int] = None,
        machine_id: Optional[str] = None,
        vehicle_model: Optional[str] = None,
        parts_code: Optional[str] = None,
        op_no: Optional[str] = None,
        current_step: Optional[str] = None,
        traceback: Optional[str] = None,
        pid: Optional[int] = None,
    ):
        sql = f"""
            INSERT INTO {self.TABLE_AUDIT}
                (level, pipeline_log_id, machine_id, vehicle_model, parts_code, op_no, current_step, message, traceback, pid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            level,
                            pipeline_log_id,
                            machine_id,
                            vehicle_model,
                            parts_code,
                            op_no,
                            current_step,
                            message[:4000],  # Guard against very long messages
                            traceback,
                            pid or os.getpid(),
                        ),
                    )
        except Exception as e:
            LOGGER.warning(f"Audit write failed: {e}")

    def _push_to_dlq(
        self,
        pipeline_log_id: int,
        file_name: str,
        file_path: str,
        machine_id: Optional[str],
        vehicle_model: Optional[str],
        parts_code: Optional[str],
        op_no: Optional[str],
        failed_step: Optional[str],
        error_category: str,
        error_message: str,
        traceback: Optional[str],
    ):
        sql = f"""
            INSERT INTO {self.TABLE_DLQ}
                (pipeline_log_id, file_name, file_path, machine_id, vehicle_model, parts_code, op_no, failed_step, error_category, error_message, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            pipeline_log_id,
                            file_name,
                            file_path,
                            machine_id,
                            vehicle_model,
                            parts_code,
                            op_no,
                            failed_step or "UNKNOWN",
                            error_category,
                            error_message[:2000],
                            traceback,
                        ),
                    )
        except Exception as e:
            LOGGER.warning(f"[LOG_ID:{pipeline_log_id}] DLQ push failed: {e}")
