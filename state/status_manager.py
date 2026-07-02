# -*- coding: utf-8 -*-
"""
Pipeline Status Manager
-----------------------
Replaces the old symbol-prefix file renaming system (♡ → ★ → ●) with a
professional, database-backed state machine.

Every file processed by the pipeline is registered here with a status that
transitions through: PENDING → PROCESSING → COMPLETED or FAILED.

This module writes to THREE tables:
  1. fs3.etl_pipeline_log  — Per-file pipeline state (the Live Queue in the dashboard).
  2. fs3.etl_audit_log     — Structured event log (the Audit Logs pillar in the dashboard).
  3. fs3.etl_dlq           — Dead Letter Queue for failed files (the DLQ pillar).

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 1: Atomic Concurrency Lock (v2)
═══════════════════════════════════════════════════════════════
  The old implementation used a "Check-then-Insert" pattern (TOCTOU race):

    OLD CODE (BROKEN):
      if _is_file_processed(file_name):  ← Thread A checks here
          return                          ← Thread B also passes this check
      register(file_name)                ← Both threads INSERT → duplicate processing!

  The new register() uses a PostgreSQL partial unique index to make the
  "claim" operation ATOMIC. The SQL is:

    INSERT INTO etl_pipeline_log (...) VALUES (...)
    ON CONFLICT (file_name) WHERE (status IN ('PENDING', 'PROCESSING'))
    DO NOTHING
    RETURNING id;

  If two threads race to claim the same file, ONLY ONE gets an id back.
  The other gets NULL and immediately aborts. The database enforces the lock,
  not application code.

  REQUIRED SCHEMA INDEX (run once — included in schema_mission_control.sql):
    CREATE UNIQUE INDEX idx_pipeline_active_file
        ON fs3.etl_pipeline_log(file_name)
        WHERE (status IN ('PENDING', 'PROCESSING'));

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 6: DLQ Retry Support
═══════════════════════════════════════════════════════════════
  Added reset_for_retry() — called by the DLQ retry watcher to reactivate
  a FAILED pipeline log entry without creating a duplicate row.

See schema_mission_control.sql for all CREATE TABLE statements.
"""

import os
import traceback as tb_module
from datetime import datetime
from enum import Enum
from typing import Optional

from ETL_Pipeline.db.db_handler import DatabaseHandler
from ETL_Pipeline.utils.logger import LOGGER


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class PipelineStatus(str, Enum):
    """
    The lifecycle states a file can be in.
    Using str mixin so the value can be stored directly as a VARCHAR in PostgreSQL.
    """
    PENDING    = "PENDING"     # File detected, not yet processed
    PROCESSING = "PROCESSING"  # Actively being worked on by the pipeline
    COMPLETED  = "COMPLETED"   # Successfully inserted into master/transaction tables
    FAILED     = "FAILED"      # Processing failed; file is in the NG folder


class DLQCategory(str, Enum):
    """
    Categorizes why a file failed — powers the DLQ filter in the dashboard.
    New categories can be added here and in the schema CHECK constraint.
    """
    SCHEMA_DRIFT       = "SCHEMA_DRIFT"
    DB_TIMEOUT         = "DB_TIMEOUT"
    VALIDATION_FAILED  = "VALIDATION_FAILED"
    FILE_NOT_FOUND     = "FILE_NOT_FOUND"
    PARSE_ERROR        = "PARSE_ERROR"
    UNKNOWN            = "UNKNOWN"


# ─────────────────────────────────────────────────────────────────────────────
# StatusManager
# ─────────────────────────────────────────────────────────────────────────────

class StatusManager:
    """
    Central controller for all pipeline state transitions and audit writes.

    Design principle: Every state change — no matter how minor — is recorded.
    The dashboard reads these records directly. What gets written here is exactly
    what an operator sees on screen.
    """

    TABLE_LOG   = "fs3.etl_pipeline_log"
    TABLE_AUDIT = "fs3.etl_audit_log"
    TABLE_DLQ   = "fs3.etl_dlq"

    def __init__(self):
        self._db = DatabaseHandler()

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC: Pipeline lifecycle methods
    # ─────────────────────────────────────────────────────────────────────────

    def register(
        self,
        file_name: str,
        file_size: int = None,
        machine_id: str = None,
        vehicle_model: str = None,
        parts_code: str = None,
        op_no: str = None,
        pid: int = None
    ) -> Optional[int]:
        """
        Atomically claims a file for processing using a PostgreSQL server-side lock.

        IMPLEMENTATION DETAIL — WHY THIS IS THREAD-SAFE:
        The SQL uses ON CONFLICT with a partial unique index on file_name WHERE
        status IN ('PENDING', 'PROCESSING'). If two worker threads call this
        simultaneously for the same file:
          - Thread A's INSERT succeeds → gets a log_id → proceeds to process
          - Thread B's INSERT hits the conflict → gets NULL → returns None → skips

        This is the ONLY correct way to prevent duplicate processing in a
        multi-threaded system. Application-level checks (if/else) create a
        TOCTOU race condition.

        Args:
            file_name (str): The bare filename (not full path) being processed.
            file_size (int): File size in bytes at the time of detection.
            machine_id (str): The CMM machine that produced this file.
            vehicle_model (str): Matched vehicle model code.
            parts_code (str): Matched parts code.
            op_no (str): Matched operation number.
            pid (int): Worker process ID for zombie-lock debugging.

        Returns:
            int: The new log entry ID if this thread won the claim.
            None: If another thread/run already holds an active claim. CALLER MUST SKIP.

        REQUIRES this index (included in schema_mission_control.sql):
            CREATE UNIQUE INDEX idx_pipeline_active_file
                ON fs3.etl_pipeline_log(file_name)
                WHERE (status IN ('PENDING', 'PROCESSING'));
        """
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
                    cur.execute(sql, (
                        file_name, file_size, machine_id, vehicle_model,
                        parts_code, op_no, pid
                    ))
                    result = cur.fetchone()

            if result is None:
                # Server-side conflict: another worker already holds an ACTIVE claim
                # on this file (PENDING or PROCESSING status). This is NOT an error.
                LOGGER.info(
                    f"File '{file_name}' already claimed by an active worker "
                    f"(PENDING/PROCESSING). Skipping — no duplicate processing.",
                    machine=machine_id, model=vehicle_model, part=parts_code, op=op_no
                )
                return None

            log_id = result[0]
            LOGGER.info(
                f"[LOG_ID:{log_id}] File '{file_name}' atomically claimed as PENDING.",
                machine=machine_id, model=vehicle_model, part=parts_code, op=op_no
            )
            self._write_audit(
                level="INFO",
                message=(
                    f"[LOG_ID:{log_id}] File '{file_name}' "
                    f"(size: {file_size} bytes) registered — pipeline starting."
                ),
                pipeline_log_id=log_id,
                machine_id=machine_id, vehicle_model=vehicle_model,
                parts_code=parts_code, op_no=op_no, current_step="REGISTER"
            )
            return log_id

        except Exception as e:
            LOGGER.error(
                f"Failed to register file '{file_name}' in pipeline log. "
                f"Cannot proceed without a log ID — pipeline aborted. Error: {e}",
                machine=machine_id, model=vehicle_model, part=parts_code, op=op_no,
                exc_info=True
            )
            raise

    def update_file_size(self, log_id: int, file_size: int):
        """
        Updates the registered file size in the state log.
        Called once the file stabilizes during the COPY/Staging phase.
        """
        sql = f"UPDATE {self.TABLE_LOG} SET file_size = %s WHERE id = %s;"
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (file_size, log_id))
        except Exception as e:
            LOGGER.warning(f"[LOG_ID:{log_id}] Failed to update stable file size: {e}")

    def reset_for_retry(
        self,
        log_id: int,
        machine: str = None,
        model: str = None,
        part: str = None,
        op: str = None
    ):
        """
        ═══════════════════════════════════════════════════════════
          ELITE FIX — SCENARIO 6: DLQ Retry Reset
        ═══════════════════════════════════════════════════════════
        Resets an existing FAILED pipeline log entry back to PROCESSING
        for a DLQ-initiated retry. This bypasses the normal register()
        step so we reuse the original log_id instead of creating a
        duplicate entry.

        Called ONLY by job_runner._run_pipeline_retry_safe() when a
        DLQ RETRYING entry is submitted back into the executor.

        Args:
            log_id (int): The existing pipeline log entry ID to reactivate.
        """
        sql = f"""
            UPDATE {self.TABLE_LOG}
            SET status       = 'PROCESSING',
                current_step = 'DLQ_RETRY',
                error_message = NULL,
                updated_at   = NOW()
            WHERE id = %s;
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (log_id,))
            LOGGER.info(
                f"[LOG_ID:{log_id}] DLQ RETRY: Existing log entry reset to PROCESSING. "
                f"Re-running pipeline for this file.",
                machine=machine, model=model, part=part, op=op
            )
            self._write_audit(
                level="INFO",
                message=(
                    f"[LOG_ID:{log_id}] DLQ RETRY initiated — pipeline entry reset "
                    f"from FAILED to PROCESSING. File has been moved back to source folder."
                ),
                pipeline_log_id=log_id,
                machine_id=machine, vehicle_model=model,
                parts_code=part, op_no=op, current_step="DLQ_RETRY"
            )
        except Exception as e:
            LOGGER.warning(
                f"[LOG_ID:{log_id}] Could not reset pipeline entry for DLQ retry: {e}"
            )

    def set_processing(
        self,
        log_id: int,
        step_name: str,
        machine: str = None,
        model: str = None,
        part: str = None,
        op: str = None
    ):
        """
        Transitions a log entry to PROCESSING and records the active step name.
        Called at the beginning of every atomic step in the pipeline.

        Args:
            log_id (int): The pipeline run ID from register().
            step_name (str): Human-readable step name (e.g. 'COPY', 'TRANSFORM', 'LOAD').
        """
        self._update_log(log_id, PipelineStatus.PROCESSING, current_step=step_name)
        LOGGER.info(
            f"[LOG_ID:{log_id}] Step [{step_name}] → PROCESSING.",
            machine=machine, model=model, part=part, op=op
        )
        self._write_audit(
            level="INFO",
            message=f"[LOG_ID:{log_id}] Step [{step_name}] started.",
            pipeline_log_id=log_id,
            machine_id=machine, vehicle_model=model,
            parts_code=part, op_no=op, current_step=step_name
        )

    def set_completed(
        self,
        log_id: int,
        machine: str = None,
        model: str = None,
        part: str = None,
        op: str = None
    ):
        """
        Marks a pipeline run as COMPLETED and computes the total processing time.
        Called after the FINALIZE step succeeds and the file is in its final folder.
        """
        self._update_log(
            log_id, PipelineStatus.COMPLETED,
            completed_at=datetime.utcnow(),
            compute_duration=True
        )
        LOGGER.info(
            f"[LOG_ID:{log_id}] Pipeline run → COMPLETED successfully.",
            machine=machine, model=model, part=part, op=op
        )
        self._write_audit(
            level="INFO",
            message=(
                f"[LOG_ID:{log_id}] Pipeline run COMPLETED — "
                f"file committed to master and transaction tables."
            ),
            pipeline_log_id=log_id,
            machine_id=machine, vehicle_model=model,
            parts_code=part, op_no=op, current_step="FINALIZE"
        )

    def set_failed(
        self,
        log_id: int,
        error_message: str,
        machine: str = None,
        model: str = None,
        part: str = None,
        op: str = None,
        failed_step: str = None,
        file_path: str = None,
        file_name: str = None,
        exc: Exception = None,
        dlq_category: DLQCategory = DLQCategory.UNKNOWN
    ):
        """
        Marks a pipeline run as FAILED, writes an ERROR audit entry, and pushes
        the file into the Dead Letter Queue for operator review.

        Args:
            log_id (int): Pipeline run ID.
            error_message (str): Human-readable description of the failure.
            failed_step (str): Which step failed (COPY, TRANSFORM, LOAD, FINALIZE).
            file_path (str): Current location of the failed file (in _NG folder).
            file_name (str): Original filename.
            exc (Exception): The caught exception — used to capture full traceback.
            dlq_category (DLQCategory): Category for DLQ dashboard filtering.
        """
        traceback_str = tb_module.format_exc() if exc else None

        self._update_log(log_id, PipelineStatus.FAILED, error_message=error_message)
        LOGGER.error(
            f"[LOG_ID:{log_id}] Pipeline run → FAILED at [{failed_step or 'UNKNOWN'}]. "
            f"Reason: {error_message}",
            machine=machine, model=model, part=part, op=op
        )

        # Write ERROR entry to audit log
        self._write_audit(
            level="ERROR",
            message=(
                f"[LOG_ID:{log_id}] FAILED at [{failed_step or 'UNKNOWN'}]. "
                f"Reason: {error_message}"
            ),
            pipeline_log_id=log_id,
            machine_id=machine, vehicle_model=model,
            parts_code=part, op_no=op,
            current_step=failed_step,
            traceback=traceback_str
        )

        # Push to DLQ
        if file_path and file_name:
            self._push_to_dlq(
                pipeline_log_id=log_id,
                file_name=file_name,
                file_path=file_path,
                machine_id=machine,
                vehicle_model=model,
                parts_code=part,
                op_no=op,
                failed_step=failed_step,
                error_category=dlq_category.value,
                error_message=error_message,
                traceback=traceback_str
            )

    def write_warning(
        self,
        message: str,
        log_id: int = None,
        machine: str = None,
        model: str = None,
        part: str = None,
        op: str = None,
        step: str = None
    ):
        """
        Writes a WARNING audit entry without changing the pipeline status.
        Use this for non-fatal issues (e.g. skipped rows, partial imports).
        """
        LOGGER.warning(message, machine=machine, model=model, part=part, op=op)
        self._write_audit(
            level="WARNING",
            message=message,
            pipeline_log_id=log_id,
            machine_id=machine, vehicle_model=model,
            parts_code=part, op_no=op, current_step=step
        )

    def write_critical(
        self,
        message: str,
        log_id: int = None,
        machine: str = None,
        model: str = None,
        part: str = None,
        op: str = None,
        step: str = None,
        exc: Exception = None
    ):
        """
        Writes a CRITICAL audit entry. Use for system-level failures (startup, DB down).
        These appear highlighted in the dashboard's Audit Logs view.
        """
        traceback_str = tb_module.format_exc() if exc else None
        LOGGER.critical(message, machine=machine, model=model, part=part, op=op, exc_info=bool(exc))
        self._write_audit(
            level="CRITICAL",
            message=message,
            pipeline_log_id=log_id,
            machine_id=machine, vehicle_model=model,
            parts_code=part, op_no=op, current_step=step,
            traceback=traceback_str
        )

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE: Database write helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _update_log(
        self,
        log_id: int,
        status: PipelineStatus,
        current_step: str = None,
        error_message: str = None,
        completed_at: datetime = None,
        compute_duration: bool = False
    ):
        """
        Applies a status update to etl_pipeline_log. Also computes processing_sec
        from started_at when compute_duration=True (called on COMPLETED transitions).
        """
        fields = ["status = %s", "updated_at = NOW()"]
        values: list = [status.value]

        if current_step is not None:
            fields.append("current_step = %s")
            values.append(current_step)
        if error_message is not None:
            fields.append("error_message = %s")
            values.append(error_message[:2000])  # Guard against runaway error strings
        if completed_at is not None:
            fields.append("completed_at = %s")
            values.append(completed_at)
        if compute_duration:
            # Store elapsed seconds in the table for the KPI avg-processing-time query
            fields.append("processing_sec = EXTRACT(EPOCH FROM (NOW() - started_at))")

        values.append(log_id)
        sql = f"UPDATE {self.TABLE_LOG} SET {', '.join(fields)} WHERE id = %s;"

        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, values)
        except Exception as e:
            # Do NOT re-raise — a status update failure must not crash the pipeline
            LOGGER.error(
                f"[LOG_ID:{log_id}] Failed to update pipeline log status to '{status}': {e}",
                exc_info=True
            )

    def _write_audit(
        self,
        level: str,
        message: str,
        pipeline_log_id: int = None,
        machine_id: str = None,
        vehicle_model: str = None,
        parts_code: str = None,
        op_no: str = None,
        current_step: str = None,
        traceback: str = None
    ):
        """
        Inserts one row into etl_audit_log.
        Silently fails — an audit write failure must never crash the main pipeline.
        """
        sql = f"""
            INSERT INTO {self.TABLE_AUDIT}
                (level, pipeline_log_id, machine_id, vehicle_model, parts_code,
                 op_no, current_step, message, traceback, pid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        level, pipeline_log_id, machine_id, vehicle_model,
                        parts_code, op_no, current_step,
                        message[:4000],  # Guard against very long messages
                        traceback,
                        os.getpid()
                    ))
        except Exception as e:
            # Fallback: write to file logger only — don't propagate
            LOGGER.warning(f"Audit log write failed (non-critical): {e}")

    def _push_to_dlq(
        self,
        pipeline_log_id: int,
        file_name: str,
        file_path: str,
        machine_id: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        failed_step: str,
        error_category: str,
        error_message: str,
        traceback: Optional[str]
    ):
        """
        Inserts a failed file into etl_dlq so it appears in the DLQ pillar
        of the Mission Control Dashboard for operator review.
        """
        sql = f"""
            INSERT INTO {self.TABLE_DLQ}
                (pipeline_log_id, file_name, file_path, machine_id, vehicle_model,
                 parts_code, op_no, failed_step, error_category, error_message, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        pipeline_log_id, file_name, file_path, machine_id,
                        vehicle_model, parts_code, op_no, failed_step,
                        error_category, error_message[:2000] if error_message else None,
                        traceback
                    ))
            LOGGER.info(
                f"[LOG_ID:{pipeline_log_id}] File '{file_name}' pushed to DLQ "
                f"[{error_category}] for operator review."
            )
        except Exception as e:
            LOGGER.warning(f"[LOG_ID:{pipeline_log_id}] Failed to push file to DLQ: {e}")
