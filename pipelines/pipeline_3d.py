# -*- coding: utf-8 -*-
"""
3D Measurement Pipeline — Elite Edition
-----------------------------------------
Defines the exact sequence of steps for processing 3D HD and 3D BL files.

Think of this as the "Assembly Line Order" — it connects the step modules in the
correct sequence and ensures proper state transitions via the StatusManager.

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 3: Single Atomic Transaction Wrapper
═══════════════════════════════════════════════════════════════
  The LOAD step is now wrapped in a single DatabaseHandler connection context.
  This means exactly ONE connection, ONE transaction, and ONE commit/rollback
  for the entire file batch.

  If ANY row fails inside load_step.run():
    → Exception is raised
    → with self._db.get_connection() as conn: __exit__ detects exc_type is not None
    → conn.rollback() is called automatically
    → ALL staging + master + transaction writes for this file are undone
    → NO GHOST MASTER RECORDS. NO ORPHANED DATA.

  If ALL rows succeed:
    → load_step.run() returns normally
    → with block exits cleanly → conn.commit() is called automatically
    → ALL writes land in the database together, atomically

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 1: Atomic File Claim (register)
═══════════════════════════════════════════════════════════════
  register() now returns None if another thread has already claimed the file.
  Pipeline.run() checks for None and skips immediately — no duplicate processing.

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 6: DLQ Retry Path
═══════════════════════════════════════════════════════════════
  run() accepts an optional existing_log_id parameter. When set (DLQ retry path),
  the registration step is bypassed and the existing log entry is reused via
  status_manager.reset_for_retry(). This prevents duplicate pipeline_log rows
  while fully reprocessing the file.

All context (model, part, op, parser settings) flows in via 'part_config' from the Dispatcher.
"""

import os
from pathlib import Path

from db.db_handler import DatabaseHandler
from steps.copy import CopyStep
from steps.transform import TransformStep
from steps.load import LoadStep
from steps.finalize import FinalizeStep
from state.status_manager import StatusManager
from utils.logger import LOGGER


class Pipeline3D:
    """
    Orchestrates the full Copy → Transform → Load → Finalize lifecycle
    for a single 3D measurement file.

    The LOAD step is wrapped in a single atomic DB transaction. All other
    steps use their own connections (stateless, retryable operations).
    """

    def __init__(self, processing_folders: dict):
        """
        Args:
            processing_folders (dict): From machines.json — contains paths for
                                       'processing', 'completed', and 'ng' folders.
        """
        self._status = StatusManager()
        self._db = DatabaseHandler()  # ← Shared Singleton pool for the LOAD transaction
        self._copy_step = CopyStep(processing_dir=processing_folders["processing"])
        self._transform_step = TransformStep()
        self._finalize_step = FinalizeStep(
            base_completed_dir=processing_folders["completed"],
            base_ng_dir=processing_folders["ng"],
        )

    def run(
        self,
        source_path: Path,
        part_config: dict,
        machine_config: dict,
        existing_log_id: int = None,
    ):
        """
        Executes the full pipeline for one file.

        Args:
            source_path (Path): The raw incoming file from the CMM source folder.
            part_config (dict): Matched part/model config block from parts_3d.json.
            machine_config (dict): Machine config block from machines.json.
            existing_log_id (int): If provided, skip registration and reuse this log entry.
                                   Used exclusively by the DLQ retry watcher (Scenario 6).
                                   If None (normal flow), register() atomically claims the file.
        """
        machine_id = machine_config.get("machine_id", "UNKNOWN")
        vehicle_model = part_config.get("vehicle_model")
        parts_code = part_config.get("parts_code")
        op_no = part_config.get("op_no")
        pipeline_type = machine_config.get("pipeline_type", "3D_HD")
        context = dict(
            machine=machine_id, model=vehicle_model, part=parts_code, op=op_no
        )

        processing_path = None
        log_id = None

        # ── REGISTRATION / CLAIM ──────────────────────────────────────────────
        if existing_log_id is not None:
            # ─── DLQ Retry Path ───────────────────────────────────────────────
            # An operator clicked "Retry" in the dashboard. The file has been moved
            # back to the source folder and its pipeline_log entry ID is known.
            # Skip register() — reuse the existing log_id to avoid duplicate rows.
            log_id = existing_log_id
            self._status.reset_for_retry(log_id, **context)
            LOGGER.info(
                f"[LOG_ID:{log_id}] DLQ RETRY: Reusing existing log entry for "
                f"'{source_path.name}'. Pipeline starting.",
                **context,
            )

        else:
            # ─── Normal New-File Path ─────────────────────────────────────────
            # Atomically claim the file via a PostgreSQL partial-index conflict guard.
            # If register() returns None, another thread beat us to this file.
            try:
                try:
                    init_size = source_path.stat().st_size
                except Exception:
                    init_size = 0

                log_id = self._status.register(
                    file_name=source_path.name,
                    file_size=init_size,
                    machine_id=machine_id,
                    vehicle_model=vehicle_model,
                    parts_code=parts_code,
                    op_no=op_no,
                    pid=os.getpid(),
                )
            except Exception as e:
                LOGGER.error(
                    f"3D PIPELINE: Could not register '{source_path.name}' in status log. "
                    f"Aborting to avoid untracked processing. Error: {e}",
                    exc_info=True,
                    **context,
                )
                return

            if log_id is None:
                # Another thread already owns this file (atomic DB lock said so).
                # This is NOT an error — just skip silently.
                return

        # ── STEP 1: COPY (Staging) ────────────────────────────────────────────
        try:
            self._status.set_processing(log_id, "COPY", **context)
            processing_path = self._copy_step.run(
                source_path=source_path,
                machine_id=machine_id,
                vehicle_model=vehicle_model,
                parts_code=parts_code,
                op_no=op_no,
                stable_wait_sec=machine_config.get("stable_wait_sec", 2.0),
                stable_required_count=machine_config.get("stable_required_count", 3),
            )

            # Update DB with the stable, finalized file size after writing to _processing
            try:
                stable_size = processing_path.stat().st_size
                self._status.update_file_size(log_id, stable_size)
            except Exception as size_err:
                LOGGER.warning(
                    f"[LOG_ID:{log_id}] Could not retrieve stable file size: {size_err}",
                    **context,
                )

        except FileExistsError:
            # Duplicate file — not a crash, clean skip with log
            self._status.set_failed(
                log_id, "Duplicate file detected — already processed.", **context
            )
            return
        except Exception as e:
            self._status.set_failed(log_id, f"COPY STEP failed: {e}", **context)
            return

        # ── STEP 2: TRANSFORM ─────────────────────────────────────────────────
        try:
            self._status.set_processing(log_id, "TRANSFORM", **context)
            rows = self._transform_step.run(
                file_path=processing_path,
                part_config=part_config,
                machine_id=machine_id,
                log_id=log_id,
            )
        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] 3D PIPELINE: TRANSFORM STEP failed for "
                f"'{processing_path.name}'. File will be moved to NG. Error: {e}",
                exc_info=True,
                **context,
            )
            self._status.set_failed(log_id, f"TRANSFORM STEP failed: {e}", **context)
            self._finalize_step.run_failure(
                source_path,
                processing_path,
                machine_id,
                vehicle_model,
                parts_code,
                op_no,
                log_id,
                pipeline_type,
            )
            return

        # ── STEP 3: LOAD — Single Atomic Transaction ──────────────────────────
        #
        #  ┌─────────────────────────────────────────────────────────────────┐
        #  │  TRANSACTION BOUNDARY                                           │
        #  │                                                                 │
        #  │  with self._db.get_connection() as conn:                        │
        #  │      load_step.run(..., conn=conn)                              │
        #  │          ├── _insert_staging(cur, ...)  ─── no commit          │
        #  │          ├── _upsert_master(cur, ...)   ─── no commit          │
        #  │          └── _insert_transaction(cur,...) ─ no commit          │
        #  │      ↓                                                          │
        #  │  __exit__(exc_type=None)  → conn.COMMIT()  ← success path      │
        #  │  __exit__(exc_type=Error) → conn.ROLLBACK() ← failure path     │
        #  └─────────────────────────────────────────────────────────────────┘
        #
        try:
            self._status.set_processing(log_id, "LOAD", **context)
            load_step = LoadStep(pipeline_type=pipeline_type)

            # === SINGLE ATOMIC TRANSACTION — START ===========================
            with self._db.get_connection() as conn:
                committed = load_step.run(
                    rows=rows,
                    part_config=part_config,
                    machine_id=machine_id,
                    log_id=log_id,
                    conn=conn,  # ← All 3 stages share this one connection
                )
            # === SINGLE ATOMIC TRANSACTION — END ==============================
            # If we reach here: conn.commit() was called. All rows are permanent.
            # If load_step.run() raised: conn.rollback() was called. Zero rows saved.

            if committed == 0:
                raise ValueError(
                    f"No rows were committed to '{pipeline_type}' tables. "
                    f"File contained no valid measurement rows after transform."
                )

        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] 3D PIPELINE: LOAD STEP failed for "
                f"'{processing_path.name}'. Full transaction rolled back. "
                f"No data saved for this file. Error: {e}",
                exc_info=True,
                **context,
            )
            self._status.set_failed(log_id, f"LOAD STEP failed: {e}", **context)
            self._finalize_step.run_failure(
                source_path,
                processing_path,
                machine_id,
                vehicle_model,
                parts_code,
                op_no,
                log_id,
                pipeline_type,
            )
            return

        # ── STEP 4: FINALIZE ─────────────────────────────────────────────────
        try:
            self._status.set_processing(log_id, "FINALIZE", **context)
            self._status.set_completed(log_id, **context)
            self._finalize_step.run_success(
                source_path,
                processing_path,
                machine_id,
                vehicle_model,
                parts_code,
                op_no,
                log_id,
                pipeline_type,
            )
        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] 3D PIPELINE: FINALIZE STEP failed. "
                f"Data was committed to DB but file may remain in _processing. Error: {e}",
                exc_info=True,
                **context,
            )
            self._status.set_failed(log_id, f"FINALIZE STEP failed: {e}", **context)
