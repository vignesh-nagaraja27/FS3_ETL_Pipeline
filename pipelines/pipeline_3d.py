# -*- coding: utf-8 -*-
"""
3D Measurement Pipeline — Elite Edition
-----------------------------------------
Defines the exact sequence of steps for processing 3D HD and 3D BL files.
"""

import os
from pathlib import Path
from typing import Optional

from db.db_handler import DatabaseHandler
from steps.copy import CopyStep
from steps.transform import TransformStep
from steps.load import LoadStep
from steps.enr_block_master import EnrBlockMasterStep
from steps.finalize import FinalizeStep
from state.status_manager import StatusManager
from utils.logger import LOGGER
from utils.path_helper import get_working_paths
from utils.enr_block import parse_enr_filename


class Pipeline3D:
    """
    Orchestrates the full Copy → Transform → Load → Finalize lifecycle
    for a single 3D measurement file.
    """

    def __init__(self, destination_root: str):
        """
        Args:
            destination_root (str): Root path used to build processing folders.
        """
        self._destination_root = destination_root or ""
        self._status = StatusManager()
        self._db = DatabaseHandler()
        self._copy_step = CopyStep()
        self._transform_step = TransformStep()
        self._finalize_step = FinalizeStep(destination_root=self._destination_root)

    def run(
        self,
        source_path: Path,
        part_config: dict,
        machine_config: dict,
        pipeline_type: str,
        existing_log_id: Optional[int] = None,
    ):
        """
        Executes the full pipeline for one file.
        """
        # ELITE FIX: Explicitly cast to string and provide defaults to satisfy type checkers
        machine_id = str(machine_config.get("machine_id", "UNKNOWN"))
        vehicle_model = str(part_config.get("vehicle_model") or "UNKNOWN")
        parts_code = str(part_config.get("parts_code") or "UNKNOWN")
        op_no = str(part_config.get("op_no") or "UNKNOWN")

        context = dict(
            machine=machine_id,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
            pipeline_type=pipeline_type,
        )
        status_context = dict(
            machine=machine_id,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
        )
        is_enr_block = machine_id == "KTS-7106" and part_config.get("enr_block_flow", False)
        if is_enr_block:
            try:
                parse_enr_filename(source_path.name)
            except ValueError as exc:
                LOGGER.warning(f"E-NR-BLOCK file skipped: {source_path.name}: {exc}", **context)
                return
            sheet_index = machine_config.get("parser", {}).get("excel_sheet_index", 0)
            try:
                is_quantitative = self._transform_step.is_enr_quantitative_file(
                    source_path, sheet_index
                )
            except Exception:
                LOGGER.error(
                    f"E-NR/BL B3 eligibility check failed: {source_path.name}",
                    exc_info=True,
                    **context,
                )
                raise
            if not is_quantitative:
                LOGGER.info(
                    f"E-NR/BL file ignored: {source_path.name}; "
                    f"B3 on measurement sheet {sheet_index} does not contain 定量",
                    **context,
                )
                return

        processing_path: Optional[Path] = None
        log_id: Optional[int] = None

        destination_root = self._destination_root or str(Path.cwd())
        processing_dir, _, _ = get_working_paths(
            destination_root=destination_root,
            mc_no=machine_id,
            vehicle_model=vehicle_model,
            parts_code=parts_code,
            op_no=op_no,
            create=True,
        )

        # ── REGISTRATION / CLAIM ──────────────────────────────────────────────
        if existing_log_id is not None:
            log_id = existing_log_id
            self._status.reset_for_retry(log_id, **status_context)
            LOGGER.info(
                f"[LOG_ID:{log_id}] DLQ RETRY: Reusing existing log entry for '{source_path.name}'.",
                **context,
            )
        else:
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
                    f"3D PIPELINE: Registration failed for '{source_path.name}'. Error: {e}",
                    exc_info=True,
                    **context,
                )
                return

            if log_id is None:
                return

        # ── STEP 1: COPY (Staging) ────────────────────────────────────────────
        try:
            self._status.set_processing(log_id, "COPY", **status_context)
            processing_path = self._copy_step.run(
                source_path=source_path,
                processing_path=processing_dir,
                machine_id=machine_id,
                vehicle_model=vehicle_model,
                parts_code=parts_code,
                op_no=op_no,
                pipeline_type=pipeline_type,
                stable_wait_sec=float(machine_config.get("stable_wait_sec", 2.0)),
                stable_required_count=int(
                    machine_config.get("stable_required_count", 3)
                ),
            )

            # Update DB with finalized size
            try:
                stable_size = processing_path.stat().st_size
                self._status.update_file_size(log_id, stable_size)
            except Exception:
                pass

        except FileExistsError:
            self._status.set_failed(
                log_id,
                exc=FileExistsError("Duplicate file detected."),
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
                failed_step="COPY",
            )
            return
        except Exception as e:
            self._status.set_failed(
                log_id,
                exc=e,
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
                failed_step="COPY",
            )
            return

        # ── STEP 2: TRANSFORM ─────────────────────────────────────────────────
        try:
            self._status.set_processing(log_id, "TRANSFORM", **status_context)
            rows = self._transform_step.run(
                file_path=processing_path,
                part_config=part_config,
                machine_config=machine_config,
                machine_id=machine_id,
                log_id=log_id,
                pipeline_type=pipeline_type,
            )
        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] TRANSFORM failed: {e}", exc_info=True, **context
            )
            self._status.set_failed(
                log_id,
                exc=e,
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
                failed_step="TRANSFORM",
            )
            self._finalize_step.run_failure(
                source_path,
                processing_path or Path(""),
                machine_id,
                vehicle_model,
                parts_code,
                op_no,
                log_id,
                pipeline_type,
            )
            return

        # ── STEP 3: LOAD ──────────────────────────────────────────────────────
        try:
            self._status.set_processing(log_id, "LOAD", **status_context)
            load_step = LoadStep(pipeline_type=pipeline_type)
            version = str(machine_config.get("version", "")).strip()
            if not version:
                raise ValueError(f"Version is not configured for machine {machine_id}")
            if is_enr_block:
                EnrBlockMasterStep().run(self._db, machine_config, log_id, context)
            with self._db.get_connection() as conn:
                committed = load_step.run(
                    rows=rows,
                    part_config=part_config,
                    machine_id=machine_id,
                    log_id=log_id,
                    conn=conn,
                    version=version,
                )

            if committed == 0:
                raise ValueError("No rows committed to database.")

        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] LOAD failed: {e}", exc_info=True, **context
            )
            self._status.set_failed(
                log_id,
                e,
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
                failed_step="LOAD",
            )
            self._finalize_step.run_failure(
                source_path,
                processing_path or Path(""),
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
            self._status.set_processing(log_id, "FINALIZE", **status_context)
            self._status.set_completed(log_id, **status_context)
            self._finalize_step.run_success(
                source_path,
                processing_path or Path(""),
                machine_id,
                vehicle_model,
                parts_code,
                op_no,
                log_id,
                pipeline_type,
            )
        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] FINALIZE failed: {e}", exc_info=True, **context
            )
            self._status.set_failed(
                log_id,
                exc=e,
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
                failed_step="FINALIZE",
            )
