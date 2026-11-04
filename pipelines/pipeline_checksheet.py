# -*- coding: utf-8 -*-
"""Orchestrate the FS3 checksheet ETL lifecycle for one workbook.

Architecture & Synchronization Principles:
------------------------------------------
1. Source Workbook is the Absolute Source of Truth:
   The database must reflect the exact contents of the source workbook for transactions.
   If a serial row is removed from the workbook, reprocessing must automatically
   delete that transaction record from fs3.pre_t_measurement.

2. Master Scope Contracts:
   - CR Checksheets: Represent complete single-file master definitions. Missing database
     PIs are treated as obsolete definitions and marked for MASTER_DELETE.
   - BL / HD Checksheets: Represent partial master segments (sequence files ①, ②, ③...).
     Unclaimed database PIs during a segment file load are preserved.

3. Full Transaction Synchronization (INSERT / UPDATE / DELETE):
   - INSERT: New serials added in the workbook.
   - UPDATE: Existing normalized transaction measurements
identified by
(version,
vpo_code,
date,
serial_no_m_last4,
pi_no,
watch_id)
   - DELETE: Serials present in DB but removed from the workbook.

4. Shared Audit Table (fs3.pre_cs_measurement_change_log):
   Master changes and transaction changes share the same audit log table using
   change_type values: MASTER_INSERT, MASTER_UPDATE, MASTER_DELETE,
   TRANSACTION_INSERT, TRANSACTION_UPDATE, TRANSACTION_DELETE.
"""

from __future__ import annotations

import os
import re
import shutil
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Dict, Optional, TypedDict

from db.db_handler import DatabaseHandler
from repositories.checksheet_download_repository import ChecksheetDownloadRepository
from repositories.file_version_repository import FileVersionRepository
from repositories.processing_lock_repository import ProcessingLockRepository
from state.status_manager import StatusManager
from steps.load_checksheet import LoadChecksheetStep
from steps.transform_checksheet import TransformChecksheetStep
from steps.transform_checksheet_blhd import TransformChecksheetBLHDStep
from utils.file_hash import generate_sha256
from utils.logger import LOGGER
from utils.path_helper import get_checksheet_paths
from utils.stability_checker import check_file_stability


class _LogContext(TypedDict, total=False):
    machine: str
    model: str
    part: str
    op: str
    watch_id: str
    source_filename: str
    pipeline_type: str


class ChecksheetPipeline:
    """Coordinate one checksheet file through Mission Control and PostgreSQL."""

    CHECKSHEET_VERSION = "00000000"
    PIPELINE_TYPE = "CHECKSHEET"

    def __init__(self, destination_root: str | Path):
        self._destination_root = Path(destination_root or Path.cwd())
        self._status = StatusManager()
        self._db = DatabaseHandler()
        self._cr_transform_step = TransformChecksheetStep()
        self._blhd_transform_step = TransformChecksheetBLHDStep()
        self._load_step = LoadChecksheetStep()

    def run(
        self,
        source_path: str | Path,
        *,
        watch_config: Optional[Dict[str, Any]] = None,
        conn: Any = None,
    ) -> Dict[str, Any]:
        """Process one stable workbook while leaving the source untouched."""
        source = Path(source_path)
        if not source.is_file():
            raise FileNotFoundError(f"Source workbook not found: {source}")

        config = watch_config or {}
        filename_parts = self._extract_filename_parts(source.name, validate=False)
        vehicle_model = filename_parts["vehicle_model"]
        parts_code = filename_parts["parts_code"]
        op_no = filename_parts["op_no"]
        machine_id = str(config.get("machine_id") or "CHECKSHEET")

        raw_watch = str(config.get("watch_id") or machine_id)
        clean_watch_id = (
            raw_watch[11:]
            if raw_watch.upper().startswith("CHECKSHEET_")
            else raw_watch
        )

        context: _LogContext = {
            "machine": machine_id,
            "model": vehicle_model,
            "part": parts_code,
            "op": op_no,
            "watch_id": clean_watch_id,
            "source_filename": source.name,
            "pipeline_type": self.PIPELINE_TYPE,
        }

        log_id = self._status.register(
            file_name=source.name,
            file_size=source.stat().st_size,
            machine_id=machine_id,
            vehicle_model=vehicle_model,
            parts_code=parts_code,
            op_no=op_no,
            pid=os.getpid(),
            watch_id=clean_watch_id,
        )
        if log_id is None:
            return {
                "status": "SKIPPED",
                "source_path": str(source),
                "vehicle_model": vehicle_model,
                "parts_code": parts_code,
                "op_no": op_no,
            }

        LOGGER.info(
            f"[LOG_ID:{log_id}] [PIPELINE START] Starting checksheet pipeline for "
            f"'{source.name}'.",
            **context,
        )

        file_hash: Optional[str] = None
        file_version_id: Optional[int] = None
        archive_path: Optional[Path] = None
        database_connection: Any = None
        failed_step = "FILENAME_VALIDATION"
        lifecycle_id = config.get("checksheet_download_id")

        try:
            self._extract_filename_parts(source.name)

            with self._db.get_connection() as lifecycle_connection:
                lifecycle_repository = ChecksheetDownloadRepository(
                    lifecycle_connection
                )
                if lifecycle_id is None:
                    latest_lifecycle = lifecycle_repository.get_latest_by_file_name(
                        source.name
                    )
                    if latest_lifecycle is None:
                        raise LookupError(
                            f"No Checksheet discovery lifecycle found for {source.name}"
                        )
                    lifecycle_id = int(latest_lifecycle["id"])
                lifecycle_repository.mark_processing(int(lifecycle_id))
            LOGGER.info(
                f"[CS FILE PROCESSING] id={lifecycle_id} "
                f"file_name={source.name}",
                **context,
            )

            self._status.set_processing(
                log_id,
                "STABILITY",
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
            )
            LOGGER.info(
                f"[LOG_ID:{log_id}] [STABILITY CHECK] Checking '{source.name}'.",
                **context,
            )
            stable = check_file_stability(
                source,
                stable_required_count=int(config.get("stable_required_count", 2)),
                stable_wait_sec=min(float(config.get("stable_wait_sec", 2.0)), 2.0),
                poll_interval_sec=float(config.get("poll_interval_sec", 1.0)),
            )
            if not stable:
                raise RuntimeError(f"Source workbook is not stable: {source}")

            with self._db.get_connection() as lifecycle_connection:
                ChecksheetDownloadRepository(lifecycle_connection).mark_stable(int(lifecycle_id))

            failed_step = "HASH"
            self._status.set_processing(
                log_id,
                "HASH",
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
            )
            file_hash = generate_sha256(source)
            LOGGER.info(
                f"[LOG_ID:{log_id}] [HASH GENERATED] SHA256={file_hash} for "
                f"'{source.name}'.",
                **context,
            )

            with self._db.get_connection() as lock_conn:
                lock_repo = ProcessingLockRepository(lock_conn)
                lock_repo.mark_processing(
                    file_hash=file_hash,
                    file_name=source.name,
                    watch_id=clean_watch_id,
                    pipeline_log_id=log_id,
                )

            connection_manager = (
                nullcontext(conn) if conn is not None else self._db.get_connection()
            )
            with connection_manager as database_connection:
                file_version_repository = FileVersionRepository(database_connection)

                failed_step = "REGISTER"
                latest_processed = file_version_repository.get_latest_processed_for_file(
                    source.name, clean_watch_id
                )
                if latest_processed and latest_processed.get("file_hash") == file_hash:
                    database_connection.rollback()
                    with self._db.get_connection() as lifecycle_connection:
                        ChecksheetDownloadRepository(
                            lifecycle_connection
                        ).mark_processed(int(lifecycle_id))
                    with self._db.get_connection() as lock_conn:
                        ProcessingLockRepository(lock_conn).mark_processed(file_hash)
                    LOGGER.info(
                        f"[HASH UNCHANGED] watch_id={clean_watch_id} file={source.name}",
                        **context,
                    )
                    self._status.set_completed(
                        log_id,
                        machine=machine_id,
                        model=vehicle_model,
                        part=parts_code,
                        op=op_no,
                    )
                    return {
                        "status": "IDEMPOTENT",
                        "source_path": str(source),
                        "file_hash": file_hash,
                        "vehicle_model": vehicle_model,
                        "parts_code": parts_code,
                        "op_no": op_no,
                    }

                old_hash = latest_processed.get("file_hash") if latest_processed else "NONE"
                LOGGER.info(
                    f"[CHANGE DETECTED] watch_id={clean_watch_id} file={source.name} "
                    f"old_hash={old_hash} new_hash={file_hash}",
                    **context,
                )

                self._status.set_processing(
                    log_id,
                    "REGISTER",
                    machine=machine_id,
                    model=vehicle_model,
                    part=parts_code,
                    op=op_no,
                )
                fv_record = file_version_repository.register_version(
                    source,
                    file_hash=file_hash,
                    watch_id=clean_watch_id,
                    vehicle_model=vehicle_model,
                    parts_code=parts_code,
                    op_no=op_no,
                    version=self.CHECKSHEET_VERSION,
                    status="DISCOVERED",
                )
                if isinstance(fv_record, dict) and "file_version_id" in fv_record:
                    file_version_id = fv_record["file_version_id"]

                failed_step = "ARCHIVE"
                self._status.set_processing(
                    log_id,
                    "ARCHIVE",
                    machine=machine_id,
                    model=vehicle_model,
                    part=parts_code,
                    op=op_no,
                )
                archive_path = self._copy_snapshot(
                    source,
                    vehicle_model,
                    parts_code,
                    op_no,
                    "_archive",
                    file_hash,
                )
                LOGGER.info(
                    f"[LOG_ID:{log_id}] [ARCHIVE SNAPSHOT] Created '{archive_path}'.",
                    **context,
                )

                try:
                    file_version_repository.mark_processing(file_hash)

                    failed_step = "TRANSFORM"
                    self._status.set_processing(
                        log_id,
                        "TRANSFORM",
                        machine=machine_id,
                        model=vehicle_model,
                        part=parts_code,
                        op=op_no,
                    )

                    is_partial_master = parts_code.upper() in {"BL", "HD-LH","HD-RH"}

                    if is_partial_master:
                        transform_step = self._blhd_transform_step
                        LOGGER.info("[TRANSFORM ROUTING] Using BL/HD transformer", **context)
                    else:
                        transform_step = self._cr_transform_step
                        LOGGER.info("[TRANSFORM ROUTING] Using CR transformer", **context)

                    transformed_records = transform_step.run(
                        source,
                        vehicle_model=vehicle_model,
                        parts_code=parts_code,
                        op_no=op_no,
                    )
                    LOGGER.info(
                        f"[TRANSFORM RESULT] "
                        f"master_records={len(transformed_records.get('master_records', []))} "
                        f"transaction_records={len(transformed_records.get('transaction_records', []))}",
                        **context,
                    )

                    failed_step = "LOAD"
                    self._status.set_processing(
                        log_id,
                        "LOAD",
                        machine=machine_id,
                        model=vehicle_model,
                        part=parts_code,
                        op=op_no,
                    )
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] [LOAD START] Loading '{source.name}' with "
                        f"audit context (watch_id={clean_watch_id}, file_version_id={file_version_id}, "
                        f"is_partial_master={is_partial_master}).",
                        **context,
                    )
                    loaded_count = self._load_step.run(
                        transformed_records,
                        conn=database_connection,
                        vehicle_model=vehicle_model,
                        parts_code=parts_code,
                        op_no=op_no,
                        file_hash=file_hash,
                        file_name=source.name,
                        version=self.CHECKSHEET_VERSION,
                        watch_id=clean_watch_id,
                        file_version_id=file_version_id,
                        is_partial_master=is_partial_master,
                    )
                    if loaded_count < 0:
                        raise ValueError("Transaction synchronization failed")

                    failed_step = "COMMIT"
                    file_version_repository.mark_processed(
                        file_hash,
                        archive_path=archive_path,
                    )
                    self._status.set_completed(
                        log_id,
                        machine=machine_id,
                        model=vehicle_model,
                        part=parts_code,
                        op=op_no,
                    )
                    database_connection.commit()

                    with self._db.get_connection() as lock_conn:
                        ProcessingLockRepository(lock_conn).mark_processed(file_hash)

                    with self._db.get_connection() as lifecycle_connection:
                        ChecksheetDownloadRepository(
                            lifecycle_connection
                        ).mark_processed(int(lifecycle_id))
                    LOGGER.info(
                        f"[CS FILE PROCESSED] id={lifecycle_id} "
                        f"file_name={source.name}",
                        **context,
                    )
                except Exception:
                    database_connection.rollback()
                    raise

            LOGGER.info(
                f"[LOG_ID:{log_id}] [PIPELINE COMPLETED] Completed checksheet "
                f"pipeline for '{source.name}'.",
                **context,
            )
            return {
                "status": "SUCCESS",
                "source_path": str(source),
                "archive_path": str(archive_path),
                "file_hash": file_hash,
                "vehicle_model": vehicle_model,
                "parts_code": parts_code,
                "op_no": op_no,
                "transaction_count": loaded_count,
            }
        except Exception as exc:
            retry_base_minutes = int(config.get("retry_base_minutes", 5))
            max_retry_attempts = int(config.get("max_retry_attempts", 4))

            if database_connection is not None:
                try:
                    database_connection.rollback()
                except Exception:
                    LOGGER.error(
                        f"[LOG_ID:{log_id}] Additional rollback attempt failed.",
                        exc_info=True,
                        **context,
                    )

            if file_hash is not None:
                try:
                    with self._db.get_connection() as lock_conn:
                        lock_repo = ProcessingLockRepository(lock_conn)
                        record = lock_repo.mark_retry_pending(
                            file_hash=file_hash,
                            retry_base_minutes=retry_base_minutes,
                            max_retry_attempts=max_retry_attempts,
                            error_message=str(exc),
                        )
                        if record:
                            status_val = record.get("processing_status")
                            retry_count = record.get("retry_count")
                            next_at = record.get("next_retry_at")
                            if status_val == "RETRY_PENDING":
                                LOGGER.warning(
                                    f"[RETRY SCHEDULED] attempt={retry_count} "
                                    f"next_retry_at={next_at} for '{source.name}'",
                                    **context,
                                )
                            else:
                                LOGGER.error(
                                    f"[RETRY EXHAUSTED] [AWAITING REVIEW] max retries reached for '{source.name}'",
                                    **context,
                                )
                except Exception:
                    LOGGER.error(
                        f"[LOG_ID:{log_id}] Failed to persist retry schedule in processing lock table for '{source.name}'.",
                        exc_info=True,
                        **context,
                    )

                try:
                    with self._db.get_connection() as fv_conn:
                        FileVersionRepository(fv_conn).mark_failed(file_hash)
                except Exception:
                    pass

            if lifecycle_id is not None and file_hash is None:
                try:
                    with self._db.get_connection() as lifecycle_connection:
                        ChecksheetDownloadRepository(
                            lifecycle_connection
                        ).mark_failed(int(lifecycle_id))
                except Exception:
                    pass

            self._status.set_failed(
                log_id,
                exc=exc,
                machine=machine_id,
                model=vehicle_model,
                part=parts_code,
                op=op_no,
                failed_step=failed_step,
            )

            try:
                failed_copy = self._copy_snapshot(
                    source,
                    vehicle_model,
                    parts_code,
                    op_no,
                    "_ng",
                    file_hash,
                )
                LOGGER.error(
                    f"[LOG_ID:{log_id}] Failed workbook snapshot copied to "
                    f"'{failed_copy}'.",
                    **context,
                )
            except Exception:
                pass

            LOGGER.error(
                f"[LOG_ID:{log_id}] [PIPELINE FAILED] Checksheet pipeline failed "
                f"for '{source.name}': {exc}",
                exc_info=True,
                **context,
            )
            raise

    process_file = run

    def _copy_snapshot(
        self,
        source: Path,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        subfolder: str,
        file_hash: Optional[str],
    ) -> Path:
        archive_path, ng_path, replay_path = get_checksheet_paths(
            str(self._destination_root),
            vehicle_model,
            parts_code,
            op_no,
            create=True,
        )

        if subfolder == "_archive":
            destination_dir = archive_path
        elif subfolder == "_ng":
            destination_dir = ng_path
        elif subfolder == "_replay":
            destination_dir = replay_path
        else:
            destination_dir = (
                self._destination_root
                / vehicle_model
                / parts_code
                / op_no
                / subfolder
            )
            destination_dir.mkdir(parents=True, exist_ok=True)

        hash_suffix = file_hash or "unhashed"
        destination = destination_dir / f"{source.stem}_{hash_suffix}{source.suffix}"
        shutil.copy2(source, destination)
        return destination

    @staticmethod
    def _extract_filename_parts(
        file_name: str,
        *,
        validate: bool = True,
    ) -> Dict[str, str]:
        match = re.fullmatch(
            r"(?P<vehicle_model>[^_]+)_"
            r"(?P<parts_code>[^_]+)_"
            r"(?P<op_no>[^_]+)(?:_.+)?",
            Path(file_name).stem,
            flags=re.IGNORECASE,
        )
        if match is None:
            raise ValueError(
                "Checksheet filename must match "
                "VehicleModel_PartsCode_OPNO_Anything.xlsm"
            )
        if validate:
            for group_name, segment_name, pattern, allowed_characters in (
                ("vehicle_model", "VehicleModel", r"[A-Za-z0-9]+", "A-Z, a-z, and 0-9"),
                ("parts_code", "PartsCode", r"[A-Za-z0-9-]+", "A-Z, a-z, 0-9, and -"),
                ("op_no", "OPNO", r"OP[0-9]+", "OP followed by digits (0-9)"),
            ):
                value = match.group(group_name)
                if re.fullmatch(pattern, value) is None:
                    raise ValueError(
                        f"Invalid checksheet filename segment '{segment_name}': "
                        f"value '{value}' in filename '{file_name}' must contain "
                        f"only {allowed_characters}"
                    )
        return {
            "vehicle_model": match.group("vehicle_model"),
            "parts_code": match.group("parts_code"),
            "op_no": match.group("op_no").upper(),
        }


PipelineChecksheet = ChecksheetPipeline

__all__ = ["ChecksheetPipeline", "PipelineChecksheet"]