# -*- coding: utf-8 -*-
"""
Job Runner — Elite Orchestration Supervisor (v2)
-------------------------------------------------
The engine that keeps the entire ETL system running.

All watcher lifecycle events, thread pool operations, heartbeats, and infrastructure
diagnostics are strictly routed to etl_system_master.log using pipeline_type="SYSTEM".
"""

import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from streamlit.watcher import watch_dir

from config_loader import ConfigLoader
from db.db_handler import DatabaseHandler
from orchestrator.dispatcher import (
    Dispatcher,
    PIPELINE_3D_HD,
    PIPELINE_3D_BL,
    PIPELINE_3D_CR,
    PIPELINE_CHECKSHEET,
    PIPELINE_UNKNOWN,
)
from pipelines.pipeline_3d import Pipeline3D
from steps.enr_block_master import EnrBlockMasterStep
from pipelines.pipeline_checksheet import ChecksheetPipeline
from repositories.checksheet_download_repository import ChecksheetDownloadRepository
from repositories.file_version_repository import FileVersionRepository
from repositories.processing_lock_repository import ProcessingLockRepository
from utils.file_hash import generate_sha256
from utils.logger import LOGGER
from utils.enr_block import parse_enr_filename
from utils.stability_checker import check_file_stability

# File extensions we process. Anything else is ignored.
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".xlsm"}

# How often the DLQ retry watcher polls for RETRYING entries (seconds)
DLQ_POLL_INTERVAL = 15


class JobRunner:
    """
    The central supervisor that starts and coordinates all pipeline workers.
    """

    def __init__(self, config: ConfigLoader):
        """
        Args:
            config (ConfigLoader): The global configuration object loaded at startup.
        """
        self._config = config
        self._dispatcher = Dispatcher(config)
        self._stop_event = threading.Event()
        self._checksheet_watch_directories = self._load_checksheet_watch_directories()
        self._checksheet_destination_root = self._get_checksheet_destination_root()

        # Blocks measurement file processing while a conversion import is in progress.
        self._conversion_lock = threading.Event()
        self._conversion_lock.set()  # Unlocked by default

        # Tracks DLQ entry IDs already submitted to the executor in this session.
        self._submitted_dlq_ids: set = set()
        self._dlq_id_lock = threading.Lock()

        self._pipeline_3d = Pipeline3D(destination_root=config.get_destination_root())

        # Dedicated thread pools per workload domain to prevent resource contention
        cmm_workers = sum(
            d.get("max_workers", 2)
            for d in config.get_watch_directories()
            if d.get("enabled", True)
        )
        checksheet_workers = sum(
            d.get("max_workers", 2)
            for d in self._checksheet_watch_directories
            if d.get("enabled", True)
        )

        self._cmm_executor = ThreadPoolExecutor(
            max_workers=max(cmm_workers, 2), thread_name_prefix="Exec-CMM"
        )
        self._checksheet_executor = ThreadPoolExecutor(
            max_workers=max(checksheet_workers, 2), thread_name_prefix="Exec-CS"
        )
        self._dlq_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="Exec-DLQ"
        )

    def _load_checksheet_watch_directories(self) -> List[dict]:
        """Loads the auxiliary checksheet watcher config from the shared ConfigLoader."""
        payload = self._config.get_checksheet_config()
        if not payload:
            return []

        dirs = self._config.get_checksheet_watch_directories()
        return [self._normalize_checksheet_machine_config(d) for d in dirs if isinstance(d, dict)]

    def _get_checksheet_destination_root(self) -> str:
        """Returns the configured destination root for the checksheet watcher set."""
        payload = self._config.get_checksheet_config()
        if not payload:
            return self._config.get_destination_root()

        destination = payload.get("destination_root", {})
        if isinstance(destination, dict):
            return str(destination.get("path") or self._config.get_destination_root())
        return str(destination or self._config.get_destination_root())

    def _normalize_checksheet_machine_config(self, machine_config: dict) -> dict:
        """Normalizes checksheet watch entries to the same machine registry format used by CMM."""
        normalized = dict(machine_config)
        raw_id = str(normalized.get("watch_id") or normalized.get("machine_id") or "CHECKSHEET").strip()

        prefix = "CHECKSHEET_"
        if raw_id.upper().startswith(prefix):
            watch_id = raw_id[len(prefix):]
        else:
            watch_id = raw_id

        if not watch_id:
            watch_id = "CHECKSHEET"

        machine_id = raw_id if raw_id.upper().startswith(prefix) else f"{prefix}{watch_id}"

        normalized["watch_id"] = watch_id
        normalized["machine_id"] = machine_id
        normalized.setdefault("description", f"Checksheet watch: {normalized.get('source_path', '')}")
        normalized.setdefault("pipeline_type", PIPELINE_CHECKSHEET)
        normalized.setdefault("enabled", True)
        return normalized

    def start(self):
        """
        Starts all watcher threads and blocks until a shutdown signal is received.
        """
        LOGGER.info("=" * 60, pipeline_type="SYSTEM")
        LOGGER.info("ETL Job Runner starting up.", pipeline_type="SYSTEM")
        LOGGER.info("=" * 60, pipeline_type="SYSTEM")

        # STARTUP CLEANUP: Reset stale PROCESSING rows
        self._cleanup_stale_tasks()

        threads = []

        # Register every configured machine in the DB registry
        for machine_config in self._config.get_watch_directories():
            self._register_machine(machine_config)
        for machine_config in self._checksheet_watch_directories:
            self._register_machine(machine_config)

        for machine_config in self._config.get_watch_directories():
            if (
                machine_config.get("enabled", True)
                and machine_config.get("machine_id") == "KTS-7106"
                and machine_config.get("enr_block_enabled", False)
            ):
                EnrBlockMasterStep().run(
                    DatabaseHandler(), machine_config, 0,
                    dict(machine="KTS-7106", model="E-NR", part="BL", pipeline_type="3D_BL"),
                )

        # Start one watcher thread per configured machine
        for machine_config in self._config.get_watch_directories():
            if not machine_config.get("enabled", True):
                LOGGER.info(
                    f"Machine '{machine_config.get('machine_id')}' is DISABLED. Skipping.",
                    pipeline_type="SYSTEM",
                )
                continue

            t = threading.Thread(
                target=self._watch_machine_folder,
                args=(machine_config,),
                name=f"Watcher-{machine_config.get('machine_id')}",
                daemon=True,
            )
            t.start()
            threads.append(t)

        # Start one watcher thread per configured checksheet directory
        for machine_config in self._checksheet_watch_directories:
            if not machine_config.get("enabled", True):
                LOGGER.info(
                    f"Checksheet watcher '{machine_config.get('machine_id')}' is DISABLED. Skipping.",
                    pipeline_type="SYSTEM",
                )
                continue

            LOGGER.info(
                f"[CHECKSHEET WATCHER START] {machine_config.get('machine_id')} -> {machine_config.get('source_path')}",
                pipeline_type="SYSTEM",
            )
            t = threading.Thread(
                target=self._watch_checksheet_folder,
                args=(machine_config,),
                name=f"Watcher-Checksheet-{machine_config.get('machine_id')}",
                daemon=True,
            )
            t.start()
            threads.append(t)

        # Start the Checksheet database-backed retry recovery watcher thread
        t_cs_retry = threading.Thread(
            target=self._watch_checksheet_retries,
            name="Watcher-Checksheet-Retry",
            daemon=True,
        )
        t_cs_retry.start()
        threads.append(t_cs_retry)

        # Start the conversion table watcher thread
        conv_config = self._config.get_conversion_watch()
        if conv_config.get("enabled", True):
            t_conv = threading.Thread(
                target=self._watch_conversion_folder,
                args=(conv_config,),
                name="Watcher-ConversionTable",
                daemon=True,
            )
            t_conv.start()
            threads.append(t_conv)

        # Start the DLQ retry watcher thread
        t_dlq = threading.Thread(
            target=self._watch_dlq_retrying, name="Watcher-DLQ-Retry", daemon=True
        )
        t_dlq.start()
        threads.append(t_dlq)

        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            LOGGER.info(
                "Shutdown signal received. Stopping watchers...",
                pipeline_type="SYSTEM",
            )
            self.stop()

    def stop(self):
        """Signals all threads to stop and shuts down all workload thread pools."""
        self._stop_event.set()
        self._cmm_executor.shutdown(wait=True)
        self._checksheet_executor.shutdown(wait=True)
        self._dlq_executor.shutdown(wait=True)
        DatabaseHandler().close_all()
        LOGGER.info("ETL Job Runner shut down cleanly.", pipeline_type="SYSTEM")

    def _cleanup_stale_tasks(self):
        """Resets rows stuck in PROCESSING to STALE on startup."""
        sql = """
            UPDATE fs3.etl_pipeline_log
            SET status       = 'STALE',
                current_step = 'STALE_RESET',
                updated_at   = NOW()
            WHERE status = 'PROCESSING'
            AND updated_at < NOW() - INTERVAL '30 minutes'
            RETURNING id, file_name;
        """
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    stale_rows = cur.fetchall()

            if stale_rows:
                LOGGER.warning(
                    f"STARTUP CLEANUP: Reset {len(stale_rows)} stale records.",
                    pipeline_type="SYSTEM",
                )
        except Exception as e:
            LOGGER.warning(
                f"STARTUP CLEANUP: Error resetting stale records: {e}",
                pipeline_type="SYSTEM",
            )

    # Retry Backoff Scheduler
    #
    # Retry delay grows using:
    #
    # retry_base_minutes * retry_attempt_number
    #
    # Example:
    #
    # attempt 2 -> 5 min
    # attempt 3 -> 10 min
    # attempt 4 -> 15 min
    #
    # Retry metadata is persisted in:
    #
    # fs3.pre_l_measurement_processing_lock
    #
    # so retries survive ETL restarts.
    #
    # After max attempts:
    #
    # AWAITING_REVIEW
    def _watch_checksheet_retries(self):
        """Polls fs3.pre_l_measurement_processing_lock for pending retries and stale processing tasks across restarts."""
        while not self._stop_event.is_set():
            try:
                db = DatabaseHandler()
                with db.get_connection() as conn:
                    lock_repo = ProcessingLockRepository(conn)
                    pending_list = lock_repo.get_pending_retries()
                    stale_list = lock_repo.get_stale_processing(stale_minutes=30)

                for item in pending_list:
                    file_name = item.get("file_name")
                    file_hash = item.get("file_hash")
                    retry_count = item.get("retry_count", 0)
                    watch_id = item.get("watch_id")

                    if not file_name:
                        continue

                    target_config = None
                    target_file_path = None
                    for machine_config in self._checksheet_watch_directories:
                        if watch_id and machine_config.get("watch_id")!=watch_id:
                            continue
                        src_dir = Path(machine_config["source_path"])
                        candidate = src_dir / str(file_name)
                        if candidate.exists():
                            target_config = machine_config
                            target_file_path = candidate
                            break

                    if not target_config or not target_file_path:
                        continue

                    pipeline_type, part_config = self._dispatcher.classify(target_file_path)
                    if pipeline_type != PIPELINE_CHECKSHEET or part_config is None:
                        continue

                    LOGGER.info(
                        f"[RETRY EXECUTED] file={file_name} attempt={retry_count + 1} hash={file_hash}",
                        pipeline_type="SYSTEM",
                    )

                    self._checksheet_executor.submit(
                        self._run_pipeline_safe,
                        target_file_path,
                        pipeline_type,
                        part_config,
                        {
                            **target_config,
                            "destination_root": self._checksheet_destination_root,
                        },
                    )

                for item in stale_list:
                    file_name = item.get("file_name")
                    file_hash = item.get("file_hash")
                    watch_id = item.get("watch_id")

                    if not file_name:
                        continue

                    target_config = None
                    target_file_path = None
                    for machine_config in self._checksheet_watch_directories:
                        if watch_id and machine_config.get("watch_id")!=watch_id:
                            continue
                        src_dir = Path(machine_config["source_path"])
                        candidate = src_dir / str(file_name)
                        if candidate.exists():
                            target_config = machine_config
                            target_file_path = candidate
                            break

                    if not target_config or not target_file_path:
                        continue

                    pipeline_type, part_config = self._dispatcher.classify(target_file_path)
                    if pipeline_type != PIPELINE_CHECKSHEET or part_config is None:
                        continue

                    LOGGER.info(
                        f"[STALE RECOVERY] file={file_name} hash={file_hash}",
                        pipeline_type="SYSTEM",
                    )

                    self._checksheet_executor.submit(
                        self._run_pipeline_safe,
                        target_file_path,
                        pipeline_type,
                        part_config,
                        {
                            **target_config,
                            "destination_root": self._checksheet_destination_root,
                        },
                    )
            except Exception as e:
                LOGGER.error(
                    f"[CHECKSHEET-RETRY] Watcher error: {e}",
                    pipeline_type="SYSTEM",
                    exc_info=True,
                )

            time.sleep(15)

    def _watch_dlq_retrying(self):
        """Polls DLQ for 'RETRYING' entries and submits them to the DLQ executor."""
        while not self._stop_event.is_set():
            try:
                db = DatabaseHandler()
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT d.id, d.file_name, d.machine_id, d.pipeline_log_id, 
                                   d.retry_count, d.max_retries
                            FROM fs3.etl_dlq d WHERE d.dlq_status = 'RETRYING'
                            ORDER BY d.last_retry_at ASC LIMIT 20;
                        """)
                        retrying_entries = cur.fetchall()

                for entry in retrying_entries:
                    dlq_id, file_name, machine_id, log_id, retries, max_r = entry

                    if not file_name:
                        continue

                    with self._dlq_id_lock:
                        if dlq_id in self._submitted_dlq_ids:
                            continue

                    if retries >= (max_r or 3):
                        continue

                    machine_config = next(
                        (
                            m
                            for m in self._config.get_watch_directories()
                            if m.get("machine_id") == machine_id
                        ),
                        None,
                    )
                    if not machine_config:
                        continue

                    file_path = Path(machine_config["source_path"]) / str(file_name)
                    if not file_path.exists():
                        continue

                    pipeline_type, part_config = self._dispatcher.classify_from_watcher(file_path=file_path,watch_config=machine_config)
                    if pipeline_type == PIPELINE_UNKNOWN or part_config is None:
                        continue

                    if self._claim_dlq_entry(dlq_id):
                        with self._dlq_id_lock:
                            self._submitted_dlq_ids.add(dlq_id)
                            LOGGER.info(
                                f"SUBMITTING {file_path.name}",
                                pipeline_type="SYSTEM",
                            )

                        self._dlq_executor.submit(
                            self._run_pipeline_retry_safe,
                            file_path,
                            pipeline_type,
                            part_config,
                            machine_config,
                            int(log_id),
                            int(dlq_id),
                        )
            except Exception as e:
                LOGGER.error(f"[DLQ-WATCHER] Error: {e}", pipeline_type="SYSTEM",exc_info=True)

            time.sleep(DLQ_POLL_INTERVAL)

    def _claim_dlq_entry(self, dlq_id: int) -> bool:
        """Atomically marks a DLQ entry as RESOLVED to prevent double-submission."""
        sql = "UPDATE fs3.etl_dlq SET dlq_status = 'RESOLVED' WHERE id = %s AND dlq_status = 'RETRYING' RETURNING id;"
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (dlq_id,))
                    return cur.fetchone() is not None
        except Exception:
            return False

    def _run_pipeline_retry_safe(
        self,
        file_path: Path,
        pipeline_type: str,
        part_config: dict,
        machine_config: dict,
        pipeline_log_id: int,
        dlq_id: int,
    ):
        """Wrapper for DLQ retry pipeline runs."""
        machine_id = str(machine_config.get("machine_id", "UNKNOWN"))
        try:
            self._pipeline_3d.run(
                source_path=file_path,
                part_config=part_config,
                machine_config=machine_config,
                pipeline_type=pipeline_type,
                existing_log_id=pipeline_log_id,
            )
            self._increment_registry_counter(machine_id, success=True)
        except Exception as e:
            LOGGER.error(
                f"[DLQ-WATCHER] Retry FAILED for '{file_path.name}': {e}",
                pipeline_type="SYSTEM",exc_info=True
            )
            try:
                db = DatabaseHandler()
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE fs3.etl_dlq SET dlq_status='AWAITING_REVIEW', error_message=%s, last_retry_at=NOW() WHERE id=%s;",
                            (str(e)[:2000], dlq_id),
                        )
            except Exception:
                pass
            with self._dlq_id_lock:
                self._submitted_dlq_ids.discard(dlq_id)

    def _watch_machine_folder(self, machine_config: dict):
        """Polls one machine's source folder on a fixed interval."""
        source_path = Path(machine_config["source_path"])
        machine_id = str(machine_config["machine_id"])
        poll_interval = machine_config.get("poll_interval_sec", 5)
        ignore_prefixes = machine_config.get("ignore_file_prefixes", [])
        seen_files: set = set()

        while not self._stop_event.is_set():
            try:
                self._heartbeat(machine_id)
                if not source_path.exists():
                    time.sleep(poll_interval)
                    continue

                for file_path in source_path.rglob("*"):
                    is_enr_block = (
                        machine_id == "KTS-7106"
                        and machine_config.get("enr_block_enabled", False)
                        and file_path.name.startswith("E-NR")
                    )
                    if is_enr_block:
                        if str(file_path) in seen_files:
                            continue
                        try:
                            parse_enr_filename(file_path.name)
                        except ValueError as exc:
                            seen_files.add(str(file_path))
                            LOGGER.warning(
                                f"[MC:{machine_id}] E-NR-BLOCK file skipped: "
                                f"{file_path.name}: {exc}",
                                pipeline_type="SYSTEM",
                            )
                            continue
                    if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                        continue
                    # Ignore configured filename prefixes
                    if any(
                        file_path.name.upper().startswith(prefix.upper())
                        for prefix in ignore_prefixes
                    ) and not is_enr_block:
                        seen_files.add(str(file_path))
                        LOGGER.info(
                            f"[MC:{machine_id}] Ignoring file by prefix rule: {file_path.name}",
                            pipeline_type="SYSTEM",
                        )
                        continue
                    if str(file_path) in seen_files:
                        continue

                    if self._is_file_processed(file_path.name):
                        seen_files.add(str(file_path))
                        continue

                    seen_files.add(str(file_path))
                    pipeline_type, part_config = self._dispatcher.classify_from_watcher(file_path=file_path,watch_config=machine_config,)

                    if pipeline_type == PIPELINE_UNKNOWN or part_config is None:
                        continue

                    if not self._conversion_lock.is_set():
                        self._conversion_lock.wait()

                    self._cmm_executor.submit(
                        self._run_pipeline_safe,
                        file_path,
                        pipeline_type,
                        part_config,
                        machine_config,
                    )
            except Exception as e:
                LOGGER.error(
                    f"[MC:{machine_id}] Watcher error: {e}",
                    pipeline_type="SYSTEM",exc_info=True
                )

            time.sleep(poll_interval)

    # Change Detection Scheduler
    #
    # Every configured interval:
    #   1. Scan workbook files.
    #   2. Calculate file hash.
    #   3. Compare against latest successful hash.
    #   4. Reprocess only when hash changes.
    #
    # This supports:
    #   - corrected workbooks
    #   - OneDrive updates
    #   - user modifications
    #   - replay scenarios
    #
    # File names are NOT used as the source of truth.
    # SHA256 hash comparison determines whether a workbook
    # requires reprocessing.
    def _watch_checksheet_folder(self, machine_config: dict):
        """Polls a checksheet folder using SHA256 change detection and backoff retry configuration."""
        source_path = Path(machine_config["source_path"])
        machine_id = str(machine_config["machine_id"])
        raw_watch = str(machine_config.get("watch_id") or machine_id)
        watch_id = raw_watch[11:] if raw_watch.upper().startswith("CHECKSHEET_") else raw_watch

        poll_interval = machine_config.get("poll_interval_sec", 30)
        change_check_interval_sec = int(machine_config.get("change_check_interval_minutes", 5)) * 60
        stable_wait_sec = float(machine_config.get("stable_wait_sec", 10.0))
        stable_required_count = int(machine_config.get("stable_required_count", 3))

        detected_files: Dict[str, int] = {}
        last_change_check: Dict[str, float] = {}
        known_files: set = set()

        while not self._stop_event.is_set():
            try:
                self._heartbeat(machine_id)
                if not source_path.exists():
                    time.sleep(poll_interval)
                    continue

                now = time.time()
                for file_path in source_path.rglob("*"):
                    if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                        continue

                    file_key = str(file_path)

                    is_new_file = file_key not in known_files
                    if not is_new_file:
                        if (now - last_change_check.get(file_key, 0)) < change_check_interval_sec:
                            continue

                    try:
                        current_hash = generate_sha256(file_path)
                    except Exception:
                        continue

                    db = DatabaseHandler()
                    with db.get_connection() as conn:
                        fv_repo = FileVersionRepository(conn)
                        latest_processed = fv_repo.get_latest_processed_for_file(file_path.name,watch_id)
                        lock_repo = ProcessingLockRepository(conn)
                        lock_status = lock_repo.get_latest_status_by_hash(current_hash)

                    # Check if hash is already PROCESSED
                    if latest_processed and latest_processed.get("file_hash") == current_hash:
                        last_change_check[file_key] = now
                        known_files.add(file_key)
                        LOGGER.info(
                            f"[HASH UNCHANGED] watch_id={watch_id} file={file_path.name}",
                            pipeline_type="CHECKSHEET",
                        )
                        continue

                    # If file is currently RETRY_PENDING, let retry watcher handle it
                    if lock_status and lock_status.get("processing_status") == "RETRY_PENDING":
                        last_change_check[file_key] = now
                        known_files.add(file_key)
                        continue

                    old_hash = latest_processed.get("file_hash") if latest_processed else "NONE"
                    LOGGER.info(
                        f"[CHANGE DETECTED] watch_id={watch_id} file={file_path.name} "
                        f"old_hash={old_hash} new_hash={current_hash}",
                        pipeline_type="CHECKSHEET",
                    )

                    # Record DISCOVERED in processing lock table
                    with db.get_connection() as conn:
                        ProcessingLockRepository(conn).mark_discovered(
                            current_hash,
                            file_path.name,
                            watch_id=watch_id,
                        )

                    lifecycle_id = detected_files.get(file_key)
                    if lifecycle_id is None:
                        detected_config = self._config.match_checksheet_part(file_path.name)
                        if detected_config is None:
                            LOGGER.warning(
                                f"[CHECKSHEET WATCHER] Ignoring non-checksheet file '{file_path.name}' in checksheet watch folder.",
                                pipeline_type="SYSTEM",
                            )
                            last_change_check[file_key] = now
                            known_files.add(file_key)
                            continue

                        with db.get_connection() as conn:
                            lifecycle_record = ChecksheetDownloadRepository(
                                conn
                            ).register_detected(
                                file_path,
                                vehicle_model=detected_config.get("vehicle_model"),
                                parts_code=detected_config.get("parts_code"),
                                op_no=detected_config.get("op_no"),
                                file_size=file_path.stat().st_size,
                            )
                        lifecycle_id = int(lifecycle_record["id"])
                        detected_files[file_key] = lifecycle_id

                    if not check_file_stability(
                        file_path,
                        stable_required_count=stable_required_count,
                        stable_wait_sec=stable_wait_sec,
                        poll_interval_sec=max(float(poll_interval) / 3.0, 1.0),
                    ):
                        continue

                    with db.get_connection() as conn:
                        ChecksheetDownloadRepository(conn).mark_stable(lifecycle_id)

                    pipeline_type, part_config = self._dispatcher.classify_from_watcher(file_path=file_path,watch_config=machine_config,)
                    if pipeline_type != PIPELINE_CHECKSHEET or part_config is None:
                        last_change_check[file_key] = now
                        known_files.add(file_key)
                        continue

                    with db.get_connection() as conn:
                        ChecksheetDownloadRepository(conn).mark_submitted(lifecycle_id)

                    LOGGER.info(
                        f"[CHECKSHEET JOB SUBMITTED] {file_path.name} -> {pipeline_type} [{machine_id}]",
                        pipeline_type="SYSTEM",
                    )
                    self._checksheet_executor.submit(
                        self._run_pipeline_safe,
                        file_path,
                        pipeline_type,
                        part_config,
                        {
                            **machine_config,
                            "destination_root": self._checksheet_destination_root,
                            "checksheet_download_id": lifecycle_id,
                        },
                    )
                    known_files.add(file_key)
                    last_change_check[file_key] = now
            except Exception as exc:
                LOGGER.error(
                    f"[CHECKSHEET WATCHER] Error for '{machine_id}': {exc}",
                    pipeline_type="SYSTEM",
                    exc_info=True,
                )

            time.sleep(poll_interval)

    def _watch_conversion_folder(self, conv_config: dict):
        """Monitors the conversion table Excel folder."""
        conv_path = Path(conv_config["path"])
        poll_interval = conv_config.get("poll_interval_sec", 10)
        seen_files: set = set()

        while not self._stop_event.is_set():
            try:
                if not conv_path.exists():
                    time.sleep(poll_interval)
                    continue

                for file_path in conv_path.glob("*.xlsx"):
                    if str(file_path) in seen_files:
                        continue

                    seen_files.add(str(file_path))
                    self._conversion_lock.clear()
                    try:
                        self._import_conversion_table(file_path)
                    finally:
                        self._conversion_lock.set()
            except Exception as e:
                LOGGER.error(f"[CONVERSION] Error: {e}", pipeline_type="SYSTEM",exc_info=True)
                self._conversion_lock.set()

            time.sleep(poll_interval)

    def _import_conversion_table(self, file_path: Path):
        """Imports a conversion mapping Excel file."""
        import pandas as pd

        try:
            df = pd.read_excel(file_path, header=0)
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE fs3.pre_w_3d_hd;")
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT INTO fs3.pre_w_3d_hd (version, vpo_code, measure_no, evaluation_item1) VALUES (%s, %s, %s, %s)",
                            (
                                str(row.get("version", "")),
                                str(row.get("vpo_code", "")),
                                int(row.get("measure_no", 0)),
                                str(row.get("evaluation_item1", "")),
                            ),
                        )
                    cur.execute("""
                        UPDATE fs3.pre_m_cmm m SET evaluation_item1 = w.evaluation_item1
                        FROM fs3.pre_m_cmm m INNER JOIN fs3.pre_w_3d_hd w ON m.vpo_code = w.vpo_code AND m.measure_no = w.measure_no AND m.version = w.version;
                    """)
        except Exception as e:
            LOGGER.error(
                f"[CONVERSION] Failed import: {e}",
                pipeline_type="SYSTEM",exc_info=True
            )
            raise

    def _run_pipeline_safe(
        self,
        file_path: Path,
        pipeline_type: str,
        part_config: Dict[Any, Any],
        machine_config: dict,
    ):
        """Catches top-level exceptions from a normal pipeline run."""
        machine_id = str(machine_config.get("machine_id", "UNKNOWN"))
        try:
            if pipeline_type in (PIPELINE_3D_HD, PIPELINE_3D_BL, PIPELINE_3D_CR):
                self._pipeline_3d.run(
                    source_path=file_path,
                    part_config=part_config,
                    machine_config=machine_config,
                    pipeline_type=pipeline_type,
                )
                self._increment_registry_counter(machine_id, success=True)
            elif pipeline_type == PIPELINE_CHECKSHEET:
                pipeline = cast(
                    ChecksheetPipeline,
                    self._dispatcher.dispatch(
                        pipeline_type,
                        destination_root=(
                            machine_config.get("destination_root")
                            or self._checksheet_destination_root
                        ),
                    ),
                )
                pipeline.run(source_path=file_path, watch_config=machine_config)
                self._increment_registry_counter(machine_id, success=True)
            else:
                LOGGER.warning(
                    f"No pipeline for type '{pipeline_type}'. Skip: '{file_path.name}'",
                    pipeline_type="SYSTEM",
                )
        except Exception as e:
            LOGGER.error(
                f"Pipeline error for '{file_path.name}': {e}",
                pipeline_type="SYSTEM",exc_info=True
            )
            self._increment_registry_counter(machine_id, success=False)

    def _increment_registry_counter(self, machine_id: str, success: bool):
        """Increments total_processed or total_failed in the machine registry."""
        column = "total_processed" if success else "total_failed"
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE fs3.etl_machine_registry SET {column} = {column} + 1, updated_at = NOW() WHERE machine_id = %s;",
                        (machine_id,),
                    )
        except Exception:
            pass

    def _register_machine(self, machine_config: dict):
        """Upserts a machine record into etl_machine_registry."""
        sql = """
            INSERT INTO fs3.etl_machine_registry
                (machine_id, machine_description, pipeline_type, source_path, watcher_started_at, is_enabled, updated_at)
            VALUES (%s, %s, %s, %s, NOW(), %s, NOW())
            ON CONFLICT (machine_id) DO UPDATE SET
                machine_description = EXCLUDED.machine_description, pipeline_type = EXCLUDED.pipeline_type,
                source_path = EXCLUDED.source_path, watcher_started_at = NOW(), is_enabled = EXCLUDED.is_enabled, updated_at = NOW();
        """
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            machine_config.get("machine_id"),
                            machine_config.get("description", ""),
                            machine_config.get("pipeline_type", "3D_HD"),
                            machine_config.get("source_path", ""),
                            machine_config.get("enabled", True),
                        ),
                    )
        except Exception as e:
            LOGGER.warning(
                f"Registry update failed: {e}",
                pipeline_type="SYSTEM",
            )

    def _heartbeat(self, machine_id: str):
        """Updates last_seen_at for a machine."""
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE fs3.etl_machine_registry SET last_seen_at = NOW(), updated_at = NOW() WHERE machine_id = %s;",
                        (machine_id,),
                    )
        except Exception:
            pass

    def _is_file_processed(self, file_name: str, watch_id: Optional[str] = None) -> bool:
        """Checks if a file is currently active or completed."""
        if watch_id:
            sql = """
                SELECT 1 FROM fs3.etl_pipeline_log
                WHERE file_name = %s AND watch_id = %s AND (status = 'COMPLETED' OR (status = 'PROCESSING' AND updated_at > NOW() - INTERVAL '30 minutes'))
                LIMIT 1;
            """
            params: tuple = (file_name, watch_id)
        else:
            sql = """
                SELECT 1 FROM fs3.etl_pipeline_log
                WHERE file_name = %s AND (status = 'COMPLETED' OR (status = 'PROCESSING' AND updated_at > NOW() - INTERVAL '30 minutes'))
                LIMIT 1;
            """
            params = (file_name,)

        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    return cur.fetchone() is not None
        except Exception:
            return False