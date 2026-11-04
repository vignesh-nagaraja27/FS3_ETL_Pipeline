# -*- coding: utf-8 -*-
"""
Job Runner — Elite Orchestration Supervisor (v2)
-------------------------------------------------
The engine that keeps the entire ETL system running.

All watcher lifecycle events, thread pool operations, heartbeats, and infrastructure
diagnostics are strictly routed to etl_system_master.log using pipeline_type="SYSTEM".
"""

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, cast

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

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

# File extensions we process. Anything else is ignored.
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".xlsm"}

# Subfolders created by the pipeline that should never be recursively scanned as sources
IGNORED_DIR_NAMES = {"_processing", "_completed", "_ng", "_archive", "_replay", ".git", "__pycache__"}

# How often the DLQ retry watcher polls for RETRYING entries (seconds)
DLQ_POLL_INTERVAL = 15

# How often configs (machines.json, checksheet.json) are checked for live reload (seconds)
CONFIG_RELOAD_INTERVAL = 2.0


def _fast_scan_files(root_dir: Path) -> Iterator[Tuple[Path, os.stat_result]]:
    """
    Lightweight, recursive directory scanner using os.scandir.
    Replaces Path.rglob('*') to avoid redundant Win32 stat() system calls
    and skips temporary Office lock files (~$*) and pipeline staging folders.
    """
    stack = [str(root_dir)]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as it:
                for entry in it:
                    name = entry.name
                    if name.startswith("~$") or name.startswith("."):
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if name.lower() not in IGNORED_DIR_NAMES:
                                stack.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            ext = os.path.splitext(name)[1].lower()
                            if ext in SUPPORTED_EXTENSIONS:
                                st = entry.stat(follow_symlinks=False)
                                yield Path(entry.path), st
                    except OSError:
                        continue
        except OSError:
            continue


class _WakeEventHandler(FileSystemEventHandler):
    """Triggers an immediate scan wake-up event when OS filesystem notifications fire."""

    def __init__(self, wake_event: threading.Event):
        super().__init__()
        self._wake_event = wake_event

    def on_any_event(self, event):
        if not getattr(event, "is_directory", False):
            self._wake_event.set()


class JobRunner:
    """
    The central supervisor that starts and coordinates all pipeline workers.
    """

    # Single-instance advisory lock ID to prevent two ETL processes from
    # competing on the same database and stealing files from each other.
    SINGLETON_ADVISORY_LOCK_ID = 839204531

    def __init__(self, config: ConfigLoader):
        """
        Args:
            config (ConfigLoader): The global configuration object loaded at startup.
        """
        self._config = config
        self._dispatcher = Dispatcher(config)
        self._stop_event = threading.Event()
        self._config_lock = threading.RLock()
        self._checksheet_watch_directories = self._load_checksheet_watch_directories()
        self._checksheet_destination_root = self._get_checksheet_destination_root()

        # Blocks measurement file processing while a conversion import is in progress.
        self._conversion_lock = threading.Event()
        self._conversion_lock.set()  # Unlocked by default

        # Tracks DLQ entry IDs already submitted to the executor in this session.
        self._submitted_dlq_ids: set = set()
        self._dlq_id_lock = threading.Lock()

        # Tracks files currently in-flight (queued or executing in worker pool)
        # so watcher loops remain 100% non-blocking without double-submitting.
        self._in_flight_files: Set[str] = set()
        self._in_flight_lock = threading.Lock()

        # Tracks active watcher threads by machine_id
        self._watcher_threads: Dict[str, threading.Thread] = {}
        self._singleton_conn: Optional[Any] = None

        self._pipeline_3d = Pipeline3D(destination_root=config.get_destination_root())

        # Dedicated thread pools per workload domain to prevent resource contention.
        # Compute worker pool size across all configured watchers (with a safe floor of 8)
        # so enabling machines dynamically via hot-reload never starves for threads.
        cmm_workers = sum(
            d.get("max_workers", 2)
            for d in config.get_watch_directories()
        )
        checksheet_workers = sum(
            d.get("max_workers", 2)
            for d in self._checksheet_watch_directories
        )

        self._cmm_executor = ThreadPoolExecutor(
            max_workers=max(cmm_workers, 8), thread_name_prefix="Exec-CMM"
        )
        self._checksheet_executor = ThreadPoolExecutor(
            max_workers=max(checksheet_workers, 8), thread_name_prefix="Exec-CS"
        )
        self._dlq_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="Exec-DLQ"
        )

    def _try_acquire_in_flight(self, file_key: str) -> bool:
        """Atomically marks a file path as in-flight; returns False if already in-flight."""
        with self._in_flight_lock:
            if file_key in self._in_flight_files:
                return False
            self._in_flight_files.add(file_key)
            return True

    def _release_in_flight(self, file_key: str) -> None:
        """Releases an in-flight file path after worker execution completes."""
        with self._in_flight_lock:
            self._in_flight_files.discard(file_key)

    def _get_machine_config(self, machine_id: str) -> Optional[dict]:
        """Returns the latest machine configuration for machine_id (thread-safe)."""
        with self._config_lock:
            for m in self._config.get_watch_directories():
                if str(m.get("machine_id")) == machine_id:
                    return dict(m)
        return None

    def _get_checksheet_machine_config(self, machine_id: str) -> Optional[dict]:
        """Returns the latest normalized checksheet configuration for machine_id (thread-safe)."""
        with self._config_lock:
            for m in self._checksheet_watch_directories:
                if str(m.get("machine_id")) == machine_id:
                    return dict(m)
        return None

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

    def _ensure_watcher_threads(self) -> None:
        """
        Starts watcher threads for any enabled machine or checksheet directory
        that does not already have a running thread. Called at startup and on
        live configuration reload.
        """
        with self._config_lock:
            machine_configs = list(self._config.get_watch_directories())
            cs_configs = list(self._checksheet_watch_directories)

        for machine_config in machine_configs:
            machine_id = str(machine_config.get("machine_id") or "").strip()
            if not machine_id:
                continue
            self._register_machine(machine_config)
            if not machine_config.get("enabled", True):
                if machine_id not in self._watcher_threads:
                    LOGGER.info(
                        f"Machine '{machine_id}' is DISABLED. Skipping.",
                        pipeline_type="SYSTEM",
                    )
                continue

            existing_thread = self._watcher_threads.get(machine_id)
            if existing_thread is not None and existing_thread.is_alive():
                continue

            if (
                machine_id == "KTS-7106"
                and machine_config.get("enr_block_enabled", False)
            ):
                try:
                    EnrBlockMasterStep().run(
                        DatabaseHandler(), machine_config, 0,
                        dict(machine="KTS-7106", model="E-NR", part="BL", pipeline_type="3D_BL"),
                    )
                except Exception as exc:
                    LOGGER.error(
                        f"Failed E-NR/BL master sync on startup/reload for KTS-7106: {exc}",
                        pipeline_type="SYSTEM",
                        exc_info=True,
                    )

            t = threading.Thread(
                target=self._watch_machine_folder,
                args=(machine_config,),
                name=f"Watcher-{machine_id}",
                daemon=True,
            )
            t.start()
            self._watcher_threads[machine_id] = t
            LOGGER.info(
                f"[MACHINE WATCHER START] {machine_id} -> {machine_config.get('source_path')}",
                pipeline_type="SYSTEM",
            )

        for machine_config in cs_configs:
            machine_id = str(machine_config.get("machine_id") or "").strip()
            if not machine_id:
                continue
            self._register_machine(machine_config)
            if not machine_config.get("enabled", True):
                if machine_id not in self._watcher_threads:
                    LOGGER.info(
                        f"Checksheet watcher '{machine_id}' is DISABLED. Skipping.",
                        pipeline_type="SYSTEM",
                    )
                continue

            existing_thread = self._watcher_threads.get(machine_id)
            if existing_thread is not None and existing_thread.is_alive():
                continue

            LOGGER.info(
                f"[CHECKSHEET WATCHER START] {machine_id} -> {machine_config.get('source_path')}",
                pipeline_type="SYSTEM",
            )
            t = threading.Thread(
                target=self._watch_checksheet_folder,
                args=(machine_config,),
                name=f"Watcher-Checksheet-{machine_id}",
                daemon=True,
            )
            t.start()
            self._watcher_threads[machine_id] = t

    def _reload_configs_if_needed(self) -> None:
        """Hot-reloads machines.json and checksheet.json if modified on disk."""
        changed = self._config.reload_if_changed()
        if not any(changed.values()):
            return

        with self._config_lock:
            if changed.get("checksheet.json"):
                self._checksheet_watch_directories = self._load_checksheet_watch_directories()
                self._checksheet_destination_root = self._get_checksheet_destination_root()
                LOGGER.info(
                    f"[CONFIG RELOAD] Reloaded checksheet.json ({len(self._checksheet_watch_directories)} watchers).",
                    pipeline_type="SYSTEM",
                )
            if changed.get("machines.json"):
                self._pipeline_3d = Pipeline3D(destination_root=self._config.get_destination_root())
                LOGGER.info(
                    f"[CONFIG RELOAD] Reloaded machines.json ({len(self._config.get_watch_directories())} machines).",
                    pipeline_type="SYSTEM",
                )

        self._ensure_watcher_threads()

    def _acquire_singleton_instance_lock(self) -> None:
        """
        Acquires a session-level PostgreSQL advisory lock so that only ONE
        ETL JobRunner instance can run against the configured database at a time.
        Also logs the exact target host and dbname at startup for full visibility.
        """
        import psycopg2

        db_cfg = self._config.get_db_config().get("db", {})
        host = db_cfg.get("host", "localhost")
        port = db_cfg.get("port", 5432)
        dbname = db_cfg.get("dbname", "")
        LOGGER.info(
            f"[DB TARGET] Connecting ETL JobRunner (PID={os.getpid()}) to "
            f"host={host}:{port} dbname='{dbname}'",
            pipeline_type="SYSTEM",
        )
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=db_cfg.get("user"),
                password=db_cfg.get("password"),
                application_name=f"FS3-ETL-Supervisor-PID-{os.getpid()}",
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_lock(%s);",
                    (self.SINGLETON_ADVISORY_LOCK_ID,),
                )
                row = cur.fetchone()
                acquired = bool(row and row[0])
                if not acquired:
                    cur.execute(
                        """
                        SELECT pid, client_addr, application_name, backend_start
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND pid IN (
                              SELECT pid FROM pg_locks
                              WHERE locktype = 'advisory'
                                AND objid = %s
                                AND granted = true
                          );
                        """,
                        (self.SINGLETON_ADVISORY_LOCK_ID,),
                    )
                    holder = cur.fetchone()
                    conn.close()
                    raise RuntimeError(
                        f"Another ETL JobRunner instance is already running against "
                        f"database '{dbname}' on {host}:{port} (lock holder={holder}). "
                        f"Stop the existing ETL process before starting a new one."
                    )
            self._singleton_conn = conn
            LOGGER.info(
                f"[SINGLETON LOCK ACQUIRED] Exclusive ETL supervisor lock acquired on dbname='{dbname}'.",
                pipeline_type="SYSTEM",
            )
        except RuntimeError:
            raise
        except Exception as exc:
            LOGGER.warning(
                f"Could not acquire advisory singleton lock: {exc}",
                pipeline_type="SYSTEM",
            )

    def start(self):
        """
        Starts all watcher threads and blocks until a shutdown signal is received.
        """
        LOGGER.info("=" * 60, pipeline_type="SYSTEM")
        LOGGER.info("ETL Job Runner starting up.", pipeline_type="SYSTEM")
        LOGGER.info("=" * 60, pipeline_type="SYSTEM")

        # Ensure only one ETL JobRunner instance runs against this database
        self._acquire_singleton_instance_lock()

        # STARTUP CLEANUP: Reset stale PROCESSING rows
        self._cleanup_stale_tasks()

        # Start machine & checksheet watcher threads (and register all in DB)
        self._ensure_watcher_threads()

        # Start the Checksheet database-backed retry recovery watcher thread
        t_cs_retry = threading.Thread(
            target=self._watch_checksheet_retries,
            name="Watcher-Checksheet-Retry",
            daemon=True,
        )
        t_cs_retry.start()

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

        # Start the DLQ retry watcher thread
        t_dlq = threading.Thread(
            target=self._watch_dlq_retrying, name="Watcher-DLQ-Retry", daemon=True
        )
        t_dlq.start()

        last_config_check = time.monotonic()
        try:
            while not self._stop_event.is_set():
                time.sleep(1)
                if (time.monotonic() - last_config_check) >= CONFIG_RELOAD_INTERVAL:
                    last_config_check = time.monotonic()
                    self._reload_configs_if_needed()
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
        if self._singleton_conn is not None:
            try:
                self._singleton_conn.close()
            except Exception:
                pass
            self._singleton_conn = None
        DatabaseHandler().close_all()
        LOGGER.info("ETL Job Runner shut down cleanly.", pipeline_type="SYSTEM")

    def _cleanup_stale_tasks(self):
        """Resets rows stuck in PROCESSING to STALE on startup and during periodic health checks."""
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

    def _watch_checksheet_retries(self):
        """Polls fs3.pre_l_measurement_processing_lock for pending retries and stale processing tasks across restarts."""
        while not self._stop_event.is_set():
            try:
                self._cleanup_stale_tasks()
                db = DatabaseHandler()
                with db.get_connection() as conn:
                    lock_repo = ProcessingLockRepository(conn)
                    pending_list = lock_repo.get_pending_retries()
                    stale_list = lock_repo.get_stale_processing(stale_minutes=30)

                with self._config_lock:
                    cs_dirs = list(self._checksheet_watch_directories)

                for item in pending_list:
                    file_name = item.get("file_name")
                    file_hash = item.get("file_hash")
                    retry_count = item.get("retry_count", 0)
                    watch_id = item.get("watch_id")

                    if not file_name:
                        continue

                    target_config = None
                    target_file_path = None
                    for machine_config in cs_dirs:
                        if not machine_config.get("enabled", True):
                            continue
                        if watch_id and machine_config.get("watch_id") != watch_id:
                            continue
                        src_dir = Path(machine_config["source_path"])
                        candidate = src_dir / str(file_name)
                        if candidate.exists():
                            target_config = machine_config
                            target_file_path = candidate
                            break

                    if not target_config or not target_file_path:
                        continue

                    file_key = str(target_file_path)
                    if not self._try_acquire_in_flight(file_key):
                        continue

                    pipeline_type, part_config = self._dispatcher.classify(target_file_path)
                    if pipeline_type != PIPELINE_CHECKSHEET or part_config is None:
                        self._release_in_flight(file_key)
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
                        file_key,
                    )

                for item in stale_list:
                    file_name = item.get("file_name")
                    file_hash = item.get("file_hash")
                    watch_id = item.get("watch_id")

                    if not file_name:
                        continue

                    target_config = None
                    target_file_path = None
                    for machine_config in cs_dirs:
                        if not machine_config.get("enabled", True):
                            continue
                        if watch_id and machine_config.get("watch_id") != watch_id:
                            continue
                        src_dir = Path(machine_config["source_path"])
                        candidate = src_dir / str(file_name)
                        if candidate.exists():
                            target_config = machine_config
                            target_file_path = candidate
                            break

                    if not target_config or not target_file_path:
                        continue

                    file_key = str(target_file_path)
                    if not self._try_acquire_in_flight(file_key):
                        continue

                    pipeline_type, part_config = self._dispatcher.classify(target_file_path)
                    if pipeline_type != PIPELINE_CHECKSHEET or part_config is None:
                        self._release_in_flight(file_key)
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
                        file_key,
                    )
            except Exception as e:
                LOGGER.error(
                    f"[CHECKSHEET-RETRY] Watcher error: {e}",
                    pipeline_type="SYSTEM",
                    exc_info=True,
                )

            self._stop_event.wait(15)

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

                    machine_config = self._get_machine_config(str(machine_id))
                    if not machine_config:
                        continue

                    file_path = Path(machine_config["source_path"]) / str(file_name)
                    if not file_path.exists():
                        continue

                    pipeline_type, part_config = self._dispatcher.classify_from_watcher(
                        file_path=file_path, watch_config=machine_config
                    )
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
                LOGGER.error(f"[DLQ-WATCHER] Error: {e}", pipeline_type="SYSTEM", exc_info=True)

            self._stop_event.wait(DLQ_POLL_INTERVAL)

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
                pipeline_type="SYSTEM",
                exc_info=True,
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

    def _start_fs_observer(self, source_path: Path, wake_event: threading.Event) -> Optional[Any]:
        """Starts a watchdog Observer on source_path if accessible, waking the poller immediately on file events."""
        if not source_path.exists():
            return None
        try:
            observer = Observer()
            observer.daemon = True
            observer.schedule(_WakeEventHandler(wake_event), str(source_path), recursive=True)
            observer.start()
            return observer
        except Exception:
            return None

    def _watch_machine_folder(self, initial_machine_config: dict):
        """
        Hybrid event-driven + fast-polling watcher for one CMM machine's source folder.
        - Uses watchdog OS events to wake up immediately (<1s) when local files arrive.
        - Uses lightweight os.scandir polling every poll_interval_sec for network/SMB reliability.
        - Tracks file state by (size, mtime_ns) rather than permanent path strings so
          overwritten, re-dropped, or retried files are always picked up immediately.
        """
        machine_id = str(initial_machine_config["machine_id"])
        # Maps file_key -> (st_size, st_mtime_ns) of files known to be processed or permanently skipped
        seen_file_states: Dict[str, Tuple[int, int]] = {}
        wake_event = threading.Event()
        observer: Optional[Any] = None
        observed_path: Optional[Path] = None

        try:
            while not self._stop_event.is_set():
                machine_config = self._get_machine_config(machine_id) or initial_machine_config
                if not machine_config.get("enabled", True):
                    if observer is not None:
                        try:
                            observer.stop()
                        except Exception:
                            pass
                        observer = None
                        observed_path = None
                    self._stop_event.wait(2.0)
                    continue

                source_path = Path(machine_config["source_path"])
                poll_interval = float(machine_config.get("poll_interval_sec", 5))
                ignore_prefixes = machine_config.get("ignore_file_prefixes", [])

                try:
                    self._heartbeat(machine_id)
                    if not source_path.exists():
                        if observer is not None:
                            try:
                                observer.stop()
                            except Exception:
                                pass
                            observer = None
                            observed_path = None
                        wake_event.wait(timeout=poll_interval)
                        wake_event.clear()
                        continue

                    if observer is None or observed_path != source_path:
                        if observer is not None:
                            try:
                                observer.stop()
                            except Exception:
                                pass
                        observer = self._start_fs_observer(source_path, wake_event)
                        observed_path = source_path

                    current_files_on_disk: Set[str] = set()

                    for file_path, st in _fast_scan_files(source_path):
                        file_key = str(file_path)
                        current_files_on_disk.add(file_key)
                        file_sig = (st.st_size, st.st_mtime_ns)

                        # Skip if currently in-flight in the worker pool
                        with self._in_flight_lock:
                            if file_key in self._in_flight_files:
                                continue

                        # Skip if this exact (size, mtime_ns) was already processed or skipped
                        if seen_file_states.get(file_key) == file_sig:
                            continue

                        is_enr_block = (
                            machine_id == "KTS-7106"
                            and machine_config.get("enr_block_enabled", False)
                            and file_path.name.startswith("E-NR")
                        )
                        if is_enr_block:
                            try:
                                parse_enr_filename(file_path.name)
                            except ValueError as exc:
                                seen_file_states[file_key] = file_sig
                                LOGGER.warning(
                                    f"[MC:{machine_id}] E-NR-BLOCK file skipped: "
                                    f"{file_path.name}: {exc}",
                                    pipeline_type="SYSTEM",
                                )
                                continue

                        # Ignore configured filename prefixes
                        if (
                            any(
                                file_path.name.upper().startswith(prefix.upper())
                                for prefix in ignore_prefixes
                            )
                            and not is_enr_block
                        ):
                            seen_file_states[file_key] = file_sig
                            LOGGER.info(
                                f"[MC:{machine_id}] Ignoring file by prefix rule: {file_path.name}",
                                pipeline_type="SYSTEM",
                            )
                            continue

                        # Check if this exact (file_name, file_size) was already processed
                        if self._is_file_processed(
                            file_path.name,
                            watch_id=machine_id,
                            file_size=st.st_size,
                        ):
                            seen_file_states[file_key] = file_sig
                            continue

                        pipeline_type, part_config = self._dispatcher.classify_from_watcher(
                            file_path=file_path,
                            watch_config=machine_config,
                        )

                        if pipeline_type == PIPELINE_UNKNOWN or part_config is None:
                            # Mark current signature so we don't spam warnings every poll unless file changes
                            seen_file_states[file_key] = file_sig
                            continue

                        if not self._conversion_lock.is_set():
                            self._conversion_lock.wait()

                        if not self._try_acquire_in_flight(file_key):
                            continue

                        LOGGER.info(
                            f"[CMM JOB SUBMITTED] {file_path.name} -> {pipeline_type} [{machine_id}]",
                            pipeline_type="SYSTEM",
                        )
                        self._cmm_executor.submit(
                            self._run_pipeline_safe,
                            file_path,
                            pipeline_type,
                            part_config,
                            machine_config,
                            file_key,
                        )

                    # Prune entries that were moved/deleted from source folder so re-dropped files are fresh
                    if len(seen_file_states) > len(current_files_on_disk):
                        for gone_key in list(seen_file_states.keys()):
                            if gone_key not in current_files_on_disk:
                                seen_file_states.pop(gone_key, None)

                except Exception as e:
                    LOGGER.error(
                        f"[MC:{machine_id}] Watcher error: {e}",
                        pipeline_type="SYSTEM",
                        exc_info=True,
                    )

                wake_event.wait(timeout=poll_interval)
                wake_event.clear()
        finally:
            if observer is not None:
                try:
                    observer.stop()
                except Exception:
                    pass

    def _watch_checksheet_folder(self, initial_machine_config: dict):
        """
        Non-blocking hybrid watcher for a checksheet folder.
        - Uses os.scandir stat metadata (st_size, st_mtime_ns) to immediately detect
          newly added or modified workbooks without waiting for change_check_interval_minutes.
        - Offloads the 20-30s stability check and SHA256 hashing to the worker pool
          (ChecksheetPipeline.run) so the watcher thread never blocks and picks up new
          files within 1-2 seconds.
        """
        machine_id = str(initial_machine_config["machine_id"])
        # Maps file_key -> (st_size, st_mtime_ns, last_checked_epoch)
        known_file_stats: Dict[str, Tuple[int, int, float]] = {}
        wake_event = threading.Event()
        observer: Optional[Any] = None
        observed_path: Optional[Path] = None

        try:
            while not self._stop_event.is_set():
                machine_config = (
                    self._get_checksheet_machine_config(machine_id)
                    or initial_machine_config
                )
                if not machine_config.get("enabled", True):
                    if observer is not None:
                        try:
                            observer.stop()
                        except Exception:
                            pass
                        observer = None
                        observed_path = None
                    self._stop_event.wait(2.0)
                    continue

                source_path = Path(machine_config["source_path"])
                raw_watch = str(machine_config.get("watch_id") or machine_id)
                watch_id = (
                    raw_watch[11:]
                    if raw_watch.upper().startswith("CHECKSHEET_")
                    else raw_watch
                )
                poll_interval = float(machine_config.get("poll_interval_sec", 2))
                change_check_interval_sec = (
                    int(machine_config.get("change_check_interval_minutes", 5)) * 60
                )

                try:
                    self._heartbeat(machine_id)
                    if not source_path.exists():
                        if observer is not None:
                            try:
                                observer.stop()
                            except Exception:
                                pass
                            observer = None
                            observed_path = None
                        wake_event.wait(timeout=poll_interval)
                        wake_event.clear()
                        continue

                    if observer is None or observed_path != source_path:
                        if observer is not None:
                            try:
                                observer.stop()
                            except Exception:
                                pass
                        observer = self._start_fs_observer(source_path, wake_event)
                        observed_path = source_path

                    now = time.time()
                    current_files_on_disk: Set[str] = set()

                    for file_path, st in _fast_scan_files(source_path):
                        file_key = str(file_path)
                        current_files_on_disk.add(file_key)

                        # Skip if currently in-flight in the checksheet worker pool
                        with self._in_flight_lock:
                            if file_key in self._in_flight_files:
                                continue

                        prev = known_file_stats.get(file_key)
                        # Trigger immediate check if:
                        #   1. File is brand new (prev is None)
                        #   2. File size or mtime_ns changed on disk (instant edit detection!)
                        #   3. Periodic fallback timer (change_check_interval_sec) elapsed
                        if prev is not None:
                            prev_size, prev_mtime_ns, last_check = prev
                            if (
                                st.st_size == prev_size
                                and st.st_mtime_ns == prev_mtime_ns
                                and (now - last_check) < change_check_interval_sec
                            ):
                                continue

                        detected_config = self._config.match_checksheet_part(file_path.name)
                        if detected_config is None:
                            if prev is None:
                                LOGGER.warning(
                                    f"[CHECKSHEET WATCHER] Ignoring non-checksheet file '{file_path.name}' in checksheet watch folder.",
                                    pipeline_type="SYSTEM",
                                )
                            known_file_stats[file_key] = (st.st_size, st.st_mtime_ns, now)
                            continue

                        try:
                            current_hash = generate_sha256(file_path)
                        except Exception:
                            continue

                        db = DatabaseHandler()
                        with db.get_connection() as conn:
                            fv_repo = FileVersionRepository(conn)
                            latest_processed = fv_repo.get_latest_processed_for_file(
                                file_path.name, watch_id
                            )
                            lock_repo = ProcessingLockRepository(conn)
                            lock_status = lock_repo.get_latest_status_by_hash(current_hash)

                        # Check if hash is already PROCESSED
                        if (
                            latest_processed
                            and latest_processed.get("file_hash") == current_hash
                        ):
                            known_file_stats[file_key] = (st.st_size, st.st_mtime_ns, now)
                            LOGGER.info(
                                f"[HASH UNCHANGED] watch_id={watch_id} file={file_path.name}",
                                pipeline_type="CHECKSHEET",
                            )
                            continue

                        # If file is currently RETRY_PENDING or AWAITING_REVIEW for this exact hash, skip
                        if lock_status and lock_status.get("processing_status") in (
                            "RETRY_PENDING",
                            "AWAITING_REVIEW",
                        ):
                            known_file_stats[file_key] = (st.st_size, st.st_mtime_ns, now)
                            continue

                        pipeline_type, part_config = self._dispatcher.classify_from_watcher(
                            file_path=file_path,
                            watch_config=machine_config,
                        )
                        if pipeline_type != PIPELINE_CHECKSHEET or part_config is None:
                            known_file_stats[file_key] = (st.st_size, st.st_mtime_ns, now)
                            continue

                        if not self._try_acquire_in_flight(file_key):
                            continue

                        old_hash = (
                            latest_processed.get("file_hash")
                            if latest_processed
                            else "NONE"
                        )
                        LOGGER.info(
                            f"[CHANGE DETECTED] watch_id={watch_id} file={file_path.name} "
                            f"old_hash={old_hash} new_hash={current_hash}",
                            pipeline_type="CHECKSHEET",
                        )

                        # Record DISCOVERED and SUBMITTED lifecycle states; stability check
                        # executes inside ChecksheetPipeline.run on the worker thread so
                        # the watcher thread never blocks!
                        with db.get_connection() as conn:
                            ProcessingLockRepository(conn).mark_discovered(
                                current_hash,
                                file_path.name,
                                watch_id=watch_id,
                            )
                            dl_repo = ChecksheetDownloadRepository(conn)
                            lifecycle_record = dl_repo.register_detected(
                                file_path,
                                vehicle_model=detected_config.get("vehicle_model"),
                                parts_code=detected_config.get("parts_code"),
                                op_no=detected_config.get("op_no"),
                                file_size=st.st_size,
                            )
                            lifecycle_id = int(lifecycle_record["id"])
                            dl_repo.mark_submitted(lifecycle_id)

                        known_file_stats[file_key] = (st.st_size, st.st_mtime_ns, now)
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
                            file_key,
                        )

                    # Prune deleted files from known_file_stats
                    if len(known_file_stats) > len(current_files_on_disk):
                        for gone_key in list(known_file_stats.keys()):
                            if gone_key not in current_files_on_disk:
                                known_file_stats.pop(gone_key, None)

                except Exception as exc:
                    LOGGER.error(
                        f"[CHECKSHEET WATCHER] Error for '{machine_id}': {exc}",
                        pipeline_type="SYSTEM",
                        exc_info=True,
                    )

                wake_event.wait(timeout=poll_interval)
                wake_event.clear()
        finally:
            if observer is not None:
                try:
                    observer.stop()
                except Exception:
                    pass

    def _watch_conversion_folder(self, conv_config: dict):
        """Monitors the conversion table Excel folder."""
        conv_path = Path(conv_config["path"])
        poll_interval = conv_config.get("poll_interval_sec", 10)
        seen_files: Dict[str, Tuple[int, int]] = {}

        while not self._stop_event.is_set():
            try:
                if not conv_path.exists():
                    self._stop_event.wait(poll_interval)
                    continue

                for file_path in conv_path.glob("*.xlsx"):
                    if file_path.name.startswith("~$"):
                        continue
                    try:
                        st = file_path.stat()
                    except OSError:
                        continue
                    sig = (st.st_size, st.st_mtime_ns)
                    if seen_files.get(str(file_path)) == sig:
                        continue

                    seen_files[str(file_path)] = sig
                    self._conversion_lock.clear()
                    try:
                        self._import_conversion_table(file_path)
                    finally:
                        self._conversion_lock.set()
            except Exception as e:
                LOGGER.error(f"[CONVERSION] Error: {e}", pipeline_type="SYSTEM", exc_info=True)
                self._conversion_lock.set()

            self._stop_event.wait(poll_interval)

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
                pipeline_type="SYSTEM",
                exc_info=True,
            )
            raise

    def _run_pipeline_safe(
        self,
        file_path: Path,
        pipeline_type: str,
        part_config: Dict[Any, Any],
        machine_config: dict,
        in_flight_key: Optional[str] = None,
    ):
        """Catches top-level exceptions from a normal pipeline run and releases in-flight lock."""
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
                pipeline_type="SYSTEM",
                exc_info=True,
            )
            self._increment_registry_counter(machine_id, success=False)
        finally:
            if in_flight_key:
                self._release_in_flight(in_flight_key)

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

    def _is_file_processed(
        self,
        file_name: str,
        watch_id: Optional[str] = None,
        file_size: Optional[int] = None,
    ) -> bool:
        """
        Checks if a file is currently active or already completed for the given machine/watcher
        and file_size. Including watch_id and file_size ensures modified or re-dropped files
        are not falsely ignored.
        """
        conditions = ["file_name = %s"]
        params: list = [file_name]

        if watch_id:
            conditions.append("(watch_id = %s OR machine_id = %s)")
            params.extend([watch_id, watch_id])

        if file_size is not None:
            conditions.append("(file_size = %s OR status = 'PROCESSING')")
            params.append(file_size)

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT 1 FROM fs3.etl_pipeline_log
            WHERE {where_clause}
              AND (status = 'COMPLETED' OR (status = 'PROCESSING' AND updated_at > NOW() - INTERVAL '30 minutes'))
            LIMIT 1;
        """

        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, tuple(params))
                    return cur.fetchone() is not None
        except Exception:
            return False