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

# How often the active mode's configuration is checked for live reload (seconds)
CONFIG_RELOAD_INTERVAL = 2.0

# A process owns exactly one workload mode for its entire lifetime.
RUNNER_MODE_3D = "3D"
RUNNER_MODE_CHECKSHEET = "CHECKSHEET"
VALID_RUNNER_MODES = frozenset({RUNNER_MODE_3D, RUNNER_MODE_CHECKSHEET})

# Fixed lock IDs are stable across Python processes and interpreter restarts.
# Different IDs allow one 3D runner and one Checksheet runner to run together.
SINGLETON_ADVISORY_LOCK_IDS = {
    RUNNER_MODE_3D: 839204531,
    RUNNER_MODE_CHECKSHEET: 839204532,
}

PIPELINE_TYPES_BY_MODE = {
    RUNNER_MODE_3D: (PIPELINE_3D_HD, PIPELINE_3D_BL, PIPELINE_3D_CR),
    RUNNER_MODE_CHECKSHEET: (PIPELINE_CHECKSHEET,),
}

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
    """Supervises exactly one ETL workload: 3D or Checksheet."""

    def __init__(self, config: ConfigLoader, runner_mode: str):
        """
        Args:
            config: Global ETL configuration loaded at process startup.
            runner_mode: Workload owned by this process: "3D" or "CHECKSHEET".
        """
        normalized_mode = str(runner_mode).strip().upper()
        if normalized_mode not in VALID_RUNNER_MODES:
            raise ValueError(
                f"Invalid runner mode '{runner_mode}'. "
                f"Expected one of: {sorted(VALID_RUNNER_MODES)}"
            )

        self._runner_mode = normalized_mode
        self._config = config
        self._dispatcher = Dispatcher(config)
        self._stop_event = threading.Event()
        self._stop_lock = threading.Lock()
        self._stopped = False
        self._config_lock = threading.RLock()

        # Only the Checksheet process loads and owns Checksheet watcher state.
        if self._runner_mode == RUNNER_MODE_CHECKSHEET:
            self._checksheet_watch_directories = self._load_checksheet_watch_directories()
            self._checksheet_destination_root = self._get_checksheet_destination_root()
        else:
            self._checksheet_watch_directories = []
            self._checksheet_destination_root = self._config.get_destination_root()

        # 3D measurement processing pauses while a conversion import is active.
        self._conversion_lock = threading.Event()
        self._conversion_lock.set()

        # DLQ state is used only by the 3D process.
        self._submitted_dlq_ids: Set[int] = set()
        self._dlq_id_lock = threading.Lock()

        # In-flight paths are local to this process and therefore to this mode.
        self._in_flight_files: Set[str] = set()
        self._in_flight_lock = threading.Lock()

        # Watcher threads are local to this process and contain one mode only.
        self._watcher_threads: Dict[str, threading.Thread] = {}
        self._singleton_conn: Optional[Any] = None

        self._pipeline_3d: Optional[Pipeline3D] = None
        self._cmm_executor: Optional[ThreadPoolExecutor] = None
        self._checksheet_executor: Optional[ThreadPoolExecutor] = None
        self._dlq_executor: Optional[ThreadPoolExecutor] = None

        if self._runner_mode == RUNNER_MODE_3D:
            self._pipeline_3d = Pipeline3D(
                destination_root=self._config.get_destination_root()
            )
            cmm_workers = sum(
                directory.get("max_workers", 2)
                for directory in self._config.get_watch_directories()
            )
            self._cmm_executor = ThreadPoolExecutor(
                max_workers=max(cmm_workers, 8),
                thread_name_prefix="Exec-CMM",
            )
            self._dlq_executor = ThreadPoolExecutor(
                max_workers=2,
                thread_name_prefix="Exec-DLQ",
            )
        else:
            checksheet_workers = sum(
                directory.get("max_workers", 2)
                for directory in self._checksheet_watch_directories
            )
            self._checksheet_executor = ThreadPoolExecutor(
                max_workers=max(checksheet_workers, 8),
                thread_name_prefix="Exec-CS",
            )

    def _require_mode(self, expected_mode: str, operation: str) -> None:
        """Rejects accidental cross-mode service execution."""
        if self._runner_mode != expected_mode:
            raise RuntimeError(
                f"{operation} is only valid in {expected_mode} mode; "
                f"current mode is {self._runner_mode}."
            )

    def _get_cmm_executor(self) -> ThreadPoolExecutor:
        """Returns the 3D executor or fails if this is not the 3D process."""
        self._require_mode(RUNNER_MODE_3D, "3D executor access")
        if self._cmm_executor is None:
            raise RuntimeError("The 3D executor is not initialized.")
        return self._cmm_executor

    def _get_checksheet_executor(self) -> ThreadPoolExecutor:
        """Returns the Checksheet executor or fails outside Checksheet mode."""
        self._require_mode(RUNNER_MODE_CHECKSHEET, "Checksheet executor access")
        if self._checksheet_executor is None:
            raise RuntimeError("The Checksheet executor is not initialized.")
        return self._checksheet_executor

    def _get_dlq_executor(self) -> ThreadPoolExecutor:
        """Returns the 3D DLQ executor or fails if it is unavailable."""
        self._require_mode(RUNNER_MODE_3D, "DLQ executor access")
        if self._dlq_executor is None:
            raise RuntimeError("The 3D DLQ executor is not initialized.")
        return self._dlq_executor

    def _get_pipeline_3d(self) -> Pipeline3D:
        """Returns the 3D pipeline or fails outside the 3D process."""
        self._require_mode(RUNNER_MODE_3D, "3D pipeline access")
        if self._pipeline_3d is None:
            raise RuntimeError("The 3D pipeline is not initialized.")
        return self._pipeline_3d

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
        """Starts watcher threads belonging to this process mode only."""
        if self._runner_mode == RUNNER_MODE_3D:
            self._ensure_3d_watcher_threads()
        elif self._runner_mode == RUNNER_MODE_CHECKSHEET:
            self._ensure_checksheet_watcher_threads()
        else:
            raise RuntimeError(f"Unsupported runner mode: {self._runner_mode}")

    def _ensure_3d_watcher_threads(self) -> None:
        """Starts enabled 3D machine watchers that are not already running."""
        self._require_mode(RUNNER_MODE_3D, "3D watcher startup")
        with self._config_lock:
            machine_configs = list(self._config.get_watch_directories())

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
                        DatabaseHandler(),
                        machine_config,
                        0,
                        {
                            "machine": "KTS-7106",
                            "model": "E-NR",
                            "part": "BL",
                            "pipeline_type": PIPELINE_3D_BL,
                        },
                    )
                except Exception as exc:
                    LOGGER.error(
                        f"Failed E-NR/BL master sync on startup/reload for "
                        f"KTS-7106: {exc}",
                        pipeline_type="SYSTEM",
                        exc_info=True,
                    )

            watcher_thread = threading.Thread(
                target=self._watch_machine_folder,
                args=(machine_config,),
                name=f"Watcher-{machine_id}",
                daemon=True,
            )
            watcher_thread.start()
            self._watcher_threads[machine_id] = watcher_thread
            LOGGER.info(
                f"[3D WATCHER START] {machine_id} -> "
                f"{machine_config.get('source_path')}",
                pipeline_type="SYSTEM",
            )

    def _ensure_checksheet_watcher_threads(self) -> None:
        """Starts enabled Checksheet watchers that are not already running."""
        self._require_mode(RUNNER_MODE_CHECKSHEET, "Checksheet watcher startup")
        with self._config_lock:
            machine_configs = list(self._checksheet_watch_directories)

        for machine_config in machine_configs:
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
                f"[CHECKSHEET WATCHER START] {machine_id} -> "
                f"{machine_config.get('source_path')}",
                pipeline_type="SYSTEM",
            )
            watcher_thread = threading.Thread(
                target=self._watch_checksheet_folder,
                args=(machine_config,),
                name=f"Watcher-Checksheet-{machine_id}",
                daemon=True,
            )
            watcher_thread.start()
            self._watcher_threads[machine_id] = watcher_thread

    def _reload_configs_if_needed(self) -> None:
        """Applies live configuration changes for this process mode only."""
        changed = self._config.reload_if_changed()
        if not changed or not any(changed.values()):
            return

        relevant_change = False
        with self._config_lock:
            if (
                self._runner_mode == RUNNER_MODE_CHECKSHEET
                and changed.get("checksheet.json")
            ):
                self._checksheet_watch_directories = (
                    self._load_checksheet_watch_directories()
                )
                self._checksheet_destination_root = (
                    self._get_checksheet_destination_root()
                )
                relevant_change = True
                LOGGER.info(
                    f"[CONFIG RELOAD] Reloaded checksheet.json "
                    f"({len(self._checksheet_watch_directories)} watchers).",
                    pipeline_type="SYSTEM",
                )
            elif (
                self._runner_mode == RUNNER_MODE_3D
                and changed.get("machines.json")
            ):
                self._pipeline_3d = Pipeline3D(
                    destination_root=self._config.get_destination_root()
                )
                relevant_change = True
                LOGGER.info(
                    f"[CONFIG RELOAD] Reloaded machines.json "
                    f"({len(self._config.get_watch_directories())} machines).",
                    pipeline_type="SYSTEM",
                )

        if relevant_change:
            self._ensure_watcher_threads()

    def _acquire_singleton_instance_lock(self) -> None:
        """
        Acquires the PostgreSQL advisory lock assigned to this process mode.

        One 3D process and one Checksheet process may run concurrently, while a
        duplicate process for either mode is rejected. Lock failures are fatal:
        processing never starts unless this process proves lock ownership.
        """
        import psycopg2

        db_cfg = self._config.get_db_config().get("db", {})
        host = db_cfg.get("host", "localhost")
        port = db_cfg.get("port", 5432)
        dbname = db_cfg.get("dbname", "")
        lock_id = SINGLETON_ADVISORY_LOCK_IDS[self._runner_mode]
        application_name = (
            f"FS3-ETL-{self._runner_mode}-Supervisor-PID-{os.getpid()}"
        )

        LOGGER.info(
            f"[DB TARGET] Connecting {self._runner_mode} JobRunner "
            f"(PID={os.getpid()}, lock_id={lock_id}) to "
            f"host={host}:{port} dbname='{dbname}'",
            pipeline_type="SYSTEM",
        )

        conn: Optional[Any] = None
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=db_cfg.get("user"),
                password=db_cfg.get("password"),
                application_name=application_name,
            )
            conn.autocommit = True

            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s);", (lock_id,))
                row = cur.fetchone()
                acquired = bool(row and row[0])

                if not acquired:
                    cur.execute(
                        """
                        SELECT
                            a.pid,
                            a.client_addr,
                            a.application_name,
                            a.backend_start
                        FROM pg_stat_activity AS a
                        INNER JOIN pg_locks AS l ON l.pid = a.pid
                        WHERE a.datname = current_database()
                          AND l.locktype = 'advisory'
                          AND l.objid = %s
                          AND l.granted = true;
                        """,
                        (lock_id,),
                    )
                    holder = cur.fetchone()
                    raise RuntimeError(
                        f"Another {self._runner_mode} ETL JobRunner instance is "
                        f"already running against database '{dbname}' on "
                        f"{host}:{port} (lock_id={lock_id}, holder={holder}). "
                        f"Stop the existing {self._runner_mode} process before "
                        f"starting another one."
                    )

            self._singleton_conn = conn
            LOGGER.info(
                f"[SINGLETON LOCK ACQUIRED] mode={self._runner_mode} "
                f"lock_id={lock_id} dbname='{dbname}'.",
                pipeline_type="SYSTEM",
            )
        except Exception:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            raise

    def start(self) -> None:
        """Starts this process mode and blocks until shutdown."""
        LOGGER.info("=" * 60, pipeline_type="SYSTEM")
        LOGGER.info(
            f"ETL Job Runner starting. Mode={self._runner_mode}",
            pipeline_type="SYSTEM",
        )
        LOGGER.info("=" * 60, pipeline_type="SYSTEM")

        try:
            self._acquire_singleton_instance_lock()
            self._cleanup_stale_tasks()
            self._ensure_watcher_threads()

            if self._runner_mode == RUNNER_MODE_CHECKSHEET:
                checksheet_retry_thread = threading.Thread(
                    target=self._watch_checksheet_retries,
                    name="Watcher-Checksheet-Retry",
                    daemon=True,
                )
                checksheet_retry_thread.start()
            else:
                conversion_config = self._config.get_conversion_watch()
                if conversion_config.get("enabled", True):
                    conversion_thread = threading.Thread(
                        target=self._watch_conversion_folder,
                        args=(conversion_config,),
                        name="Watcher-ConversionTable",
                        daemon=True,
                    )
                    conversion_thread.start()

                dlq_thread = threading.Thread(
                    target=self._watch_dlq_retrying,
                    name="Watcher-DLQ-Retry",
                    daemon=True,
                )
                dlq_thread.start()

            last_config_check = time.monotonic()
            while not self._stop_event.is_set():
                self._stop_event.wait(1.0)
                if (
                    time.monotonic() - last_config_check
                    >= CONFIG_RELOAD_INTERVAL
                ):
                    last_config_check = time.monotonic()
                    self._reload_configs_if_needed()
        except KeyboardInterrupt:
            LOGGER.info(
                f"Shutdown signal received for mode={self._runner_mode}.",
                pipeline_type="SYSTEM",
            )
        finally:
            self.stop()

    def stop(self) -> None:
        """Stops this mode's threads, executors, database pool, and advisory lock."""
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True

        self._stop_event.set()

        for executor in (
            self._cmm_executor,
            self._checksheet_executor,
            self._dlq_executor,
        ):
            if executor is not None:
                executor.shutdown(wait=True)

        if self._singleton_conn is not None:
            try:
                self._singleton_conn.close()
            except Exception:
                pass
            self._singleton_conn = None

        DatabaseHandler().close_all()
        LOGGER.info(
            f"ETL Job Runner shut down cleanly. Mode={self._runner_mode}",
            pipeline_type="SYSTEM",
        )

    def _cleanup_stale_tasks(self) -> None:
        """Marks only this mode's abandoned PROCESSING rows as STALE."""
        pipeline_types = PIPELINE_TYPES_BY_MODE[self._runner_mode]
        placeholders = ", ".join(["%s"] * len(pipeline_types))
        sql = f"""
            UPDATE fs3.etl_pipeline_log
            SET status       = 'STALE',
                current_step = 'STALE_RESET',
                updated_at   = NOW()
            WHERE status = 'PROCESSING'
              AND updated_at < NOW() - INTERVAL '30 minutes'
              AND pipeline_type IN ({placeholders})
            RETURNING id, file_name;
        """
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, pipeline_types)
                    stale_rows = cur.fetchall()

            if stale_rows:
                LOGGER.warning(
                    f"STALE CLEANUP: mode={self._runner_mode} reset "
                    f"{len(stale_rows)} stale records.",
                    pipeline_type="SYSTEM",
                )
        except Exception as exc:
            LOGGER.warning(
                f"STALE CLEANUP: mode={self._runner_mode} failed: {exc}",
                pipeline_type="SYSTEM",
            )

    def _watch_checksheet_retries(self):
        """Polls Checksheet retries and stale tasks across process restarts."""
        self._require_mode(RUNNER_MODE_CHECKSHEET, "Checksheet retry watcher")
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

                    self._get_checksheet_executor().submit(
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

                    self._get_checksheet_executor().submit(
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
        """Polls the 3D DLQ for RETRYING entries."""
        self._require_mode(RUNNER_MODE_3D, "3D DLQ watcher")
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

                        self._get_dlq_executor().submit(
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
        """Wrapper for 3D DLQ retry pipeline runs."""
        self._require_mode(RUNNER_MODE_3D, "3D DLQ retry execution")
        if pipeline_type not in PIPELINE_TYPES_BY_MODE[RUNNER_MODE_3D]:
            raise RuntimeError(
                f"DLQ pipeline type '{pipeline_type}' is not a 3D pipeline."
            )
        machine_id = str(machine_config.get("machine_id", "UNKNOWN"))
        try:
            self._get_pipeline_3d().run(
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
        self._require_mode(RUNNER_MODE_3D, "3D machine watcher")
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
                        self._get_cmm_executor().submit(
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
        self._require_mode(RUNNER_MODE_CHECKSHEET, "Checksheet folder watcher")
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
                        self._get_checksheet_executor().submit(
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
        """Monitors the 3D conversion table Excel folder."""
        self._require_mode(RUNNER_MODE_3D, "3D conversion watcher")
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
    ) -> None:
        """Runs one job only when its pipeline type belongs to this process mode."""
        machine_id = str(machine_config.get("machine_id", "UNKNOWN"))
        try:
            allowed_pipeline_types = PIPELINE_TYPES_BY_MODE[self._runner_mode]
            if pipeline_type not in allowed_pipeline_types:
                raise RuntimeError(
                    f"Pipeline type '{pipeline_type}' does not belong to "
                    f"runner mode {self._runner_mode}."
                )

            if self._runner_mode == RUNNER_MODE_3D:
                self._get_pipeline_3d().run(
                    source_path=file_path,
                    part_config=part_config,
                    machine_config=machine_config,
                    pipeline_type=pipeline_type,
                )
            else:
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
        except Exception as exc:
            LOGGER.error(
                f"Pipeline error for '{file_path.name}': {exc}",
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
