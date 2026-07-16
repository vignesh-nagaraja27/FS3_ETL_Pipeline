# -*- coding: utf-8 -*-
"""
Job Runner — Elite Orchestration Supervisor (v2)
-------------------------------------------------
The engine that keeps the entire ETL system running.
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Any

from config_loader import ConfigLoader
from db.db_handler import DatabaseHandler
from orchestrator.dispatcher import (
    Dispatcher,
    PIPELINE_3D_HD,
    PIPELINE_3D_BL,
    PIPELINE_UNKNOWN,
)
from pipelines.pipeline_3d import Pipeline3D
from utils.logger import LOGGER

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

        # Blocks measurement file processing while a conversion import is in progress.
        self._conversion_lock = threading.Event()
        self._conversion_lock.set()  # Unlocked by default

        # Tracks DLQ entry IDs already submitted to the executor in this session.
        self._submitted_dlq_ids: set = set()
        self._dlq_id_lock = threading.Lock()

        self._pipeline_3d = Pipeline3D(destination_root=config.get_destination_root())

        # Shared thread pool — all pipeline runs go through here
        total_workers = sum(
            d.get("max_workers", 2)
            for d in config.get_watch_directories()
            if d.get("enabled", True)
        )
        self._executor = ThreadPoolExecutor(max_workers=max(total_workers, 4))

    def start(self):
        """
        Starts all watcher threads and blocks until a shutdown signal is received.
        """
        LOGGER.info("=" * 60)
        LOGGER.info("ETL Job Runner starting up.")
        LOGGER.info("=" * 60)

        # STARTUP CLEANUP: Reset stale PROCESSING rows
        self._cleanup_stale_tasks()

        threads = []

        # Register every configured machine in the DB registry
        for machine_config in self._config.get_watch_directories():
            self._register_machine(machine_config)

        # Start one watcher thread per configured machine
        for machine_config in self._config.get_watch_directories():
            if not machine_config.get("enabled", True):
                LOGGER.info(
                    f"Machine '{machine_config.get('machine_id')}' is DISABLED. Skipping."
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
            LOGGER.info("Shutdown signal received. Stopping watchers...")
            self.stop()

    def stop(self):
        """Signals all threads to stop and shuts down the thread pool."""
        self._stop_event.set()
        self._executor.shutdown(wait=True)
        DatabaseHandler().close_all()
        LOGGER.info("ETL Job Runner shut down cleanly.")

    def _cleanup_stale_tasks(self):
        """Resets rows stuck in PROCESSING to PENDING on startup."""
        sql = """
            UPDATE fs3.etl_pipeline_log
            SET status       = 'PENDING',
                current_step = 'STALE_RESET',
                updated_at   = NOW()
            WHERE status = 'PROCESSING'
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
                    f"STARTUP CLEANUP: Reset {len(stale_rows)} stale records."
                )
        except Exception as e:
            LOGGER.warning(f"STARTUP CLEANUP: Error resetting stale records: {e}")

    def _watch_dlq_retrying(self):
        """Polls DLQ for 'RETRYING' entries and submits them to the executor."""
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

                    file_path = Path(machine_config["source_path"]) / file_name
                    if not file_path.exists():
                        continue

                    pipeline_type, part_config = self._dispatcher.classify(file_path)
                    if pipeline_type == PIPELINE_UNKNOWN or part_config is None:
                        continue

                    if self._claim_dlq_entry(dlq_id):
                        with self._dlq_id_lock:
                            self._submitted_dlq_ids.add(dlq_id)
                            LOGGER.info(f"SUBMITTING {file_path.name}")

                        self._executor.submit(
                            self._run_pipeline_retry_safe,
                            file_path,
                            pipeline_type,
                            part_config,
                            machine_config,
                            int(log_id),
                            int(dlq_id),
                        )
            except Exception as e:
                LOGGER.error(f"[DLQ-WATCHER] Error: {e}")

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
            LOGGER.error(f"[DLQ-WATCHER] Retry FAILED for '{file_path.name}': {e}")
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
                    if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                        continue
                    # Ignore configured filename prefixes
                    if any(
                        file_path.name.upper().startswith(prefix.upper())
                        for prefix in ignore_prefixes
                    ):
                        seen_files.add(str(file_path))
                        LOGGER.info(
                            f"[MC:{machine_id}] Ignoring file by prefix rule: {file_path.name}"
                        )
                        continue
                    if str(file_path) in seen_files:
                        continue

                    if self._is_file_processed(file_path.name):
                        seen_files.add(str(file_path))
                        continue

                    seen_files.add(str(file_path))
                    pipeline_type, part_config = self._dispatcher.classify(file_path)

                    # ELITE FIX: Explicitly ensure part_config is not None for type safety
                    if pipeline_type == PIPELINE_UNKNOWN or part_config is None:
                        continue

                    if not self._conversion_lock.is_set():
                        self._conversion_lock.wait()

                    self._executor.submit(
                        self._run_pipeline_safe,
                        file_path,
                        pipeline_type,
                        part_config,
                        machine_config,
                    )
            except Exception as e:
                LOGGER.error(f"[MC:{machine_id}] Watcher error: {e}")

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
                LOGGER.error(f"[CONVERSION] Error: {e}")
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
                        FROM fs3.pre_w_3d_hd w WHERE m.vpo_code = w.vpo_code AND m.measure_no = w.measure_no AND m.version = w.version;
                    """)
        except Exception as e:
            LOGGER.error(f"[CONVERSION] Failed import: {e}")
            raise

    def _run_pipeline_safe(
        self,
        file_path: Path,
        pipeline_type: str,
        part_config: Dict[Any, Any],
        machine_config: dict,
    ):
        """Catches top-level exceptions from a normal pipeline run."""
        # ELITE FIX: Ensure machine_id is a string and not None
        machine_id = str(machine_config.get("machine_id", "UNKNOWN"))
        try:
            if pipeline_type in (PIPELINE_3D_HD, PIPELINE_3D_BL):
                self._pipeline_3d.run(
                    source_path=file_path,
                    part_config=part_config,
                    machine_config=machine_config,
                    pipeline_type=pipeline_type,
                )
                self._increment_registry_counter(machine_id, success=True)
            else:
                LOGGER.warning(
                    f"No pipeline for type '{pipeline_type}'. Skip: '{file_path.name}'"
                )
        except Exception as e:
            LOGGER.error(f"Pipeline error for '{file_path.name}': {e}")
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
            LOGGER.warning(f"Registry update failed: {e}")

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

    def _is_file_processed(self, file_name: str) -> bool:
        """Checks if a file is currently active or completed."""
        sql = """
            SELECT 1 FROM fs3.etl_pipeline_log
            WHERE file_name = %s AND (status = 'COMPLETED' OR (status = 'PROCESSING' AND updated_at > NOW() - INTERVAL '30 minutes'))
            LIMIT 1;
        """
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (file_name,))
                    return cur.fetchone() is not None
        except Exception:
            return False
