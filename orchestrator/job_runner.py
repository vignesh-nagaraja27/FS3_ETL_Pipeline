# -*- coding: utf-8 -*-
"""
Job Runner — Elite Orchestration Supervisor (v2)
-------------------------------------------------
The engine that keeps the entire ETL system running.

Its responsibilities:
  1. WATCH   — Monitors every machine folder defined in 'configs/machines.json'.
  2. CLASSIFY — Hands each detected file to the Dispatcher for identification.
  3. ROUTE   — Passes classified files to the correct pipeline (3D_HD, 3D_BL, CHECKSHEET).
  4. CONVERT — Watches for new conversion mapping Excel files and auto-imports them
               into the master table BEFORE releasing new measurement files to the queue.
  5. SCALE   — Uses a thread pool to process multiple machines concurrently without
               opening multiple database connections (shared pool via DatabaseHandler).
  6. RECOVER — Cleans up stale PROCESSING entries on startup and processes DLQ retries.

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 5: Startup Stale Task Cleanup
═══════════════════════════════════════════════════════════════
  If the server crashed or was killed mid-processing, rows in etl_pipeline_log
  may be stuck at status='PROCESSING' forever. On restart:
    _cleanup_stale_tasks() resets ALL PROCESSING rows → PENDING
  This makes the system self-healing. Stale files re-enter the queue
  automatically within one poll cycle.

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 6: DLQ Retry Watcher Thread
═══════════════════════════════════════════════════════════════
  A dedicated background thread (_watch_dlq_retrying) polls etl_dlq for
  entries with dlq_status='RETRYING'. These are files that an operator
  clicked "Retry" on in the Mission Control Dashboard.

  The flow for a DLQ retry:
    1. Operator clicks "Retry" in dashboard.
    2. Dashboard physically moves file from _NG → source_path.
    3. Dashboard resets etl_pipeline_log.status → PENDING.
    4. Dashboard sets etl_dlq.dlq_status → 'RETRYING'.
    5. THIS THREAD detects the RETRYING entry.
    6. Optimistically marks the DLQ entry as RESOLVED (prevents double-submit).
    7. Submits the file to the executor via _run_pipeline_retry_safe().
    8. _run_pipeline_retry_safe() calls pipeline.run(existing_log_id=...) to
       reuse the original log entry (no duplicate pipeline_log rows).
    9. If retry fails again: re-queues as AWAITING_REVIEW with updated error.

Threading model:
  - One polling thread per machine folder.
  - One dedicated polling thread for the conversion table folder.
  - One dedicated polling thread for DLQ retries.
  - All pipeline executions dispatched to a shared ThreadPoolExecutor.
  - Only ONE connection pool, shared across all threads via the Singleton DatabaseHandler.
"""

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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

# File extensions we process. Anything else is ignored. CASE-INSENSITIVE via .lower() below.
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
        # Ensures mappings are always up-to-date before new measurements land.
        self._conversion_lock = threading.Event()
        self._conversion_lock.set()  # Unlocked by default (set = ready to process)

        # Tracks DLQ entry IDs already submitted to the executor in this session.
        # Prevents the DLQ watcher from submitting the same entry twice if it polls
        # before the entry's status has been updated in the DB.
        self._submitted_dlq_ids: set = set()
        self._dlq_id_lock = threading.Lock()

        folders = config.get_processing_folders()
        self._pipeline_3d = Pipeline3D(processing_folders=folders)

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
        Call this from main.py. Press Ctrl+C to stop cleanly.
        """
        LOGGER.info("=" * 60)
        LOGGER.info("ETL Job Runner starting up.")
        LOGGER.info("=" * 60)

        # ── STARTUP CLEANUP ─────────────────────────────────────────────────
        # ELITE FIX — Scenario 5: Reset stale PROCESSING rows from a previous
        # crash before accepting any new work. This is always safe on startup
        # because no pipeline threads are running yet.
        self._cleanup_stale_tasks()

        threads = []

        # Register every configured machine in the DB registry (upsert on startup)
        for machine_config in self._config.get_watch_directories():
            self._register_machine(machine_config)

        # Start one watcher thread per configured machine
        for machine_config in self._config.get_watch_directories():
            if not machine_config.get("enabled", True):
                LOGGER.info(
                    f"Machine '{machine_config.get('machine_id')}' is DISABLED in machines.json. Skipping."
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
            LOGGER.info(
                f"Watcher started for machine '{machine_config.get('machine_id')}' "
                f"→ '{machine_config.get('source_path')}'."
            )

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
            LOGGER.info(
                f"Conversion table watcher started → '{conv_config.get('path')}'."
            )

        # Start the DLQ retry watcher thread (Scenario 6 fix)
        t_dlq = threading.Thread(
            target=self._watch_dlq_retrying, name="Watcher-DLQ-Retry", daemon=True
        )
        t_dlq.start()
        threads.append(t_dlq)
        LOGGER.info(f"DLQ retry watcher started. Polling every {DLQ_POLL_INTERVAL}s.")

        LOGGER.info(f"All watchers running. Monitoring {len(threads)} thread(s).")

        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            LOGGER.info(
                "Shutdown signal received (Ctrl+C). Stopping all watchers gracefully..."
            )
            self.stop()

    def stop(self):
        """
        Signals all watcher threads to stop and shuts down the thread pool.
        The current in-flight pipeline run will complete before the thread exits.
        """
        self._stop_event.set()
        self._executor.shutdown(wait=True)
        DatabaseHandler().close_all()
        LOGGER.info("ETL Job Runner shut down cleanly.")

    # ─────────────────────────────────────────────────────────────────────────
    # ELITE FIX — Scenario 5: Startup Stale Task Cleanup
    # ─────────────────────────────────────────────────────────────────────────

    def _cleanup_stale_tasks(self):
        """
        Resets all pipeline log entries stuck in PROCESSING status back to PENDING.

        This is safe to call ONLY at startup, before any watcher threads launch.
        PROCESSING entries from a previous (crashed) run will never complete on
        their own — they block the watcher's _is_file_processed() check forever.

        By resetting them to PENDING, the file watcher will detect these files
        again on its next poll and requeue them naturally.

        If no stale entries exist, this is a no-op (fast, low cost).
        """
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
                    f"STARTUP CLEANUP: Found {len(stale_rows)} stale PROCESSING record(s). "
                    f"Reset to PENDING — these files will be requeued automatically. "
                    f"Affected: {[r[1] for r in stale_rows]}"
                )
            else:
                LOGGER.info(
                    "STARTUP CLEANUP: No stale PROCESSING records found. System is clean."
                )

        except Exception as e:
            # Non-fatal — log and continue. Worst case: some files won't be auto-retried
            # until the operator manually resets them or they are processed from DLQ.
            LOGGER.warning(
                f"STARTUP CLEANUP: Could not query or reset stale records. "
                f"System will continue, but stale files may need manual reset. Error: {e}"
            )

    # ─────────────────────────────────────────────────────────────────────────
    # ELITE FIX — Scenario 6: DLQ Retry Watcher
    # ─────────────────────────────────────────────────────────────────────────

    def _watch_dlq_retrying(self):
        """
        Background thread that polls etl_dlq for entries in RETRYING status.

        When the Mission Control Dashboard "Retry" button is clicked:
          1. The dashboard physically moves the file from _NG → machine source_path.
          2. The dashboard resets etl_pipeline_log.status → PENDING.
          3. The dashboard sets etl_dlq.dlq_status → 'RETRYING'.
          4. THIS THREAD detects it and submits it to the pipeline executor.

        The thread uses an in-memory set (_submitted_dlq_ids) to prevent double
        submission between poll intervals. It immediately marks the DLQ entry as
        RESOLVED (optimistic claim) before submitting to the executor.
        If the retry fails, _run_pipeline_retry_safe() re-queues the entry as
        AWAITING_REVIEW with an updated error message.
        """
        LOGGER.info("[DLQ-WATCHER] DLQ retry watcher is active.")

        while not self._stop_event.is_set():
            try:
                db = DatabaseHandler()
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT
                                d.id            AS dlq_id,
                                d.file_name,
                                d.machine_id,
                                d.pipeline_log_id,
                                d.retry_count,
                                d.max_retries
                            FROM fs3.etl_dlq d
                            WHERE d.dlq_status = 'RETRYING'
                            ORDER BY d.last_retry_at ASC
                            LIMIT 20;
                        """)
                        retrying_entries = cur.fetchall()

                for entry in retrying_entries:
                    (
                        dlq_id,
                        file_name,
                        machine_id,
                        pipeline_log_id,
                        retry_count,
                        max_retries,
                    ) = entry

                    # Deduplication guard: skip if already submitted in this session
                    with self._dlq_id_lock:
                        if dlq_id in self._submitted_dlq_ids:
                            continue

                    # Hard limit on retries to prevent infinite loops
                    if retry_count >= (max_retries or 3):
                        LOGGER.warning(
                            f"[DLQ-WATCHER] '{file_name}' has reached max retries "
                            f"({retry_count}/{max_retries}). Leaving in RETRYING state for manual review."
                        )
                        continue

                    # Find the machine configuration for this entry
                    machine_config = next(
                        (
                            m
                            for m in self._config.get_watch_directories()
                            if m.get("machine_id") == machine_id
                        ),
                        None,
                    )
                    if not machine_config:
                        LOGGER.warning(
                            f"[DLQ-WATCHER] Machine '{machine_id}' not found in machines.json. "
                            f"Cannot retry '{file_name}'."
                        )
                        continue

                    # The dashboard moved the file back to source_path
                    file_path = Path(machine_config["source_path"]) / file_name
                    if not file_path.exists():
                        LOGGER.warning(
                            f"[DLQ-WATCHER] File '{file_name}' not found at source path "
                            f"'{file_path}'. It may not have been moved yet. Will retry next poll."
                        )
                        continue

                    # Classify the file to get part_config
                    pipeline_type, part_config = self._dispatcher.classify(file_path)
                    if pipeline_type == PIPELINE_UNKNOWN:
                        LOGGER.warning(
                            f"[DLQ-WATCHER] '{file_name}' could not be classified. "
                            f"Skipping retry."
                        )
                        continue

                    # Optimistic claim: mark RESOLVED immediately to prevent double submission.
                    # If the retry itself fails, _run_pipeline_retry_safe will re-push to
                    # AWAITING_REVIEW so the operator can see it again in the dashboard.
                    claim_ok = self._claim_dlq_entry(dlq_id)
                    if not claim_ok:
                        continue  # Another watcher iteration already claimed it

                    # Register in session memory to handle fast re-polls
                    with self._dlq_id_lock:
                        self._submitted_dlq_ids.add(dlq_id)

                    LOGGER.info(
                        f"[DLQ-WATCHER] Submitting retry for '{file_name}' "
                        f"(DLQ ID: {dlq_id}, LOG ID: {pipeline_log_id})."
                    )
                    self._executor.submit(
                        self._run_pipeline_retry_safe,
                        file_path,
                        part_config,
                        machine_config,
                        int(pipeline_log_id),
                        int(dlq_id),
                    )

            except Exception as e:
                LOGGER.error(
                    f"[DLQ-WATCHER] Error in DLQ retry watcher loop: {e}", exc_info=True
                )

            time.sleep(DLQ_POLL_INTERVAL)

    def _claim_dlq_entry(self, dlq_id: int) -> bool:
        """
        Atomically marks a DLQ entry as RESOLVED to prevent double-submission
        between poll intervals. Uses a WHERE guard to ensure only one caller wins.

        Returns:
            True  — this caller won the claim (entry was RETRYING, now RESOLVED).
            False — another caller already claimed it.
        """
        sql = """
            UPDATE fs3.etl_dlq
            SET dlq_status = 'RESOLVED'
            WHERE id = %s AND dlq_status = 'RETRYING'
            RETURNING id;
        """
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (dlq_id,))
                    result = cur.fetchone()
            return result is not None
        except Exception as e:
            LOGGER.warning(f"[DLQ-WATCHER] Could not claim DLQ entry {dlq_id}: {e}")
            return False

    def _run_pipeline_retry_safe(
        self,
        file_path: Path,
        part_config: dict,
        machine_config: dict,
        pipeline_log_id: int,
        dlq_id: int,
    ):
        """
        Wrapper for DLQ retry pipeline runs. Uses the existing log entry (no new row).
        On success: DLQ entry is already RESOLVED (set by _claim_dlq_entry).
        On failure: Re-queues the DLQ entry as AWAITING_REVIEW with updated error.
        """
        machine_id = machine_config.get("machine_id")
        try:
            self._pipeline_3d.run(
                source_path=file_path,
                part_config=part_config,
                machine_config=machine_config,
                existing_log_id=pipeline_log_id,  # ← Reuse existing log entry
            )
            LOGGER.info(
                f"[DLQ-WATCHER] Retry SUCCEEDED for '{file_path.name}'. "
                f"DLQ entry {dlq_id} resolved. LOG_ID: {pipeline_log_id}."
            )
            # Increment success counter in registry
            self._increment_registry_counter(machine_id, success=True)

        except Exception as e:
            LOGGER.error(
                f"[DLQ-WATCHER] Retry FAILED for '{file_path.name}' "
                f"(DLQ ID: {dlq_id}). Error: {e}",
                exc_info=True,
            )
            # Re-push to AWAITING_REVIEW so operator sees the failure in dashboard
            try:
                db = DatabaseHandler()
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE fs3.etl_dlq
                            SET dlq_status   = 'AWAITING_REVIEW',
                                error_message = %s,
                                last_retry_at = NOW()
                            WHERE id = %s;
                        """,
                            (str(e)[:2000], dlq_id),
                        )
                LOGGER.warning(
                    f"[DLQ-WATCHER] DLQ entry {dlq_id} re-queued as AWAITING_REVIEW "
                    f"after failed retry."
                )
            except Exception as db_err:
                LOGGER.error(
                    f"[DLQ-WATCHER] Could not re-queue DLQ entry {dlq_id}: {db_err}"
                )
            # Remove from session memory so it can be retried again later
            with self._dlq_id_lock:
                self._submitted_dlq_ids.discard(dlq_id)

    # ─────────────────────────────────────────────────────────────────────────
    # Machine watcher threads
    # ─────────────────────────────────────────────────────────────────────────

    def _watch_machine_folder(self, machine_config: dict):
        """
        Polls one machine's source folder on a fixed interval.
        Any new files matching our supported extensions are dispatched to the pipeline.

        Polling rather than OS file events is intentional — CMM machines write files
        over network shares where OS file events are unreliable.
        """
        source_path = Path(machine_config["source_path"])
        machine_id = machine_config.get("machine_id")
        poll_interval = machine_config.get("poll_interval_sec", 5)
        seen_files: set = set()

        LOGGER.info(
            f"[MC:{machine_id}] Watcher active. Polling every {poll_interval}s.",
            machine=machine_id,
        )

        while not self._stop_event.is_set():
            try:
                # Heartbeat: update last_seen_at in the machine registry on every poll
                self._heartbeat(machine_id)

                if not source_path.exists():
                    LOGGER.warning(
                        f"[MC:{machine_id}] Source folder '{source_path}' not found. "
                        f"Will retry next poll. Check that the drive/network path is accessible.",
                        machine=machine_id,
                    )
                    time.sleep(poll_interval)
                    continue

                for file_path in source_path.rglob("*"):
                    # Case-insensitive extension check (handles .CSV, .XLSX etc.)
                    if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                        continue
                    if str(file_path) in seen_files:
                        continue

                    # State check: skip if already PROCESSING or COMPLETED in DB
                    if self._is_file_processed(file_path.name):
                        seen_files.add(str(file_path))
                        continue

                    seen_files.add(str(file_path))

                    # Classify the file before dispatching to a worker thread
                    pipeline_type, part_config = self._dispatcher.classify(file_path)

                    if pipeline_type == PIPELINE_UNKNOWN:
                        LOGGER.warning(
                            f"[MC:{machine_id}] '{file_path.name}' was not recognized by any rule. "
                            f"Skipping. Add a 'match_pattern' in 'configs/parts_3d.json' to handle it.",
                            machine=machine_id,
                        )
                        continue

                    # Wait if a conversion import is in progress
                    if not self._conversion_lock.is_set():
                        LOGGER.info(
                            f"[MC:{machine_id}] Pausing '{file_path.name}' — conversion import in progress.",
                            machine=machine_id,
                        )
                        self._conversion_lock.wait()

                    # Submit the pipeline run to the shared thread pool.
                    # StatusManager.register() will do the final atomic DB-level
                    # duplicate check — so this submit() is safe even if two machines
                    # somehow detect the same file.
                    self._executor.submit(
                        self._run_pipeline_safe, file_path, part_config, machine_config
                    )

            except Exception as e:
                LOGGER.error(
                    f"[MC:{machine_id}] Unexpected error in watcher loop: {e}. "
                    f"Watcher will continue polling.",
                    machine=machine_id,
                    exc_info=True,
                )

            time.sleep(poll_interval)

    def _watch_conversion_folder(self, conv_config: dict):
        """
        Monitors the conversion table Excel folder.
        When a new file arrives, it blocks the measurement pipeline queue (via _conversion_lock),
        imports the mapping, then releases the lock to resume normal processing.
        """
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
                    LOGGER.info(
                        f"[CONVERSION] New conversion table detected: '{file_path.name}'. "
                        f"Blocking measurement queue and importing..."
                    )

                    # Block measurement processing during import
                    self._conversion_lock.clear()
                    try:
                        self._import_conversion_table(file_path)
                    finally:
                        # Always release the lock — even if import fails
                        self._conversion_lock.set()
                        LOGGER.info(
                            "[CONVERSION] Conversion import complete. Measurement queue released."
                        )

            except Exception as e:
                LOGGER.error(
                    f"[CONVERSION] Error in conversion watcher loop: {e}.",
                    exc_info=True,
                )
                self._conversion_lock.set()  # Safety: always release lock on error

            time.sleep(poll_interval)

    def _import_conversion_table(self, file_path: Path):
        """
        Imports a conversion mapping Excel file into pre_m_cmm (via pre_w_3d_hd).
        """
        import pandas as pd

        LOGGER.info(f"[CONVERSION] Reading mapping file '{file_path.name}'...")
        try:
            df = pd.read_excel(file_path, header=0)
            db = DatabaseHandler()

            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE fs3.pre_w_3d_hd;")

                    for _, row in df.iterrows():
                        cur.execute(
                            """
                            INSERT INTO fs3.pre_w_3d_hd
                                (version, vpo_code, measure_no, evaluation_item1)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """,
                            (
                                str(row.get("version", "")),
                                str(row.get("vpo_code", "")),
                                int(row.get("measure_no", 0)),
                                str(row.get("evaluation_item1", "")),
                            ),
                        )

                    cur.execute("""
                        UPDATE fs3.pre_m_cmm m
                        SET evaluation_item1 = w.evaluation_item1
                        FROM fs3.pre_w_3d_hd w
                        WHERE m.vpo_code   = w.vpo_code
                          AND m.measure_no = w.measure_no
                          AND m.version    = w.version;
                    """)

            LOGGER.info(
                f"[CONVERSION] Successfully imported '{file_path.name}' into pre_m_cmm."
            )

        except Exception as e:
            LOGGER.error(
                f"[CONVERSION] Failed to import '{file_path.name}'. "
                f"Master table (pre_m_cmm) was NOT updated. Error: {e}",
                exc_info=True,
            )
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Pipeline execution helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _run_pipeline_safe(
        self, file_path: Path, part_config: dict, machine_config: dict
    ):
        """
        Wrapper that catches any top-level exception from a normal (non-retry) pipeline run.
        Prevents one bad file from crashing the entire worker thread.
        """
        machine_id = machine_config.get("machine_id")
        pipeline_type = machine_config.get("pipeline_type", "3D_HD")
        try:
            if pipeline_type in (PIPELINE_3D_HD, PIPELINE_3D_BL):
                self._pipeline_3d.run(
                    source_path=file_path,
                    part_config=part_config,
                    machine_config=machine_config,
                )
                self._increment_registry_counter(machine_id, success=True)
            else:
                LOGGER.warning(
                    f"No pipeline registered for type '{pipeline_type}'. "
                    f"File skipped: '{file_path.name}'."
                )
        except Exception as e:
            LOGGER.error(
                f"Unhandled top-level pipeline error for '{file_path.name}': {e}",
                exc_info=True,
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
                        f"UPDATE fs3.etl_machine_registry "
                        f"SET {column} = {column} + 1, updated_at = NOW() "
                        f"WHERE machine_id = %s;",
                        (machine_id,),
                    )
        except Exception:
            pass  # Counter update failure is never worth crashing the pipeline

    # ─────────────────────────────────────────────────────────────────────────
    # Registry & heartbeat helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _register_machine(self, machine_config: dict):
        """
        Upserts a machine record into etl_machine_registry at system startup.
        """
        sql = """
            INSERT INTO fs3.etl_machine_registry
                (machine_id, machine_description, pipeline_type, source_path,
                 watcher_started_at, is_enabled, updated_at)
            VALUES (%s, %s, %s, %s, NOW(), %s, NOW())
            ON CONFLICT (machine_id) DO UPDATE SET
                machine_description = EXCLUDED.machine_description,
                pipeline_type       = EXCLUDED.pipeline_type,
                source_path         = EXCLUDED.source_path,
                watcher_started_at  = NOW(),
                is_enabled          = EXCLUDED.is_enabled,
                updated_at          = NOW();
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
            LOGGER.info(
                f"[MC:{machine_config.get('machine_id')}] Registered in machine registry."
            )
        except Exception as e:
            LOGGER.warning(
                f"[MC:{machine_config.get('machine_id')}] Could not update machine registry: {e}"
            )

    def _heartbeat(self, machine_id: str):
        """
        Updates last_seen_at for a machine in the registry on every poll iteration.
        Fire-and-forget — failure is silently swallowed.
        """
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE fs3.etl_machine_registry "
                        "SET last_seen_at = NOW(), updated_at = NOW() "
                        "WHERE machine_id = %s;",
                        (machine_id,),
                    )
        except Exception:
            pass

    def _is_file_processed(self, file_name: str) -> bool:
        """
        Checks whether a file is currently active or has been fully completed.

        SELF-HEALING STALE TIMEOUT:
          A PROCESSING entry older than 30 minutes means the server crashed mid-run.
          Those files are NOT considered 'processed' — they are considered 'stuck'.
          The startup _cleanup_stale_tasks() will reset them to PENDING on restart.
          However, if somehow a stale entry was missed at startup, this query also
          allows re-processing after 30 minutes to prevent permanent blockage.

          SQL LOGIC:
            COMPLETED entries  → always blocked (file was successfully committed).
            PROCESSING < 30min → blocked (active run in progress right now).
            PROCESSING > 30min → NOT blocked (assumed crash, allow re-queue).
            PENDING entries    → NOT blocked (handled by DLQ watcher or just registered).
        """
        sql = """
            SELECT 1 FROM fs3.etl_pipeline_log
            WHERE file_name = %s
              AND (
                  status = 'COMPLETED'
                  OR (status = 'PROCESSING' AND updated_at > NOW() - INTERVAL '30 minutes')
              )
            LIMIT 1;
        """
        try:
            db = DatabaseHandler()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (file_name,))
                    return cur.fetchone() is not None
        except Exception as e:
            LOGGER.error(f"Error checking file processed state for '{file_name}': {e}")
            return False
