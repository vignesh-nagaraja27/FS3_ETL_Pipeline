# -*- coding: utf-8 -*-
"""
Copy Step
---------
The first step in every pipeline run.

Responsibility: Move the incoming raw file from the CMM machine's source folder
into the '_processing' staging area. This is a physical file move, not a copy,
so the source folder stays clean and we avoid re-processing the same file.

Key behavior:
  - Waits for the file to be 'stable' (file size stops changing) before moving.
    This prevents partial reads of files still being written by the CMM machine.
  - Records the file in 'fs3.pre_l_3d_dldate' for duplicate detection.
  - On any failure, the original source file is left untouched.
"""

import shutil
import time
from pathlib import Path

from db.db_handler import DatabaseHandler
from utils.logger import LOGGER


# Tables used in this step
TABLE_DLDATA = "fs3.pre_l_3d_dldate"


class CopyStep:
    """
    Copies a stable raw file into the per-operation _processing staging folder
    and registers it in the DB.

    The processing_path is built per-run by the pipeline using
    utils.path_helper.get_working_paths() — no flat shared folder anymore.
    """

    def __init__(self):
        """
        CopyStep no longer owns a fixed processing directory.
        The per-operation _processing path is passed directly to run().
        """
        self._db = DatabaseHandler()

    def run(
        self,
        source_path: Path,
        processing_path: Path,
        machine_id: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        pipeline_type: str,
        stable_wait_sec: float = 2.0,
        stable_required_count: int = 3,
    ) -> Path:
        """
        Waits for the file to stabilize, checks for duplicates, then copies it
        into the per-operation _processing folder.

        Args:
            source_path (Path): Full path to the raw file detected by the watcher.
            processing_path (Path): The per-operation _processing folder
                                    (e.g. C:/FS3/DataCollect/KTSF-0453/RC/HD-RH/OP010/_processing).
                                    Created on demand if it doesn't exist.
            machine_id (str): Machine that produced this file.
            vehicle_model (str): Matched vehicle model code.
            parts_code (str): Matched parts code.
            op_no (str): Matched operation number.
            stable_wait_sec (float): Interval between file size checks.
            stable_required_count (int): How many consecutive stable checks before moving.

        Returns:
            Path: The new file path inside the '_processing' folder.

        Raises:
            FileNotFoundError: If the source file disappears before we can move it.
            FileExistsError: If the file is already registered in the DB (duplicate).
            RuntimeError: If the file never stabilizes within the timeout window.
        """
        context = dict(
            machine=machine_id, model=vehicle_model, part=parts_code, op=op_no,pipeline_type=pipeline_type,
        )

        LOGGER.info(f"COPY STEP started for '{source_path.name}'.", **context)

        # Ensure the per-operation _processing folder exists
        processing_path.mkdir(parents=True, exist_ok=True)

        # --- PHASE 1: Wait for file to become stable (not still being written) ---
        stable_dest = self._wait_for_stable(
            source_path, stable_wait_sec, stable_required_count, context
        )
        file_size = stable_dest

        # --- PHASE 2: Duplicate check via pre_l_3d_dldate ---
        self._check_duplicate(source_path.name, file_size, context)

        # --- PHASE 3: Copy file to per-operation _processing folder ---
        dest_path = processing_path / source_path.name
        try:
            shutil.copy2(str(source_path), str(dest_path))
            LOGGER.info(
                f"COPY STEP: Copied '{source_path.name}' → '{dest_path}'. Original remains untouched.",
                **context,
            )
        except Exception as e:
            LOGGER.error(
                f"COPY STEP: Failed to copy file '{source_path.name}' to processing folder. Error: {e}",
                exc_info=True,
                **context,
            )
            raise

        return dest_path

    def register_download(
        self, file_name: str, file_size: int, parts_code: str, context: dict
    ):
        """
        Inserts a new row into pre_l_3d_dldate to mark this file as processed.
        Called after LOAD succeeds so failed runs or DLQ retries are never
        falsely blocked as duplicates.
        """
        return self._register_download(file_name, file_size, parts_code, context)

    def _wait_for_stable(
        self, path: Path, interval: float, required_count: int, context: dict
    ) -> int:
        """
        Polls the file size repeatedly until it stops changing for 'required_count' intervals.
        Returns the final stable file size in bytes.

        Raises:
            RuntimeError: If the file does not stabilize within 60 seconds.
        """
        stable_hits = 0
        last_size = -1
        elapsed = 0
        timeout = 60  # seconds

        while elapsed < timeout:
            if not path.exists():
                raise FileNotFoundError(
                    f"COPY STEP: Source file disappeared before it could be moved: '{path}'"
                )
            current_size = path.stat().st_size
            if current_size == last_size and current_size > 0:
                stable_hits += 1
                if stable_hits >= required_count:
                    LOGGER.info(
                        f"COPY STEP: File '{path.name}' is stable at {current_size} bytes.",
                        **context,
                    )
                    return current_size
            else:
                stable_hits = 0
            last_size = current_size
            time.sleep(interval)
            elapsed += interval

        raise RuntimeError(
            f"COPY STEP: File '{path.name}' never stabilized within {timeout}s. "
            f"It may still be actively written by the CMM machine. Skipping."
        )

    def _check_duplicate(self, file_name: str, file_size: int, context: dict):
        """
        Queries pre_l_3d_dldate to see if this exact file (by name + size) was already processed.
        Raises FileExistsError if it's a duplicate — this is a safe early exit, not a crash.
        """
        sql = f"SELECT 1 FROM {TABLE_DLDATA} WHERE data_name = %s AND file_saize = %s LIMIT 1;"
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (file_name, file_size))
                    if cur.fetchone():
                        raise FileExistsError(
                            f"COPY STEP: Duplicate detected — '{file_name}' (size: {file_size} bytes) "
                            f"already exists in '{TABLE_DLDATA}'. Skipping to avoid re-import."
                        )
        except FileExistsError:
            LOGGER.warning(
                f"COPY STEP: Skipping duplicate file '{file_name}'.", **context
            )
            raise
        except Exception as e:
            LOGGER.error(
                f"COPY STEP: Error during duplicate check for '{file_name}': {e}",
                exc_info=True,
                **context,
            )
            raise

    def _register_download(
        self, file_name: str, file_size: int, parts_code: str, context: dict
    ):
        """
        Inserts a new row into pre_l_3d_dldate to mark this file as seen.
        """
        sql = f"""
            INSERT INTO {TABLE_DLDATA} (data_name, file_saize, parts, dl_time)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (data_name, file_saize) DO NOTHING;
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (file_name, file_size, parts_code))
            LOGGER.info(
                f"COPY STEP: Registered '{file_name}' in '{TABLE_DLDATA}'.", **context
            )
        except Exception as e:
            LOGGER.error(
                f"COPY STEP: Failed to register '{file_name}' in '{TABLE_DLDATA}'. "
                f"The file was moved but not recorded — may be re-processed next run. Error: {e}",
                exc_info=True,
                **context,
            )
            raise
