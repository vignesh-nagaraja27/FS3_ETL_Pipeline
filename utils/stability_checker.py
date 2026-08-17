# -*- coding: utf-8 -*-
"""Workbook stability checks for safe ETL processing.

Purpose
-------
The ETL platform must never read a workbook while it is still being saved or
modified. This module verifies that a file has remained stable across several
successive scans before it is allowed to flow into the business transformation.

Business rules
--------------
- File size and last-modified timestamp must remain unchanged.
- Stability must hold for a configured number of consecutive scans.
- Processing waits until the file is stable enough to read without corruption.

Why this logic exists
---------------------
Excel workbooks are often written incrementally by the user. A partial write can
produce unreadable data, missing rows, or inconsistent values. The stability
check prevents the ETL layer from reading a workbook while it is still in a save
cycle.

All stability checker events are strictly routed to etl_system_master.log
using pipeline_type="SYSTEM".
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple, Union

from utils.logger import LOGGER

PathLike = Union[str, Path]


def _resolve_stability_config(
    stable_required_count: int,
    stable_wait_sec: float,
    *,
    config: Optional[Mapping[str, Any]] = None,
) -> Tuple[int, float]:
    """Resolve stability settings while preserving explicit defaults."""
    configured_count = (
        config.get("stable_required_count", stable_required_count)
        if config is not None
        else stable_required_count
    )
    configured_wait = (
        config.get("stable_wait_sec", stable_wait_sec)
        if config is not None
        else stable_wait_sec
    )
    return max(1, int(configured_count)), max(0.0, float(configured_wait))


def check_file_stability(
    file_path: PathLike,
    stable_required_count: int = 3,
    stable_wait_sec: float = 5.0,
    poll_interval_sec: float = 1.0,
    *,
    config: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Wait until a file remains unchanged for the configured scan count.

    A file is considered stable only when both file size and file modification time
    remain unchanged across consecutive scans. The scan window is capped by
    stable_wait_sec, individual polls occur at poll_interval_sec intervals, and
    the required count is controlled by configuration.
    """
    path = Path(file_path)
    required, wait_sec = _resolve_stability_config(
        stable_required_count,
        stable_wait_sec,
        config=config,
    )
    poll_interval = max(0.1, float(poll_interval_sec))

    if not path.exists():
        LOGGER.warning(
            f"[STABILITY FAILED] File not found: {path}",
            pipeline_type="SYSTEM",
        )
        return False

    last_size: Optional[int] = None
    last_mtime_ns: Optional[int] = None
    stable_count = 0

    while stable_count < required:
        try:
            stat = path.stat()
        except FileNotFoundError:
            LOGGER.warning(
                f"[STABILITY FAILED] File disappeared while checking stability: {path}",
                pipeline_type="SYSTEM",
            )
            return False

        current_size = stat.st_size
        current_mtime_ns = stat.st_mtime_ns

        if (
            last_size is None
            or last_mtime_ns is None
        ):
            stable_count = 1
        elif (
            current_size == last_size
            and current_mtime_ns == last_mtime_ns
        ):
            stable_count += 1
        else:
            stable_count = 1

        LOGGER.info(
            f"[STABILITY SCAN] "
            f"file={path} "
            f"size={current_size} "
            f"mtime_ns={current_mtime_ns} "
            f"stable_count={stable_count} "
            f"required={required}",
            pipeline_type="SYSTEM",
        )

        last_size = current_size
        last_mtime_ns = current_mtime_ns

        if stable_count >= required:
            LOGGER.info(
                f"[STABILITY PASSED] "
                f"file={path} "
                f"stable for {required} consecutive scans",
                pipeline_type="SYSTEM",
            )
            return True

        time.sleep(wait_sec)

    return False


def is_file_stable(
    file_path: PathLike,
    stable_required_count: int = 3,
    stable_wait_sec: float = 5.0,
    poll_interval_sec: float = 1.0,
) -> bool:
    """Alias focused on the stability decision itself."""
    return check_file_stability(
        file_path=file_path,
        stable_required_count=stable_required_count,
        stable_wait_sec=stable_wait_sec,
        poll_interval_sec=poll_interval_sec,
    )


wait_for_stability = check_file_stability
check_stability = check_file_stability