# -*- coding: utf-8 -*-
"""
Path Helper
-----------
Centralized helper for building the per-operation destination folder structure
used by the ETL pipeline.

Folder layout produced by this helper:

    <destination_root>/<MC_NO>/<VEHICLE_MODEL>/<PARTS_CODE>/<OP_NO>/_processing
    <destination_root>/<MC_NO>/<VEHICLE_MODEL>/<PARTS_CODE>/<OP_NO>/_completed
    <destination_root>/<MC_NO>/<VEHICLE_MODEL>/<PARTS_CODE>/<OP_NO>/_NG

Example:
    C:/FS3/DataCollect/KTSF-0453/RC/HD-RH/OP010/_processing
    C:/FS3/DataCollect/KTSF-0453/RC/HD-RH/OP010/_completed
    C:/FS3/DataCollect/KTSF-0453/RC/HD-RH/OP010/_NG

All three subdirectories are created on demand (parents=True, exist_ok=True)
so callers never need to mkdir before writing.
"""

from pathlib import Path
from typing import Tuple


# Subfolder names — kept as constants so the rest of the codebase can reference
# them without hardcoding strings.
PROCESSING_DIR_NAME = "_processing"
COMPLETED_DIR_NAME = "_completed"
NG_DIR_NAME = "_NG"


def _sanitize_segment(value: str, fallback: str = "UNKNOWN") -> str:
    """
    Sanitize a single path segment so it cannot break the folder structure.

    Rules:
      - Strip whitespace.
      - Replace any character that is illegal in Windows file paths
        (< > : " / \\ | ? *) with '_'.
      - If the result is empty, use the fallback.

    Args:
        value (str): The raw segment (e.g. machine_id, vehicle_model).
        fallback (str): Used when the sanitized value is empty.

    Returns:
        str: A safe, non-empty path segment.
    """
    if value is None:
        return fallback
    cleaned = str(value).strip()
    if not cleaned:
        return fallback
    # Replace illegal Windows path characters with underscore
    illegal = '<>:"/\\|?*'
    for ch in illegal:
        cleaned = cleaned.replace(ch, "_")
    return cleaned or fallback


def get_working_paths(
    destination_root: str,
    mc_no: str,
    vehicle_model: str,
    parts_code: str,
    op_no: str,
    create: bool = True,
) -> Tuple[Path, Path, Path]:
    """
    Build the three per-operation working folders for a single pipeline run.

    Args:
        destination_root (str): Root folder for all output (e.g. 'C:/FS3/DataCollect').
        mc_no (str): Machine identifier (e.g. 'KTSF-0453').
        vehicle_model (str): Vehicle model code (e.g. 'RC').
        parts_code (str): Parts code (e.g. 'HD-RH').
        op_no (str): Operation number (e.g. 'OP010').
        create (bool): If True (default), create all three folders on disk.

    Returns:
        Tuple[Path, Path, Path]: (processing_path, completed_path, ng_path)

    Raises:
        OSError: If 'create=True' and the OS refuses to create the folders
                 (e.g. permission denied, invalid root path).
    """
    root = Path(destination_root)
    op_root = (
        root
        / _sanitize_segment(mc_no)
        / _sanitize_segment(vehicle_model)
        / _sanitize_segment(parts_code)
        / _sanitize_segment(op_no)
    )

    processing_path = op_root / PROCESSING_DIR_NAME
    completed_path = op_root / COMPLETED_DIR_NAME
    ng_path = op_root / NG_DIR_NAME

    if create:
        processing_path.mkdir(parents=True, exist_ok=True)
        completed_path.mkdir(parents=True, exist_ok=True)
        ng_path.mkdir(parents=True, exist_ok=True)

    return processing_path, completed_path, ng_path


# Backward-compatible alias.
build_working_paths = get_working_paths
