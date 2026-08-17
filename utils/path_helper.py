# -*- coding: utf-8 -*-
"""Centralized path generation for CMM and Checksheet ETL pipelines.

CMM layout::

    <root>/<mc_no>/<model>/<part>/<op>/_processing
    <root>/<mc_no>/<model>/<part>/<op>/_completed
    <root>/<mc_no>/<model>/<part>/<op>/_NG

Checksheet layout::

    <root>/<model>/<part>/<op>/_archive
    <root>/<model>/<part>/<op>/_ng
    <root>/<model>/<part>/<op>/_replay
"""

from pathlib import Path
from typing import Tuple


# Existing CMM folder names. Their values and casing are compatibility-sensitive.
PROCESSING_DIR_NAME = "_processing"
COMPLETED_DIR_NAME = "_completed"
NG_DIR_NAME = "_NG"

# Checksheet-only folder names.
ARCHIVE_DIR_NAME = "_archive"
CHECKSHEET_NG_DIR_NAME = "_ng"
REPLAY_DIR_NAME = "_replay"


def _sanitize_segment(value: str, fallback: str = "UNKNOWN") -> str:
    """Return a safe, non-empty Windows-compatible path segment."""
    if value is None:
        return fallback

    cleaned = str(value).strip()
    if not cleaned:
        return fallback

    illegal = '<>:"/\\|?*'
    for character in illegal:
        cleaned = cleaned.replace(character, "_")

    return cleaned or fallback


def get_working_paths(
    destination_root: str,
    mc_no: str,
    vehicle_model: str,
    parts_code: str,
    op_no: str,
    create: bool = True,
) -> Tuple[Path, Path, Path]:
    """Build the existing CMM processing, completed, and NG directories."""
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


def get_checksheet_paths(
    destination_root: str,
    vehicle_model: str,
    parts_code: str,
    op_no: str,
    create: bool = True,
) -> Tuple[Path, Path, Path]:
    """Build the Checksheet archive, NG, and replay directories."""
    root = Path(destination_root)
    op_root = (
        root
        / _sanitize_segment(vehicle_model)
        / _sanitize_segment(parts_code)
        / _sanitize_segment(op_no)
    )

    archive_path = op_root / ARCHIVE_DIR_NAME
    ng_path = op_root / CHECKSHEET_NG_DIR_NAME
    replay_path = op_root / REPLAY_DIR_NAME

    if create:
        archive_path.mkdir(parents=True, exist_ok=True)
        ng_path.mkdir(parents=True, exist_ok=True)
        replay_path.mkdir(parents=True, exist_ok=True)

    return archive_path, ng_path, replay_path


# Backward-compatible CMM alias.
build_working_paths = get_working_paths

# Checksheet alias.
build_checksheet_paths = get_checksheet_paths


__all__ = [
    "ARCHIVE_DIR_NAME",
    "CHECKSHEET_NG_DIR_NAME",
    "COMPLETED_DIR_NAME",
    "NG_DIR_NAME",
    "PROCESSING_DIR_NAME",
    "REPLAY_DIR_NAME",
    "build_checksheet_paths",
    "build_working_paths",
    "get_checksheet_paths",
    "get_working_paths",
]