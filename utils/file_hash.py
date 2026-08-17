# -*- coding: utf-8 -*-
"""Utilities for deterministic file hashing.

Purpose
-------
This module creates a stable SHA256 fingerprint for each workbook so the ETL
platform can detect the same file version, prevent duplicate loads, and support
replay without modifying the source workbook.

Business rules
--------------
- Every discovered workbook must be hashed before processing.
- Idempotency depends on the observed file hash, not only the filename.
- Replay support relies on a stable fingerprint to re-run the same file version.

Why this logic exists
---------------------
A workbook can be edited repeatedly, so filenames are not enough to tell whether
we are processing the same version. The SHA256 hash tells us if the content has
changed and prevents duplicate transaction writes.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Union

from utils.logger import LOGGER

PathLike = Union[str, Path]


def generate_sha256(file_path: PathLike) -> str:
    """Return the SHA256 digest for a file using chunked reads.

    The file is read incrementally so large Excel workbooks do not require loading
    the entire payload into memory. This keeps the hash deterministic and safe for
    large production files.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    LOGGER.info(f"[HASH START] file={path}")

    digest = hashlib.sha256()
    chunk_size = 1024 * 1024

    try:
        with path.open("rb") as file_handle:
            for chunk in iter(lambda: file_handle.read(chunk_size), b""):
                digest.update(chunk)
    except OSError as exc:
        LOGGER.error(f"[HASH START] Unable to hash '{path}': {exc}")
        raise

    digest_hex = digest.hexdigest()
    LOGGER.info(f"[HASH GENERATED] file={path} sha256={digest_hex}")
    return digest_hex


def hash_file(file_path: PathLike) -> str:
    """Backward-compatible alias used by ETL workflows."""
    return generate_sha256(file_path)


sha256 = generate_sha256
