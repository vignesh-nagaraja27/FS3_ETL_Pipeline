# -*- coding: utf-8 -*-
"""PostgreSQL repository for the Checksheet file-discovery lifecycle.

Architectural Separation Notice:
    - This repository manages physical file discovery events in fs3.pre_l_cs_dldate.
    - Runtime execution locks, backoff timers, and retry scheduling are managed
      authoritatively by ProcessingLockRepository against fs3.pre_l_measurement_processing_lock.
"""

from __future__ import annotations
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union


PathLike = Union[str, Path]

from utils.logger import LOGGER


class ChecksheetDownloadRepository:
    """Persist lifecycle transitions in ``fs3.pre_l_cs_dldate``."""

    TABLE_NAME = "fs3.pre_l_cs_dldate"
    ALLOWED_STATUSES = frozenset(
        {
            "DETECTED",
            "DISCOVERED",
            "STABLE",
            "SUBMITTED",
            "PROCESSING",
            "PROCESSED",
            "FAILED",
            "RETRY_PENDING",
            "AWAITING_REVIEW",
        }
    )
    STATUS_LOG_MARKERS = {
        "DETECTED": "[CS FILE DETECTED]",
        "DISCOVERED": "[CS FILE DISCOVERED]",
        "STABLE": "[CS FILE STABLE]",
        "SUBMITTED": "[CS FILE SUBMITTED]",
        "PROCESSING": "[CS FILE PROCESSING]",
        "PROCESSED": "[CS FILE PROCESSED]",
        "FAILED": "[CS FILE FAILED]",
        "RETRY_PENDING": "[CS FILE RETRY_PENDING]",
        "AWAITING_REVIEW": "[CS FILE AWAITING_REVIEW]",
    }

    def __init__(self, conn: Any):
        if conn is None:
            raise ValueError("Database connection 'conn' is required")
        self.conn = conn

    @staticmethod
    def _row_to_dict(cur: Any, row: Optional[Any]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        return {description[0]: value for description, value in zip(cur.description, row)}

    @staticmethod
    def _extract_filename_parts(file_name: str) -> Dict[str, str]:
        stem = Path(file_name).stem
        match = re.fullmatch(
            r"(?P<vehicle_model>[^_]+)_(?P<parts_code>[^_]+)_(?P<op_no>[^_]+)(?:_.+)?",
            stem,
            flags=re.IGNORECASE,
        )
        if match is None:
            raise ValueError(
                "Checksheet filename must match VehicleModel_PartsCode_OPNO_Anything.xlsm"
            )
        for group_name, segment_name, pattern, allowed_characters in (
            ("vehicle_model", "VehicleModel", r"[A-Za-z0-9]+", "A-Z, a-z, and 0-9"),
            ("parts_code", "PartsCode", r"[A-Za-z0-9-]+", "A-Z, a-z, 0-9, and -"),
            ("op_no", "OPNO", r"OP[0-9]+", "OP followed by digits (0-9)"),
        ):
            value = match.group(group_name)
            if re.fullmatch(pattern, value) is None:
                raise ValueError(
                    f"Invalid checksheet filename segment '{segment_name}': "
                    f"value '{value}' in filename '{file_name}' must contain "
                    f"only {allowed_characters}"
                )
        return {
            "vehicle_model": match.group("vehicle_model"),
            "parts_code": match.group("parts_code"),
            "op_no": match.group("op_no").upper(),
        }

    def register_detected(
        self,
        file_path: PathLike,
        *,
        vehicle_model: Optional[str] = None,
        parts_code: Optional[str] = None,
        op_no: Optional[str] = None,
        file_size: Optional[int] = None,
        insert_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Insert one physical-file discovery event with status ``DETECTED``."""
        path = Path(file_path)
        resolved_size = path.stat().st_size if file_size is None else int(file_size)

        if vehicle_model is None or parts_code is None or op_no is None:
            filename_parts = self._extract_filename_parts(path.name)
            vehicle_model = vehicle_model or filename_parts["vehicle_model"]
            parts_code = parts_code or filename_parts["parts_code"]
            op_no = op_no or filename_parts["op_no"]

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    file_name,
                    source_path,
                    vehicle_model,
                    parts_code,
                    op_no,
                    file_size,
                    status,
                    insert_id,
                    update_id
                ) VALUES (%s, %s, %s, %s, %s, %s, 'DETECTED', %s, %s)
                RETURNING *;
                """,
                (
                    path.name,
                    str(path),
                    vehicle_model,
                    parts_code,
                    op_no,
                    resolved_size,
                    insert_id,
                    insert_id,
                ),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        if record is None:
            raise RuntimeError(f"Failed to register detected Checksheet file: {path}")

        LOGGER.info(
f"[CS FILE DETECTED] "
f"id={record['id']} "
f"file_name={path.name} "
f"source_path={path}",
pipeline_type="CHECKSHEET",
)
        return record

    def mark_stable(
        self,
        lifecycle_id: int,
        *,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Mark a discovery lifecycle row as ``STABLE``."""
        return self._mark_status(lifecycle_id, "STABLE", update_id)

    def mark_submitted(
        self,
        lifecycle_id: int,
        *,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Mark a discovery lifecycle row as ``SUBMITTED``."""
        return self._mark_status(lifecycle_id, "SUBMITTED", update_id)

    def mark_processing(
        self,
        lifecycle_id: int,
        *,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Mark a discovery lifecycle row as ``PROCESSING``."""
        return self._mark_status(lifecycle_id, "PROCESSING", update_id)

    def mark_processed(
        self,
        lifecycle_id: int,
        *,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Mark a discovery lifecycle row as ``PROCESSED``."""
        return self._mark_status(lifecycle_id, "PROCESSED", update_id)

    def mark_failed(
        self,
        lifecycle_id: int,
        *,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Mark a discovery lifecycle row as ``FAILED``."""
        return self._mark_status(lifecycle_id, "FAILED", update_id)

    def mark_retry_pending(
        self,
        lifecycle_id: int,
        *,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Mark a discovery lifecycle row as ``RETRY_PENDING``."""
        return self._mark_status(lifecycle_id, "RETRY_PENDING", update_id)

    def mark_awaiting_review(
        self,
        lifecycle_id: int,
        *,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Mark a discovery lifecycle row as ``AWAITING_REVIEW``."""
        return self._mark_status(lifecycle_id, "AWAITING_REVIEW", update_id)

    def get_latest_by_file_name(self, file_name: str) -> Optional[Dict[str, Any]]:
        """Return the newest physical lifecycle row for a filename."""
        if not file_name:
            raise ValueError("file_name is required")

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE file_name = %s
                ORDER BY detected_time DESC, id DESC
                LIMIT 1;
                """,
                (file_name,),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def _mark_status(
        self,
        lifecycle_id: int,
        status: str,
        update_id: str,
    ) -> Dict[str, Any]:
        if status not in self.ALLOWED_STATUSES:
            raise ValueError(f"Unsupported Checksheet lifecycle status: {status}")

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET status = %s,
                    update_date = CURRENT_TIMESTAMP,
                    update_id = %s
                WHERE id = %s
                RETURNING *;
                """,
                (status, update_id, int(lifecycle_id)),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        if record is None:
            raise LookupError(f"Checksheet lifecycle id not found: {lifecycle_id}")

        LOGGER.info(
f"{self.STATUS_LOG_MARKERS.get(status, f'[CS FILE {status}]')} "
f"id={lifecycle_id} "
f"file_name={record['file_name']}",
pipeline_type="CHECKSHEET",
)
        return record


__all__ = ["ChecksheetDownloadRepository"]