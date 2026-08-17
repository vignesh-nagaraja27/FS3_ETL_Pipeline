# -*- coding: utf-8 -*-
"""PostgreSQL repository for Checksheet workbook version metadata.

The production table ``fs3.pre_l_measurement_file_version`` stores the physical
file fingerprint, processing state, archive location, pipeline linkage, and
audit information. The repository participates in the caller's transaction and
never commits or rolls back the supplied connection.
"""

from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from repositories.processing_lock_repository import ProcessingLockRepository

PathLike = Union[str, Path]

from utils.logger import LOGGER


class FileVersionRepository:
    """Track workbook versions in PostgreSQL using the production schema."""

    TABLE_NAME = "fs3.pre_l_measurement_file_version"

    def __init__(self, conn: Any):
        if conn is None:
            raise ValueError("Database connection 'conn' is required")
        self.conn = conn

    @staticmethod
    def _row_to_dict(cur: Any, row: Optional[Any]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        return {
            description[0]: value
            for description, value in zip(cur.description, row)
        }

    @staticmethod
    def _validate_file_hash(file_hash: str) -> str:
        normalized_hash = str(file_hash).strip()
        if not normalized_hash:
            raise ValueError("file_hash is required")
        return normalized_hash

    def exists_by_hash(self, file_hash: str) -> bool:
        """Return whether the exact physical file version is registered."""
        hash_value = self._validate_file_hash(file_hash)
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {self.TABLE_NAME} WHERE file_hash = %s LIMIT 1;",
                (hash_value,),
            )
            exists = cur.fetchone() is not None

        LOGGER.info(
f"[FILE VERSION EXISTS] Hash {hash_value} exists={exists}",
pipeline_type="CHECKSHEET",
)
        return exists

    def get_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Return the newest row for a content hash."""
        hash_value = self._validate_file_hash(file_hash)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE file_hash = %s
                ORDER BY file_version_id DESC
                LIMIT 1;
                """,
                (hash_value,),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def get_file_version_id_by_hash(self, file_hash: str) -> Optional[int]:
        """Return the primary key file_version_id for a given content hash."""
        record = self.get_by_hash(file_hash)
        if record and "file_version_id" in record:
            return int(record["file_version_id"])
        return None

    def get_latest_for_file(self, file_name: str) -> Optional[Dict[str, Any]]:
        """Return the latest registered version for a filename."""
        if not file_name:
            raise ValueError("file_name is required")

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE file_name = %s
                ORDER BY insert_date DESC, file_version_id DESC
                LIMIT 1;
                """,
                (file_name,),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def get_latest_processed_for_file(self,file_name: str,watch_id: str) -> Optional[Dict[str, Any]]:
        """Return the latest successfully PROCESSED version for a filename.

        Used by the Change Detection Scheduler to perform content hash comparisons:
        new_hash != latest_processed_hash.
        """
        if not file_name:
            raise ValueError("file_name is required")
        if not watch_id:
            raise ValueError("Watch id is required")

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE file_name = %s AND watch_id = %s AND status = 'PROCESSED'
                ORDER BY insert_date DESC, file_version_id DESC
                LIMIT 1;
                """,
                (file_name,watch_id),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def register_version(
        self,
        file_path: PathLike,
        file_hash: str,
        watch_id: Optional[str] = None,
        vehicle_model: Optional[str] = None,
        parts_code: Optional[str] = None,
        op_no: Optional[str] = None,
        version: str = "00000000",
        status: str = "DISCOVERED",
        *,
        archive_path: Optional[PathLike] = None,
        pipeline_log_id: Optional[int] = None,
        retry_count: int = 0,
        insert_id: str = "Vig",
    ) -> Dict[str, Any]:
        """Register one physical file version idempotently."""
        del vehicle_model, parts_code, op_no, version

        path = Path(file_path)
        stat = path.stat()
        hash_value = self._validate_file_hash(file_hash)
        modified_time = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        archive_value = str(Path(archive_path)) if archive_path is not None else None

        existing = self.get_by_hash(hash_value)
        if existing is not None:
            LOGGER.info(
f"[FILE VERSION REGISTER] Version already exists for hash {hash_value}",
pipeline_type="CHECKSHEET",
)
            return existing

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s));",
                (hash_value,),
            )

            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE file_hash = %s
                ORDER BY file_version_id DESC
                LIMIT 1;
                """,
                (hash_value,),
            )
            existing_after_lock = self._row_to_dict(cur, cur.fetchone())
            if existing_after_lock is not None:
                LOGGER.info(
f"[FILE VERSION REGISTER] Registered version for hash {hash_value}",
pipeline_type="CHECKSHEET",
)
                return existing_after_lock

            cur.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    file_name,
                    file_hash,
                    watch_id,
                    file_size,
                    modified_time,
                    archive_path,
                    status,
                    pipeline_log_id,
                    retry_count,
                    insert_date,
                    insert_id,
                    update_date,
                    update_id
                ) VALUES (
                    %s,%s, %s, %s, %s, %s, %s, %s, %s,
                    CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP, %s
                )
                ON CONFLICT DO NOTHING
                RETURNING *;
                """,
                (
                    path.name,
                    hash_value,
                    watch_id,
                    int(stat.st_size),
                    modified_time,
                    archive_value,
                    status,
                    pipeline_log_id,
                    max(0, int(retry_count)),
                    insert_id,
                    insert_id,
                ),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        if record is None:
            record = self.get_by_hash(hash_value)
        if record is None:
            raise RuntimeError(
                f"Failed to register file version for hash {hash_value}"
            )

        LOGGER.info(
f"[FILE VERSION PROCESSING] Marked file version {hash_value} as PROCESSING",
pipeline_type="CHECKSHEET",
)
        return record

    register_file_version = register_version
    upsert_version = register_version

    def mark_processing(
        self,
        file_hash: str,
        *,
        pipeline_log_id: Optional[int] = None,
        update_id: str = "Vig",
    ) -> Optional[Dict[str, Any]]:
        """Mark a file version as ``PROCESSING``."""
        hash_value = self._validate_file_hash(file_hash)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'PROCESSING',
                    pipeline_log_id = COALESCE(%s, pipeline_log_id),
                    update_date = CURRENT_TIMESTAMP,
                    update_id = %s
                WHERE file_hash = %s
                RETURNING *;
                """,
                (pipeline_log_id, update_id, hash_value),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        LOGGER.info(
f"[FILE VERSION PROCESSING] Marked file version {hash_value} as PROCESSING",
pipeline_type="CHECKSHEET",
)
        return record

    def mark_processed(
        self,
        file_hash: str,
        processed_time: Optional[datetime] = None,
        *,
        archive_path: Optional[PathLike] = None,
        pipeline_log_id: Optional[int] = None,
        update_id: str = "Vig",
    ) -> Optional[Dict[str, Any]]:
        """Mark a file version as ``PROCESSED``."""
        hash_value = self._validate_file_hash(file_hash)
        archive_value = str(Path(archive_path)) if archive_path is not None else None
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'PROCESSED',
                    processed_time = COALESCE(%s, CURRENT_TIMESTAMP),
                    archive_path = COALESCE(%s, archive_path),
                    pipeline_log_id = COALESCE(%s, pipeline_log_id),
                    update_date = CURRENT_TIMESTAMP,
                    update_id = %s
                WHERE file_hash = %s
                RETURNING *;
                """,
                (
                    processed_time,
                    archive_value,
                    pipeline_log_id,
                    update_id,
                    hash_value,
                ),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        LOGGER.info(
f"[FILE VERSION PROCESSED] Marked file version {hash_value} as PROCESSED",
pipeline_type="CHECKSHEET",
)
        return record

    def schedule_retry(
        self,
        file_hash: str,
        retry_base_minutes: int = 5,
        max_retry_attempts: int = 4,
        error_message: str = "",
        update_id: str = "Vig",
    ) -> Optional[Dict[str, Any]]:
        """Delegates retry scheduling to ProcessingLockRepository (authoritative retry table)."""
        return ProcessingLockRepository(self.conn).mark_retry_pending(
            file_hash=file_hash,
            retry_base_minutes=retry_base_minutes,
            max_retry_attempts=max_retry_attempts,
            error_message=error_message,
            update_id=update_id,
        )

    def get_pending_retries(self) -> List[Dict[str, Any]]:
        """Delegates pending retry retrieval to ProcessingLockRepository."""
        return ProcessingLockRepository(self.conn).get_pending_retries()

    def mark_failed(
        self,
        file_hash: str,
        processed_time: Optional[datetime] = None,
        *,
        pipeline_log_id: Optional[int] = None,
        update_id: str = "Vig",
    ) -> Optional[Dict[str, Any]]:
        """Mark a file version as ``FAILED``."""
        hash_value = self._validate_file_hash(file_hash)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'FAILED',
                    processed_time = COALESCE(%s, CURRENT_TIMESTAMP),
                    pipeline_log_id = COALESCE(%s, pipeline_log_id),
                    update_date = CURRENT_TIMESTAMP,
                    update_id = %s
                WHERE file_hash = %s
                RETURNING *;
                """,
                (processed_time, pipeline_log_id, update_id, hash_value),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        LOGGER.info(
f"[FILE VERSION FAILED] Marked file version {hash_value} as FAILED",
pipeline_type="CHECKSHEET",
)
        return record


__all__ = ["FileVersionRepository"]