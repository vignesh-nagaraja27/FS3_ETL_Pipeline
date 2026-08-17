# -*- coding: utf-8 -*-
"""
Processing Lifecycle Repository
--------------------------------
Owns all interaction with fs3.pre_l_measurement_processing_lock.

This table tracks processing state rather than implementing traditional database locking.

Supported statuses:
    - DISCOVERED
    - PROCESSING
    - PROCESSED
    - FAILED
    - RETRY_PENDING
    - AWAITING_REVIEW

Retry scheduling and operational monitoring depend on this repository.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# Processing Lifecycle Repository
#
# Owns all interaction with:
#
# fs3.pre_l_measurement_processing_lock
#
# This table tracks processing state rather than
# implementing traditional database locking.
#
# Supported statuses:
#
# DISCOVERED
# PROCESSING
# PROCESSED
# FAILED
# RETRY_PENDING
# AWAITING_REVIEW
#
# Retry scheduling and operational monitoring
# depend on this repository.

class ProcessingLockRepository:
    """Authoritative repository for fs3.pre_l_measurement_processing_lock lifecycle events."""

    TABLE_NAME = "fs3.pre_l_measurement_processing_lock"

    ALLOWED_STATUSES = frozenset(
        {
            "DISCOVERED",
            "PROCESSING",
            "PROCESSED",
            "FAILED",
            "RETRY_PENDING",
            "AWAITING_REVIEW",
        }
    )

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

    def mark_discovered(
        self,
        file_hash: str,
        file_name: str,
        watch_id: Optional[str] = None,
        pipeline_log_id: Optional[int] = None,
        insert_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Record file discovery with status DISCOVERED."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    file_hash, file_name, watch_id, processing_status,
                    pipeline_log_id, insert_date, insert_id, last_updated_at
                ) VALUES (
                    %s, %s, %s, 'DISCOVERED', %s, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (file_hash) DO UPDATE SET
file_name = EXCLUDED.file_name,
watch_id = COALESCE(EXCLUDED.watch_id, {self.TABLE_NAME}.watch_id),
pipeline_log_id = COALESCE(
EXCLUDED.pipeline_log_id,
{self.TABLE_NAME}.pipeline_log_id
),
last_updated_at =
CASE
WHEN {self.TABLE_NAME}.processing_status = 'DISCOVERED'
THEN CURRENT_TIMESTAMP
ELSE {self.TABLE_NAME}.last_updated_at
END
                RETURNING *;
                """,
                (file_hash, file_name, watch_id, pipeline_log_id, insert_id),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        if record is None:
            raise RuntimeError(f"Failed to record DISCOVERED status for hash {file_hash}")
        return record

    def mark_processing(
        self,
        file_hash: str,
        file_name: Optional[str] = None,
        watch_id: Optional[str] = None,
        pipeline_log_id: Optional[int] = None,
        update_id: str = "SYSTEM",
    ) -> Dict[str, Any]:
        """Transition state to PROCESSING."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    file_hash, file_name, watch_id, processing_status,
                    started_at, pipeline_log_id, insert_date, insert_id, last_updated_at
                ) VALUES (
                    %s, %s, %s, 'PROCESSING', CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (file_hash) DO UPDATE SET
                    file_name = COALESCE(EXCLUDED.file_name, {self.TABLE_NAME}.file_name),
                    watch_id = COALESCE(EXCLUDED.watch_id, {self.TABLE_NAME}.watch_id),
                    processing_status = 'PROCESSING',
                    started_at = CURRENT_TIMESTAMP,
                    pipeline_log_id = COALESCE(EXCLUDED.pipeline_log_id, {self.TABLE_NAME}.pipeline_log_id),
                    last_updated_at = CURRENT_TIMESTAMP
                RETURNING *;
                """,
                (file_hash, file_name, watch_id, pipeline_log_id, update_id),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        if record is None:
            raise RuntimeError(f"Failed to mark PROCESSING for hash {file_hash}")
        return record

    def mark_processed(
        self,
        file_hash: str,
        update_id: str = "SYSTEM",
    ) -> Optional[Dict[str, Any]]:
        """Transition state to PROCESSED upon successful pipeline execution."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET processing_status = 'PROCESSED',
                    finished_at = CURRENT_TIMESTAMP,
                    next_retry_at = NULL,
                    last_updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s
                RETURNING *;
                """,
                (file_hash,),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def mark_failed(
        self,
        file_hash: str,
        error_message: str = "",
        update_id: str = "SYSTEM",
    ) -> Optional[Dict[str, Any]]:
        """Transition state to FAILED."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET processing_status = 'FAILED',
                    finished_at = CURRENT_TIMESTAMP,
                    last_error_message = %s,
                    last_updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s
                RETURNING *;
                """,
                (error_message[:2000] if error_message else None, file_hash),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def mark_retry_pending(
        self,
        file_hash: str,
        retry_base_minutes: int = 5,
        max_retry_attempts: int = 4,
        error_message: str = "",
        update_id: str = "SYSTEM",
    ) -> Optional[Dict[str, Any]]:
        """Schedule a backoff retry in fs3.pre_l_measurement_processing_lock."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET retry_count = retry_count + 1,
                    last_retry_at = CURRENT_TIMESTAMP,
                    last_error_message = %s,
                    processing_status = CASE
                        WHEN (retry_count + 1) >= %s THEN 'AWAITING_REVIEW'
                        ELSE 'RETRY_PENDING'
                    END,
                    next_retry_at = CASE
                        WHEN (retry_count + 1) < %s THEN CURRENT_TIMESTAMP + (%s * (retry_count + 1) * INTERVAL '1 minute')
                        ELSE NULL
                    END,
                    last_updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s
                RETURNING *;
                """,
                (
                    error_message[:2000] if error_message else None,
                    max_retry_attempts,
                    max_retry_attempts,
                    retry_base_minutes,
                    file_hash,
                ),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def mark_awaiting_review(
        self,
        file_hash: str,
        error_message: str = "",
        update_id: str = "SYSTEM",
    ) -> Optional[Dict[str, Any]]:
        """Transition state to AWAITING_REVIEW when max retries are exceeded."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.TABLE_NAME}
                SET processing_status = 'AWAITING_REVIEW',
                    next_retry_at = NULL,
                    last_error_message = COALESCE(%s, last_error_message),
                    last_updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s
                RETURNING *;
                """,
                (error_message[:2000] if error_message else None, file_hash),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def get_pending_retries(self) -> List[Dict[str, Any]]:
        """Fetch all RETRY_PENDING files where next_retry_at <= CURRENT_TIMESTAMP."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE processing_status = 'RETRY_PENDING'
                  AND next_retry_at IS NOT NULL
                  AND next_retry_at <= CURRENT_TIMESTAMP
                ORDER BY next_retry_at ASC;
                """
            )
            rows = cur.fetchall()
            return [
                {description[0]: val for description, val in zip(cur.description, r)}
                for r in rows
            ]

    def get_stale_processing(
        self, stale_minutes: int = 30
    ) -> List[Dict[str, Any]]:
        """Fetch all records stuck in 'PROCESSING' status beyond the stale threshold.

        Read-only query used by recovery watchers and operational diagnostics to
        identify orphaned tasks caused by unhandled ETL crashes or abrupt restarts.

        Args:
            stale_minutes (int): Inactivity threshold in minutes. Rows last updated
                before (CURRENT_TIMESTAMP - stale_minutes) will be returned.
                Defaults to 30.

        Returns:
            List[Dict[str, Any]]: List of dictionary representations of stale processing lock rows.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE processing_status = 'PROCESSING'
                  AND last_updated_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                ORDER BY last_updated_at ASC;
                """,
                (stale_minutes,),
            )
            rows = cur.fetchall()
            return [
                {description[0]: val for description, val in zip(cur.description, r)}
                for r in rows
            ]

    def get_latest_status_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Return the current processing lock row for a content hash."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {self.TABLE_NAME} WHERE file_hash = %s LIMIT 1;",
                (file_hash,),
            )
            return self._row_to_dict(cur, cur.fetchone())

    def get_latest_status_by_filename(self, file_name: str) -> Optional[Dict[str, Any]]:
        """Return the current processing lock row for a filename."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT * FROM {self.TABLE_NAME}
                WHERE file_name = %s
                ORDER BY last_updated_at DESC
                LIMIT 1;
                """,
                (file_name,),
            )
            return self._row_to_dict(cur, cur.fetchone())


__all__ = ["ProcessingLockRepository"]