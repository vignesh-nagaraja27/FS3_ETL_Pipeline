# -*- coding: utf-8 -*-
"""PostgreSQL repository for the Checksheet master and transaction audit log.

Owns all interaction with fs3.pre_cs_measurement_change_log.
Stores generic key-value audit records for both master definition changes
and transaction measurement synchronization events.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from utils.logger import LOGGER


# Master & Transaction Audit Log Repository
#
# Owns all interaction with:
#
# fs3.pre_cs_measurement_change_log
#
# This table stores generic key-value audit log entries for:
#   - Master Definition Changes (MASTER_INSERT, MASTER_UPDATE, MASTER_DELETE,
#     LIMIT_UPDATE, SPECIFICATION_UPDATE, GENERAL_UPDATE)
#   - Transaction Measurement Sync (TRANSACTION_INSERT, TRANSACTION_UPDATE,
#     TRANSACTION_DELETE)
#
# Shared audit table providing end-to-end traceability across:
#   watch_id, file_version_id, file_name, file_hash, vpo_code, pi_no, change_key

class MeasurementChangeLogRepository:
    """Authoritative repository for fs3.pre_cs_measurement_change_log audit entries."""

    TABLE_NAME = "fs3.pre_cs_measurement_change_log"

    ALLOWED_CHANGE_TYPES = frozenset(
        {
            "MASTER_INSERT",
            "MASTER_UPDATE",
            "MASTER_DELETE",
            "LIMIT_UPDATE",
            "SPECIFICATION_UPDATE",
            "GENERAL_UPDATE",
            "TRANSACTION_INSERT",
            "TRANSACTION_UPDATE",
            "TRANSACTION_DELETE",
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

    def record_change(
        self,
        vpo_code: str,
        pi_no: int,
        change_key: str,
        change_type: str,
        *,
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
        version: str = "00000000",
        watch_id: Optional[str] = None,
        file_version_id: Optional[int] = None,
        file_name: Optional[str] = None,
        file_hash: Optional[str] = None,
        remarks: Optional[str] = None,
        insert_id: str = "Vig",
    ) -> Dict[str, Any]:
        """Insert a single generic key-value audit log record."""
        if change_type not in self.ALLOWED_CHANGE_TYPES:
            raise ValueError(f"Unsupported change_type: {change_type}")

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    version, watch_id, file_version_id, file_name, file_hash,
                    vpo_code, pi_no, change_key, old_value, new_value,
                    change_type, remarks, change_time, insert_date, insert_id
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s
                )
                RETURNING *;
                """,
                (
                    version,
                    watch_id,
                    file_version_id,
                    file_name,
                    file_hash,
                    vpo_code,
                    int(pi_no),
                    change_key,
                    old_value,
                    new_value,
                    change_type,
                    remarks,
                    insert_id,
                ),
            )
            record = self._row_to_dict(cur, cur.fetchone())

        if record is None:
            raise RuntimeError("Failed to insert audit record")
        return record

    def record_changes_batch(
        self,
        audit_logs: List[Dict[str, Any]],
        insert_id: str = "Vig",
    ) -> int:
        """Batch insert generic key-value audit log records."""
        if not audit_logs:
            return 0

        insert_sql = f"""
            INSERT INTO {self.TABLE_NAME} (
                version, watch_id, file_version_id, file_name, file_hash,
                vpo_code, pi_no, change_key, old_value, new_value,
                change_type, remarks, change_time, insert_date, insert_id
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s
            );
        """
        with self.conn.cursor() as cur:
            for log in audit_logs:
                change_type = log["change_type"]
                if change_type not in self.ALLOWED_CHANGE_TYPES:
                    raise ValueError(f"Unsupported change_type: {change_type}")

                cur.execute(
                    insert_sql,
                    (
                        log.get("version", "00000000"),
                        log.get("watch_id"),
                        log.get("file_version_id"),
                        log.get("file_name"),
                        log.get("file_hash"),
                        log["vpo_code"],
                        int(log.get("pi_no", 0)),
                        log["change_key"],
                        log.get("old_value"),
                        log.get("new_value"),
                        change_type,
                        log.get("remarks"),
                        log.get("insert_id", insert_id),
                    ),
                )
        return len(audit_logs)

    def get_audit_trail_for_vpo(
        self,
        vpo_code: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query audit trail records for a specific vpo_code."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {self.TABLE_NAME}
                WHERE vpo_code = %s
                ORDER BY change_time DESC, change_id DESC
                LIMIT %s;
                """,
                (vpo_code, max(1, limit)),
            )
            rows = cur.fetchall()
            return [
                {description[0]: val for description, val in zip(cur.description, r)}
                for r in rows
            ]


__all__ = ["MeasurementChangeLogRepository"]