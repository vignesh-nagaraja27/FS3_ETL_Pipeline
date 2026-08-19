# -*- coding: utf-8 -*-
"""Load canonical FS3 checksheet records within the caller's transaction.

Architecture Principles:
------------------------
1. Source Workbook is Source of Truth:
   Replaces individual row upserts with full transaction synchronization (sync_transactions).
   Any serial row present in the database but removed from the source workbook is
   automatically deleted from fs3.pre_t_measurement.

2. Master Synchronization & Limit Ownership:
   Master synchronization (sync_masters) owns PI_NO business resolution, master definition
   upserts, and limit-update evaluations using transaction business date precedence.

3. Full Audit Traceability:
   Forwards watch_id, file_version_id, file_name, and file_hash down to
   MeasurementRepository methods, generating MASTER_INSERT, MASTER_UPDATE, LIMIT_UPDATE,
   TRANSACTION_INSERT, TRANSACTION_UPDATE, and TRANSACTION_DELETE audit records in
   fs3.pre_cs_measurement_change_log.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional

from repositories.file_version_repository import FileVersionRepository
from repositories.measurement_repository import MeasurementRepository
from utils.logger import LOGGER

VALID_DATA_COLUMNS = frozenset(f"data_{number:03d}" for number in range(1, 501))


class LoadChecksheetStep:
    """Persist one transformed checksheet without owning transaction control."""

    CHECKSHEET_VERSION = "00000000"
    MAX_DATA_COLUMNS = 500

    def run(
        self,
        records: Mapping[str, Any],
        *,
        conn: Any,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        file_hash: Optional[str] = None,
        file_name: Optional[str] = None,
        version: str = CHECKSHEET_VERSION,
        watch_id: Optional[str] = None,
        file_version_id: Optional[int] = None,
        **kwargs: Any,
    ) -> int:
        """Load masters and transactions atomically under the caller's transaction."""
        if conn is None:
            raise ValueError("conn is required")
        if not isinstance(records, Mapping):
            raise TypeError("records must contain master_records and transaction_records")
        if version != self.CHECKSHEET_VERSION:
            raise ValueError(f"version must be {self.CHECKSHEET_VERSION!r}")
        if not vehicle_model:
            raise ValueError("vehicle_model is required")
        if not parts_code:
            raise ValueError("parts_code is required")
        if not op_no:
            raise ValueError("op_no is required")
        if not watch_id:
            raise ValueError("watch_id is required")
        if not file_hash:
            raise ValueError("file_hash is required")

        master_records = self._record_list(records, "master_records")
        transaction_records = self._record_list(records, "transaction_records")
        if not master_records:
            raise ValueError("master_records must not be empty")
        if not transaction_records:
            raise ValueError("transaction_records must not be empty")

        expected_vpo_code = f"{vehicle_model}_{parts_code}_{op_no}"
        self._validate_master_records(
            master_records,
            expected_vpo_code,
            vehicle_model,
            parts_code,
            op_no,
        )
        self._validate_transaction_records(transaction_records, expected_vpo_code)
        incoming_file_date = self._max_transaction_date(transaction_records)

        lock_key = self._lock_key(vehicle_model, parts_code, op_no)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s);", (lock_key,))

        file_version_repository = FileVersionRepository(conn)
        file_version_repository.mark_processing(file_hash)

        measurement_repository = MeasurementRepository(conn)

        # Master synchronization owns limit-update processing.
        # Limits are evaluated after PI_NO resolution using
        # business-date precedence derived from
        # fs3.pre_t_measurement MAX(date) scoped by:
        # version + vpo_code + watch_id.
        column_map = measurement_repository.sync_masters(
            master_records,
            version=self.CHECKSHEET_VERSION,
            watch_id=watch_id,
            file_version_id=file_version_id,
            file_name=file_name,
            file_hash=file_hash,
            incoming_file_date=incoming_file_date,
        )
        LOGGER.info(
            f"[PI_NO RESOLVED] Synchronized {len(master_records)} master definitions "
            f"and resolved column map: {column_map}",
            pipeline_type="CHECKSHEET",
            watch_id=watch_id,
            source_filename=file_name,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
        )

        # Prepare transaction records with vehicle/parts/op context
        prepared_transaction_records = []
        for source_record in transaction_records:
            transaction_record = dict(source_record)
            transaction_record["vehicle_model"] = vehicle_model
            transaction_record["parts_code"] = parts_code
            transaction_record["op_no"] = op_no
            prepared_transaction_records.append(transaction_record)

        # Full Transaction Synchronization (INSERT / UPDATE / DELETE) with Transaction Audit Logging
        synced_count = measurement_repository.sync_transactions(
            prepared_transaction_records,
            version=self.CHECKSHEET_VERSION,
            watch_id=watch_id,
            file_version_id=file_version_id,
            file_name=file_name,
            file_hash=file_hash,
            column_map=column_map,
        )
        LOGGER.info(
            f"[TRANSACTION SYNC] Synchronized {synced_count} transaction records "
            f"for vpo_code={expected_vpo_code}",
            pipeline_type="CHECKSHEET",
            watch_id=watch_id,
            source_filename=file_name,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
        )

        file_version_repository.mark_processed(file_hash)
        LOGGER.info(
            f"[FILE VERSION UPDATE] Updated file version: file_hash={file_hash}, status=PROCESSED",
            pipeline_type="CHECKSHEET",
            watch_id=watch_id,
            source_filename=file_name,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
        )

        LOGGER.info(
            f"[LOAD COMPLETED] Completed checksheet load: masters={len(master_records)}, "
            f"transactions={synced_count}, incoming_file_date={incoming_file_date}",
            pipeline_type="CHECKSHEET",
            watch_id=watch_id,
            source_filename=file_name,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
        )
        return synced_count

    load = run

    @staticmethod
    def _record_list(records: Mapping[str, Any], key: str) -> List[Dict[str, Any]]:
        value = records.get(key)
        if value is None:
            return []
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"{key} must be a list of records")
        if not all(isinstance(record, dict) for record in value):
            raise TypeError(f"every {key} entry must be a dictionary")
        return [dict(record) for record in value]

    @classmethod
    def _validate_master_records(
        cls,
        master_records: List[Dict[str, Any]],
        expected_vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
    ) -> None:
        if len(master_records) > cls.MAX_DATA_COLUMNS:
            raise ValueError(
                f"master_records must contain at most {cls.MAX_DATA_COLUMNS} entries"
            )

        for index, master_record in enumerate(master_records, start=1):
            if master_record.get("version") != cls.CHECKSHEET_VERSION:
                raise ValueError(
                    f"master_records[{index - 1}].version must be {cls.CHECKSHEET_VERSION!r}"
                )
            if master_record.get("vpo_code") != expected_vpo_code:
                raise ValueError(
                    f"master_records[{index - 1}].vpo_code mismatch: expected "
                    f"{expected_vpo_code!r}, got {master_record.get('vpo_code')!r}"
                )

            for field_name, expected_value in (
                ("vehicle_model", vehicle_model),
                ("parts_code", parts_code),
                ("op_no", op_no),
            ):
                if master_record.get(field_name) != expected_value:
                    raise ValueError(
                        f"master_records[{index - 1}].{field_name} mismatch: "
                        f"expected {expected_value!r}, got {master_record.get(field_name)!r}"
                    )

            expected_data_column = f"data_{index:03d}"
            actual_data_column = master_record.get("data_column")
            if actual_data_column != expected_data_column:
                raise ValueError(
                    f"master_records[{index - 1}].data_column must be "
                    f"{expected_data_column!r}, got {actual_data_column!r}"
                )

    @classmethod
    def _validate_transaction_records(
        cls,
        transaction_records: List[Dict[str, Any]],
        expected_vpo_code: str,
    ) -> None:
        seen_keys = set()
        for index, transaction_record in enumerate(transaction_records):
            if transaction_record.get("version") != cls.CHECKSHEET_VERSION:
                raise ValueError(
                    f"transaction_records[{index}].version must be {cls.CHECKSHEET_VERSION!r}"
                )
            if transaction_record.get("vpo_code") != expected_vpo_code:
                raise ValueError(
                    f"transaction_records[{index}].vpo_code mismatch: expected "
                    f"{expected_vpo_code!r}, got {transaction_record.get('vpo_code')!r}"
                )
            if not transaction_record.get("serial_no_m_last4"):
                raise ValueError(f"transaction_records[{index}].serial_no_m_last4 is required")
            if transaction_record.get("date") is None:
                raise ValueError(f"transaction_records[{index}].date is required")

            transaction_date = cls._coerce_date(transaction_record["date"])
            if transaction_date is None:
                raise ValueError(f"transaction_records[{index}].date is invalid")
            key = (
                expected_vpo_code,
                transaction_date,
                str(transaction_record["serial_no_m_last4"]),
            )
            if key in seen_keys:
                raise ValueError(f"duplicate transaction key: {key!r}")
            seen_keys.add(key)

            for name in transaction_record:
                if not name.startswith("data_"):
                    continue
                if name not in VALID_DATA_COLUMNS:
                    raise ValueError(
                        f"transaction_records[{index}] contains invalid data column {name!r}"
                    )

    @classmethod
    def _max_transaction_date(
        cls,
        transaction_records: List[Dict[str, Any]],
    ) -> date:
        parsed_dates = [
            cls._coerce_date(transaction_record["date"])
            for transaction_record in transaction_records
        ]
        if any(parsed_date is None for parsed_date in parsed_dates):
            raise ValueError("every transaction date must be a valid date or datetime")
        return max(parsed_date for parsed_date in parsed_dates if parsed_date is not None)

    @staticmethod
    def _coerce_date(value: Any) -> Optional[date]:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).date()
            except ValueError:
                try:
                    return date.fromisoformat(value)
                except ValueError:
                    return None
        return None

    @staticmethod
    def _lock_key(vehicle_model: str, parts_code: str, op_no: str) -> int:
        token = "\x1f".join((vehicle_model, parts_code, op_no)).encode("utf-8")
        digest = hashlib.sha256(token).digest()
        return int.from_bytes(digest[:8], byteorder="big", signed=True)


__all__ = ["LoadChecksheetStep"]