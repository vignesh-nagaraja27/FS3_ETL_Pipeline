# -*- coding: utf-8 -*-
"""Load canonical FS3 checksheet records within the caller's transaction.

Architecture Principles:
------------------------
1. Source Workbook is Source of Truth:
   Replaces individual row upserts with full transaction synchronization (sync_transactions).
   Any serial row present in the database but removed from the source workbook is
   automatically deleted from fs3.pre_t_measurement.

2. Master Synchronization & Segment Contracts:
   - CR Checksheets (Full Master Snapshot): File scope equals logical master scope. Unclaimed
     DB PI slots are obsolete and marked for MASTER_DELETE.
   - BL/HD Checksheets (Partial Master Segment): File scope is a subset of the logical master scope.
     Unclaimed DB PI slots belong to other sequence files and MUST NOT be deleted or marked as MASTER_DELETE.

3. Full Audit Traceability:
   Forwards watch_id, file_version_id, file_name, and file_hash down to
   MeasurementRepository methods, generating MASTER_INSERT, MASTER_UPDATE, LIMIT_UPDATE,
   TRANSACTION_INSERT, TRANSACTION_UPDATE, and TRANSACTION_DELETE audit records in
   fs3.pre_cs_measurement_change_log.

4. PI_NO Resolution Ownership:
   TransformChecksheetStep never assigns pi_no. It only assigns a transform-run-local
   ``source_master_index`` shared by a master record and the transaction rows measured
   against it. MeasurementRepository.sync_masters() is the single source of truth for
   PI_NO allocation/matching and mutates each master_records dict in place, adding
   "pi_no". This step is responsible for translating that resolved pi_no from the
   master records onto the corresponding transaction records
   (source_master_index -> pi_no) before transactions are validated or persisted.
   No PI allocation or business-identity matching logic lives here; this step only
   relays an identifier the repository already computed.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional

from repositories.file_version_repository import FileVersionRepository
from repositories.measurement_repository import MeasurementRepository
from utils.logger import LOGGER


class LoadChecksheetStep:
    """Persist one transformed checksheet without owning transaction control."""

    CHECKSHEET_VERSION = "00000000"

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
        is_partial_master: bool = False,
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

        # Transaction records cannot be fully validated yet: pi_no does not
        # exist on them until sync_masters() below resolves it. Only the
        # fields that are independent of pi_no (vpo_code, date, serial)
        # are safe to check here. Business-identity + pi_no validation
        # happens after resolution, further down in this method.
        self._validate_transaction_shape(transaction_records, expected_vpo_code)

        incoming_file_date = self._max_transaction_date(transaction_records)

        lock_key = self._lock_key(vehicle_model, parts_code, op_no)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s);", (lock_key,))

        file_version_repository = FileVersionRepository(conn)
        file_version_repository.mark_processing(file_hash)

        measurement_repository = MeasurementRepository(conn)

        # Synchronize master definitions and limits.
        # Pass is_partial_master to prevent improper MASTER_DELETE logic on BL/HD sequence files.
        #
        # sync_masters() mutates each dict in master_records IN PLACE, adding
        # "pi_no" once PI allocation/matching completes. This step relies on
        # that in-place contract (rather than a return value) to build the
        # source_master_index -> pi_no map used below. If sync_masters() is
        # ever changed to operate on copies instead of the original dicts,
        # this resolution step will silently stop working.
        measurement_repository.sync_masters(
            master_records,
            version=self.CHECKSHEET_VERSION,
            watch_id=watch_id,
            file_version_id=file_version_id,
            file_name=file_name,
            file_hash=file_hash,
            incoming_file_date=incoming_file_date,
            is_partial_master=is_partial_master,
        )
        LOGGER.info(
            f"[MASTER SYNCED] Synchronized {len(master_records)} master definitions "
            f"(is_partial_master={is_partial_master})",
            pipeline_type="CHECKSHEET",
            watch_id=watch_id,
            source_filename=file_name,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
        )

        # Resolve each transaction's temporary source_master_index to the
        # final pi_no now present on the corresponding master record, and
        # attach vehicle/parts/op context. This is the step that was
        # previously missing: TransformChecksheetStep intentionally never
        # assigns pi_no, so transactions cannot be validated or persisted
        # until this translation happens.
        prepared_transaction_records = self._resolve_transaction_pi_numbers(
            transaction_records,
            master_records,
            vehicle_model=vehicle_model,
            parts_code=parts_code,
            op_no=op_no,
        )

        LOGGER.info(
            f"[PI_NO RESOLVED] Resolved pi_no for {len(prepared_transaction_records)} "
            "transaction records via source_master_index",
            pipeline_type="CHECKSHEET",
            watch_id=watch_id,
            source_filename=file_name,
            model=vehicle_model,
            part=parts_code,
            op=op_no,
        )

        # Now that every transaction carries a real pi_no, run full
        # business-identity + pi_no validation before persisting.
        self._validate_transaction_records(prepared_transaction_records, expected_vpo_code)

        # Full Transaction Synchronization (INSERT / UPDATE / DELETE) with Transaction Audit Logging
        synced_count = measurement_repository.sync_transactions(
            prepared_transaction_records,
            version=self.CHECKSHEET_VERSION,
            watch_id=watch_id,
            file_version_id=file_version_id,
            file_name=file_name,
            file_hash=file_hash,
            is_partial_master=is_partial_master,
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

            # Every master must carry the temporary identity that
            # transactions will later be matched against. Without it,
            # PI resolution has nothing to join on.
            if master_record.get("source_master_index") is None:
                raise ValueError(
                    f"master_records[{index - 1}].source_master_index is required"
                )

    @classmethod
    def _validate_transaction_shape(
        cls,
        transaction_records: List[Dict[str, Any]],
        expected_vpo_code: str,
    ) -> None:
        """
        Validate the subset of transaction fields that do not depend on
        pi_no having been resolved yet.

        Full identity + pi_no validation happens later, in
        _validate_transaction_records(), once pi_no has been attached by
        _resolve_transaction_pi_numbers().
        """
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

            # This is what PI resolution below joins on. Its absence here
            # means the transform step is broken, not that resolution
            # failed, so it is checked eagerly.
            if transaction_record.get("source_master_index") is None:
                raise ValueError(
                    f"transaction_records[{index}].source_master_index is required"
                )

    @classmethod
    def _resolve_transaction_pi_numbers(
        cls,
        transaction_records: List[Dict[str, Any]],
        master_records: List[Dict[str, Any]],
        *,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
    ) -> List[Dict[str, Any]]:
        """
        Translate each transaction's temporary source_master_index into the
        final pi_no assigned to the matching master record.

        This is the resolution step described in the module docstring
        (PI_NO Resolution Ownership). It performs no PI allocation and no
        business-identity matching itself -- MeasurementRepository already
        did that inside sync_masters() and recorded the result as
        master_record["pi_no"]. This method only relays that value onto
        the transactions that share the same source_master_index, and
        attaches vehicle/parts/op context needed by the repository.

        Raises:
            ValueError: if a master is missing pi_no (sync_masters()
                contract violation) or a transaction's source_master_index
                does not correspond to any master in this run.
        """
        pi_no_by_source_index: Dict[int, int] = {}

        for master_record in master_records:
            source_master_index = master_record.get("source_master_index")
            pi_no = master_record.get("pi_no")

            # _validate_master_records() already enforces this earlier in
            # run(), but this method must not assume that caller ordering
            # holds -- it re-checks so int() below never receives None.
            if source_master_index is None:
                raise ValueError(
                    "master_record is missing source_master_index"
                )

            if pi_no is None:
                raise ValueError(
                    "master_record is missing pi_no after sync_masters(); "
                    f"source_master_index={source_master_index!r}"
                )
            source_index = int(source_master_index)
            if source_index in pi_no_by_source_index:
                raise ValueError(f"duplicate source_master_index detected: "f"{source_index}")

            pi_no_by_source_index[int(source_master_index)] = int(pi_no)

        resolved_transaction_records: List[Dict[str, Any]] = []

        for index, source_record in enumerate(transaction_records):
            source_master_index = source_record.get("source_master_index")

            if source_master_index is None:
                raise ValueError(
                    f"transaction_records[{index}].source_master_index is required"
                )

            pi_no = pi_no_by_source_index.get(int(source_master_index))

            if pi_no is None:
                raise ValueError(
                    f"transaction_records[{index}] has source_master_index="
                    f"{source_master_index!r} with no matching master record; "
                    "master and transaction records are out of sync"
                )

            transaction_record = dict(source_record)
            transaction_record["pi_no"] = pi_no
            transaction_record["vehicle_model"] = vehicle_model
            transaction_record["parts_code"] = parts_code
            transaction_record["op_no"] = op_no

            resolved_transaction_records.append(transaction_record)

        return resolved_transaction_records

    @classmethod
    def _validate_transaction_records(
        cls,
        transaction_records: List[Dict[str, Any]],
        expected_vpo_code: str,
    ) -> None:
        """
        Final validation of transaction records, run only after
        _resolve_transaction_pi_numbers() has attached pi_no. This is the
        authoritative identity check before persistence.
        """
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
            pi_no = transaction_record.get("pi_no")
            if pi_no is None:
                raise ValueError(f"transaction_records[{index}].pi_no is required")
            if isinstance(pi_no, bool) or not isinstance(pi_no, int):
                raise ValueError(f"transaction_records[{index}].pi_no must be an integer")
            if pi_no <= 0:
                raise ValueError(f"transaction_records[{index}].pi_no must be greater than 0")

            key = (
                expected_vpo_code,
                transaction_date,
                str(transaction_record["serial_no_m_last4"]),
                pi_no,
            )
            if key in seen_keys:
                raise ValueError(f"duplicate transaction key: {key!r}")
            seen_keys.add(key)

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