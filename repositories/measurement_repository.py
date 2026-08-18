# -*- coding: utf-8 -*-
"""PostgreSQL repository for FS3 checksheet master and transaction records."""

from __future__ import annotations
import unicodedata
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Set, Tuple, Union
from utils.logger import LOGGER
from psycopg2 import sql

DateValue = Union[date, datetime]


class _ConnectionProtocol(Protocol):
    """Structural type required from a PostgreSQL connection."""

    def cursor(self) -> Any:
        """Return a DB-API cursor."""
        ...


class MeasurementRepository:
    """SQL-only repository participating in the caller-owned transaction."""

    CHECKSHEET_VERSION = "00000000"
    TABLE_MASTER = "fs3.pre_m_measurement"
    TABLE_TRANSACTION = "fs3.pre_t_measurement"
    TABLE_CHANGE_LOG = "fs3.pre_cs_measurement_change_log"
    MAX_PI_NO = 500

    _MASTER_DB_COLUMNS = (
        "version",
        "mc_no",
        "vpo_code",
        "vehicle_model",
        "parts_code",
        "op_no",
        "op_no_2nd",
        "op_no_3rd",
        "op_name",
        "pi_no",
        "Inspection_no",
        "machining_surface",
        "measure_item",
        "measure_item2",
        "measure_part_no",
        "measure_part",
        "measure_direction",
        "measure_direction_1st",
        "measure_direction_2nd",
        "measure_rank",
        "measure_point",
        "measure_depth",
        "specification",
        "upper_limit",
        "lower_limit",
        "timing",
        "measure_method",
        "measure_frequency",
        "judgment_method",
    )
    _MASTER_VALUE_KEYS = _MASTER_DB_COLUMNS

    # Fields in fs3.pre_m_measurement subject to generic key-value auditing
    _AUDITABLE_MASTER_FIELDS = (
        "mc_no",
        "vehicle_model",
        "parts_code",
        "op_no",
        "op_no_2nd",
        "op_no_3rd",
        "op_name",
        "Inspection_no",
        "machining_surface",
        "measure_item",
        "measure_item2",
        "measure_part_no",
        "measure_part",
        "measure_direction",
        "measure_direction_1st",
        "measure_direction_2nd",
        "measure_rank",
        "measure_point",
        "measure_depth",
        "specification",
        "upper_limit",
        "lower_limit",
        "timing",
        "measure_method",
        "measure_frequency",
        "judgment_method",
    )

    _MASTER_NORMALIZED_KEYS = (
        "mc_no",
        "vpo_code",
        "vehicle_model",
        "parts_code",
        "op_no",
        "op_no_2nd",
        "op_no_3rd",
        "op_name",
        "Inspection_no",
        "machining_surface",
        "measure_item",
        "measure_item2",
        "measure_part_no",
        "measure_part",
        "measure_direction",
        "measure_direction_1st",
        "measure_direction_2nd",
        "measure_rank",
        "measure_point",
        "measure_depth",
        "specification",
        "upper_limit",
        "lower_limit",
        "timing",
        "measure_method",
        "measure_frequency",
        "judgment_method",
    )
    _MASTER_REQUIRED_KEYS = (
        "vpo_code",
        "vehicle_model",
        "parts_code",
        "op_no",
    )

    _TRANSACTION_BUSINESS_COLUMNS = (
        "version",
        "mc_no",
        "vpo_code",
        "vehicle_model",
        "parts_code",
        "op_no",
        "op_no_2nd",
        "op_no_3rd",
        "date",
        "serial_no",
        "serial_no_m_last4",
        "eng_no",
        "watch_id",
        "file_version_id",
    )
    _TRANSACTION_DATA_COLUMNS = tuple(
        f"data_{number:03d}" for number in range(1, 501)
    )
    _TRANSACTION_CONFLICT_KEYS = {"vpo_code", "date", "serial_no_m_last4"}

    def __init__(self, connection: _ConnectionProtocol):
        if connection is None:
            raise ValueError("MeasurementRepository requires a live database connection")
        self.conn = connection
        self._last_column_map: Optional[Dict[str, str]] = None
        self._last_vpo_code: Optional[str] = None

    @staticmethod
    def _normalize_text(value: Any) -> str:
        if value is None:
            return ""
        return unicodedata.normalize("NFKC", str(value)).strip()

    @classmethod
    def _normalize_nullable_text(cls, value: Any) -> Optional[str]:
        normalized = cls._normalize_text(value)
        return normalized or None

    @classmethod
    def _validated_version(cls, version: Any) -> str:
        if not isinstance(version, str):
            raise TypeError("version must be a str")
        if version != cls.CHECKSHEET_VERSION:
            raise ValueError(f"version must be {cls.CHECKSHEET_VERSION!r}")
        return version

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

    @classmethod
    def _prepare_master_values(
        cls,
        master_data: Mapping[str, Any],
        *,
        version: str,
        pi_no: int,
    ) -> Dict[str, Any]:
        if not 1 <= pi_no <= cls.MAX_PI_NO:
            raise ValueError(f"pi_no must be between 1 and {cls.MAX_PI_NO}")

        values = dict(master_data)
        values["version"] = cls._validated_version(version)
        values["pi_no"] = pi_no
        for key in cls._MASTER_NORMALIZED_KEYS:
            values[key] = cls._normalize_nullable_text(values.get(key))
        values["op_name"] = cls._normalize_text(master_data.get("op_name"))

        for key in cls._MASTER_REQUIRED_KEYS:
            if not values.get(key):
                raise ValueError(f"{key} is required")

        return values

    def _pi_search(
        self,
        cur: Any,
        record_data: Mapping[str, Any],
        version: str,
    ) -> Optional[int]:
        """Find a PI number by the historical measurement business identity."""
        direction = self._normalize_text(record_data.get("measure_direction"))
        depth = self._normalize_text(record_data.get("measure_depth"))
        common_params = (
            version,
            self._normalize_text(record_data.get("vpo_code")),
            self._normalize_text(record_data.get("op_name")),
            self._normalize_text(record_data.get("measure_item")),
            self._normalize_text(record_data.get("measure_point")),
        )

        if direction:
            sql_stmt = """
                SELECT pi_no
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND op_name = %s
                  AND measure_item = %s
                  AND measure_point = %s
                  AND measure_direction IS NOT DISTINCT FROM %s
                  AND specification IS NOT DISTINCT FROM %s
                ORDER BY pi_no
                LIMIT 1;
            """
            params = common_params + (
                direction,
                self._normalize_nullable_text(record_data.get("specification")),
            )
        else:
            sql_stmt = """
                SELECT pi_no
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND op_name = %s
                  AND measure_item = %s
                  AND measure_point = %s
                  AND measure_depth IS NOT DISTINCT FROM %s
                  AND specification IS NOT DISTINCT FROM %s
                ORDER BY pi_no
                LIMIT 1;
            """
            params = common_params + (
                depth or None,
                self._normalize_nullable_text(record_data.get("specification")),
            )

        cur.execute(sql_stmt, params)
        row = cur.fetchone()
        return None if row is None else int(row[0])

    def resolve_or_create_pi_no(
        self,
        record_data: Dict[str, Any],
        version: str = CHECKSHEET_VERSION,
    ) -> int:
        """Retain the historical business-key resolver for compatible callers."""
        version_value = self._validated_version(version)
        vpo_code = self._normalize_text(record_data.get("vpo_code"))
        if not vpo_code:
            raise ValueError("vpo_code is required")

        LOGGER.info(
            "[PI_NO SEARCH] Searching for existing PI_NO",
            pipeline_type="CHECKSHEET",
        )
        with self.conn.cursor() as cur:
            existing_pi_no = self._pi_search(cur, record_data, version_value)
            if existing_pi_no is not None:
                LOGGER.info(
                    f"[PI_NO FOUND] Found existing PI_NO: {existing_pi_no}",
                    pipeline_type="CHECKSHEET",
                )
                LOGGER.info(
                    f"[PI_NO REUSE] Reusing existing PI_NO: {existing_pi_no}",
                    pipeline_type="CHECKSHEET",
                )
                return existing_pi_no

            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s));",
                (version_value, vpo_code),
            )
            LOGGER.info(
                f"[ADVISORY LOCK ACQUIRED] version={version_value}, vpo_code={vpo_code}",
                pipeline_type="CHECKSHEET",
            )

            existing_pi_no = self._pi_search(cur, record_data, version_value)
            if existing_pi_no is not None:
                LOGGER.info(
                    f"[PI_NO FOUND] Found existing PI_NO: {existing_pi_no}",
                    pipeline_type="CHECKSHEET",
                )
                LOGGER.info(
                    f"[PI_NO REUSE] Reusing existing PI_NO: {existing_pi_no}",
                    pipeline_type="CHECKSHEET",
                )
                return existing_pi_no

            cur.execute(
                """
                SELECT COALESCE(MAX(pi_no), 0) + 1
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s;
                """,
                (version_value, vpo_code),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("PI number generation returned no row")
            next_pi_no = int(row[0])
            if next_pi_no > self.MAX_PI_NO:
                raise ValueError(f"PI_NO exceeds supported maximum {self.MAX_PI_NO}")

            master_data = dict(record_data)
            master_data.update(
                {
                    "version": version_value,
                    "vpo_code": vpo_code,
                    "pi_no": next_pi_no,
                }
            )
            self.insert_master(master_data)
            LOGGER.info(
                f"[PI_NO CREATE] Created PI_NO={next_pi_no}, version={version_value}, vpo_code={vpo_code}",
                pipeline_type="CHECKSHEET",
            )
            return next_pi_no

    def update_limits_if_newer(
        self,
        pi_no: int,
        incoming_date: DateValue,
        upper_limit: Any,
        lower_limit: Any,
        version: str = CHECKSHEET_VERSION,
        vpo_code: str = "",
        *,
        watch_id: Optional[str] = None,
        file_version_id: Optional[int] = None,
        file_name: Optional[str] = None,
        file_hash: Optional[str] = None,
        insert_id: str = "Vig",
    ) -> bool:
        """Update limits when file date is newer than master audit update_date."""
        version_value = self._validated_version(version)
        vpo_code_value = self._normalize_text(vpo_code)
        incoming_value = self._coerce_date(incoming_date)
        if not vpo_code_value:
            raise ValueError("vpo_code is required")
        if incoming_value is None:
            raise ValueError("incoming_date must be a valid date or datetime")

        new_upper = self._normalize_nullable_text(upper_limit)
        new_lower = self._normalize_nullable_text(lower_limit)

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT upper_limit, lower_limit, update_date
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND pi_no = %s
                FOR UPDATE;
                """,
                (version_value, vpo_code_value, int(pi_no)),
            )
            row = cur.fetchone()
            if row is None:
                LOGGER.info(
                    f"[LIMIT SKIP] PI_NO={pi_no} master row does not yet exist",
                    pipeline_type="CHECKSHEET",
                )
                return False

            old_upper, old_lower, existing_update_date = row[0], row[1], row[2]
            existing_date = self._coerce_date(existing_update_date)
            if existing_date is not None and incoming_value <= existing_date:
                LOGGER.info(
                    f"[LIMIT SKIP] PI_NO={pi_no} incoming={incoming_value} existing_update_date={existing_update_date}",
                    pipeline_type="CHECKSHEET",
                )
                return False

            cur.execute(
                """
                UPDATE fs3.pre_m_measurement
                SET upper_limit = %s,
                    lower_limit = %s,
                    update_date = CURRENT_TIMESTAMP,
                    update_id = %s
                WHERE version = %s
                  AND vpo_code = %s
                  AND pi_no = %s;
                """,
                (
                    new_upper,
                    new_lower,
                    insert_id,
                    version_value,
                    vpo_code_value,
                    int(pi_no),
                ),
            )
            updated = cur.rowcount == 1

            if updated:
                audit_logs = []
                if (old_upper or "") != (new_upper or ""):
                    audit_logs.append({
                        "version": version_value,
                        "watch_id": watch_id,
                        "file_version_id": file_version_id,
                        "file_name": file_name,
                        "file_hash": file_hash,
                        "vpo_code": vpo_code_value,
                        "pi_no": int(pi_no),
                        "change_key": "upper_limit",
                        "old_value": old_upper,
                        "new_value": new_upper,
                        "change_type": "LIMIT_UPDATE",
                        "remarks": f"Limit update from file date {incoming_value}",
                        "insert_id": insert_id,
                    })
                if (old_lower or "") != (new_lower or ""):
                    audit_logs.append({
                        "version": version_value,
                        "watch_id": watch_id,
                        "file_version_id": file_version_id,
                        "file_name": file_name,
                        "file_hash": file_hash,
                        "vpo_code": vpo_code_value,
                        "pi_no": int(pi_no),
                        "change_key": "lower_limit",
                        "old_value": old_lower,
                        "new_value": new_lower,
                        "change_type": "LIMIT_UPDATE",
                        "remarks": f"Limit update from file date {incoming_value}",
                        "insert_id": insert_id,
                    })
                if audit_logs:
                    self._insert_audit_logs(cur, audit_logs)

        if updated:
            LOGGER.info(
                f"[LIMIT UPDATE] PI_NO={pi_no} incoming={incoming_value} previous_update_date={existing_update_date}",
                pipeline_type="CHECKSHEET",
            )
        else:
            LOGGER.info(
                f"[LIMIT SKIP] PI_NO={pi_no} master row was not updated",
                pipeline_type="CHECKSHEET",
            )
        return updated

    def insert_master(self, master_data: Mapping[str, Any]) -> int:
        """Insert one master row using the explicitly supplied positional PI_NO."""
        version_value = self._validated_version(master_data.get("version"))
        pi_no = int(master_data["pi_no"])
        values = self._prepare_master_values(
            master_data,
            version=version_value,
            pi_no=pi_no,
        )
        params = tuple(values.get(key) for key in self._MASTER_VALUE_KEYS)
        statement = sql.SQL(
            """
            INSERT INTO fs3.pre_m_measurement (
                {}, insert_date, insert_id, update_date, update_id
            ) VALUES (
                {}, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP, %s
            );
            """
        ).format(
            sql.SQL(", ").join(
                sql.Identifier(column) for column in self._MASTER_DB_COLUMNS
            ),
            sql.SQL(", ").join(
                sql.Placeholder() for _ in self._MASTER_DB_COLUMNS
            ),
        )

        with self.conn.cursor() as cur:
            cur.execute(statement, params + ("Vig", "Vig"))
        return pi_no

    # =========================================================================
    # ARCHITECTURAL DESIGN: Generic Key-Value Master Definition Change Log
    # =========================================================================

    def sync_masters(
        self,
        master_records: List[Dict[str, Any]],
        version: str = CHECKSHEET_VERSION,
        *,
        watch_id: Optional[str] = None,
        file_version_id: Optional[int] = None,
        file_name: Optional[str] = None,
        file_hash: Optional[str] = None,
        insert_id: str = "Vig",
    ) -> Dict[str, str]:
        """Synchronize master records using stable business identity resolution.

        Performs field-by-field diff comparison against fs3.pre_m_measurement before
        upserting, generating generic key-value audit log entries in
        fs3.pre_cs_measurement_change_log for every changed master field.

        Returns the resolved workbook-column -> PI_NO data-column mapping so the
        caller can explicitly pass it to sync_transactions(), including when that
        method runs on a different repository instance.
        """
        if not isinstance(master_records, list):
            raise TypeError("master_records must be a list of master dictionaries")
        if not master_records:
            raise ValueError("master_records must not be empty")
        if len(master_records) > self.MAX_PI_NO:
            raise ValueError(f"master_records cannot exceed {self.MAX_PI_NO}")

        version_value = self._validated_version(version)
        vpo_code = self._normalize_text(master_records[0].get("vpo_code"))
        if not vpo_code:
            raise ValueError("vpo_code is required in master_records")

        conflict_update_columns = [
            column
            for column in self._MASTER_DB_COLUMNS
            if column not in {"version", "vpo_code", "pi_no", "upper_limit", "lower_limit"}
        ]
        statement = sql.SQL(
            """
            INSERT INTO fs3.pre_m_measurement (
                {}, insert_date, insert_id, update_date, update_id
            ) VALUES (
                {}, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP, %s
            )
            ON CONFLICT (vpo_code, pi_no, version)
            DO UPDATE SET
                {},
                update_date = CURRENT_TIMESTAMP,
                update_id = EXCLUDED.update_id;
            """
        ).format(
            sql.SQL(", ").join(
                sql.Identifier(column) for column in self._MASTER_DB_COLUMNS
            ),
            sql.SQL(", ").join(
                sql.Placeholder() for _ in self._MASTER_DB_COLUMNS
            ),
            sql.SQL(",\n                ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column),
                    sql.Identifier(column),
                )
                for column in conflict_update_columns
            ),
        )

        with self.conn.cursor() as cur:
            # Step 1: Read existing master rows to perform pre-upsert diff auditing
            fetch_sql = sql.SQL(
                """
                SELECT pi_no, {}
                FROM fs3.pre_m_measurement
                WHERE version = %s AND vpo_code = %s;
                """
            ).format(
                sql.SQL(", ").join(sql.Identifier(col) for col in self._AUDITABLE_MASTER_FIELDS)
            )
            cur.execute(fetch_sql, (version_value, vpo_code))
            existing_db_tuples = cur.fetchall()

            # Map pi_no -> dict of auditable field values
            existing_rows: Dict[int, Dict[str, Any]] = {}
            for row in existing_db_tuples:
                pi = int(row[0])
                existing_rows[pi] = dict(zip(self._AUDITABLE_MASTER_FIELDS, row[1:]))

            audit_logs: List[Dict[str, Any]] = []

            # Step 2: Resolve PI_NO for each incoming record via business identity
            claimed_db_pi_nos: Set[int] = set()
            used_pi_nos: Set[int] = set(existing_rows.keys())
            resolved_records: List[Tuple[int, Dict[str, Any]]] = []
            column_map: Dict[str, str] = {}

            for idx, master_record in enumerate(master_records, start=1):
                if not isinstance(master_record, dict):
                    raise TypeError("every master_records entry must be a dictionary")
                record_vpo_code = self._normalize_text(master_record.get("vpo_code"))
                if record_vpo_code != vpo_code:
                    raise ValueError("all master_records must use the same vpo_code")
                if master_record.get("version") != version_value:
                    raise ValueError("all master_records must use version '00000000'")

                orig_data_col = master_record.get("data_column") or f"data_{idx:03d}"
                pi_no: Optional[int] = None

                # STEP 1: Search DB via business-key resolver (_pi_search)
                existing_pi = self._pi_search(cur, master_record, version_value)
                if existing_pi is not None and existing_pi not in claimed_db_pi_nos:
                    pi_no = existing_pi

                # STEP 2: Search unclaimed DB master rows matching:
                # op_name, Inspection_no, measure_item, measure_point, measure_direction, measure_depth
                if pi_no is None:
                    rec_op_name = self._normalize_text(master_record.get("op_name"))
                    rec_inspection_no = self._normalize_text(master_record.get("Inspection_no"))
                    rec_item = self._normalize_text(master_record.get("measure_item"))
                    rec_point = self._normalize_text(master_record.get("measure_point"))
                    rec_dir = self._normalize_text(master_record.get("measure_direction"))
                    rec_depth = self._normalize_text(master_record.get("measure_depth"))

                    for ex_pi, ex_fields in existing_rows.items():
                        if ex_pi in claimed_db_pi_nos:
                            continue

                        ex_op_name = self._normalize_text(ex_fields.get("op_name"))
                        ex_inspection_no = self._normalize_text(ex_fields.get("Inspection_no"))
                        ex_item = self._normalize_text(ex_fields.get("measure_item"))
                        ex_point = self._normalize_text(ex_fields.get("measure_point"))
                        ex_dir = self._normalize_text(ex_fields.get("measure_direction"))
                        ex_depth = self._normalize_text(ex_fields.get("measure_depth"))

                        if (
                            rec_op_name == ex_op_name
                            and rec_inspection_no == ex_inspection_no
                            and rec_item == ex_item
                            and rec_point == ex_point
                            and rec_dir == ex_dir
                            and rec_depth == ex_depth
                        ):
                            pi_no = ex_pi
                            break

                # STEP 3: Allocate a new PI_NO if no existing definition matches
                if pi_no is None:
                    next_pi = max(used_pi_nos, default=0) + 1
                    if next_pi > self.MAX_PI_NO:
                        raise ValueError(f"PI_NO exceeds supported maximum {self.MAX_PI_NO}")
                    pi_no = next_pi

                claimed_db_pi_nos.add(pi_no)
                used_pi_nos.add(pi_no)

                resolved_data_col = f"data_{pi_no:03d}"
                master_record["pi_no"] = pi_no
                master_record["data_column"] = resolved_data_col
                master_record["orig_data_column"] = orig_data_col
                column_map[orig_data_col] = resolved_data_col

                resolved_records.append((pi_no, master_record))

            # Return the resolved column mapping explicitly to the caller.
            # Repository instance state is not required for transaction remapping.
            # Keep the legacy state only as a backward-compatible fallback.
            self._last_column_map = column_map
            self._last_vpo_code = vpo_code

            # Step 3: Compare resolved master records against existing DB rows for audit logging
            for pi_no, master_record in resolved_records:
                values = self._prepare_master_values(
                    master_record,
                    version=version_value,
                    pi_no=pi_no,
                )

                if pi_no not in existing_rows:
                    # New PI_NO slot creation -> MASTER_INSERT
                    for field in self._AUDITABLE_MASTER_FIELDS:
                        new_val = values.get(field)
                        if new_val is not None and str(new_val).strip() != "":
                            audit_logs.append({
                                "version": version_value,
                                "watch_id": watch_id,
                                "file_version_id": file_version_id,
                                "file_name": file_name,
                                "file_hash": file_hash,
                                "vpo_code": vpo_code,
                                "pi_no": pi_no,
                                "change_key": field,
                                "old_value": None,
                                "new_value": str(new_val),
                                "change_type": "MASTER_INSERT",
                                "remarks": "New master definition inserted",
                                "insert_id": insert_id,
                            })
                else:
                    # Existing PI_NO slot -> Field-by-field comparison
                    ex_fields = existing_rows[pi_no]
                    for field in self._AUDITABLE_MASTER_FIELDS:
                        # Note: sync_masters excludes limits from conflict update
                        if field in {"upper_limit", "lower_limit"}:
                            continue

                        old_val = ex_fields.get(field)
                        new_val = values.get(field)

                        old_str = self._normalize_text(old_val)
                        new_str = self._normalize_text(new_val)

                        if old_str != new_str:
                            change_type = "GENERAL_UPDATE"
                            if field == "specification":
                                change_type = "SPECIFICATION_UPDATE"

                            audit_logs.append({
                                "version": version_value,
                                "watch_id": watch_id,
                                "file_version_id": file_version_id,
                                "file_name": file_name,
                                "file_hash": file_hash,
                                "vpo_code": vpo_code,
                                "pi_no": pi_no,
                                "change_key": field,
                                "old_value": old_str or None,
                                "new_value": new_str or None,
                                "change_type": change_type,
                                "remarks": "Master definition modified",
                                "insert_id": insert_id,
                            })

            # Step 4: Audit deleted master slots (unclaimed DB rows)
            unclaimed_pi_nos = sorted(set(existing_rows.keys()) - claimed_db_pi_nos)
            for old_pi_no in unclaimed_pi_nos:
                audit_logs.append({
                    "version": version_value,
                    "watch_id": watch_id,
                    "file_version_id": file_version_id,
                    "file_name": file_name,
                    "file_hash": file_hash,
                    "vpo_code": vpo_code,
                    "pi_no": old_pi_no,
                    "change_key": "master_row",
                    "old_value": f"PI_NO={old_pi_no}",
                    "new_value": None,
                    "change_type": "MASTER_DELETE",
                    "remarks": f"Master slot {old_pi_no} deleted (master count reduced)",
                    "insert_id": insert_id,
                })

            # Step 5: Write all audit entries to fs3.pre_cs_measurement_change_log
            if audit_logs:
                self._insert_audit_logs(cur, audit_logs)

            # Step 6: Execute master upserts
            for pi_no, master_record in resolved_records:
                values = self._prepare_master_values(
                    master_record,
                    version=version_value,
                    pi_no=pi_no,
                )
                params = tuple(values.get(key) for key in self._MASTER_VALUE_KEYS)
                cur.execute(statement, params + (insert_id, insert_id))

            # Step 7: Clean up deleted master rows
            if unclaimed_pi_nos:
                cur.execute(
                    """
                    DELETE FROM fs3.pre_m_measurement
                    WHERE version = %s
                      AND vpo_code = %s
                      AND pi_no = ANY(%s);
                    """,
                    (version_value, vpo_code, unclaimed_pi_nos),
                )

        return column_map

    def _insert_audit_logs(
        self,
        cur: Any,
        audit_logs: List[Dict[str, Any]],
    ) -> None:
        """Batch insert generic key-value audit log records."""
        insert_sql = """
            INSERT INTO fs3.pre_cs_measurement_change_log (
                version,
                watch_id,
                file_version_id,
                file_name,
                file_hash,
                vpo_code,
                pi_no,
                change_key,
                old_value,
                new_value,
                change_type,
                remarks,
                change_time,
                insert_date,
                insert_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s
            );
        """
        for log in audit_logs:
            cur.execute(
                insert_sql,
                (
                    log["version"],
                    log.get("watch_id"),
                    log.get("file_version_id"),
                    log.get("file_name"),
                    log.get("file_hash"),
                    log["vpo_code"],
                    log["pi_no"],
                    log["change_key"],
                    log.get("old_value"),
                    log.get("new_value"),
                    log["change_type"],
                    log.get("remarks"),
                    log.get("insert_id", "Vig"),
                ),
            )

    @classmethod
    def remap_transaction_records(
        cls,
        transaction_records: List[Dict[str, Any]],
        column_map: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Remap transaction record data_xxx keys using resolved PI_NO data columns.

        Ensures workbook-position data columns (e.g. data_118) are remapped to their
        resolved PI_NO data columns (e.g. data_122) prior to transaction sync.
        """
        if not column_map or not transaction_records:
            return transaction_records

        # Fail fast if the supplied mapping itself resolves two workbook columns
        # to the same PI_NO data column.
        destination_sources: Dict[str, str] = {}
        for source_col, resolved_col in column_map.items():
            previous_source = destination_sources.get(resolved_col)
            if previous_source is not None and previous_source != source_col:
                raise ValueError(
                    "Transaction remap collision detected: "
                    f"{previous_source} and {source_col} both resolved to {resolved_col}"
                )
            destination_sources[resolved_col] = source_col

        remapped_list: List[Dict[str, Any]] = []
        for record in transaction_records:
            new_record = dict(record)

            # Collect existing transaction measurement values from record
            data_values: Dict[str, Any] = {}
            for col in cls._TRANSACTION_DATA_COLUMNS:
                if col in record:
                    data_values[col] = record[col]

            # Clear all data_xxx columns in new_record
            for col in cls._TRANSACTION_DATA_COLUMNS:
                new_record[col] = None

            # Track which source column populated each resolved destination.
            resolved_sources: Dict[str, str] = {}

            # Remap values using column_map (orig_data_col -> resolved_data_col)
            for orig_col, val in data_values.items():
                if val is not None:
                    resolved_col = column_map.get(orig_col, orig_col)

                    previous_source = resolved_sources.get(resolved_col)
                    if previous_source is not None and previous_source != orig_col:
                        raise ValueError(
                            "Transaction remap collision detected: "
                            f"{previous_source} and {orig_col} both resolved to {resolved_col}"
                        )

                    # Never overwrite a non-null measurement originating from
                    # another source column.
                    if (
                        new_record.get(resolved_col) is not None
                        and resolved_sources.get(resolved_col) != orig_col
                    ):
                        raise ValueError(
                            "Transaction remap collision detected: "
                            f"{resolved_sources[resolved_col]} and {orig_col} "
                            f"both resolved to {resolved_col}"
                        )

                    resolved_sources[resolved_col] = orig_col
                    new_record[resolved_col] = val

            remapped_list.append(new_record)

        return remapped_list

# =========================================================================
# ARCHITECTURAL DESIGN: Watch-ID Scoped Transaction Synchronization
# =========================================================================

    def sync_transactions(
        self,
        transaction_records: List[Dict[str, Any]],
        version: str = CHECKSHEET_VERSION,
        *,
        watch_id: Optional[str] = None,
        file_version_id: Optional[int] = None,
        file_name: Optional[str] = None,
        file_hash: Optional[str] = None,
        insert_id: str = "Vig",
        master_records: Optional[List[Dict[str, Any]]] = None,
        column_map: Optional[Dict[str, str]] = None,
    ) -> int:
        """Synchronize transaction records so fs3.pre_t_measurement matches the workbook scope.

        Performs full INSERT / UPDATE / DELETE synchronization scoped strictly to transactions
        belonging to the specified watch_id, preserving historical records from other files/months
        while allowing deletions of corrected records within the target file.
        """
        if not isinstance(transaction_records, list):
            raise TypeError("transaction_records must be a list of dictionaries")
        if not transaction_records:
            raise ValueError("transaction_records must not be empty")

        version_value = self._validated_version(version)
        vpo_code = self._normalize_text(transaction_records[0].get("vpo_code"))
        if not vpo_code:
            raise ValueError("vpo_code is required in transaction_records")

        # Resolve active column mapping for remapping workbook-position data columns
        active_column_map: Dict[str, str] = {}
        if column_map is not None:
            active_column_map = column_map
        elif master_records:
            for idx, m_rec in enumerate(master_records, start=1):
                orig = m_rec.get("orig_data_column") or f"data_{idx:03d}"
                res = m_rec.get("data_column") or f"data_{m_rec.get('pi_no', idx):03d}"
                active_column_map[orig] = res
        elif self._last_column_map is not None and self._last_vpo_code == vpo_code:
            active_column_map = self._last_column_map

        if active_column_map:
            transaction_records = self.remap_transaction_records(transaction_records, active_column_map)

        # Normalize incoming records
        prepared_incoming: Dict[tuple, Dict[str, Any]] = {}
        for idx, rec in enumerate(transaction_records):
            rec_vpo = self._normalize_text(rec.get("vpo_code"))
            if rec_vpo != vpo_code:
                raise ValueError("all transaction_records must use the same vpo_code")

            dt_val = self._coerce_date(rec.get("date"))
            serial_last4 = self._normalize_text(rec.get("serial_no_m_last4"))
            if dt_val is None:
                raise ValueError(f"transaction_records[{idx}].date is invalid")
            if not serial_last4:
                raise ValueError(f"transaction_records[{idx}].serial_no_m_last4 is required")

            biz_key = (dt_val, serial_last4)
            prepared_incoming[biz_key] = rec

        audit_logs: List[Dict[str, Any]] = []

        with self.conn.cursor() as cur:
            # Step 1: Read existing DB transaction rows scoped strictly to watch_id boundary
            select_cols = (
                [
                    "date",
                    "serial_no_m_last4",
                    "serial_no",
                    "mc_no",
                    "vehicle_model",
                    "parts_code",
                    "op_no",
                    "eng_no",
                    "watch_id",
                    "file_version_id",
                ]
                + list(self._TRANSACTION_DATA_COLUMNS)
            )

            if not watch_id:
                raise ValueError("watch_id is required")
            query_sql = sql.SQL(
                    """
                    SELECT {}
                    FROM fs3.pre_t_measurement
                    WHERE version = %s
                      AND vpo_code = %s
                      AND watch_id = %s;
                    """
            ).format(sql.SQL(", ").join(sql.Identifier(col) for col in select_cols))
            cur.execute(query_sql, (version_value, vpo_code, watch_id))

            existing_db_rows = cur.fetchall()

            existing_db_map: Dict[tuple, Dict[str, Any]] = {}
            for row in existing_db_rows:
                db_dt = self._coerce_date(row[0])
                db_last4 = row[1]
                if db_dt and db_last4:
                    row_dict = {col: val for col, val in zip(select_cols, row)}
                    existing_db_map[(db_dt, db_last4)] = row_dict

            incoming_keys: Set[tuple] = set(prepared_incoming.keys())
            existing_keys: Set[tuple] = set(existing_db_map.keys())

            # Step 2: Identify TRANSACTION_DELETE (scoped strictly to current watch_id)
            keys_to_delete = existing_keys - incoming_keys
            for del_key in keys_to_delete:
                deleted_rec = existing_db_map[del_key]
                serial_key = deleted_rec.get("serial_no") or del_key[1]
                audit_logs.append({
                    "version": version_value,
                    "watch_id": watch_id,
                    "file_version_id": file_version_id,
                    "file_name": file_name,
                    "file_hash": file_hash,
                    "vpo_code": vpo_code,
                    "pi_no": 0,
                    "change_key": str(serial_key),
                    "old_value": "EXISTS",
                    "new_value": "DELETED",
                    "change_type": "TRANSACTION_DELETE",
                    "remarks": "Transaction record removed from source workbook",
                    "insert_id": insert_id,
                })

                # Execute physical deletion from fs3.pre_t_measurement
                del_sql = """
                    DELETE FROM fs3.pre_t_measurement
                    WHERE version = %s AND vpo_code = %s AND watch_id = %s AND date = %s AND serial_no_m_last4 = %s;
                """
                cur.execute(del_sql, (version_value, vpo_code, watch_id, del_key[0], del_key[1]))

            # Step 3: Identify TRANSACTION_INSERT & TRANSACTION_UPDATE
            for inc_key, inc_rec in prepared_incoming.items():
                serial_key = inc_rec.get("serial_no") or inc_key[1]

                if inc_key not in existing_keys:
                    # New Serial Added -> TRANSACTION_INSERT
                    audit_logs.append({
                        "version": version_value,
                        "watch_id": watch_id,
                        "file_version_id": file_version_id,
                        "file_name": file_name,
                        "file_hash": file_hash,
                        "vpo_code": vpo_code,
                        "pi_no": 0,
                        "change_key": str(serial_key),
                        "old_value": None,
                        "new_value": "INSERTED",
                        "change_type": "TRANSACTION_INSERT",
                        "remarks": "Transaction record added in source workbook",
                        "insert_id": insert_id,
                    })
                    self._upsert_single_transaction(
                        cur,
                        inc_rec,
                        version_value,
                        insert_id,
                        watch_id=watch_id,
                        file_version_id=file_version_id,
                    )
                else:
                    # Serial Exists in Both -> Check for TRANSACTION_UPDATE in data_001..data_500
                    existing_rec = existing_db_map[inc_key]
                    has_changes = False

                    for data_col in self._TRANSACTION_DATA_COLUMNS:
                        old_val = self._normalize_text(existing_rec.get(data_col))
                        new_val = self._normalize_text(inc_rec.get(data_col))

                        if old_val != new_val:
                            has_changes = True
                            col_idx = int(data_col.split("_")[1])  # e.g., data_005 -> pi_no 5
                            audit_logs.append({
                                "version": version_value,
                                "watch_id": watch_id,
                                "file_version_id": file_version_id,
                                "file_name": file_name,
                                "file_hash": file_hash,
                                "vpo_code": vpo_code,
                                "pi_no": col_idx,
                                "change_key": str(serial_key),
                                "old_value": old_val or None,
                                "new_value": new_val or None,
                                "change_type": "TRANSACTION_UPDATE",
                                "remarks": f"Updated transaction value of {data_col}",
                                "insert_id": insert_id,
                            })

                    if has_changes:
                        self._upsert_single_transaction(
                            cur,
                            inc_rec,
                            version_value,
                            insert_id,
                            watch_id=watch_id,
                            file_version_id=file_version_id,
                        )

            # Step 4: Write all transaction audit log entries
            if audit_logs:
                self._insert_audit_logs(cur, audit_logs)

        LOGGER.info(
            f"[TRANSACTION SYNC COMPLETE] "
            f"vpo_code={vpo_code} "
            f"watch_id={watch_id} "
            f"file_version_id={file_version_id} "
            f"incoming={len(transaction_records)} "
            f"deleted={len(keys_to_delete)} "
            f"audit_records={len(audit_logs)}",
            pipeline_type="CHECKSHEET",
        )
        return len(transaction_records)

    def _upsert_single_transaction(
        self,
        cur: Any,
        transaction_data: Mapping[str, Any],
        version_value: str,
        insert_id: str,
        watch_id: Optional[str] = None,
        file_version_id: Optional[int] = None,
    ) -> None:
        """Internal helper to upsert a single transaction row into fs3.pre_t_measurement."""
        values = dict(transaction_data)
        values["version"] = version_value
        if watch_id is not None:
            values["watch_id"] = self._normalize_nullable_text(watch_id)
        if file_version_id is not None:
            values["file_version_id"] = file_version_id

        for key in (
            "mc_no",
            "vpo_code",
            "vehicle_model",
            "parts_code",
            "op_no",
            "op_no_2nd",
            "op_no_3rd",
            "serial_no",
            "serial_no_m_last4",
            "eng_no",
        ):
            values[key] = self._normalize_nullable_text(values.get(key))

        for column in self._TRANSACTION_DATA_COLUMNS:
            value = values.get(column)
            if value is not None and not isinstance(value, str):
                values[column] = str(value)

        insert_columns: Sequence[str] = (
            self._TRANSACTION_BUSINESS_COLUMNS + self._TRANSACTION_DATA_COLUMNS
        )
        params = tuple(values.get(column) for column in insert_columns) + (insert_id, insert_id)
        mutable_columns = [
            column
            for column in insert_columns
            if column not in self._TRANSACTION_CONFLICT_KEYS
        ]
        statement = sql.SQL(
            """
            INSERT INTO fs3.pre_t_measurement (
                {}, insert_date, insert_id, update_date, update_id
            ) VALUES (
                {}, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP, %s
            )
            ON CONFLICT (vpo_code, date, serial_no_m_last4)
            DO UPDATE SET
                {},
                update_date = CURRENT_TIMESTAMP,
                update_id = EXCLUDED.update_id;
            """
        ).format(
            sql.SQL(", ").join(sql.Identifier(column) for column in insert_columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in insert_columns),
            sql.SQL(",\n                ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column),
                    sql.Identifier(column),
                )
                for column in mutable_columns
            ),
        )
        cur.execute(statement, params)

    def insert_transaction(self, transaction_data: Mapping[str, Any]) -> None:
        """Legacy helper retained for compatibility."""
        version_value = self._validated_version(transaction_data.get("version"))
        with self.conn.cursor() as cur:
            self._upsert_single_transaction(cur, transaction_data, version_value, "Vig")


__all__ = ["MeasurementRepository"]