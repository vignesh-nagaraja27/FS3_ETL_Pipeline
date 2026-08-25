# -*- coding: utf-8 -*-
"""PostgreSQL repository for FS3 checksheet master and transaction records."""

from __future__ import annotations

import unicodedata
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Set, Tuple, Union

from psycopg2 import sql
from utils.logger import LOGGER


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
        "pi_no",
        "serial_no",
        "serial_no_m_last4",
        "eng_no",
        "value",
        "watch_id",
        "file_version_id",
    )

    _TRANSACTION_CONFLICT_KEYS = {
        "version",
        "vpo_code",
        "date",
        "serial_no_m_last4",
        "pi_no",
        "watch_id",
    }

    def __init__(self, connection: _ConnectionProtocol):
        if connection is None:
            raise ValueError(
                "MeasurementRepository requires a live database connection"
            )

        self.conn = connection

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
            raise ValueError(
                f"version must be {cls.CHECKSHEET_VERSION!r}"
            )

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
        if pi_no <= 0:
            raise ValueError(
                f"pi_no must be greater than 0"
            )

        values = dict(master_data)

        values["version"] = cls._validated_version(version)
        values["pi_no"] = pi_no

        for key in cls._MASTER_NORMALIZED_KEYS:
            values[key] = cls._normalize_nullable_text(
                values.get(key)
            )

        values["op_name"] = cls._normalize_text(
            master_data.get("op_name")
        )

        for key in cls._MASTER_REQUIRED_KEYS:
            if not values.get(key):
                raise ValueError(f"{key} is required")

        return values

    def _pi_search(
        self,
        cur: Any,
        record_data: Mapping[str, Any],
        version: str,
        is_partial_master: bool = False,
    ) -> Optional[int]:
        """Find a PI number by the historical measurement business identity."""

        direction = self._normalize_text(
            record_data.get("measure_direction")
        )

        depth = self._normalize_text(
            record_data.get("measure_depth")
        )

        machining_surface = self._normalize_nullable_text(
            record_data.get("machining_surface")
        )

        op_no_2nd = self._normalize_nullable_text(
            record_data.get("op_no_2nd")
        )

        op_no_3rd = self._normalize_nullable_text(
            record_data.get("op_no_3rd")
        )

        specification = self._normalize_nullable_text(
            record_data.get("specification")
        )

        base_params = (
            version,
            self._normalize_text(
                record_data.get("vpo_code")
            ),
            self._normalize_text(
                record_data.get("op_name")
            ),
            self._normalize_text(
                record_data.get("measure_item")
            ),
            self._normalize_text(
                record_data.get("measure_point")
            ),
        )

        if is_partial_master:
            common_params = base_params + (
                op_no_2nd,
                op_no_3rd,
            )

            op_sql_clause = """
                  AND op_no_2nd IS NOT DISTINCT FROM %s
                  AND op_no_3rd IS NOT DISTINCT FROM %s"""

        else:
            common_params = base_params
            op_sql_clause = ""

        if direction:
            sql_stmt = f"""
                SELECT pi_no
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND op_name = %s
                  AND measure_item = %s
                  AND measure_point = %s{op_sql_clause}
                  AND machining_surface IS NOT DISTINCT FROM %s
                  AND measure_direction IS NOT DISTINCT FROM %s
                  AND specification IS NOT DISTINCT FROM %s
                ORDER BY pi_no
                LIMIT 1;
            """

            params = common_params + (
                machining_surface,
                direction,
                specification,
            )

        elif depth:
            sql_stmt = f"""
                SELECT pi_no
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND op_name = %s
                  AND measure_item = %s
                  AND measure_point = %s{op_sql_clause}
                  AND machining_surface IS NOT DISTINCT FROM %s
                  AND measure_depth IS NOT DISTINCT FROM %s
                  AND specification IS NOT DISTINCT FROM %s
                ORDER BY pi_no
                LIMIT 1;
            """

            params = common_params + (
                machining_surface,
                depth,
                specification,
            )

        else:
            sql_stmt = f"""
                SELECT pi_no
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND op_name = %s
                  AND measure_item = %s
                  AND measure_point = %s{op_sql_clause}
                  AND machining_surface IS NOT DISTINCT FROM %s
                  AND specification IS NOT DISTINCT FROM %s
                ORDER BY pi_no
                LIMIT 1;
            """

            params = common_params + (
                machining_surface,
                specification,
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

        vpo_code = self._normalize_text(
            record_data.get("vpo_code")
        )

        if not vpo_code:
            raise ValueError("vpo_code is required")

        LOGGER.info(
            "[PI_NO SEARCH] Searching for existing PI_NO",
            pipeline_type="CHECKSHEET",
        )

        with self.conn.cursor() as cur:
            existing_pi_no = self._pi_search(
                cur,
                record_data,
                version_value,
            )

            if existing_pi_no is not None:
                LOGGER.info(
                    f"[PI_NO FOUND] Found existing PI_NO: {existing_pi_no}",
                    pipeline_type="CHECKSHEET",
                )

                return existing_pi_no

            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s));",
                (
                    version_value,
                    vpo_code,
                ),
            )

            existing_pi_no = self._pi_search(
                cur,
                record_data,
                version_value,
            )

            if existing_pi_no is not None:
                LOGGER.info(
                    f"[PI_NO FOUND] Found existing PI_NO: {existing_pi_no}",
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
                (
                    version_value,
                    vpo_code,
                ),
            )

            row = cur.fetchone()

            if row is None:
                raise RuntimeError(
                    "PI number generation returned no row"
                )

            next_pi_no = int(row[0])

            if next_pi_no <= 0:
                raise ValueError("Invalid PI_NO generated")

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
                f"[PI_NO CREATE] Created PI_NO={next_pi_no}, "
                f"version={version_value}, vpo_code={vpo_code}",
                pipeline_type="CHECKSHEET",
            )

            return next_pi_no

    def _get_latest_transaction_business_date(
        self,
        version: str,
        vpo_code: str,
        watch_id: Optional[str] = None,
        *,
        cur: Optional[Any] = None,
    ) -> Optional[date]:
        """Return the latest transaction business date MAX(date)."""

        version_value = self._validated_version(version)

        vpo_code_value = self._normalize_text(vpo_code)

        if not watch_id:
            raise ValueError("watch_id is required")

        watch_id_value = self._normalize_text(watch_id)

        def _execute(cursor: Any) -> Optional[date]:
            cursor.execute(
                """
                SELECT MAX(date)
                FROM fs3.pre_t_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND watch_id = %s;
                """,
                (
                    version_value,
                    vpo_code_value,
                    watch_id_value,
                ),
            )

            row = cursor.fetchone()

            if row is None or row[0] is None:
                return None

            return self._coerce_date(row[0])

        if cur is not None:
            return _execute(cur)

        with self.conn.cursor() as cursor:
            return _execute(cursor)

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
        cur: Optional[Any] = None,
    ) -> bool:
        """Update limits and generate LIMIT_UPDATE audit records."""

        version_value = self._validated_version(version)

        vpo_code_value = self._normalize_text(vpo_code)

        incoming_value = self._coerce_date(incoming_date)

        if not vpo_code_value:
            raise ValueError("vpo_code is required")

        if incoming_value is None:
            raise ValueError(
                "incoming_date must be a valid date or datetime"
            )

        new_upper = self._normalize_nullable_text(
            upper_limit
        )

        new_lower = self._normalize_nullable_text(
            lower_limit
        )

        def _execute_update(cursor: Any) -> bool:
            existing_business_date = (
                self._get_latest_transaction_business_date(
                    version=version_value,
                    vpo_code=vpo_code_value,
                    watch_id=watch_id,
                    cur=cursor,
                )
            )

            if (
                existing_business_date is not None
                and incoming_value < existing_business_date
            ):
                return False

            cursor.execute(
                """
                SELECT upper_limit, lower_limit, update_date
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND pi_no = %s
                FOR UPDATE;
                """,
                (
                    version_value,
                    vpo_code_value,
                    int(pi_no),
                ),
            )

            row = cursor.fetchone()

            if row is None:
                return False

            old_upper_raw = row[0]
            old_lower_raw = row[1]

            old_upper = self._normalize_nullable_text(
                old_upper_raw
            )

            old_lower = self._normalize_nullable_text(
                old_lower_raw
            )

            audit_logs: List[Dict[str, Any]] = []

            if (old_upper or "") != (new_upper or ""):
                audit_logs.append(
                    {
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
                        "remarks": (
                            f"Limit update from file date "
                            f"{incoming_value}"
                        ),
                        "insert_id": insert_id,
                    }
                )

            if (old_lower or "") != (new_lower or ""):
                audit_logs.append(
                    {
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
                        "remarks": (
                            f"Limit update from file date "
                            f"{incoming_value}"
                        ),
                        "insert_id": insert_id,
                    }
                )

            if not audit_logs:
                return False

            cursor.execute(
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

            updated = cursor.rowcount == 1

            if updated:
                self._insert_audit_logs(
                    cursor,
                    audit_logs,
                )

            return updated

        if cur is not None:
            return _execute_update(cur)

        with self.conn.cursor() as cursor:
            return _execute_update(cursor)

    def insert_master(
        self,
        master_data: Mapping[str, Any],
    ) -> int:
        """Insert one master row using the explicitly supplied PI_NO."""

        version_value = self._validated_version(
            master_data.get("version")
        )

        pi_no = int(master_data["pi_no"])

        values = self._prepare_master_values(
            master_data,
            version=version_value,
            pi_no=pi_no,
        )

        params = tuple(
            values.get(key)
            for key in self._MASTER_VALUE_KEYS
        )

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
                sql.Identifier(column)
                for column in self._MASTER_DB_COLUMNS
            ),
            sql.SQL(", ").join(
                sql.Placeholder()
                for _ in self._MASTER_DB_COLUMNS
            ),
        )

        with self.conn.cursor() as cur:
            cur.execute(
                statement,
                params + ("Vig", "Vig"),
            )

        return pi_no

    def sync_masters(
        self,
        master_records: List[Dict[str, Any]],
        version: str = CHECKSHEET_VERSION,
        *,
        incoming_file_date: DateValue,
        watch_id: Optional[str] = None,
        file_version_id: Optional[int] = None,
        file_name: Optional[str] = None,
        file_hash: Optional[str] = None,
        insert_id: str = "Vig",
        is_partial_master: bool = False,
    ) -> None:
        """
        Synchronize master records using stable business identity resolution.

        Contract relied on by callers (notably LoadChecksheetStep):
        this method mutates each dict inside ``master_records`` IN PLACE,
        adding/overwriting "pi_no" once identity resolution or allocation
        completes for that record. It does not return the resolved
        mapping. LoadChecksheetStep depends on this in-place mutation to
        translate each transaction's temporary source_master_index into
        the corresponding pi_no after this call returns. If this method
        is ever changed to operate on copies of the input dicts (as
        _prepare_master_values() already does internally for DB param
        building), that downstream resolution will silently break.
        """

        if not isinstance(master_records, list):
            raise TypeError(
                "master_records must be a list of master dictionaries"
            )

        if not master_records:
            raise ValueError(
                "master_records must not be empty"
            )

        coerced_incoming_date = self._coerce_date(
            incoming_file_date
        )

        if coerced_incoming_date is None:
            raise ValueError(
                "incoming_file_date is required and must be a valid "
                "date or datetime"
            )

        version_value = self._validated_version(version)

        vpo_code = self._normalize_text(
            master_records[0].get("vpo_code")
        )

        if not vpo_code:
            raise ValueError(
                "vpo_code is required in master_records"
            )

        conflict_update_columns = [
            column
            for column in self._MASTER_DB_COLUMNS
            if column not in {
                "version",
                "vpo_code",
                "pi_no",
                "upper_limit",
                "lower_limit",
            }
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
                sql.Identifier(column)
                for column in self._MASTER_DB_COLUMNS
            ),
            sql.SQL(", ").join(
                sql.Placeholder()
                for _ in self._MASTER_DB_COLUMNS
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
            fetch_sql = sql.SQL(
                """
                SELECT pi_no, {}
                FROM fs3.pre_m_measurement
                WHERE version = %s
                  AND vpo_code = %s;
                """
            ).format(
                sql.SQL(", ").join(
                    sql.Identifier(col)
                    for col in self._AUDITABLE_MASTER_FIELDS
                )
            )

            cur.execute(
                fetch_sql,
                (
                    version_value,
                    vpo_code,
                ),
            )

            existing_db_tuples = cur.fetchall()

            existing_rows: Dict[int, Dict[str, Any]] = {}

            for row in existing_db_tuples:
                pi = int(row[0])

                existing_rows[pi] = dict(
                    zip(
                        self._AUDITABLE_MASTER_FIELDS,
                        row[1:],
                    )
                )

            audit_logs: List[Dict[str, Any]] = []

            claimed_db_pi_nos: Set[int] = set()
            used_pi_nos: Set[int] = set(
                existing_rows.keys()
            )

            resolved_records: List[
                Tuple[int, Dict[str, Any]]
            ] = []

            for master_record in master_records:
                if not isinstance(master_record, dict):
                    raise TypeError(
                        "every master_records entry must be a dictionary"
                    )

                record_vpo_code = self._normalize_text(
                    master_record.get("vpo_code")
                )

                if record_vpo_code != vpo_code:
                    raise ValueError(
                        "all master_records must use the same vpo_code"
                    )

                if master_record.get("version") != version_value:
                    raise ValueError(
                        "all master_records must use version '00000000'"
                    )

                rec_op_name = self._normalize_text(
                    master_record.get("op_name")
                )

                rec_inspection_no = self._normalize_text(
                    master_record.get("Inspection_no")
                )

                rec_item = self._normalize_text(
                    master_record.get("measure_item")
                )

                rec_point = self._normalize_text(
                    master_record.get("measure_point")
                )

                rec_surface = self._normalize_nullable_text(
                    master_record.get("machining_surface")
                )

                rec_spec = self._normalize_nullable_text(
                    master_record.get("specification")
                )

                rec_dir = self._normalize_text(
                    master_record.get("measure_direction")
                )

                rec_depth = self._normalize_text(
                    master_record.get("measure_depth")
                )

                rec_op_no_2nd = self._normalize_text(
                    master_record.get("op_no_2nd")
                )

                rec_op_no_3rd = self._normalize_text(
                    master_record.get("op_no_3rd")
                )

                pi_no: Optional[int] = None

                existing_pi = self._pi_search(
                    cur,
                    master_record,
                    version_value,
                    is_partial_master=is_partial_master,
                )

                if (
                    existing_pi is not None
                    and existing_pi not in claimed_db_pi_nos
                ):
                    pi_no = existing_pi

                if pi_no is None:
                    for ex_pi, ex_fields in existing_rows.items():
                        if ex_pi in claimed_db_pi_nos:
                            continue

                        ex_op_name = self._normalize_text(
                            ex_fields.get("op_name")
                        )

                        ex_inspection_no = self._normalize_text(
                            ex_fields.get("Inspection_no")
                        )

                        ex_item = self._normalize_text(
                            ex_fields.get("measure_item")
                        )

                        ex_point = self._normalize_text(
                            ex_fields.get("measure_point")
                        )

                        ex_surface = self._normalize_nullable_text(
                            ex_fields.get("machining_surface")
                        )

                        ex_spec = self._normalize_nullable_text(
                            ex_fields.get("specification")
                        )

                        ex_dir = self._normalize_text(
                            ex_fields.get("measure_direction")
                        )

                        ex_depth = self._normalize_text(
                            ex_fields.get("measure_depth")
                        )

                        ex_op_no_2nd = self._normalize_text(
                            ex_fields.get("op_no_2nd")
                        )

                        ex_op_no_3rd = self._normalize_text(
                            ex_fields.get("op_no_3rd")
                        )

                        if is_partial_master:
                            op_no_2nd_3rd_match = (
                                rec_op_no_2nd == ex_op_no_2nd
                                and rec_op_no_3rd == ex_op_no_3rd
                            )

                            if not op_no_2nd_3rd_match:
                                continue

                        direction_depth_match = (
                            (
                                bool(rec_dir)
                                and rec_dir == ex_dir
                            )
                            or (
                                not rec_dir
                                and bool(rec_depth)
                                and rec_depth == ex_depth
                            )
                            or (
                                not rec_dir
                                and not rec_depth
                            )
                        )

                        if not rec_dir and not rec_depth:
                            if (
                                rec_op_name == ex_op_name
                                and rec_item == ex_item
                                and rec_point == ex_point
                                and rec_surface == ex_surface
                                and rec_spec == ex_spec
                            ):
                                pi_no = ex_pi
                                break

                        else:
                            if (
                                rec_op_name == ex_op_name
                                and rec_inspection_no == ex_inspection_no
                                and rec_item == ex_item
                                and rec_point == ex_point
                                and rec_surface == ex_surface
                                and rec_spec == ex_spec
                                and direction_depth_match
                            ):
                                pi_no = ex_pi
                                break

                if pi_no is None:
                    next_pi = max(
                        used_pi_nos,
                        default=0,
                    ) + 1


                    pi_no = next_pi

                claimed_db_pi_nos.add(pi_no)
                used_pi_nos.add(pi_no)

                # In-place mutation is intentional and load-bearing: see
                # the sync_masters() docstring above. LoadChecksheetStep
                # reads this same dict back after this call to build its
                # source_master_index -> pi_no map for transactions.
                master_record["pi_no"] = pi_no

                LOGGER.info(
                    f"[MASTER PI] assigned_pi={pi_no} "
                    f"vpo_code={vpo_code}",
                    pipeline_type="CHECKSHEET",
                )

                resolved_records.append(
                    (
                        pi_no,
                        master_record,
                    )
                )

            for pi_no, master_record in resolved_records:
                values = self._prepare_master_values(
                    master_record,
                    version=version_value,
                    pi_no=pi_no,
                )

                if pi_no not in existing_rows:
                    for field in self._AUDITABLE_MASTER_FIELDS:
                        new_val = values.get(field)

                        if (
                            new_val is not None
                            and str(new_val).strip() != ""
                        ):
                            audit_logs.append(
                                {
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
                                    "remarks": (
                                        "New master definition inserted"
                                    ),
                                    "insert_id": insert_id,
                                }
                            )

                else:
                    ex_fields = existing_rows[pi_no]

                    for field in self._AUDITABLE_MASTER_FIELDS:
                        if field in {
                            "upper_limit",
                            "lower_limit",
                        }:
                            continue

                        old_val = ex_fields.get(field)
                        new_val = values.get(field)

                        old_str = self._normalize_text(old_val)
                        new_str = self._normalize_text(new_val)

                        if old_str != new_str:
                            change_type = (
                                "SPECIFICATION_UPDATE"
                                if field == "specification"
                                else "GENERAL_UPDATE"
                            )

                            audit_logs.append(
                                {
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
                                    "remarks": (
                                        "Master definition modified"
                                    ),
                                    "insert_id": insert_id,
                                }
                            )

            unclaimed_pi_nos: List[int] = []

            if not is_partial_master:
                unclaimed_pi_nos = sorted(
                    set(existing_rows.keys())
                    - claimed_db_pi_nos
                )

                for old_pi_no in unclaimed_pi_nos:
                    audit_logs.append(
                        {
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
                            "remarks": (
                                "Master slot deleted "
                                "(master count reduced)"
                            ),
                            "insert_id": insert_id,
                        }
                    )

            if audit_logs:
                self._insert_audit_logs(
                    cur,
                    audit_logs,
                )

            for pi_no, master_record in resolved_records:
                values = self._prepare_master_values(
                    master_record,
                    version=version_value,
                    pi_no=pi_no,
                )

                params = tuple(
                    values.get(key)
                    for key in self._MASTER_VALUE_KEYS
                )

                cur.execute(
                    statement,
                    params + (
                        insert_id,
                        insert_id,
                    ),
                )

            for pi_no, master_record in resolved_records:
                self.update_limits_if_newer(
                    pi_no=pi_no,
                    incoming_date=coerced_incoming_date,
                    upper_limit=master_record.get(
                        "upper_limit"
                    ),
                    lower_limit=master_record.get(
                        "lower_limit"
                    ),
                    version=version_value,
                    vpo_code=vpo_code,
                    watch_id=watch_id,
                    file_version_id=file_version_id,
                    file_name=file_name,
                    file_hash=file_hash,
                    insert_id=insert_id,
                    cur=cur,
                )

            if (
                not is_partial_master
                and unclaimed_pi_nos
            ):
                cur.execute(
                    """
                    DELETE FROM fs3.pre_m_measurement
                    WHERE version = %s
                      AND vpo_code = %s
                      AND pi_no = ANY(%s);
                    """,
                    (
                        version_value,
                        vpo_code,
                        unclaimed_pi_nos,
                    ),
                )

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
        is_partial_master: bool = False,
    ) -> int:
        """
        Synchronize normalized transaction records.

        Callers must supply "pi_no" on every record before calling this
        method -- this repository does not resolve pi_no itself here.
        (LoadChecksheetStep resolves it beforehand from source_master_index,
        using the pi_no values sync_masters() attached to master_records.)
        """

        if (
            not isinstance(transaction_records, list)
            or not transaction_records
        ):
            raise ValueError(
                "transaction_records must be a non-empty "
                "list of dictionaries"
            )

        version_value = self._validated_version(version)

        vpo_code = self._normalize_text(
            transaction_records[0].get("vpo_code")
        )

        prepared_incoming: Dict[
            Tuple[date, str, int],
            Dict[str, Any],
        ] = {}

        for rec in transaction_records:
            dt_val = self._coerce_date(
                rec.get("date")
            )

            serial_last4 = self._normalize_text(
                rec.get("serial_no_m_last4")
            )

            pi_no_raw = rec.get("pi_no")

            if (
                dt_val is None
                or not serial_last4
                or pi_no_raw is None
            ):
                continue

            pi_no = int(pi_no_raw)

            business_key = (
                dt_val,
                serial_last4,
                pi_no,
            )

            prepared_incoming[business_key] = dict(rec)

        with self.conn.cursor() as cur:
            select_cols = [
                "date",
                "serial_no_m_last4",
                "serial_no",
                "mc_no",
                "vehicle_model",
                "parts_code",
                "op_no",
                "op_no_2nd",
                "op_no_3rd",
                "pi_no",
                "value",
                "eng_no",
                "watch_id",
                "file_version_id",
            ]

            query_sql = sql.SQL(
                """
                SELECT {}
                FROM fs3.pre_t_measurement
                WHERE version = %s
                  AND vpo_code = %s
                  AND watch_id = %s;
                """
            ).format(
                sql.SQL(", ").join(
                    sql.Identifier(col)
                    for col in select_cols
                )
            )

            cur.execute(
                query_sql,
                (
                    version_value,
                    vpo_code,
                    watch_id,
                ),
            )

            existing_db_rows = cur.fetchall()

            existing_db_map: Dict[
                Tuple[date, str, int],
                Dict[str, Any],
            ] = {}

            for row in existing_db_rows:
                db_dt = self._coerce_date(row[0])
                db_last4 = row[1]

                db_pi_no = (
                    int(row[9])
                    if row[9] is not None
                    else None
                )

                if (
                    db_dt
                    and db_last4
                    and db_pi_no is not None
                ):
                    existing_db_map[
                        (
                            db_dt,
                            db_last4,
                            db_pi_no,
                        )
                    ] = {
                        col: val
                        for col, val in zip(
                            select_cols,
                            row,
                        )
                    }

            incoming_keys = set(
                prepared_incoming.keys()
            )

            existing_keys = set(
                existing_db_map.keys()
            )

            keys_to_delete = (
                existing_keys - incoming_keys
                if not is_partial_master
                else set()
            )

            for del_key in keys_to_delete:
                (
                    del_date,
                    del_last4,
                    del_pi_no,
                ) = del_key

                del_sql = """
                    DELETE FROM fs3.pre_t_measurement
                    WHERE version = %s
                      AND vpo_code = %s
                      AND watch_id = %s
                      AND date = %s
                      AND serial_no_m_last4 = %s
                      AND pi_no = %s;
                """

                cur.execute(
                    del_sql,
                    (
                        version_value,
                        vpo_code,
                        watch_id,
                        del_date,
                        del_last4,
                        del_pi_no,
                    ),
                )

            for _, inc_rec in prepared_incoming.items():
                LOGGER.info(
                    f"[TX INSERT] "
                    f"serial={inc_rec.get('serial_no')} "
                    f"serial_last4="
                    f"{inc_rec.get('serial_no_m_last4')} "
                    f"pi_no={inc_rec.get('pi_no')} "
                    f"value={inc_rec.get('value')}",
                    pipeline_type="CHECKSHEET",
                )

                self._upsert_single_transaction(
                    cur,
                    inc_rec,
                    version_value,
                    insert_id,
                    watch_id=watch_id,
                    file_version_id=file_version_id,
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
        """
        Upsert one normalized transaction row.

        Conflict identity:
            (
                version,
                vpo_code,
                date,
                serial_no_m_last4,
                pi_no,
                watch_id,
            )
        """

        values = dict(transaction_data)

        values["version"] = version_value

        if watch_id is not None:
            values["watch_id"] = self._normalize_nullable_text(
                watch_id
            )

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
            "value",
        ):
            values[key] = self._normalize_nullable_text(
                values.get(key)
            )

        if values.get("pi_no") is not None:
            values["pi_no"] = int(
                values["pi_no"]
            )

        if values.get("date") is not None:
            values["date"] = self._coerce_date(
                values["date"]
            )

        insert_columns: Sequence[str] = (
            self._TRANSACTION_BUSINESS_COLUMNS
        )

        params = (
            tuple(
                values.get(column)
                for column in insert_columns
            )
            + (
                insert_id,
                insert_id,
            )
        )

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
            ON CONFLICT (
                version,
                vpo_code,
                date,
                serial_no_m_last4,
                pi_no,
                watch_id
            )
            DO UPDATE SET
                {},
                update_date = CURRENT_TIMESTAMP,
                update_id = EXCLUDED.update_id;
            """
        ).format(
            sql.SQL(", ").join(
                sql.Identifier(column)
                for column in insert_columns
            ),
            sql.SQL(", ").join(
                sql.Placeholder()
                for _ in insert_columns
            ),
            sql.SQL(",\n                ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column),
                    sql.Identifier(column),
                )
                for column in mutable_columns
            ),
        )

        cur.execute(
            statement,
            params,
        )

    def insert_transaction(
        self,
        transaction_data: Mapping[str, Any],
    ) -> None:
        """Legacy helper retained for compatibility."""

        version_value = self._validated_version(
            transaction_data.get("version")
        )

        with self.conn.cursor() as cur:
            self._upsert_single_transaction(
                cur,
                transaction_data,
                version_value,
                "Vig",
            )


__all__ = ["MeasurementRepository"]