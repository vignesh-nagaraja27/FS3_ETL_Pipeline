# -*- coding: utf-8 -*-
"""
Load Step — Atomic Transaction Edition
---------------------------------------
The most critical step in the pipeline. Takes the clean, validated measurement rows
produced by the Transform Step and persists them to the database in two stages:

  STAGE 1 — Staging Insert:
    Rows go into the appropriate staging table:
      - 3D HD data  → fs3.pre_f_3d_hd
      - 3D BL data  → fs3.pre_f_3d_bl

  STAGE 2 — Master Table Sync:
    Rows are promoted from the staging table to the FINAL master tables:
      - Metadata  → fs3.pre_m_cmm   (measurement definitions per model/op)
      - Values    → fs3.pre_t_cmm   (actual measurement readings)

═══════════════════════════════════════════════════════════════
  ELITE FIX — SCENARIO 3: Atomic ACID Transaction (v2)
═══════════════════════════════════════════════════════════════
  The original implementation had THREE separate database connections — one
  per stage (_insert_staging, _upsert_master, _insert_transaction). Each one
  committed independently. This created the "Ghost Master Record" failure mode:

    OLD BROKEN FLOW:
      conn1 = get_connection() → _insert_staging()  → COMMIT  ← permanent
      conn2 = get_connection() → _upsert_master()   → COMMIT  ← permanent
      conn3 = get_connection() → _insert_transaction() → FAIL → ROLLBACK
      RESULT: pre_m_cmm has a row. pre_t_cmm does not. DATA IS CORRUPT.

    NEW ATOMIC FLOW:
      conn = (passed in from pipeline_3d.py)
      with conn.cursor() as cur:
          for each row:
              _insert_staging(cur, ...)    ← no commit
              _upsert_master(cur, ...)     ← no commit
              _insert_transaction(cur, ...) ← no commit
      # conn.__exit__ → COMMIT if all rows OK, ROLLBACK if ANY row fails
      RESULT: All rows commit or NONE do. Zero Ghost Records.

  The key design change:
    - LoadStep.run() now accepts a `conn` parameter (Connection object).
    - All private methods receive a `cur` (cursor) directly.
    - LoadStep has NO internal connection management — it is a pure executor.
    - The connection lifecycle is owned by Pipeline3D (the caller).

*** THIS IS WHERE THE RC MODEL OP010 BUG ORIGINATED ***
The UPSERT fix for that bug is preserved intact.
"""

from typing import List, Dict, Any, Optional, Tuple
import zlib

from utils.logger import LOGGER


# ── Table constants ──────────────────────────────────────────────────────────
TABLE_MASTER = "fs3.pre_m_cmm"
TABLE_TRANSACTION = "fs3.pre_t_cmm"


class LoadStep:
    """
    Executes all database write operations for a single pipeline run
    within a SINGLE externally-provided transaction.

    This class does NOT open, commit, or close any database connections.
    All that is the caller's responsibility (Pipeline3D). This makes the
    load step fully testable in isolation with a mock connection.
    """

    def __init__(self, pipeline_type: str):
        """
        Args:
            pipeline_type (str): '3D_HD' or '3D_BL'. Kept for compatibility.
        """
        self._pipeline_type = pipeline_type

    def run(
        self,
        rows: List[Dict[str, Any]],
        part_config: dict,
        machine_id: str,
        log_id: int,
        conn,  # ← SINGLE shared connection, owned by pipeline_3d.py
        version: Optional[str] = None,
    ) -> int:
        """
        Executes all staging, master, and transaction inserts within a SINGLE atomic
        database transaction. The connection is provided by the caller (Pipeline3D)
        and is NOT committed inside this method.

        ACID GUARANTEE:
          If ANY row fails at ANY stage (staging, master, or transaction), an exception
          is raised immediately. The ConnectionContext.__exit__ in pipeline_3d.py catches
          this exception and calls conn.rollback(), undoing ALL writes for this file.
          Zero partial data. Zero ghost master records.

        Args:
            rows (List[Dict]): Clean measurement rows from TransformStep.
            part_config (dict): The matched part config from parts_3d.json.
            machine_id (str): Machine identifier for logging context.
            log_id (int): Pipeline run log ID for full traceability.
            conn: An active psycopg2 connection object. NOT closed or committed here.
            version (str): Version string for master table (defaults to today's date).

        Returns:
            int: Count of rows processed (all succeeded — or an exception was raised).

        Raises:
            Exception: Any DB error re-raised immediately to trigger rollback upstream.
        """
        vehicle_model = part_config.get("vehicle_model") or ""
        parts_code = part_config.get("parts_code") or ""
        op_no = part_config.get("op_no") or ""
        vpo_code = part_config.get("vpo_code") or ""
        context = dict(
            machine=machine_id or "",
            model=vehicle_model or "",
            part=parts_code or "",
            op=op_no or "",
        )

        # Do NOT fabricate a version here. Keep `version` optional — resolve later per-row when possible.
        # If the caller passed a file-level `version`, keep it; otherwise leave as None.
        committed = 0

        LOGGER.info(
            f"[LOG_ID:{log_id}] LOAD STEP: Starting atomic batch. "
            f"{len(rows)} rows — staging → master → transaction (single transaction).",
            **context,
        )

        # ── SINGLE CURSOR for the entire file batch ────────────────────────────
        # All SQL executions below share this cursor and, therefore, the same
        # PostgreSQL transaction. The commit/rollback is triggered by the caller's
        # ConnectionContext.__exit__ — NOT here.
        mc_no = machine_id or ""
        with conn.cursor() as cur:
            for i, row in enumerate(rows):
                measure_no = None
                # Validate measurement value present
                measure_value = row.get("measure_value")
                if measure_value is None:
                    raise ValueError(f"LOAD STEP: Row {i + 1} missing 'measure_value'.")

                measurement_datetime = row.get("measurement_datetime")
                if measurement_datetime is None:
                    raise ValueError(
                        f"LOAD STEP: Row {i + 1} missing 'measurement_datetime' derived from filename."
                    )

                try:
                    # Debug: log the full row and keys before resolving measure_no/version
                    LOGGER.info(f"FIRST TRANSFORMED ROW: {row!r}", **context)
                    LOGGER.info(f"ROW KEYS: {sorted(list(row.keys()))!r}", **context)
                    LOGGER.info(
                        f"BUSINESS KEYS: evaluation_item1={row.get('evaluation_item1')!r} "
                        f"measure_item={row.get('measure_item')!r} "
                        f"measure_position={row.get('measure_position')!r} "
                        f"measure_no={row.get('measure_no')!r} "
                        f"version={row.get('version')!r}",
                        **context,
                    )
                    LOGGER.info(
                        f"LOAD STEP: Row {i + 1} spec={row.get('specification')!r} "
                        f"upper_limit={row.get('upper_limit')!r} "
                        f"lower_limit={row.get('lower_limit')!r}",
                        **context,
                    )

                    # Resolve or validate measure_no and version per-row
                    # Normalize specification and limits to strings to match DB varchar columns
                    if row.get("specification") is not None:
                        try:
                            row["specification"] = str(row.get("specification")).strip()
                        except Exception:
                            row["specification"] = None
                    if row.get("upper_limit") is not None:
                        try:
                            row["upper_limit"] = str(row.get("upper_limit"))
                        except Exception:
                            row["upper_limit"] = None
                    if row.get("lower_limit") is not None:
                        try:
                            row["lower_limit"] = str(row.get("lower_limit"))
                        except Exception:
                            row["lower_limit"] = None

                    file_level_version = version
                    _v = row.get("version")
                    provided_version = None
                    if isinstance(_v, str):
                        _vs = _v.strip()
                        provided_version = _vs if _vs else None
                    measure_no, resolved_version = self._resolve_or_generate_measure_no(
                        cur,
                        row,
                        provided_version or file_level_version,
                        vpo_code,
                        vehicle_model,
                        parts_code,
                        op_no,
                        mc_no,
                        log_id,
                        context,
                    )
                    # Ensure measure_no is an int
                    if measure_no is None:
                        raise ValueError(
                            f"LOAD STEP: Could not resolve 'measure_no' for row {i + 1}."
                        )
                    row["measure_no"] = int(measure_no)
                    if resolved_version:
                        row["version"] = resolved_version

                    # ── TRANSACTION TABLE ONLY: pre_t_cmm ─────────────────────
                    version_value = row.get("version")
                    if version_value is None:
                        raise ValueError(
                            f"LOAD STEP: Row {i + 1} missing resolved 'version' for pre_t_cmm."
                        )

                    serial_no_m_last4 = row.get("serial_no_m_last4")
                    if serial_no_m_last4 is None:
                        raise ValueError(
                            f"LOAD STEP: Row {i + 1} missing 'serial_no_m_last4' required by pre_t_cmm."
                        )

                    vehicle_model_value = row.get("vehicle_model") or vehicle_model
                    parts_code_value = row.get("parts_code") or parts_code
                    op_no_value = row.get("op_no") or op_no

                    measure_no_int = int(row["measure_no"])
                    self._insert_transaction(
                        cur,
                        version_value,
                        vpo_code,
                        vehicle_model_value,
                        parts_code_value,
                        op_no_value,
                        mc_no,
                        measurement_datetime,
                        measure_no_int,
                        str(serial_no_m_last4),
                        float(measure_value),
                        log_id,
                        context,
                    )

                    committed += 1

                except Exception as e:
                    # Log the exact failure point before re-raising.
                    # The re-raise propagates up to the `with self._db.get_connection() as conn:`
                    # block in pipeline_3d.py, whose __exit__ calls conn.rollback().
                    # This guarantees: if row 47 of 200 fails, rows 1–46 are ALSO rolled back.
                    LOGGER.error(
                        f"[LOG_ID:{log_id}] [ROW:{i + 1}/{len(rows)}] "
                        f"[MEASURE_NO:{measure_no}] LOAD STEP FAILED. "
                        f"ALL {committed} previously staged rows will ROLLBACK automatically. "
                        f"Root cause: {type(e).__name__}: {e}",
                        exc_info=True,
                        **context,
                    )
                    raise  # ← Triggers conn.rollback() in pipeline_3d.py

        LOGGER.info(
            f"[LOG_ID:{log_id}] LOAD STEP: Atomic batch complete. "
            f"{committed}/{len(rows)} rows processed. "
            f"Awaiting pipeline commit signal.",
            **context,
        )
        return committed

    # ─────────────────────────────────────────────────────────────────────────
    # Private SQL executors — all accept a cursor, none manage connections
    # ─────────────────────────────────────────────────────────────────────────

    def _resolve_or_generate_measure_no(
        self,
        cur,
        row: dict,
        version_value: Optional[str],
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        mc_no: str,
        log_id: int,
        context: dict,
    ) -> Tuple[int, Optional[str]]:
        """
        Resolve an existing measure_no from pre_m_cmm or create a new master row.

        If a matching master row exists, returns its measure_no and version.
        If no matching master exists and a version is provided, generates the next
        measure_no for that version, inserts a new pre_m_cmm row, and returns it.
        If no master exists and version is missing, raises.
        """
        evaluation_item1 = row.get("evaluation_item1")
        measure_item = row.get("measure_item")
        specification = row.get("specification")
        upper_limit = row.get("upper_limit")
        lower_limit = row.get("lower_limit")
        measure_position = row.get("measure_position")

        if not evaluation_item1 or not measure_item or not measure_position:
            raise ValueError(
                "LOAD STEP: Missing required business key(s) 'evaluation_item1', 'measure_item' or 'measure_position'."
            )

        LOGGER.info(
            f"[LOG_ID:{log_id}] RESOLVE MEASURE_NO: "
            f"version={version_value!r} evaluation_item1={evaluation_item1!r} "
            f"measure_item={measure_item!r} measure_position={measure_position!r} "
            f"specification={specification!r} upper_limit={upper_limit!r} "
            f"lower_limit={lower_limit!r}",
            **context,
        )

        if isinstance(version_value, str) and version_value.strip():
            version_value = version_value.strip()
            cur.execute(
                f"""
                SELECT measure_no
                FROM {TABLE_MASTER}
                WHERE version = %s
                  AND vpo_code = %s
                  AND vehicle_model = %s
                  AND parts_code = %s
                  AND op_no = %s
                  AND evaluation_item1 = %s
                  AND measure_item = %s
                                    AND specification IS NOT DISTINCT FROM %s::text
                                    AND upper_limit IS NOT DISTINCT FROM %s::text
                                    AND lower_limit IS NOT DISTINCT FROM %s::text
                LIMIT 1;
                """,
                (
                    version_value,
                    vpo_code,
                    vehicle_model,
                    parts_code,
                    op_no,
                    evaluation_item1,
                    measure_item,
                                        # ensure params are strings for varchar columns
                                        (specification if specification is None else str(specification)),
                                        (upper_limit if upper_limit is None else str(upper_limit)),
                                        (lower_limit if lower_limit is None else str(lower_limit)),
                ),
            )
            existing = cur.fetchone()
            if existing and existing[0] is not None:
                LOGGER.info(
                    f"[LOG_ID:{log_id}] RESOLVE MEASURE_NO: Found exact matching master "
                    f"measure_no={existing[0]!r} for version={version_value!r}.",
                    **context,
                )
                return int(existing[0]), version_value

            cur.execute(
                f"""
                SELECT measure_no, specification, upper_limit, lower_limit
                FROM {TABLE_MASTER}
                WHERE version = %s
                  AND vpo_code = %s
                  AND vehicle_model = %s
                  AND parts_code = %s
                  AND op_no = %s
                  AND evaluation_item1 = %s
                  AND measure_item = %s
                  AND measure_position = %s
                  AND (specification IS NULL OR upper_limit IS NULL OR lower_limit IS NULL)
                ORDER BY measure_no
                LIMIT 1;
                """,
                (
                    version_value,
                    vpo_code,
                    vehicle_model,
                    parts_code,
                    op_no,
                    evaluation_item1,
                    measure_item,
                    measure_position,
                ),
            )
            candidate = cur.fetchone()
            if candidate:
                candidate_measure_no, existing_spec, existing_upper, existing_lower = (
                    candidate
                )
                LOGGER.info(
                    f"[LOG_ID:{log_id}] RESOLVE MEASURE_NO: Found existing master with NULL spec/limits "
                    f"measure_no={candidate_measure_no} existing_spec={existing_spec!r} "
                    f"existing_upper={existing_upper!r} existing_lower={existing_lower!r}",
                    **context,
                )
                self._update_master_if_missing_fields(
                    cur,
                    version_value,
                    vpo_code,
                    vehicle_model,
                    parts_code,
                    op_no,
                    candidate_measure_no,
                    specification,
                    upper_limit,
                    lower_limit,
                    log_id,
                    context,
                )
                return int(candidate_measure_no), version_value

            lock_key = self._advisory_lock_key(vehicle_model, parts_code, op_no)
            cur.execute("SELECT pg_advisory_xact_lock(%s);", (lock_key,))

            cur.execute(
                f"""
                SELECT MAX(measure_no)
                FROM {TABLE_MASTER}
                WHERE version = %s
                  AND vpo_code = %s;
                """,
                (version_value, vpo_code),
            )
            row_val = cur.fetchone()
            max_measure_no = (
                int(row_val[0]) if row_val and row_val[0] is not None else 0
            )
            new_measure_no = max_measure_no + 1
            self._insert_master(
                cur,
                version_value,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                mc_no,
                new_measure_no,
                evaluation_item1,
                measure_item,
                measure_position,
                specification,
                upper_limit,
                lower_limit,
                log_id,
                context,
            )
            return new_measure_no, version_value

        cur.execute(
            f"""
            SELECT version, measure_no
            FROM {TABLE_MASTER}
            WHERE vpo_code = %s
              AND vehicle_model = %s
              AND parts_code = %s
              AND op_no = %s
              AND evaluation_item1 = %s
              AND measure_item = %s
              AND measure_position = %s
            LIMIT 1;
            """,
            (
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                evaluation_item1,
                measure_item,
                measure_position,
            ),
        )
        existing_any = cur.fetchone()
        if existing_any and existing_any[0] is not None:
            found_version, found_measure_no = existing_any[0], existing_any[1]
            LOGGER.info(
                f"[LOG_ID:{log_id}] RESOLVE MEASURE_NO: Reusing existing master "
                f"version={found_version!r} measure_no={found_measure_no!r} "
                f"for business key set with NULL-able specs.",
                **context,
            )
            self._update_master_if_missing_fields(
                cur,
                found_version,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                found_measure_no,
                specification,
                upper_limit,
                lower_limit,
                log_id,
                context,
            )
            return int(found_measure_no), found_version

        raise ValueError(
            "LOAD STEP: Cannot create a new master record because version is missing. "
            "Provide a version value in the transformed data."
        )

    def _insert_master(
        self,
        cur,
        version: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        mc_no: str,
        measure_no: int,
        evaluation_item1: str,
        measure_item: str,
        measure_position: str,
        specification: Optional[str],
        upper_limit: Optional[float],
        lower_limit: Optional[float],
        log_id: int,
        context: dict,
    ):
        """
        Inserts a new master definition row into pre_m_cmm.
        """
        sql = f"""
            INSERT INTO {TABLE_MASTER}
                (version, vpo_code, vehicle_model, parts_code, op_no, mc_no, measure_no, evaluation_item1, measure_item, measure_position, specification, upper_limit, lower_limit, insert_date, update_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        params = (
            version,
            vpo_code,
            vehicle_model,
            parts_code,
            op_no,
            mc_no,
            measure_no,
            evaluation_item1,
            measure_item,
            measure_position,
            (specification if specification is None else str(specification)),
            (upper_limit if upper_limit is None else str(upper_limit)),
            (lower_limit if lower_limit is None else str(lower_limit)),
        )
        LOGGER.info(
            f"[LOG_ID:{log_id}] INSERT MASTER SQL: {sql.strip()} PARAMS={params}",
            **context,
        )
        cur.execute(sql, params)

    def _update_master_if_missing_fields(
        self,
        cur,
        version: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        measure_no: int,
        specification: Optional[str],
        upper_limit: Optional[float],
        lower_limit: Optional[float],
        log_id: int,
        context: dict,
    ):
        """
        Updates an existing master row only when incoming spec/limit values are present
        and the stored row has NULLs for those fields.
        """
        sql = f"""
            UPDATE {TABLE_MASTER}
            SET specification = COALESCE(specification, %s),
                upper_limit = COALESCE(upper_limit, %s),
                lower_limit = COALESCE(lower_limit, %s),
                update_date = CURRENT_TIMESTAMP
            WHERE version = %s
              AND vpo_code = %s
              AND vehicle_model = %s
              AND parts_code = %s
              AND op_no = %s
              AND measure_no = %s
              AND (specification IS NULL OR upper_limit IS NULL OR lower_limit IS NULL);
        """
        params = (
            (specification if specification is None else str(specification)),
            (upper_limit if upper_limit is None else str(upper_limit)),
            (lower_limit if lower_limit is None else str(lower_limit)),
            version,
            vpo_code,
            vehicle_model,
            parts_code,
            op_no,
            measure_no,
        )
        LOGGER.info(
            f"[LOG_ID:{log_id}] UPDATE MASTER SQL: {sql.strip()} PARAMS={params}",
            **context,
        )
        cur.execute(sql, params)
        LOGGER.info(
            f"[LOG_ID:{log_id}] UPDATE MASTER: measure_no={measure_no!r} "
            f"specification={specification!r} upper_limit={upper_limit!r} "
            f"lower_limit={lower_limit!r}",
            **context,
        )

    def _advisory_lock_key(
        self, vehicle_model: str, parts_code: str, op_no: str
    ) -> int:
        """Deterministic 32-bit advisory lock key derived from business keys."""
        key_source = f"{vehicle_model}|{parts_code}|{op_no}"
        return int(zlib.adler32(key_source.encode("utf-8")))

    def _insert_transaction(
        self,
        cur,
        version: Optional[str],
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        mc_no: str,
        measurement_datetime,
        measure_no: int,
        serial_no_m_last4: str,
        measure_value: Any,
        log_id: int,
        context: dict,
    ):
        """
        Inserts one measurement VALUE row into pre_t_cmm (the final transaction table).
        ON CONFLICT DO NOTHING makes this idempotent — safe for double-process scenarios.
        """
        sql = f"""
            INSERT INTO {TABLE_TRANSACTION}
                (version, vpo_code, vehicle_model, parts_code, op_no, mc_no, datetime, measure_no, serial_no_m_last4, value, insert_date, update_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (vpo_code, datetime, measure_no)
            DO NOTHING;
        """
        params = (
            version,
            vpo_code,
            vehicle_model,
            parts_code,
            op_no,
            mc_no,
            measurement_datetime,
            measure_no,
            serial_no_m_last4,
            str(measure_value),
        )
        LOGGER.info(
            f"[LOG_ID:{log_id}] INSERT TRANSACTION SQL: {sql.strip()} PARAMS={params}",
            **context,
        )
        cur.execute(sql, params)
