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

        # Version is now strictly provided from the machine config/caller
        if not version:
            raise ValueError(
                f"LOAD STEP: Version configuration is missing for machine {machine_id}."
            )

        committed = 0
        mc_no = machine_id or ""

        LOGGER.info(
            f"[LOG_ID:{log_id}] LOAD STEP: Starting atomic batch for {len(rows)} rows. Version: {version}",
            **context,
        )

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
                    # ── STAGE 1: NORMALIZE DATA ──
                    # We normalize here once to ensure Resolver, Master, and Transaction
                    # all see the exact same string representations.
                    specification = (
                        str(row.get("specification")).strip()
                        if row.get("specification") is not None
                        else None
                    )
                    upper_limit = (
                        str(row.get("upper_limit")).strip()
                        if row.get("upper_limit") is not None
                        else None
                    )
                    lower_limit = (
                        str(row.get("lower_limit")).strip()
                        if row.get("lower_limit") is not None
                        else None
                    )

                    # ── STAGE 2: RESOLVE MASTER ──
                    measure_no, resolved_version = self._resolve_or_generate_measure_no(
                        cur,
                        row,
                        version,
                        vpo_code,
                        vehicle_model,
                        parts_code,
                        op_no,
                        mc_no,
                        log_id,
                        context,
                        specification,
                        upper_limit,
                        lower_limit,
                    )
                    row["measure_no"] = int(measure_no)
                    row["version"] = resolved_version
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] LOAD ROW "
                        f"eval={row.get('evaluation_item1')} "
                        f"item={row.get('measure_item')} "
                        f"spec={row.get('specification')} "
                        f"value={row.get('measure_value')}",
                        **context,
                    )

                    # ── STAGE 3: INSERT TRANSACTION ──
                    serial_no_m_last4 = row.get("serial_no_m_last4")
                    if serial_no_m_last4 is None:
                        raise ValueError(
                            f"LOAD STEP: Row {i + 1} missing 'serial_no_m_last4'."
                        )

                    self._insert_transaction(
                        cur,
                        resolved_version,
                        vpo_code,
                        vehicle_model,
                        parts_code,
                        op_no,
                        mc_no,
                        measurement_datetime,
                        int(measure_no),
                        str(serial_no_m_last4),
                        float(measure_value),
                        log_id,
                        context,
                    )

                    committed += 1

                except Exception as e:
                    LOGGER.error(
                        f"[LOG_ID:{log_id}] [ROW:{i + 1}] LOAD STEP FAILED. Rolling back batch. "
                        f"Cause: {type(e).__name__}: {e}",
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
        version_value: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        mc_no: str,
        log_id: int,
        context: dict,
        specification: Optional[str],
        upper_limit: Optional[str],
        lower_limit: Optional[str],
    ) -> Tuple[int, str]:
        """
        Resolves measure_no using Double-Check Locking pattern.
        Uniqueness key: item1 + measure_item + specification.
        """
        evaluation_item1 = row.get("evaluation_item1")
        measure_item = row.get("measure_item")
        measure_point = row.get("measure_point")
        reference_position = row.get("reference_position")

        if mc_no == "KTS-7106":
            if not evaluation_item1:
                raise ValueError(
                    "LOAD STEP: Missing required business key 'evaluation_item1'"
                )
            if not measure_item:
                measure_item = ""
                LOGGER.info(
                    f"[LOG_ID:{log_id}] KTS-7106: measure_item missing. Using empty string.",
                    **context,
                )
        else:
            if not evaluation_item1 or not measure_item:
                raise ValueError(
                    "LOAD STEP: Missing required business keys 'evaluation_item1' or 'measure_item'"
                )

        # SQL for finding the master record
        if mc_no == "KTS-7898":
            search_sql = f"""
                SELECT measure_no
                FROM {TABLE_MASTER}
                WHERE version = %s
                  AND vpo_code = %s
                  AND vehicle_model = %s
                  AND parts_code = %s
                  AND op_no = %s
                  AND evaluation_item1 = %s
                  AND measure_item = %s
                  AND reference_position IS NOT DISTINCT FROM %s::text
                  AND specification IS NOT DISTINCT FROM %s::text
                LIMIT 1;
            """
            search_params = (
                version_value,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                evaluation_item1,
                measure_item,
                reference_position,
                specification,
            )
        elif mc_no == "KTS-7106":
            search_sql = f"""
                SELECT measure_no
                FROM {TABLE_MASTER}
                WHERE version = %s
                  AND vpo_code = %s
                  AND vehicle_model = %s
                  AND parts_code = %s
                  AND op_no = %s
                  AND evaluation_item1 = %s
                  AND measure_item = %s
                  AND measure_point IS NOT DISTINCT FROM %s::text
                  AND specification IS NOT DISTINCT FROM %s::text
                LIMIT 1;
            """
            search_params = (
                version_value,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                evaluation_item1,
                measure_item,
                measure_point,
                specification,
            )
        else:
            search_sql = f"""
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
                LIMIT 1;
            """
            search_params = (
                version_value,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                evaluation_item1,
                measure_item,
                specification,
            )

        # ── 1. INITIAL CHECK (Optimization) ──
        cur.execute(search_sql, search_params)
        existing = cur.fetchone()

        if existing:
            measure_no = int(existing[0])
            latest_measurement_datetime = self._get_latest_measurement_datetime(
                cur,
                version_value,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                measure_no,
            )
            incoming_measurement_datetime = row["measurement_datetime"]
            LOGGER.info(
                f"[LOG_ID:{log_id}] DATETIME CHECK: "
                f"incoming={incoming_measurement_datetime} "
                f"existing={latest_measurement_datetime} "
                f"measure_no={measure_no}",
                **context,
            )
            if (
                latest_measurement_datetime is None
                or incoming_measurement_datetime > latest_measurement_datetime
            ):
                self._update_master_limits(
                    cur,
                    version_value,
                    vpo_code,
                    vehicle_model,
                    parts_code,
                    op_no,
                    measure_no,
                    upper_limit,
                    lower_limit,
                    log_id,
                    context,
                )
            else:
                LOGGER.info(
                    f"[LOG_ID:{log_id}] SKIP MASTER LIMIT UPDATE: "
                    f"incoming={incoming_measurement_datetime} "
                    f"existing={latest_measurement_datetime} "
                    f"measure_no={measure_no}",
                    **context,
                )
            return measure_no, version_value

        # ── 2. ACQUIRE LOCK ──
        # Lock is specific to the part/op to prevent parallel creation of the same measure.
        lock_key = self._advisory_lock_key(vehicle_model, parts_code, op_no)
        cur.execute("SELECT pg_advisory_xact_lock(%s);", (lock_key,))

        # ── 3. DOUBLE-CHECK (Post-Lock) ──
        # Check again in case another worker inserted it while we were waiting for the lock.
        cur.execute(search_sql, search_params)
        existing_post_lock = cur.fetchone()

        if existing_post_lock:
            measure_no = int(existing_post_lock[0])
            latest_measurement_datetime = self._get_latest_measurement_datetime(
                cur,
                version_value,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                measure_no,
            )
            incoming_measurement_datetime = row["measurement_datetime"]
            LOGGER.info(
                f"[LOG_ID:{log_id}] DATETIME CHECK: "
                f"incoming={incoming_measurement_datetime} "
                f"existing={latest_measurement_datetime} "
                f"measure_no={measure_no}",
                **context,
            )
            if (
                latest_measurement_datetime is None
                or incoming_measurement_datetime > latest_measurement_datetime
            ):
                self._update_master_limits(
                    cur,
                    version_value,
                    vpo_code,
                    vehicle_model,
                    parts_code,
                    op_no,
                    measure_no,
                    upper_limit,
                    lower_limit,
                    log_id,
                    context,
                )
            else:
                LOGGER.info(
                    f"[LOG_ID:{log_id}] SKIP MASTER LIMIT UPDATE: "
                    f"incoming={incoming_measurement_datetime} "
                    f"existing={latest_measurement_datetime} "
                    f"measure_no={measure_no}",
                    **context,
                )
            return measure_no, version_value

        # ── 4. GENERATE NEXT NO & INSERT ──
        cur.execute(
            f"SELECT COALESCE(MAX(measure_no), 0) FROM {TABLE_MASTER} WHERE version = %s AND vpo_code = %s;",
            (version_value, vpo_code),
        )
        new_measure_no = int(cur.fetchone()[0]) + 1

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
            measure_point,
            reference_position,
            specification,
            upper_limit,
            lower_limit,
            log_id,
            context,
        )

        return new_measure_no, version_value

    def _update_master_limits(
        self,
        cur,
        version: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        measure_no: int,
        upper_limit: Optional[str],
        lower_limit: Optional[str],
        log_id: int,
        context: dict,
    ):
        """Always overwrite limits when measure_no is reused."""
        sql = f"""
            UPDATE {TABLE_MASTER}
            SET upper_limit = %s,
                lower_limit = %s,
                update_date = CURRENT_TIMESTAMP
            WHERE version = %s
              AND vpo_code = %s
              AND vehicle_model = %s
              AND parts_code = %s
              AND op_no = %s
              AND measure_no = %s;
        """
        params = (
            upper_limit,
            lower_limit,
            version,
            vpo_code,
            vehicle_model,
            parts_code,
            op_no,
            measure_no,
        )
        cur.execute(sql, params)
        LOGGER.info(
            f"[LOG_ID:{log_id}] UPDATE MASTER LIMITS: measure_no={measure_no} u={upper_limit}, l={lower_limit}",
            **context,
        )

    def _get_latest_measurement_datetime(
        self,
        cur,
        version: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        measure_no: int,
    ):
        sql = f"""
        SELECT MAX(datetime)
        FROM {TABLE_TRANSACTION}
        WHERE version = %s
          AND vpo_code = %s
          AND vehicle_model = %s
          AND parts_code = %s
          AND op_no = %s
          AND measure_no = %s;
    """
        cur.execute(
            sql,
            (
                version,
                vpo_code,
                vehicle_model,
                parts_code,
                op_no,
                measure_no,
            ),
        )
        result = cur.fetchone()
        return result[0] if result else None

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
        measure_point: Optional[str],
        reference_position: Optional[str],
        specification: Optional[str],
        upper_limit: Optional[str],
        lower_limit: Optional[str],
        log_id: int,
        context: dict,
    ):
        """
        Inserts a new master definition row into pre_m_cmm.
        """
        sql = f"""
            INSERT INTO {TABLE_MASTER}
                (version, vpo_code, vehicle_model, parts_code, op_no, mc_no, measure_no, 
                 evaluation_item1, measure_item, measure_point,reference_position, specification, 
                 upper_limit, lower_limit, insert_date, update_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
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
            measure_point,
            reference_position,
            specification,
            upper_limit,
            lower_limit,
        )
        cur.execute(sql, params)
        LOGGER.info(
            f"[LOG_ID:{log_id}] INSERT MASTER: measure_no={measure_no}"
            f"reference_position='{reference_position}'",
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
                (version, vpo_code, vehicle_model, parts_code, op_no, mc_no, datetime, 
                 measure_no, serial_no_m_last4, value, insert_date, update_date)
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
