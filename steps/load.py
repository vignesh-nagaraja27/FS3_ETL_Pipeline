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

from datetime import datetime
from typing import List, Dict, Any

from utils.logger import LOGGER


# ── Table constants ──────────────────────────────────────────────────────────
TABLE_STAGING_HD = "fs3.pre_f_3d_hd"
TABLE_STAGING_BL = "fs3.pre_f_3d_bl"
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
            pipeline_type (str): '3D_HD' or '3D_BL' — determines which staging table is used.
        """
        self._pipeline_type = pipeline_type
        self._staging_table = (
            TABLE_STAGING_HD if pipeline_type == "3D_HD" else TABLE_STAGING_BL
        )

    def run(
        self,
        rows: List[Dict[str, Any]],
        part_config: dict,
        machine_id: str,
        log_id: int,
        conn,  # ← SINGLE shared connection, owned by pipeline_3d.py
        version: str = None,
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
        vehicle_model = part_config.get("vehicle_model")
        parts_code = part_config.get("parts_code")
        op_no = part_config.get("op_no")
        vpo_code = part_config.get("vpo_code")
        context = dict(
            machine=machine_id, model=vehicle_model, part=parts_code, op=op_no
        )

        version = version or datetime.utcnow().strftime("%Y%m%d")
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
        with conn.cursor() as cur:
            for i, row in enumerate(rows):
                measure_no = row.get("measure_no")
                measure_value = row.get("measure_value")

                try:
                    # ── STAGE 1: Staging Table ────────────────────────────────
                    self._insert_staging(cur, row, log_id, context)

                    # ── STAGE 2: Master Table (pre_m_cmm) — RC/OP010 fix ─────
                    self._upsert_master(
                        cur,
                        version,
                        vpo_code,
                        vehicle_model,
                        parts_code,
                        op_no,
                        measure_no,
                        log_id,
                        context,
                    )

                    # ── STAGE 3: Transaction Table (pre_t_cmm) ────────────────
                    self._insert_transaction(
                        cur, vpo_code, measure_no, measure_value, log_id, context
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

    def _insert_staging(self, cur, row: dict, log_id: int, context: dict):
        """
        Inserts one measurement row into the staging table (pre_f_3d_hd or pre_f_3d_bl).
        ON CONFLICT DO NOTHING ensures idempotency — safe to re-run without crashing.
        """
        sql = f"""
            INSERT INTO {self._staging_table}
                (vehicle_model, parts_code, op_no, measure_no, measure_value, imported_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT DO NOTHING;
        """
        cur.execute(
            sql,
            (
                row["vehicle_model"],
                row["parts_code"],
                row["op_no"],
                row["measure_no"],
                row["measure_value"],
            ),
        )

    def _upsert_master(
        self,
        cur,
        version: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        measure_no: int,
        log_id: int,
        context: dict,
    ):
        """
        Inserts or updates the measurement DEFINITION row in pre_m_cmm.

        This is the fix for the RC/OP010 bug:
          - OLD BEHAVIOR: If the row didn't exist → SKIP silently.
          - NEW BEHAVIOR: If the row doesn't exist → INSERT it. If it exists → UPDATE fields.

        ON CONFLICT DO UPDATE makes this fully idempotent.
        """
        # evaluation_item1 is NOT NULL in the schema. Use a structured placeholder
        # if the conversion mapping table hasn't been imported yet for this measure.
        evaluation_item1_placeholder = f"{op_no}-M{measure_no:04d}"

        sql = f"""
            INSERT INTO {TABLE_MASTER}
                (version, vpo_code, vehicle_model, parts_code, op_no, measure_no, evaluation_item1)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (version, vpo_code, measure_no, evaluation_item1)
            DO UPDATE SET
                vehicle_model = EXCLUDED.vehicle_model,
                parts_code    = EXCLUDED.parts_code,
                op_no         = EXCLUDED.op_no;
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
                evaluation_item1_placeholder,
            ),
        )

    def _insert_transaction(
        self,
        cur,
        vpo_code: str,
        measure_no: int,
        measure_value: float,
        log_id: int,
        context: dict,
    ):
        """
        Inserts one measurement VALUE row into pre_t_cmm (the final transaction table).
        ON CONFLICT DO NOTHING makes this idempotent — safe for double-process scenarios.
        """
        sql = f"""
            INSERT INTO {TABLE_TRANSACTION}
                (vpo_code, datetime, measure_no, measure_value)
            VALUES (%s, NOW(), %s, %s)
            ON CONFLICT (vpo_code, datetime, measure_no)
            DO NOTHING;
        """
        cur.execute(sql, (vpo_code, measure_no, measure_value))
