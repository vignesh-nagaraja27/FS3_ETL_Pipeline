"""Import E-NR/BL masters without deriving definitions from measurement files."""

from collections import defaultdict, deque
from pathlib import Path
from typing import Any
import zlib

from openpyxl import load_workbook

from utils.enr_block import normalize_enr_name, normalize_enr_op
from utils.logger import LOGGER


class EnrBlockMasterStep:
    _FIELDS = (
        "op_name", "op_no", "evaluation_item1", "reference_position",
        "measure_item", "measure_point", "specification", "upper_limit", "lower_limit",
    )
    _COLUMN_INDEXES = (4, 5, 9, 10, 11, 13, 15, 16, 17)

    def run(self, db: Any, machine_config: dict, log_id: int, context: dict) -> None:
        directory = machine_config.get("enr_block_master_path")
        version = str(machine_config.get("version") or "").strip()
        machine_id = str(machine_config.get("machine_id") or "")
        if not directory or not version or machine_id != "KTS-7106":
            raise ValueError("E-NR/BL master import requires KTS-7106, version and master path")
        records = self.read(Path(directory), log_id, context)
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                self.sync(cur, records, version, machine_id, log_id, context)
        LOGGER.info(f"[LOG_ID:{log_id}] E-NR/BL master import committed before matching", **context)

    def read(self, directory: Path, log_id: int, context: dict) -> list[dict[str, Any]]:
        if not directory.is_dir():
            raise ValueError(f"E-NR/BL master directory does not exist: {directory}")
        files = sorted(
            path for path in directory.iterdir()
            if path.is_file() and path.suffix.lower() == ".xlsm" and not path.name.startswith("~$")
        )
        if not files:
            raise ValueError(f"No .xlsm master workbooks found in {directory}")

        records: list[dict[str, Any]] = []
        for path in files:
            workbook = load_workbook(path, read_only=True, data_only=True)
            try:
                if "BL用" not in workbook.sheetnames:
                    raise ValueError(f"E-NR/BL master workbook has no BL用 sheet: {path.name}")
                sheet = workbook["BL用"]
                for number, cells in enumerate(
                    sheet.iter_rows(min_row=4, max_col=18, values_only=True), start=4
                ):
                    values = [cells[index] for index in self._COLUMN_INDEXES]
                    if values[0] is None or not str(values[0]).strip():
                        if any(value is not None and str(value).strip() for value in values):
                            LOGGER.warning(
                                f"[LOG_ID:{log_id}] E-NR/BL master row skipped: "
                                f"file='{path.name}', row={number}, column E (op_name) is empty",
                                **context,
                            )
                        continue
                    record = {
                        field: str(value) if value is not None else None
                        for field, value in zip(self._FIELDS, values)
                    }
                    try:
                        record["op_no"] = normalize_enr_op(record["op_no"])
                    except ValueError as exc:
                        raise ValueError(f"{path.name}, BL用 row {number}: {exc}") from exc
                    records.append(record)
            finally:
                workbook.close()
        if not records:
            raise ValueError(f"No named E-NR/BL master rows found in {directory}")
        LOGGER.info(
            f"[LOG_ID:{log_id}] Read {len(records)} E-NR/BL master rows from {len(files)} workbooks",
            **context,
        )
        return records

    @classmethod
    def _identity(cls, record: dict[str, Any]) -> tuple:
        return tuple(
            normalize_enr_name(record[field]) if field == "op_name" else record[field]
            for field in cls._FIELDS
        )

    def sync(
        self, cur: Any, records: list[dict[str, Any]], version: str,
        machine_id: str, log_id: int, context: dict,
    ) -> None:
        # Share the per-OP numbering lock with the ordinary CMM master allocator.
        for op_no in sorted({record["op_no"] for record in records}):
            lock_key = zlib.adler32(f"E-NR|BL|{op_no}".encode("utf-8"))
            cur.execute("SELECT pg_advisory_xact_lock(%s);", (lock_key,))
        cur.execute(
            """
                SELECT op_name, op_no, evaluation_item1, reference_position,
                       measure_item, measure_point, specification, upper_limit, lower_limit,
                       measure_no
                FROM fs3.pre_m_cmm
                WHERE version = %s AND vehicle_model = 'E-NR' AND parts_code = 'BL'
                ORDER BY measure_no;
            """,
            (version,),
        )
        existing: dict[tuple, deque[int]] = defaultdict(deque)
        for values in cur.fetchall():
            record = dict(zip(self._FIELDS, values[:9]))
            existing[self._identity(record)].append(int(values[9]))

        next_numbers: dict[str, int] = {}
        inserted = 0
        for record in records:
            identity = self._identity(record)
            if existing[identity]:
                existing[identity].popleft()
                continue
            op_no = record["op_no"]
            vpo_code = f"E-NR_BL_{op_no}"
            if vpo_code not in next_numbers:
                cur.execute(
                    "SELECT COALESCE(MAX(measure_no), 0) FROM fs3.pre_m_cmm "
                    "WHERE version = %s AND vpo_code = %s;",
                    (version, vpo_code),
                )
                next_numbers[vpo_code] = int(cur.fetchone()[0])
            next_numbers[vpo_code] += 1
            cur.execute(
                """
                    INSERT INTO fs3.pre_m_cmm
                        (version, vpo_code, vehicle_model, parts_code, mc_no, measure_no,
                         op_name, op_no, evaluation_item1, reference_position, measure_item,
                         measure_point, specification, upper_limit, lower_limit,
                         insert_date, update_date)
                    VALUES (%s, %s, 'E-NR', 'BL', %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """,
                (
                    version, vpo_code, machine_id, next_numbers[vpo_code],
                    *(record[field] for field in self._FIELDS),
                ),
            )
            inserted += 1
        LOGGER.info(
            f"[LOG_ID:{log_id}] E-NR/BL master import: "
            f"inserted={inserted}, reused={len(records) - inserted}",
            **context,
        )
