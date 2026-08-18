# -*- coding: utf-8 -*-
"""Transform an FS3 checksheet workbook into canonical ETL records."""

from __future__ import annotations
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.utils.datetime import from_excel
from utils.workbook_normalizer import WorkbookNormalizer
PathLike = Union[str, Path]
from utils.logger import LOGGER


class TransformChecksheetStep:
    """Parse normalized OP sheets into canonical master and transaction data."""

    CHECKSHEET_VERSION = "00000000"
    FIRST_MASTER_ROW = 7
    SERIAL_ROW = 3
    DATE_ROW = 4
    MAX_DATA_COLUMNS = 500

    def run(
        self,
        file_path: PathLike,
        *,
        vehicle_model: Optional[str] = None,
        parts_code: Optional[str] = None,
        op_no: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Open, normalize, and parse one checksheet workbook."""
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Checksheet workbook not found: {path}")

        filename_parts = self._extract_filename_parts(path.name)
        vehicle_model = vehicle_model or filename_parts["vehicle_model"]
        parts_code = parts_code or filename_parts["parts_code"]
        op_no = op_no or filename_parts["op_no"]
        vpo_code = self._build_vpo_code(vehicle_model, parts_code, op_no)

        LOGGER.info(
f"[WORKBOOK OPEN] Opening workbook: {path.name}",
pipeline_type="CHECKSHEET",
)
        workbook = load_workbook(filename=str(path), data_only=True, read_only=False)
        # WorkbookNormalizer applies NFKC to text. A separate data-only workbook
        # preserves transaction values exactly as stored in the source cells.
        value_workbook = load_workbook(filename=str(path), data_only=True, read_only=False)

        try:
            WorkbookNormalizer.normalize_workbook(workbook)

            master_records: List[Dict[str, Any]] = []
            transaction_by_key: Dict[Tuple[str, date, str], Dict[str, Any]] = {}
            op_sheet_count = 0

            for worksheet in workbook.worksheets:
                if not worksheet.title or not worksheet.title.upper().startswith("OP"):
                    continue

                op_sheet_count += 1
                LOGGER.info(
f"[OP SHEET FOUND] Found OP sheet: {worksheet.title}",
pipeline_type="CHECKSHEET",
)
                value_worksheet = value_workbook[worksheet.title]
                sheet_masters, sheet_transactions = self._parse_sheet(
                    worksheet,
                    value_worksheet=value_worksheet,
                    first_data_position=len(master_records) + 1,
                    vpo_code=vpo_code,
                    vehicle_model=vehicle_model,
                    parts_code=parts_code,
                    op_no=op_no,
                )
                master_records.extend(sheet_masters)

                if len(master_records) > self.MAX_DATA_COLUMNS:
                    raise ValueError(
                        f"Workbook contains more than {self.MAX_DATA_COLUMNS} "
                        "master definitions across OP sheets"
                    )

                for transaction in sheet_transactions:
                    self._merge_transaction(transaction_by_key, transaction)

            if op_sheet_count == 0:
                LOGGER.warning(
f"[TRANSFORM COMPLETED] No OP sheets found: {path.name}",
pipeline_type="CHECKSHEET",
)

            transaction_records = list(transaction_by_key.values())
            LOGGER.info(
f"[TRANSFORM COMPLETED] "
f"master_records={len(master_records)} "
f"transaction_records={len(transaction_records)}",
pipeline_type="CHECKSHEET",
)
            return {
                "master_records": master_records,
                "transaction_records": transaction_records,
            }
        finally:
            value_workbook.close()
            workbook.close()

    transform = run

    def _parse_sheet(
        self,
        worksheet: Any,
        *,
        value_worksheet: Any,
        first_data_position: int,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Parse one normalized OP worksheet using workbook-wide positions."""
        mc_no = self._compose_mc_no(worksheet["B7"].value, worksheet["C7"].value)
        master_records: List[Dict[str, Any]] = []
        for row_index in range(self.FIRST_MASTER_ROW, worksheet.max_row + 1):
            if not self._is_master_row(worksheet, row_index):
                continue

            position = first_data_position + len(master_records)
            if position > self.MAX_DATA_COLUMNS:
                raise ValueError(
                    f"Workbook contains more than {self.MAX_DATA_COLUMNS} "
                    "master definitions across OP sheets"
                )

            data_column = f"data_{position:03d}"
            op_no_2nd, op_no_3rd = self._parse_op_no_2nd_3rd(
                worksheet.cell(row=row_index, column=5).value
            )
            op_name = worksheet.cell(row=row_index, column=6).value
            master_records.append(
                self._build_master_record(
                    worksheet,
                    row_index=row_index,
                    data_column=data_column,
                    vpo_code=vpo_code,
                    vehicle_model=vehicle_model,
                    parts_code=parts_code,
                    op_no=op_no,
                    mc_no=mc_no,
                    op_no_2nd=op_no_2nd,
                    op_no_3rd=op_no_3rd,
                    op_name=op_name,
                )
            )
            LOGGER.info(
f"[MASTER ROW PARSED] "
f"sheet={worksheet.title} "
f"row={row_index} "
f"data_column={data_column}",
pipeline_type="CHECKSHEET",
)

        transaction_records: List[Dict[str, Any]] = []
        for column_index in self._discover_transaction_columns(
            worksheet,
            value_worksheet,
        ):
            serial_no = self._source_value(
                value_worksheet,
                worksheet,
                row=self.SERIAL_ROW,
                column=column_index,
            )

            transaction_date = self._source_value(
                value_worksheet,
                worksheet,
                row=self.DATE_ROW,
                column=column_index,
            )
            parsed_date = self._coerce_date(
                transaction_date,
                epoch=getattr(worksheet.parent, "epoch", None),
            )
            if parsed_date is None:
                continue

            transaction_record: Dict[str, Any] = {
                "version": self.CHECKSHEET_VERSION,
                "mc_no": mc_no,
                "vpo_code": vpo_code,
                "vehicle_model": vehicle_model,
                "parts_code": parts_code,
                "op_no": op_no,
                # E-column operation metadata belongs to master rows. One
                # transaction spans multiple masters and cannot carry one
                # authoritative row-level secondary/tertiary operation value.
                "op_no_2nd": None,
                "op_no_3rd": None,
                "date": parsed_date,
                "serial_no": serial_no,
                "serial_no_m_last4": self._serial_no_m_last4(serial_no),
                "eng_no": None,
                "insert_id": "Vig",
                "update_id": "Vig",
            }

            for master_record in master_records:
                transaction_record[
                    master_record["data_column"]
                ] = self._normalize_measurement_value(
                    self._source_value(
                        value_worksheet,
                        worksheet,
                        row=master_record["row_index"],
                        column=column_index,
                    )
                )

            transaction_records.append(transaction_record)
            LOGGER.info(
f"[TRANSACTION COLUMN PARSED] "
f"sheet={worksheet.title} "
f"column={get_column_letter(column_index)} "
f"serial_no={serial_no}",
pipeline_type="CHECKSHEET",
)

        return master_records, transaction_records

    @classmethod
    def _merge_transaction(
        cls,
        transaction_by_key: Dict[Tuple[str, date, str], Dict[str, Any]],
        incoming: Dict[str, Any],
    ) -> None:
        """Merge identical serial/date columns found across multiple OP sheets."""
        key = (
            str(incoming["vpo_code"]),
            incoming["date"],
            str(incoming["serial_no_m_last4"]),
        )
        existing = transaction_by_key.get(key)
        if existing is None:
            transaction_by_key[key] = incoming
            return

        if str(existing.get("serial_no")) != str(incoming.get("serial_no")):
            raise ValueError(
                "serial_no_m_last4 collision for the same vpo_code and date: "
                f"{existing.get('serial_no')!r} and {incoming.get('serial_no')!r}"
            )

        for name in (
            "version",
            "mc_no",
            "vehicle_model",
            "parts_code",
            "op_no",
            "eng_no",
        ):
            existing_value = existing.get(name)
            incoming_value = incoming.get(name)
            if (
                existing_value is not None
                and incoming_value is not None
                and existing_value != incoming_value
            ):
                raise ValueError(
                    f"conflicting {name} for transaction key {key!r}: "
                    f"{existing_value!r} != {incoming_value!r}"
                )
            if existing_value is None:
                existing[name] = incoming_value

        for name, value in incoming.items():
            if not name.startswith("data_"):
                continue
            if name in existing and existing[name] != value:
                raise ValueError(
                    f"conflicting measurement value for {name} and transaction key {key!r}"
                )
            existing[name] = value

    @classmethod
    def _is_master_row(cls, worksheet: Any, row_index: int) -> bool:
        return not all(
            cls._is_blank(worksheet.cell(row=row_index, column=column).value)
            for column in (6, 8, 11)
        )

    @classmethod
    def _discover_transaction_columns(
        cls,
        worksheet: Any,
        value_worksheet: Any,
    ) -> List[int]:
        """Return every used column containing a serial number in row 3."""
        max_column = max(worksheet.max_column, value_worksheet.max_column, 1)
        return [
            column_index
            for column_index in range(1, max_column + 1)
            if not cls._is_blank(
                cls._source_value(
                    value_worksheet,
                    worksheet,
                    row=cls.SERIAL_ROW,
                    column=column_index,
                )
            )
        ]

    @classmethod
    def _build_master_record(
        cls,
        worksheet: Any,
        *,
        row_index: int,
        data_column: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        mc_no: str,
        op_no_2nd: Any,
        op_no_3rd: Optional[Any],
        op_name: Any,
    ) -> Dict[str, Any]:
        j_value = worksheet.cell(row=row_index, column=10).value
        if cls._contains_direction_marker(j_value):
            measure_direction = j_value
            measure_depth = None
        else:
            measure_direction = None
            measure_depth = j_value

        return {
            "version": cls.CHECKSHEET_VERSION,
            "mc_no": mc_no,
            "vpo_code": vpo_code,
            "vehicle_model": vehicle_model,
            "parts_code": parts_code,
            "op_no": op_no,
            "op_no_2nd": op_no_2nd,
            "op_no_3rd": op_no_3rd,
            "op_name": op_name,
            "Inspection_no": worksheet.cell(row=row_index, column=7).value,
            "measure_item": worksheet.cell(row=row_index, column=8).value,
            "measure_point": worksheet.cell(row=row_index, column=9).value,
            "measure_direction": measure_direction,
            "measure_depth": measure_depth,
            "specification": worksheet.cell(row=row_index, column=11).value,
            "upper_limit": worksheet.cell(row=row_index, column=12).value,
            "lower_limit": worksheet.cell(row=row_index, column=13).value,
            "measure_method": worksheet.cell(row=row_index, column=18).value,
            "measure_frequency": worksheet.cell(row=row_index, column=19).value,
            "data_column": data_column,
            "row_index": row_index,
            "insert_id": "Vig",
            "update_id": "Vig",
        }

    @staticmethod
    def _source_value(
        value_worksheet: Any,
        normalized_worksheet: Any,
        *,
        row: int,
        column: int,
    ) -> Any:
        value = value_worksheet.cell(row=row, column=column).value
        if value is not None:
            return value
        return normalized_worksheet.cell(row=row, column=column).value

    @staticmethod
    def _normalize_measurement_value(value: Any) -> Any:
        """Preserve Excel values while avoiding scientific notation."""
        if isinstance(value, float):
            text = format(value, ".15f")
            return text.rstrip("0").rstrip(".")
        return value

    @staticmethod
    def _contains_direction_marker(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        comparison_value = unicodedata.normalize("NFKC", value).upper()
        return any(marker in comparison_value for marker in ("X", "Y", "Z"))

    @staticmethod
    def _compose_mc_no(value_b: Any, value_c: Any) -> str:
        left = "" if value_b is None else str(value_b)
        right = "" if value_c is None else str(value_c)
        return f"{left}{right}"

    @staticmethod
    def _parse_op_no_2nd_3rd(value: Any) -> Tuple[Any, Optional[Any]]:
        if value is None:
            return None, None
        if isinstance(value, str):
            text = value.strip()
            if "-" in text:
                op_no_2nd, op_no_3rd = text.split("-", 1)
                return op_no_2nd, op_no_3rd or None
            return text, None
        return value, None

    @staticmethod
    def _serial_no_m_last4(value: Any) -> str:
        if isinstance(value, float) and value.is_integer():
            text = str(int(value))
        else:
            text = str(value)
        if not text:
            raise ValueError("serial_no is required")
        return text[-4:]

    @staticmethod
    def _coerce_date(value: Any, *, epoch: Any = None) -> Optional[date]:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                converted = (
                    from_excel(value, epoch=epoch)
                    if epoch
                    else from_excel(value)
                )
            except (TypeError, ValueError, OverflowError):
                return None
            if isinstance(converted, datetime):
                return converted.date()
            return converted if isinstance(converted, date) else None
        if isinstance(value, str):
            text = unicodedata.normalize("NFKC", value).strip()
            if not text:
                return None
            try:
                return datetime.fromisoformat(text).date()
            except ValueError:
                pass
            for date_format in (
                "%Y/%m/%d",
                "%Y.%m.%d",
                "%Y年%m月%d日",
                "%Y%m%d",
            ):
                try:
                    return datetime.strptime(text, date_format).date()
                except ValueError:
                    continue
            try:
                numeric_value = float(text)
                converted = (
                    from_excel(numeric_value, epoch=epoch)
                    if epoch
                    else from_excel(numeric_value)
                )
            except (TypeError, ValueError, OverflowError):
                return None
            if isinstance(converted, datetime):
                return converted.date()
            return converted if isinstance(converted, date) else None
        return None

    @staticmethod
    def _is_blank(value: Any) -> bool:
        return value is None or (isinstance(value, str) and not value.strip())

    @staticmethod
    def _build_vpo_code(vehicle_model: str, parts_code: str, op_no: str) -> str:
        return f"{vehicle_model}_{parts_code}_{op_no}"

    @staticmethod
    def _extract_filename_parts(file_name: str) -> Dict[str, str]:
        stem = Path(file_name).stem
        match = re.fullmatch(
            r"(?P<vehicle_model>[^_]+)_"
            r"(?P<parts_code>[^_]+)_"
            r"(?P<op_no>OP[^_]+)(?:_.+)?",
            stem,
            flags=re.IGNORECASE,
        )
        if match is None:
            raise ValueError(
                "Checksheet filename must match "
                "VehicleModel_PartsCode_OPNO_Anything.xlsm"
            )
        return {
            "vehicle_model": match.group("vehicle_model"),
            "parts_code": match.group("parts_code"),
            "op_no": match.group("op_no").upper(),
        }


__all__ = ["TransformChecksheetStep"]