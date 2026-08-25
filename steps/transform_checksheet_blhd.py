# -*- coding: utf-8 -*-
"""Transform a BL/HD checksheet workbook into canonical ETL records.

Architecture & Business Rules:
------------------------------
1. Partial Master Segment Parsing:
   BL and HD checksheets for an operation are delivered as sequence-split partial segment
   files (e.g. GT3_BL_OP010_①.xlsx, GT3_BL_OP010_②.xlsx, GT3_BL_OP010_③.xlsx).
   Together, all sequence files construct ONE logical master definition. This parser extracts
   the master segment present in the target workbook without making assumptions about master completeness.

2. Operation-Segmented Transactions:
   Transaction records are segmented by (vpo_code, date, serial_no_m_last4, op_no_2nd, op_no_3rd).
   Cell A21 metadata (op_no_2nd, op_no_3rd) is populated directly onto transaction records, allowing
   measurements for the same serial number across distinct operation subgroups to coexist independently.

3. Local Position Allocation:
   Each qualified master row receives a workbook-local source_master_index.
   The identifier is used only to preserve master-to-transaction relationships
   during the transform phase and is later resolved to the final pi_no by
   MeasurementRepository.sync_masters().

4. Transaction-Driven Master Qualification Rule:
   NO TRANSACTION VALUE = NO PI ALLOCATION.
   A row starting at Row 21 is qualified as a valid measurement master definition IF AND ONLY IF
   it contains at least one non-blank transaction measurement value across the discovered serial columns.
   "No PI allocation" here means the row never becomes a master record at all -- this transformer
   itself never allocates pi_no (see point 6); a disqualified row simply never enters the pool of
   masters that downstream PI resolution can assign a pi_no to.

5. Serial Number Preservation Rule:
   Serial numbers are preserved exactly as formatted in Excel. Numeric serials under 4 digits
   are padded with leading zeros (e.g., "0001", "0003", "0056") to guarantee exact serial_no_m_last4 matching.

6. PI_NO Resolution Ownership:
   Like TransformChecksheetStep, this transformer never assigns pi_no. Each qualified master
   receives a workbook-wide, transform-run-local "source_master_index", and every transaction
   measured against that master carries the same source_master_index. LoadChecksheetStep resolves
   source_master_index -> pi_no after MeasurementRepository.sync_masters() runs, identical to the
   CR checksheet flow. This transformer must not fabricate or reuse a pi_no-shaped value for any
   other purpose (a prior revision mistakenly stamped a per-sheet position counter directly into a
   "pi_no" field on transactions, which is incompatible with PI resolution and has been removed).
"""

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

from utils.logger import LOGGER
from utils.workbook_normalizer import WorkbookNormalizer

PathLike = Union[str, Path]


class TransformChecksheetBLHDStep:
    """Parse normalized BL/HD worksheets into canonical master and transaction data."""

    CHECKSHEET_VERSION = "00000000"
    FIRST_MASTER_ROW = 21
    TRANSACTION_DATE_ROW = 11
    TRANSACTION_SERIAL_ROW = 14
    FIRST_TRANSACTION_COL = 18  # Column R

    def run(
        self,
        file_path: PathLike,
        *,
        vehicle_model: Optional[str] = None,
        parts_code: Optional[str] = None,
        op_no: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Open, normalize, and parse one BL/HD checksheet workbook."""
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"BL/HD Checksheet workbook not found: {path}")

        filename_parts = self._extract_filename_parts(path.name)
        vehicle_model = vehicle_model or filename_parts["vehicle_model"]
        parts_code = parts_code or filename_parts["parts_code"]
        op_no = op_no or filename_parts["op_no"]
        vpo_code = self._build_vpo_code(vehicle_model, parts_code, op_no)

        LOGGER.info(
            f"[WORKBOOK OPEN] Opening BL/HD workbook: {path.name}",
            pipeline_type="CHECKSHEET",
        )

        workbook = load_workbook(filename=str(path), data_only=True, read_only=False)
        value_workbook = load_workbook(
            filename=str(path), data_only=True, read_only=False
        )

        try:
            WorkbookNormalizer.normalize_workbook(workbook)

            master_records: List[Dict[str, Any]] = []
            transaction_by_key: Dict[Tuple[str, date, str, int], Dict[str, Any]] = {}

            total_candidates = 0
            total_qualified = 0
            total_skipped = 0

            # Workbook-wide master identity, mirroring TransformChecksheetStep.
            #
            # This is NOT pi_no. It is a transform-run-local counter that
            # connects a qualified master row to its measurement transactions
            # until MeasurementRepository.sync_masters() (called later, from
            # LoadChecksheetStep) resolves the real pi_no.
            next_master_index = 1

            for worksheet in workbook.worksheets:
                if not worksheet.title:
                    continue

                LOGGER.info(
                    f"[SHEET FOUND] Processing sheet: {worksheet.title}",
                    pipeline_type="CHECKSHEET",
                )

                value_worksheet = value_workbook[worksheet.title]

                (
                    sheet_masters,
                    sheet_transactions,
                    sheet_candidates,
                    sheet_qualified,
                    sheet_skipped,
                    next_master_index,
                ) = self._parse_sheet(
                    worksheet,
                    value_worksheet=value_worksheet,
                    first_master_index=next_master_index,
                    vpo_code=vpo_code,
                    vehicle_model=vehicle_model,
                    parts_code=parts_code,
                    op_no=op_no,
                )

                total_candidates += sheet_candidates
                total_qualified += sheet_qualified
                total_skipped += sheet_skipped

                if not sheet_masters:
                    LOGGER.info(
                        f"[SHEET SKIPPED] No qualified master rows in sheet: {worksheet.title}",
                        pipeline_type="CHECKSHEET",
                    )
                    continue

                master_records.extend(sheet_masters)
                for transaction in sheet_transactions:
                    self._merge_transaction(transaction_by_key, transaction)

            transaction_records = list(transaction_by_key.values())

            LOGGER.info(
                f"[MASTER QUALIFICATION SUMMARY] "
                f"total_candidates={total_candidates} "
                f"qualified={total_qualified} "
                f"skipped={total_skipped}",
                pipeline_type="CHECKSHEET",
            )

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
        first_master_index: int,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, int, int, int]:
        """
        Parse one BL/HD worksheet for qualified master rows and transaction columns.

        Returns:
            (
                sheet_masters,
                sheet_transactions,
                sheet_candidates,
                sheet_qualified,
                sheet_skipped,
                next_available_master_index,
            )
        """
        next_master_index = first_master_index
        col_a21_value = worksheet.cell(row=21, column=1).value
        sheet_op_no_2nd, sheet_op_no_3rd = self._parse_op_no_2nd_3rd(col_a21_value)

        sheet_machining_surface = worksheet.cell(row=21, column=2).value

        valid_columns = self._discover_transaction_columns(
            worksheet,
            value_worksheet,
        )

        sheet_masters: List[Dict[str, Any]] = []
        sheet_candidates = 0
        sheet_with_tx_values = 0
        sheet_without_tx_values = 0
        sheet_qualified = 0
        sheet_skipped = 0

        for row_index in range(self.FIRST_MASTER_ROW, worksheet.max_row + 1):
            if self._is_completely_blank_row(
                worksheet,
                value_worksheet,
                row_index=row_index,
                valid_columns=valid_columns,
            ):
                continue

            sheet_candidates += 1

            has_transaction_value = any(
                not self._is_blank(
                    self._source_value(
                        value_worksheet,
                        worksheet,
                        row=row_index,
                        column=col_index,
                    )
                )
                for col_index in valid_columns
            )

            LOGGER.info(
                f"[MASTER QUALIFICATION] sheet={worksheet.title} row={row_index} "
                f"has_transaction_value={str(has_transaction_value).lower()}",
                pipeline_type="CHECKSHEET",
            )

            if has_transaction_value:
                sheet_with_tx_values += 1
            else:
                sheet_without_tx_values += 1

            is_qualified = has_transaction_value

            if not is_qualified:
                sheet_skipped += 1
                LOGGER.info(
                    f"[MASTER ROW SKIP] sheet={worksheet.title} row={row_index} "
                    f"reason=no transaction values",
                    pipeline_type="CHECKSHEET",
                )
                continue

            sheet_qualified += 1

            source_master_index = next_master_index
            next_master_index += 1

            master_record = self._build_master_record(
                worksheet,
                row_index=row_index,
                source_master_index=source_master_index,
                vpo_code=vpo_code,
                vehicle_model=vehicle_model,
                parts_code=parts_code,
                op_no=op_no,
                op_no_2nd=sheet_op_no_2nd,
                op_no_3rd=sheet_op_no_3rd,
                machining_surface=sheet_machining_surface,
            )
            sheet_masters.append(master_record)

            LOGGER.info(
                f"[MASTER ROW PARSED] "
                f"sheet={worksheet.title} "
                f"row={row_index} "
                f"row_index={row_index} "
                f"source_master_index={source_master_index}",
                pipeline_type="CHECKSHEET",
            )

        LOGGER.info(
            f"[MASTER ANALYSIS] "
            f"sheet={worksheet.title} "
            f"candidates={sheet_candidates} "
            f"with_transaction_values={sheet_with_tx_values} "
            f"without_transaction_values={sheet_without_tx_values}",
            pipeline_type="CHECKSHEET",
        )

        if not sheet_masters:
            return [], [], sheet_candidates, sheet_qualified, sheet_skipped, next_master_index

        sheet_transactions: List[Dict[str, Any]] = []

        for column_index in valid_columns:
            raw_serial = self._source_value(
                value_worksheet,
                worksheet,
                row=self.TRANSACTION_SERIAL_ROW,
                column=column_index,
            )
            serial_no = self._format_serial_no(raw_serial)

            transaction_date = self._source_value(
                value_worksheet,
                worksheet,
                row=self.TRANSACTION_DATE_ROW,
                column=column_index,
            )
            parsed_date = self._coerce_date(
                transaction_date,
                epoch=getattr(worksheet.parent, "epoch", None),
            )
            if parsed_date is None:
                continue

            serial_last4 = self._serial_no_m_last4(serial_no)
            for master_record in sheet_masters:
                raw_measurement = self._source_value(
                    value_worksheet,
                    worksheet,
                    row=master_record["row_index"],
                    column=column_index,
                )
                norm_val = self._normalize_measurement_value(raw_measurement)
                if self._is_blank(norm_val):
                    continue

                transaction_record: Dict[str, Any] = {
                    "version": self.CHECKSHEET_VERSION,
                    "mc_no": "",
                    "vpo_code": vpo_code,
                    "vehicle_model": vehicle_model,
                    "parts_code": parts_code,
                    "op_no": op_no,
                    "op_no_2nd": sheet_op_no_2nd,
                    "op_no_3rd": sheet_op_no_3rd,
                    "date": parsed_date,
                    "serial_no": serial_no,
                    "serial_no_m_last4": serial_last4,
                    "eng_no": None,
                    "value": norm_val,

                    # Temporary master identity, NOT pi_no. See class
                    # docstring point 6 -- LoadChecksheetStep resolves this
                    # to the final pi_no after MeasurementRepository
                    # sync_masters() runs.
                    "source_master_index": master_record["source_master_index"],
                }
                sheet_transactions.append(transaction_record)
                LOGGER.info(
                    f"[TRANSACTION VALUE PARSED] sheet={worksheet.title} "
                    f"column={get_column_letter(column_index)} serial_no={serial_no} "
                    f"source_master_index={transaction_record['source_master_index']} "
                    f"value={norm_val} "
                    f"op_no_2nd={sheet_op_no_2nd} op_no_3rd={sheet_op_no_3rd}",
                    pipeline_type="CHECKSHEET",
                )

        return (
            sheet_masters,
            sheet_transactions,
            sheet_candidates,
            sheet_qualified,
            sheet_skipped,
            next_master_index,
        )

    def _build_master_record(
        self,
        worksheet: Any,
        *,
        row_index: int,
        source_master_index: int,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        op_no_2nd: Optional[str] = None,
        op_no_3rd: Optional[str] = None,
        machining_surface: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Construct one canonical master record dictionary from BL/HD cell values.

        ``source_master_index`` is a temporary workbook-local identifier used
        solely to connect this master definition to its transaction rows
        until MeasurementRepository resolves the final pi_no. It is NOT
        pi_no and must never be persisted or treated as one.
        """
        col_g_value = worksheet.cell(row=row_index, column=7).value
        col_h_value = worksheet.cell(row=row_index, column=8).value
        measure_point = self._combine_measure_point(col_g_value, col_h_value)

        return {
            "version": self.CHECKSHEET_VERSION,
            "mc_no": "",
            "vpo_code": vpo_code,
            "vehicle_model": vehicle_model,
            "parts_code": parts_code,
            "op_no": op_no,
            "op_no_2nd": op_no_2nd,
            "op_no_3rd": op_no_3rd,
            "machining_surface": machining_surface,
            "op_name": worksheet.cell(row=row_index, column=3).value,
            "Inspection_no": worksheet.cell(row=row_index, column=4).value,
            "measure_item": worksheet.cell(row=row_index, column=5).value,
            "measure_rank": worksheet.cell(row=row_index, column=6).value,
            "measure_point": measure_point,
            "measure_direction": None,
            "measure_depth": None,
            "measure_method": worksheet.cell(row=row_index, column=9).value,
            "measure_frequency": worksheet.cell(row=row_index, column=12).value,
            "specification": worksheet.cell(row=row_index, column=13).value,
            "upper_limit": worksheet.cell(row=row_index, column=15).value,
            "lower_limit": worksheet.cell(row=row_index, column=16).value,

            # Temporary workbook-local identity connecting this master to
            # its transaction rows. See method docstring above.
            "source_master_index": source_master_index,

            # Physical source location, used only while reading this
            # workbook during this transform run.
            "row_index": row_index,

            "insert_id": "Vig",
            "update_id": "Vig",
        }

    @classmethod
    def _is_completely_blank_row(
        cls,
        worksheet: Any,
        value_worksheet: Any,
        *,
        row_index: int,
        valid_columns: List[int],
    ) -> bool:
        """Check if a row contains no cell values across both master and transaction columns."""
        master_cols_blank = all(
            cls._is_blank(worksheet.cell(row=row_index, column=col).value)
            for col in (3, 4, 5, 6, 7, 8, 9, 12, 13, 15, 16)
        )
        if not master_cols_blank:
            return False

        tx_cols_blank = all(
            cls._is_blank(
                cls._source_value(
                    value_worksheet,
                    worksheet,
                    row=row_index,
                    column=col,
                )
            )
            for col in valid_columns
        )
        return tx_cols_blank

    @classmethod
    def _discover_transaction_columns(
        cls,
        worksheet: Any,
        value_worksheet: Any,
    ) -> List[int]:
        """Find all columns containing valid serial numbers starting from Column R."""
        max_col = max(worksheet.max_column, value_worksheet.max_column, 1)
        valid_columns: List[int] = []

        for col_index in range(cls.FIRST_TRANSACTION_COL, max_col + 1):
            serial_val = cls._source_value(
                value_worksheet,
                worksheet,
                row=cls.TRANSACTION_SERIAL_ROW,
                column=col_index,
            )
            if not cls._is_blank(serial_val):
                valid_columns.append(col_index)

        return valid_columns

    @staticmethod
    def _combine_measure_point(val_g: Any, val_h: Any) -> Optional[str]:
        """Combine Column G and Column H cell values into one measure_point string."""
        str_g = str(val_g).strip() if val_g is not None else ""
        str_h = str(val_h).strip() if val_h is not None else ""

        if str_g and str_h:
            return f"{str_g}{str_h}"
        if str_g:
            return str_g
        if str_h:
            return str_h
        return None

    @staticmethod
    def _parse_op_no_2nd_3rd(val_a: Any) -> Tuple[Optional[str], Optional[str]]:
        """Parse secondary and tertiary operation numbers from Cell A21."""
        if val_a is None:
            return None, None

        text = str(val_a).strip()
        if not text:
            return None, None

        if "-" in text:
            parts = text.split("-", 1)
            op_2nd = parts[0].strip() or None
            op_3rd = parts[1].strip() or None
            return op_2nd, op_3rd

        return text, None

    @classmethod
    def _merge_transaction(
        cls,
        transaction_by_key: Dict[Tuple[str, date, str, int], Dict[str, Any]],
        incoming: Dict[str, Any],
    ) -> None:
        """
        Validate normalized transaction uniqueness and reject conflicting duplicates.

        Transaction identity during transformation is:

            (vpo_code, date, serial_no_m_last4, source_master_index)

        source_master_index is a temporary workbook-local master identity
        (see class docstring point 6) and is intentionally not a final
        pi_no. Because each master's source_master_index already implies
        which (op_no_2nd, op_no_3rd) subgroup it belongs to, this key does
        not need to separately include those fields.
        """
        source_master_index = incoming.get("source_master_index")

        if source_master_index is None:
            raise ValueError(
                "transaction record requires source_master_index"
            )

        try:
            source_master_index = int(source_master_index)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "transaction source_master_index must be an integer"
            ) from exc

        if source_master_index <= 0:
            raise ValueError(
                "transaction source_master_index must be greater than zero"
            )

        required_fields = (
            "vpo_code",
            "date",
            "serial_no",
            "serial_no_m_last4",
            "value",
        )

        for field_name in required_fields:
            if field_name not in incoming:
                raise ValueError(
                    f"transaction record missing required field: {field_name}"
                )

        key = (
            str(incoming["vpo_code"]),
            incoming["date"],
            str(incoming["serial_no_m_last4"]),
            source_master_index,
        )
        existing = transaction_by_key.get(key)
        if existing is None:
            transaction_by_key[key] = incoming
            return

        if str(existing.get("serial_no")) != str(incoming.get("serial_no")):
            raise ValueError(
                "serial_no_m_last4 collision for the same vpo_code, date, and source_master_index: "
                f"{existing.get('serial_no')!r} and {incoming.get('serial_no')!r}"
            )

        for name in (
            "version", "mc_no", "vehicle_model", "parts_code", "op_no",
            "op_no_2nd", "op_no_3rd", "eng_no",
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

        if existing.get("value") != incoming.get("value"):
            raise ValueError(
                f"conflicting measurement value for transaction key {key!r}: "
                f"{existing.get('value')!r} != {incoming.get('value')!r}"
            )

    @staticmethod
    def _source_value(
        value_worksheet: Any,
        normalized_worksheet: Any,
        *,
        row: int,
        column: int,
    ) -> Any:
        """Retrieve raw cell value with fallback to normalized sheet cell."""
        val = value_worksheet.cell(row=row, column=column).value
        if val is not None:
            return val
        return normalized_worksheet.cell(row=row, column=column).value

    @staticmethod
    def _normalize_measurement_value(value: Any) -> Any:
        """Format floating-point numbers preserving exact display values."""
        if isinstance(value, float):
            text = str(value)
            if "e" in text.lower():
                d = Decimal(text)
                text = f"{d:f}"
            return text
        return value

    @staticmethod
    def _format_serial_no(value: Any) -> str:
        """Format serial number preserving exact string values and padding numeric values."""
        if value is None:
            raise ValueError("serial_no is required")
        
        if isinstance(value, (int, float)):
            try:
                ival = int(value)
                return f"{ival:04d}"
            except (ValueError, OverflowError):
                text = str(value).strip()
        else:
            text = str(value).strip()

        if text.isdigit() and len(text) < 4:
            return text.zfill(4)
        return text

    @classmethod
    def _serial_no_m_last4(cls, value: Any) -> str:
        """Extract last 4 characters from formatted serial number for composite key creation."""
        formatted = cls._format_serial_no(value)
        if not formatted:
            raise ValueError("serial_no is required")
        return formatted[-4:]

    @staticmethod
    def _coerce_date(value: Any, *, epoch: Any = None) -> Optional[date]:
        """Convert various date representations to python date."""
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
        """Check if a cell value is None or empty string."""
        return value is None or (isinstance(value, str) and not value.strip())

    @staticmethod
    def _build_vpo_code(vehicle_model: str, parts_code: str, op_no: str) -> str:
        """Construct canonical VPO code string."""
        return f"{vehicle_model}_{parts_code}_{op_no}"

    @staticmethod
    def _extract_filename_parts(file_name: str) -> Dict[str, str]:
        """Extract model, parts code, and OP number from BL/HD filename stem."""
        stem = Path(file_name).stem
        match = re.fullmatch(
            r"(?P<vehicle_model>[^_]+)_"
            r"(?P<parts_code>[^_]+)_"
            r"(?P<op_no>[^_]+)(?:_.+)?",
            stem,
            flags=re.IGNORECASE,
        )
        if match is None:
            raise ValueError(
                "Checksheet filename must match "
                "VehicleModel_PartsCode_OPNO_Anything.xlsx"
            )
        for group_name, segment_name in (
            ("vehicle_model", "VehicleModel"),
            ("parts_code", "PartsCode"),
            ("op_no", "OPNO"),
        ):
            value = match.group(group_name)
            if re.fullmatch(r"[A-Za-z0-9]+", value) is None:
                raise ValueError(
                    f"Invalid checksheet filename segment '{segment_name}': "
                    f"value '{value}' in filename '{file_name}' must contain "
                    "only A-Z, a-z, and 0-9"
                )
        if not match.group("op_no").upper().startswith("OP"):
            raise ValueError(
                "Checksheet filename must match "
                "VehicleModel_PartsCode_OPNO_Anything.xlsx"
            )
        return {
            "vehicle_model": match.group("vehicle_model"),
            "parts_code": match.group("parts_code"),
            "op_no": match.group("op_no").upper(),
        }


__all__ = ["TransformChecksheetBLHDStep"]