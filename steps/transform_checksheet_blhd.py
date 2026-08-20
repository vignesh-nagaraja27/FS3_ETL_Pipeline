# -*- coding: utf-8 -*-
"""Transform a BL/HD checksheet workbook into canonical ETL records.

This module provides a dedicated, isolated parser for BL and HD checksheets.
BL and HD workbooks share a common layout that differs completely from CR.
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

# Type alias for file paths to support both string and Path objects
PathLike = Union[str, Path]


class TransformChecksheetBLHDStep:
    """Parse normalized BL/HD worksheets into canonical master and transaction data.

    This parser implements the BL/HD specific workbook specification:
      - Traverses ALL worksheets in the workbook without title filtering.
      - Cell A21 contains sheet-level operation breakdown metadata (op_no_2nd, op_no_3rd).
      - Cell B21 contains sheet-level machining surface metadata (machining_surface).
      - Master definition rows begin at Row 21.
      - Transaction date is stored in Row 11.
      - Transaction serial number is stored in Row 14.
      - Transaction measurement values start at Column R (column index 18).
    """

    # Constants defining the BL/HD workbook geometry and limits
    CHECKSHEET_VERSION = "00000000"
    FIRST_MASTER_ROW = 21
    TRANSACTION_DATE_ROW = 11
    TRANSACTION_SERIAL_ROW = 14
    FIRST_TRANSACTION_COL = 18  # Column R
    MAX_DATA_COLUMNS = 500

    def run(
        self,
        file_path: PathLike,
        *,
        vehicle_model: Optional[str] = None,
        parts_code: Optional[str] = None,
        op_no: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Open, normalize, and parse one BL/HD checksheet workbook.

        This is the main public entry point for the BL/HD transform step.
        It processes every worksheet in the target workbook, extracts master
        definitions and transaction measurements, and aggregates them into
        canonical dictionary records.

        Purpose:
            Process a BL/HD Excel workbook and produce standardized records for database loading.

        Inputs:
            file_path: Path to the BL/HD Excel workbook file on disk.
            vehicle_model: Optional explicit vehicle model override string.
            parts_code: Optional explicit parts code override string.
            op_no: Optional explicit operation number override string.

        Outputs:
            A dictionary containing two lists:
              - 'master_records': List of extracted master record dictionaries.
              - 'transaction_records': List of aggregated transaction record dictionaries.
        """
        path = Path(file_path)
        if not path.is_file():
            # Stop early if the specified file does not exist on disk
            raise FileNotFoundError(f"BL/HD Checksheet workbook not found: {path}")

        # Extract filename metadata if explicit parameters were not provided
        filename_parts = self._extract_filename_parts(path.name)
        vehicle_model = vehicle_model or filename_parts["vehicle_model"]
        parts_code = parts_code or filename_parts["parts_code"]
        op_no = op_no or filename_parts["op_no"]
        vpo_code = self._build_vpo_code(vehicle_model, parts_code, op_no)

        LOGGER.info(
            f"[WORKBOOK OPEN] Opening BL/HD workbook: {path.name}",
            pipeline_type="CHECKSHEET",
        )

        # Load normalized workbook for text parsing and raw value workbook for measurements.
        # Two workbook instances are opened to ensure NFKC text normalization does not
        # alter numerical floating-point precision stored in measurement cells.
        workbook = load_workbook(filename=str(path), data_only=True, read_only=False)
        value_workbook = load_workbook(
            filename=str(path), data_only=True, read_only=False
        )

        try:
            # Apply Unicode NFKC normalization to standard text cells across all sheets
            WorkbookNormalizer.normalize_workbook(workbook)

            master_records: List[Dict[str, Any]] = []
            transaction_by_key: Dict[Tuple[str, date, str], Dict[str, Any]] = {}
            processed_sheet_count = 0

            # Process every worksheet in the workbook in sequential order (BL/HD rule: process ALL sheets)
            for worksheet in workbook.worksheets:
                if not worksheet.title:
                    continue

                processed_sheet_count += 1
                LOGGER.info(
                    f"[SHEET FOUND] Processing sheet: {worksheet.title}",
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

                # Skip empty sheets gracefully without raising an exception or interrupting processing
                if not sheet_masters:
                    LOGGER.info(
                        f"[SHEET SKIPPED] No master rows in sheet: {worksheet.title}",
                        pipeline_type="CHECKSHEET",
                    )
                    continue

                master_records.extend(sheet_masters)

                # Enforce safety ceiling on total master definitions across all sheets
                if len(master_records) > self.MAX_DATA_COLUMNS:
                    raise ValueError(
                        f"Workbook contains more than {self.MAX_DATA_COLUMNS} "
                        "master definitions across worksheets"
                    )

                # Merge sheet-level transactions into global transaction storage by composite key
                for transaction in sheet_transactions:
                    self._merge_transaction(transaction_by_key, transaction)

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
            # Ensure workbooks are closed to release memory and file handles
            value_workbook.close()
            workbook.close()

    # Alias method to match pipeline step execution conventions
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
        """Parse one BL/HD worksheet for master rows and transaction columns.

        Reads sheet-level metadata from Cell A21 (op_no_2nd, op_no_3rd) and
        Cell B21 (machining_surface), master definition rows starting at Row 21,
        and transaction measurement columns starting at Column R (index 18).

        Purpose:
            Extract master definitions and measurement columns from a single worksheet tab.

        Inputs:
            worksheet: The NFKC-normalized openpyxl worksheet object.
            value_worksheet: The raw data-only openpyxl worksheet object.
            first_data_position: The starting integer index for data_001 allocation.
            vpo_code: Formatted vehicle-part-op string key.
            vehicle_model: Vehicle model code string.
            parts_code: Parts code string.
            op_no: Primary operation number string.

        Outputs:
            A tuple containing:
              - List of master record dictionaries for this worksheet.
              - List of transaction record dictionaries for this worksheet.
        """
        # Read sheet-level operation breakdown metadata once from cell A21.
        # In BL/HD checksheets, the manufacturer writes the operation breakdown only once
        # per worksheet in A21, which applies to all master records in that worksheet.
        col_a21_value = worksheet.cell(row=21, column=1).value
        sheet_op_no_2nd, sheet_op_no_3rd = self._parse_op_no_2nd_3rd(col_a21_value)

        # Read sheet-level machining surface metadata once from cell B21.
        sheet_machining_surface = worksheet.cell(row=21, column=2).value

        sheet_masters: List[Dict[str, Any]] = []

        # Iterate through rows starting from the defined BL/HD master start row (Row 21)
        for row_index in range(self.FIRST_MASTER_ROW, worksheet.max_row + 1):
            if not self._is_master_row(worksheet, row_index):
                continue

            position = first_data_position + len(sheet_masters)
            if position > self.MAX_DATA_COLUMNS:
                raise ValueError(
                    f"Workbook contains more than {self.MAX_DATA_COLUMNS} "
                    "master definitions across worksheets"
                )

            # Assign contiguous position keys (data_001, data_002, etc.)
            data_column = f"data_{position:03d}"
            master_record = self._build_master_record(
                worksheet,
                row_index=row_index,
                data_column=data_column,
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
                f"data_column={data_column}",
                pipeline_type="CHECKSHEET",
            )

        # If no valid master rows exist on this sheet, stop transaction parsing for this sheet
        if not sheet_masters:
            return [], []

        sheet_transactions: List[Dict[str, Any]] = []

        # Discover used transaction columns starting from Column R (index 18)
        for column_index in self._discover_transaction_columns(
            worksheet,
            value_worksheet,
        ):
            # Read serial number from Row 14
            serial_no = self._source_value(
                value_worksheet,
                worksheet,
                row=self.TRANSACTION_SERIAL_ROW,
                column=column_index,
            )

            # Read transaction date from Row 11
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

            # Skip columns with unparseable or missing dates
            if parsed_date is None:
                continue

            # Construct canonical transaction dictionary header.
            # Operation breakdown metadata belongs strictly to master rows in pre_m_measurement.
            # One transaction spans multiple master definitions and cannot carry one
            # authoritative row-level secondary/tertiary operation or surface value.
            transaction_record: Dict[str, Any] = {
                "version": self.CHECKSHEET_VERSION,
                "mc_no": "",
                "vpo_code": vpo_code,
                "vehicle_model": vehicle_model,
                "parts_code": parts_code,
                "op_no": op_no,
                "op_no_2nd": None,
                "op_no_3rd": None,
                "date": parsed_date,
                "serial_no": serial_no,
                "serial_no_m_last4": self._serial_no_m_last4(serial_no),
                "eng_no": None,
                "insert_id": "Vig",
                "update_id": "Vig",
            }

            # Map raw measurement cell values to master data_column identifiers
            for master_record in sheet_masters:
                raw_measurement = self._source_value(
                    value_worksheet,
                    worksheet,
                    row=master_record["row_index"],
                    column=column_index,
                )
                transaction_record[master_record["data_column"]] = (
                    self._normalize_measurement_value(raw_measurement)
                )

            sheet_transactions.append(transaction_record)
            LOGGER.info(
                f"[TRANSACTION COLUMN PARSED] "
                f"sheet={worksheet.title} "
                f"column={get_column_letter(column_index)} "
                f"serial_no={serial_no}",
                pipeline_type="CHECKSHEET",
            )

        return sheet_masters, sheet_transactions

    def _build_master_record(
        self,
        worksheet: Any,
        *,
        row_index: int,
        data_column: str,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        op_no_2nd: Optional[str] = None,
        op_no_3rd: Optional[str] = None,
        machining_surface: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Construct one canonical master record dictionary from BL/HD cell values.

        Applies BL/HD column layout mappings:
          A21 -> op_no_2nd / op_no_3rd (sheet-level metadata passed in)
          B21 -> machining_surface (sheet-level metadata passed in)
          C -> op_name
          D -> Inspection_no
          E -> measure_item
          F -> measure_rank
          G + H -> measure_point
          I -> measure_method
          L -> measure_frequency
          M -> specification
          O -> upper_limit
          P -> lower_limit

        Purpose:
            Read cell values from a master row and map them into the canonical schema.

        Inputs:
            worksheet: Openpyxl worksheet object.
            row_index: Integer row index in worksheet.
            data_column: Canonical position column name (e.g. 'data_001').
            vpo_code: Formatted vehicle-part-op string.
            vehicle_model: Vehicle model code string.
            parts_code: Parts code string.
            op_no: Operation number string.
            op_no_2nd: Sheet-level secondary operation number extracted from A21.
            op_no_3rd: Sheet-level tertiary operation number extracted from A21.
            machining_surface: Sheet-level machining surface string extracted from B21.

        Outputs:
            Canonical master dictionary matching LoadChecksheetStep schema.
        """
        # Concatenate Column G and Column H for measure_point
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
            "data_column": data_column,
            "row_index": row_index,
            "insert_id": "Vig",
            "update_id": "Vig",
        }

    @classmethod
    def _is_master_row(cls, worksheet: Any, row_index: int) -> bool:
        """Determine if a worksheet row contains a valid master definition.

        Checks key BL/HD master columns (Column D: Inspection_no, Column E:
        measure_item, Column M: specification) to ensure the row is not blank.

        Purpose:
            Filter out empty or decorative non-master rows in the master section.

        Inputs:
            worksheet: Openpyxl worksheet object.
            row_index: Integer row index to evaluate.

        Outputs:
            True if row contains master definition data, False otherwise.
        """
        # Read Column D (col 4), Column E (col 5), and Column M (col 13)
        return not all(
            cls._is_blank(worksheet.cell(row=row_index, column=col).value)
            for col in (4, 5, 13)
        )

    @classmethod
    def _discover_transaction_columns(
        cls,
        worksheet: Any,
        value_worksheet: Any,
    ) -> List[int]:
        """Find all columns containing valid serial numbers starting from Column R.

        Iterates from Column R (column index 18) to max_column and collects
        indexes of columns where Row 14 contains a serial number value.

        Purpose:
            Identify which worksheet columns contain measurement transactions.

        Inputs:
            worksheet: Normalized openpyxl worksheet object.
            value_worksheet: Raw data-only openpyxl worksheet object.

        Outputs:
            List of integer column indices containing transaction serials.
        """
        max_col = max(worksheet.max_column, value_worksheet.max_column, 1)
        valid_columns: List[int] = []

        # Scan columns starting at Column R (index 18)
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
        """Combine Column G and Column H cell values into one measure_point string.

        Implements BL/HD concatenation rules:
          - Both populated: concatenated string (e.g., "面取り部#12")
          - Only G populated: string G
          - Only H populated: string H
          - Both blank/null: None

        Purpose:
            Produce the unified measure_point string required for PI_NO identity matching.

        Inputs:
            val_g: Raw value from Column G (Column 7).
            val_h: Raw value from Column H (Column 8).

        Outputs:
            Combined measure_point string or None if both cells are empty.
        """
        str_g = str(val_g).strip() if val_g is not None else ""
        str_h = str(val_h).strip() if val_h is not None else ""

        if str_g and str_h:
            # Join both column values directly without adding extra delimiter characters
            return f"{str_g}{str_h}"
        if str_g:
            return str_g
        if str_h:
            return str_h
        return None

    @staticmethod
    def _parse_op_no_2nd_3rd(val_a: Any) -> Tuple[Optional[str], Optional[str]]:
        """Parse secondary and tertiary operation numbers from Cell A21.

        Splits hyphenated text (e.g. "1-3") into op_no_2nd and op_no_3rd.
        Maps non-hyphenated values (e.g. "12") to op_no_2nd with op_no_3rd=None.

        Purpose:
            Extract secondary and tertiary operation numbers from Cell A21 metadata.

        Inputs:
            val_a: Raw cell value from Cell A21 (Column 1, Row 21).

        Outputs:
            Tuple containing (op_no_2nd_string, op_no_3rd_string).
        """
        if val_a is None:
            return None, None

        # Convert numeric or string value to trimmed string
        text = str(val_a).strip()
        if not text:
            return None, None

        if "-" in text:
            # Split hyphenated operation indicator into 2nd and 3rd parts
            parts = text.split("-", 1)
            op_2nd = parts[0].strip() or None
            op_3rd = parts[1].strip() or None
            return op_2nd, op_3rd

        # Single non-hyphenated value maps to op_no_2nd
        return text, None

    @classmethod
    def _merge_transaction(
        cls,
        transaction_by_key: Dict[Tuple[str, date, str], Dict[str, Any]],
        incoming: Dict[str, Any],
    ) -> None:
        """Merge transaction measurements across multiple worksheets.

        Uses composite key (vpo_code, transaction_date, serial_no_m_last4) to combine
        data_001..data_500 columns for serial numbers appearing on multiple sheets.

        Purpose:
            Consolidate multi-sheet measurements for the same serial into a single database row.

        Inputs:
            transaction_by_key: Dictionary storing transactions by composite key.
            incoming: Candidate transaction dictionary extracted from current sheet.

        Outputs:
            None (updates transaction_by_key in-place).

        Raises:
            ValueError: If full serial numbers conflict under identical composite key.
        """
        key = (
            str(incoming["vpo_code"]),
            incoming["date"],
            str(incoming["serial_no_m_last4"]),
        )
        existing = transaction_by_key.get(key)
        if existing is None:
            transaction_by_key[key] = incoming
            return

        # Ensure full serial numbers match when composite key collides
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

        # Merge data_XXX measurement fields into existing transaction record
        for name, value in incoming.items():
            if not name.startswith("data_"):
                continue
            if name in existing and existing[name] != value:
                raise ValueError(
                    f"conflicting measurement value for {name} and transaction key {key!r}"
                )
            existing[name] = value

    @staticmethod
    def _source_value(
        value_worksheet: Any,
        normalized_worksheet: Any,
        *,
        row: int,
        column: int,
    ) -> Any:
        """Retrieve raw cell value with fallback to normalized sheet cell.

        Purpose:
            Prefer raw cell value from data_only workbook to preserve numeric accuracy.

        Inputs:
            value_worksheet: Data-only worksheet object.
            normalized_worksheet: NFKC normalized worksheet object.
            row: Integer row number.
            column: Integer column number.

        Outputs:
            Cell value from value_worksheet if not None, else from normalized_worksheet.
        """
        val = value_worksheet.cell(row=row, column=column).value
        if val is not None:
            return val
        return normalized_worksheet.cell(row=row, column=column).value

    @staticmethod
    def _normalize_measurement_value(value: Any) -> Any:
        """Format floating-point numbers preserving exact display values without scientific notation.

        Purpose:
            Convert openpyxl numeric cell values to clean strings while avoiding
            IEEE 754 binary floating-point artifacts (e.g. 37.57 -> 37.569999999999998).

        Inputs:
            value: Raw cell value from openpyxl.

        Outputs:
            Clean string representation for floats, or original value for other types.
        """
        if isinstance(value, float):
            text = str(value)
            if "e" in text.lower():
                d = Decimal(text)
                text = f"{d:f}"
            return text
        return value

    @staticmethod
    def _serial_no_m_last4(value: Any) -> str:
        """Extract last 4 characters from serial number for composite key creation.

        Purpose:
            Generate uniform 4-character trailing string for transaction key matching.

        Inputs:
            value: Raw serial number value.

        Outputs:
            4-character trailing string slice of the serial number.
        """
        if isinstance(value, float) and value.is_integer():
            text = str(int(value))
        else:
            text = str(value)
        if not text:
            raise ValueError("serial_no is required")
        return text[-4:]

    @staticmethod
    def _coerce_date(value: Any, *, epoch: Any = None) -> Optional[date]:
        """Convert various date representations (ISO, Excel float, Japanese format) to date.

        Purpose:
            Robustly convert arbitrary cell date inputs into a python datetime.date object.

        Inputs:
            value: Raw input date cell value.
            epoch: Optional workbook epoch date for Excel float conversion.

        Outputs:
            datetime.date object if conversion succeeds, or None.
        """
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
        """Check if a cell value is None or empty string.

        Purpose:
            Test if cell has no meaningful text or numeric data.

        Inputs:
            value: Raw cell value.

        Outputs:
            True if blank or empty, False otherwise.
        """
        return value is None or (isinstance(value, str) and not value.strip())

    @staticmethod
    def _build_vpo_code(vehicle_model: str, parts_code: str, op_no: str) -> str:
        """Construct canonical VPO code string.

        Purpose:
            Format standardized key joining vehicle model, parts code, and operation number.

        Inputs:
            vehicle_model: Model string (e.g. 'BL').
            parts_code: Parts code string.
            op_no: OP number string.

        Outputs:
            Formatted VPO code '{vehicle_model}_{parts_code}_{op_no}'.
        """
        return f"{vehicle_model}_{parts_code}_{op_no}"

    @staticmethod
    def _extract_filename_parts(file_name: str) -> Dict[str, str]:
        """Extract model, parts code, and OP number from BL/HD filename stem.

        Filename pattern: VehicleModel_PartsCode_OPNO_Anything.xlsx

        Purpose:
            Parse metadata components directly from the file name.

        Inputs:
            file_name: File name string.

        Outputs:
            Dictionary containing 'vehicle_model', 'parts_code', and 'op_no'.
        """
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
                "VehicleModel_PartsCode_OPNO_Anything.xlsx"
            )
        return {
            "vehicle_model": match.group("vehicle_model"),
            "parts_code": match.group("parts_code"),
            "op_no": match.group("op_no").upper(),
        }


__all__ = ["TransformChecksheetBLHDStep"]