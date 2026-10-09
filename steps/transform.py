# -*- coding: utf-8 -*-
"""
Transform Step
--------------
Reads the raw measurement file (CSV or Excel) from the processing folder,
applies parsing logic using parser settings from machines.json, and classification metadata returned by Dispatcher.
returns a clean list of row dictionaries ready for the database load step.

This step does NOT write to the database. It only produces validated,
cleaned data structures in memory. This separation keeps each step testable
in isolation.

Parser settings like 'header_row', 'data_start_row', 'value_column_idx', and 'excel_sheet_index' come from machine_config["parser"].
"""

import math
import re
from decimal import Decimal, ROUND_HALF_UP, localcontext
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import xlrd
from xlrd.sheet import Sheet
from utils.logger import LOGGER
from utils.enr_block import parse_enr_filename


class TransformStep:
    """
    Parses a raw measurement file into a list of clean row dictionaries.
    Supports both CSV and Excel file formats as specified in machine_config["parser"].
    """

    def run(
        self,
        file_path: Path,
        part_config: dict,
        machine_config: dict,
        machine_id: str,
        log_id: int,
        pipeline_type: str,
    ) -> List[Dict[str, Any]]:
        """
        Reads and transforms a file into a list of measurement row dicts.

        Args:
            file_path (Path): Path to the file in the processing folder.
            part_config (dict): Classification metadata returned by Dispatcher from fs3.pre_m_file_pattern.
            machine_id (str): Machine identifier for log context.
            log_id (int): The pipeline run log ID for correlation.
            pipeline_type (str): Operational category ('3D_HD', '3D_BL', etc.).

        Returns:
            List[Dict]: Each dict represents one measurement row with keys
                        matching the columns of pre_f_3d_hd / pre_f_3d_bl.

        Raises:
            ValueError: If the file is empty or missing expected columns.
            RuntimeError: If the file type is not recognized.
        """
        vehicle_model = part_config.get("vehicle_model")
        parts_code = part_config.get("parts_code")
        op_no = part_config.get("op_no")
        context = dict(
            machine=machine_id or "Unknown",
            model=vehicle_model or "Unknown",
            part=parts_code or "Unknown",
            op=op_no or "Unknown",
            pipeline_type=pipeline_type,
        )

        parser = machine_config.get("parser", {})
        file_type = parser.get("file_type", "csv").lower()
        header_row = parser.get("header_row", 0)
        data_start_row = parser.get("data_start_row", 1)
        value_col_idx = parser.get("value_column_idx", 5)
        spec_col_idx = parser.get("specification_column_idx")
        upper_col_idx = parser.get("upper_limit_column_idx")
        lower_col_idx = parser.get("lower_limit_column_idx")
        eval_col_idx = parser.get("evaluation_item1_column_idx", 1)
        meas_col_idx = parser.get("measure_item_column_idx", 2)
        sheet_index = parser.get("excel_sheet_index", 0)
        # Apply layout overrides based on filename
        for override in parser.get("layout_overrides", []):
            pattern = override.get("file_pattern", "")
            if pattern and pattern in file_path.name:
                header_row = override.get("header_row", header_row)
                data_start_row = override.get("data_start_row", data_start_row)
                spec_col_idx = override.get(
                    "specification_column_idx",
                    spec_col_idx,
                )
                upper_col_idx = override.get(
                    "upper_limit_column_idx",
                    upper_col_idx,
                )
                lower_col_idx = override.get(
                    "lower_limit_column_idx",
                    lower_col_idx,
                )
                value_col_idx = override.get(
                    "value_column_idx",
                    value_col_idx,
                )
                eval_col_idx = override.get(
                    "evaluation_item1_column_idx",
                    eval_col_idx,
                )
                meas_col_idx = override.get(
                    "measure_item_column_idx",
                    meas_col_idx,
                )
                LOGGER.info(
                    f"[LOG_ID:{log_id}] LAYOUT OVERRIDE APPLIED "
                    f"pattern='{pattern}' "
                    f"header_row={header_row} "
                    f"data_start_row={data_start_row}",
                    **context,
                )
                break
        LOGGER.info(
            f"[LOG_ID:{log_id}] TRANSFORM CONFIG: "
            f"header_row={header_row}, "
            f"data_start_row={data_start_row}, "
            f"spec_col_idx={spec_col_idx}, "
            f"upper_col_idx={upper_col_idx}, "
            f"lower_col_idx={lower_col_idx}, "
            f"value_col_idx={value_col_idx}",
            **context,
        )

        LOGGER.info(
            f"[LOG_ID:{log_id}] TRANSFORM STEP started. Reading '{file_path.name}' as '{file_type}'.",
            **context,
        )
        is_enr_block_name = (
            machine_id == "KTS-7106"
            and part_config.get("enr_block_flow", False)
            and file_path.name.startswith("E-NR")
        )
        if is_enr_block_name and file_path.suffix.lower() != ".xls":
            LOGGER.warning(
                f"[LOG_ID:{log_id}] E-NR-BLOCK file skipped because extension is not .xls: "
                f"{file_path.name}",
                **context,
            )
            raise ValueError("E-NR-BLOCK files must use the legacy .xls extension")

        enr_block_metadata = None
        if is_enr_block_name:
            try:
                enr_block_metadata = self._parse_enr_block_filename(file_path.name)
            except ValueError as exc:
                LOGGER.warning(
                    f"[LOG_ID:{log_id}] Invalid E-NR-BLOCK filename "
                    f"'{file_path.name}': {exc}",
                    **context,
                )
                raise

        if machine_id == "KTS-7898":
            raw_df = pd.DataFrame()
        else:
            try:
                if is_enr_block_name:
                    raw_df = self._read_enr_block_file(file_path, sheet_index)
                else:
                    raw_df = self._read_file(
                        file_path,
                        file_type,
                        header_row,
                        data_start_row,
                        sheet_index,
                        context,
                        log_id,
                    )
            except Exception as e:
                LOGGER.error(
                    f"[LOG_ID:{log_id}] TRANSFORM STEP: Failed to read '{file_path.name}'. Error: {e}",
                    exc_info=True,
                    **context,
                )
                raise

        if machine_id != "KTS-7898" and raw_df.empty:
            raise ValueError(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: File '{file_path.name}' produced zero data rows. "
                f"Verify that 'data_start_row' ({data_start_row}) and the file format are correct "
                f"in machine_config['parser']."
            )
        if is_enr_block_name:
            rows = self._parse_enr_block_rows(
                raw_df,
                context,
                log_id,
                file_path.name,
            )
        elif machine_id == "KTS-7106":
            rows = self._parse_kts7106(
                raw_df,
                part_config,
                context,
                log_id,
            )
        elif machine_id == "KTS-7898":
            rows = self._parse_kts7898(
                file_path, part_config, machine_config, context, log_id
            )
        else:
            rows = self._parse_rows(
                raw_df,
                part_config,
                value_col_idx,
                spec_col_idx,
                upper_col_idx,
                lower_col_idx,
                eval_col_idx,
                meas_col_idx,
                context,
                log_id,
            )
        if enr_block_metadata is not None:
            serial_no_m_last4 = enr_block_metadata["serial_number"]
        elif machine_id == "KTS-7898":
            serial_no_m_last4 = self._derive_kts7898_serial_no(file_path.name)
        else:
            serial_no_m_last4 = self._derive_serial_no_m_last4_from_filename(
                file_path.name
            )
        if serial_no_m_last4:
            for r in rows:
                r["serial_no_m_last4"] = serial_no_m_last4
        if enr_block_metadata is not None:
            measurement_datetime = enr_block_metadata["measurement_datetime"]
        elif machine_id == "KTS-7898":
            measurement_datetime = self._derive_kts7898_datetime(file_path.name)
        else:
            measurement_datetime = self._derive_measurement_datetime_from_filename(
                file_path.name
            )
        if measurement_datetime:
            for r in rows:
                r["measurement_datetime"] = measurement_datetime

        LOGGER.info(
            f"[LOG_ID:{log_id}] TRANSFORM STEP: Parsed {len(rows)} measurement rows from '{file_path.name}'.",
            **context,
        )
        return rows

    @staticmethod
    def _enr_b3_matches(sheet: Sheet) -> bool:
        return (
            sheet.nrows > 2
            and sheet.ncols > 1
            and "定量" in str(sheet.cell_value(2, 1))
        )

    def is_enr_quantitative_file(self, file_path: Path, sheet_index: int) -> bool:
        workbook = xlrd.open_workbook(str(file_path))
        try:
            return self._enr_b3_matches(workbook.sheets()[sheet_index])
        finally:
            workbook.release_resources()

    def _read_enr_block_file(self, file_path: Path, sheet_index: int) -> pd.DataFrame:
        workbook = xlrd.open_workbook(str(file_path), formatting_info=True)
        try:
            sheet = workbook.sheets()[sheet_index]
            if not self._enr_b3_matches(sheet):
                raise ValueError(
                    f"E-NR/BL measurement file is not eligible: "
                    f"{file_path.name}, sheet={sheet.name}, B3 does not contain 定量"
                )
            records = []
            for number in range(8, sheet.nrows):
                values: List[Any] = list(sheet.row_values(number))
                if len(values) > 5 and str(values[0]).strip():
                    cell = sheet.cell(number, 5)
                    if cell.ctype == xlrd.XL_CELL_NUMBER:
                        format_index = cell.xf_index
                        numeric_value = cell.value
                        if format_index is None or not isinstance(numeric_value, (int, float)):
                            raise ValueError(
                                f"{file_path.name}, sheet={sheet.name}, F{number + 1}: "
                                "numeric cell is missing its format index or numeric value"
                            )
                        number_format = workbook.format_map[
                            workbook.xf_list[format_index].format_key
                        ].format_str
                        try:
                            values[5] = self._format_enr_excel_number(
                                numeric_value, number_format
                            )
                        except ValueError as exc:
                            raise ValueError(
                                f"{file_path.name}, sheet={sheet.name}, F{number + 1}: {exc}"
                            ) from exc
                records.append(values)
            return pd.DataFrame(records)
        finally:
            workbook.release_resources()

    @staticmethod
    def _format_enr_excel_number(value: float, number_format: str) -> str:
        # Excel's underscore directives reserve padding, not value characters.
        pattern = re.sub(r"_.", "", number_format).strip()
        match = re.fullmatch(r"0(?:\.(0+))?", pattern)
        if match is None:
            raise ValueError(
                f"Unsupported E-NR column-F number format {number_format!r}; "
                "cannot safely reproduce its Excel display"
            )
        places = len(match.group(1) or "")
        decimal_value = Decimal(str(value))
        with localcontext() as context:
            context.prec = max(28, decimal_value.adjusted() + places + 2)
            rounded = decimal_value.quantize(
                Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP
            )
        if rounded.is_zero():
            rounded = abs(rounded)
        return format(rounded, f".{places}f")

    def _parse_enr_block_filename(self, file_name: str) -> Dict[str, Any]:
        serial_number, measurement_datetime = parse_enr_filename(file_name)
        return {
            "vehicle_model": "E-NR",
            "parts_code": "BL",
            "serial_number": serial_number,
            "measurement_datetime": measurement_datetime,
        }

    def _parse_enr_block_rows(
        self,
        df: pd.DataFrame,
        context: dict,
        log_id: int,
        file_name: str,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        for position, (_, row) in enumerate(df.iterrows()):
            excel_row = position + 9
            op_name = self._get_optional_string_by_pos(row, 0)
            measure_value = self._get_optional_string_by_pos(row, 5)
            if op_name is None or measure_value is None:
                LOGGER.info(
                    f"[LOG_ID:{log_id}] E-NR-BLOCK row skipped because column A or F "
                    f"is empty: file='{file_name}', row={excel_row}",
                    **context,
                )
                continue

            rows.append(
                {
                    "op_name": op_name,
                    "measure_value": measure_value,
                    "excel_row": excel_row,
                    "enr_block_flow": True,
                }
            )

        LOGGER.info(
            f"[LOG_ID:{log_id}] E-NR-BLOCK parser created {len(rows)} rows.",
            **context,
        )
        return rows

    def _derive_kts7898_serial_no(
        self,
        file_name: str,
    ) -> Optional[str]:
        parts = Path(file_name).stem.split("_")
        for i, part in enumerate(parts):
            if re.fullmatch(r"\d{14}", part):
                if i > 0:
                    serial_no = parts[i - 1].strip()
                    if serial_no:
                        return serial_no
        if parts:
            serial_no = parts[-1].strip()
            return serial_no if serial_no else None

    def _derive_serial_no_m_last4_from_filename(self, file_name: str) -> Optional[str]:
        try:
            parts = Path(file_name).stem.split("_")
        except Exception:
            return None
        timestamp_idx = None
        for i, part in enumerate(parts):
            if re.fullmatch(r"\d{14}", part):
                timestamp_idx = i
                break
        if timestamp_idx is None or timestamp_idx < 1:
            return None

        serial_part = parts[timestamp_idx - 1].strip()
        if not serial_part:
            return None

        normalized = re.sub(r"\s+", "", serial_part)
        return normalized if normalized else None

    def _derive_kts7898_datetime(
        self,
        file_name: str,
    ) -> Optional[datetime]:
        parts = Path(file_name).stem.split("_")
        # Generic format used by VN and future files
        for part in parts:
            if re.fullmatch(r"\d{14}", part):
                try:
                    return datetime.strptime(
                        part,
                        "%Y%m%d%H%M%S",
                    )
                except ValueError:
                    pass
        # Existing RC format
        if len(parts) < 4:
            return None

        try:
            date_part = parts[2].strip()
            time_part = parts[3].strip()

            dt_string = f"20{date_part}{time_part}"

            return datetime.strptime(
                dt_string,
                "%Y%m%d%H%M%S",
            )

        except Exception:
            return None

    def _derive_measurement_datetime_from_filename(
        self, file_name: str
    ) -> Optional[datetime]:
        """
        Derive the measurement datetime from the filename timestamp.

        Example:
            193E★R10_連続測定_9999999999999999_撮影_20260629205959.CSV
            -> 2026-06-29 20:59:59
        """
        parts = Path(file_name).stem.split("_")
        for part in parts:
            if re.fullmatch(r"\d{14}", part):
                try:
                    return datetime.strptime(part, "%Y%m%d%H%M%S")
                except ValueError:
                    return None
        return None

    def _read_file(
        self,
        file_path: Path,
        file_type: str,
        header_row: Optional[int],
        data_start_row: int,
        sheet_index: int,
        context: dict,
        log_id: int,
    ) -> pd.DataFrame:
        """
        Reads the file into a pandas DataFrame based on its type.
        Uses 'header_row' and 'data_start_row' from the config, not guessing.
        """
        # Compute skip_rows robustly. If header_row is None, there is no header line.
        if header_row is None:
            skip_rows = list(range(0, data_start_row))
        else:
            skip_rows = list(range(header_row)) + list(
                range(header_row + 1, data_start_row)
            )

        if file_type == "csv":
            encodings = [
                "utf-8-sig",
                "cp932",
                "shift_jis",
            ]
            last_error = None
            for encoding in encodings:
                try:
                    # Use skiprows for CSV as configured (keeps parity with Excel branch)
                    df = pd.read_csv(
                        file_path,
                        header=header_row,
                        encoding=encoding,
                        skiprows=skip_rows,
                    )
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] TRANSFORM STEP: Successfully read "
                        f"'{file_path.name}' using encoding '{encoding}'.",
                        **context,
                    )

                    # If pandas returned a single-column frame, try auto-detecting delimiter
                    if df.shape[1] == 1:
                        LOGGER.warning(
                            f"[LOG_ID:{log_id}] TRANSFORM STEP: Single-column CSV read detected ({df.shape}). Trying delimiter autodetect.",
                            **context,
                        )
                        try:
                            # Try reading without treating any row as header (safer for files without headers)
                            df2 = pd.read_csv(
                                file_path,
                                header=None,
                                encoding=encoding,
                                sep=None,
                                engine="python",
                                skiprows=skip_rows,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: Delimiter autodetect succeeded for '{file_path.name}' (shape {df2.shape}).",
                                **context,
                            )
                            # Diagnostics: DataFrame info for debugging collapsed CSVs
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame type={type(df2).__name__} shape={df2.shape}",
                                **context,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM CONFIG: header_row={header_row} data_start_row={data_start_row}",
                                **context,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame columns={list(df2.columns)}",
                                **context,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame head (20 rows):\n{df2.head(20).to_string()}",
                                **context,
                            )
                            try:
                                LOGGER.info(
                                    f"[LOG_ID:{log_id}] SAMPLE ROW 8 = {df.iloc[8].tolist()}",
                                    **context,
                                )
                            except Exception:
                                pass
                            return df2
                        except Exception:
                            # Fall back to the original df if autodetect fails
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: Delimiter autodetect failed for '{file_path.name}'. Using original parse.",
                                **context,
                            )
                    # If still single-column, try explicit header=None and comma sep fallback
                    if df.shape[1] == 1:
                        try:
                            df3 = pd.read_csv(
                                file_path,
                                header=None,
                                encoding=encoding,
                                sep=",",
                                skiprows=skip_rows,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: Comma-sep fallback succeeded for '{file_path.name}' (shape {df3.shape}).",
                                **context,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame type={type(df3).__name__} shape={df3.shape}",
                                **context,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM CONFIG: header_row={header_row} data_start_row={data_start_row}",
                                **context,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame columns={list(df3.columns)}",
                                **context,
                            )
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame head (20 rows):\n{df3.head(20).to_string()}",
                                **context,
                            )
                            return df3
                        except Exception:
                            LOGGER.info(
                                f"[LOG_ID:{log_id}] TRANSFORM STEP: Comma-sep fallback failed for '{file_path.name}'. Using original parse.",
                                **context,
                            )
                    # Diagnostics for the original dataframe
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame type={type(df).__name__} shape={df.shape}",
                        **context,
                    )
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] TRANSFORM CONFIG: header_row={header_row} data_start_row={data_start_row}",
                        **context,
                    )
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame columns={list(df.columns)}",
                        **context,
                    )
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame head (20 rows):\n{df.head(20).to_string()}",
                        **context,
                    )
                    return df
                except UnicodeDecodeError as e:
                    last_error = e
            if last_error is not None:
                raise last_error
            raise RuntimeError(
                f"[LOG_ID:{log_id}] Unable to read CSV file '{file_path.name}'."
            )
        elif file_type in ("excel", "xlsx", "xls", "xlsm"):
            df = pd.read_excel(
                file_path, sheet_name=sheet_index, header=header_row, skiprows=skip_rows
            )
            # Diagnostics for Excel-read DataFrame
            LOGGER.info(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame type={type(df).__name__} shape={df.shape}",
                **context,
            )
            LOGGER.info(
                f"[LOG_ID:{log_id}] TRANSFORM CONFIG: header_row={header_row} data_start_row={data_start_row}",
                **context,
            )
            LOGGER.info(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame columns={list(df.columns)}",
                **context,
            )
            LOGGER.info(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: DataFrame head (20 rows):\n{df.head(20).to_string()}",
                **context,
            )
            return df
        raise RuntimeError(
            f"[LOG_ID:{log_id}] TRANSFORM STEP: Unrecognized file_type '{file_type}' "
            f"in machine_config['parser']. Supported values: 'csv', 'excel'."
        )

    def _parse_rows(
        self,
        df: pd.DataFrame,
        part_config: dict,
        value_col_idx: int,
        spec_col_idx: Optional[int],
        upper_col_idx: Optional[int],
        lower_col_idx: Optional[int],
        eval_col_idx: int,
        meas_col_idx: int,
        context: dict,
        log_id: int,
    ) -> List[Dict[str, Any]]:
        """
        Iterates the DataFrame rows and builds structured dicts for the load step.
        Skips rows with NaN measure values cleanly (not silently — they are counted and logged).
        """
        vehicle_model = part_config.get("vehicle_model")
        parts_code = part_config.get("parts_code")
        op_no = part_config.get("op_no")
        vpo_code = part_config.get("vpo_code")
        side = part_config.get("side")

        # Normalize column headers so optional business fields can be looked up case-insensitively
        df.columns = [
            col.strip() if isinstance(col, str) else col for col in df.columns
        ]

        rows = []
        skipped = 0

        for idx, row in df.iterrows():
            try:
                # Diagnostic: show row type and content before any positional access
                LOGGER.info(
                    f"[LOG_ID:{log_id}] TRANSFORM STEP: Row type={type(row)} repr={repr(row)}",
                    **context,
                )

                row_width = len(row)

                # If the row is a single-element Series containing a tuple/list, expand it
                if row_width == 1 and isinstance(row.iloc[0], (list, tuple)):
                    original = row.iloc[0]
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] TRANSFORM STEP: Expanding single-cell tuple/list row to {len(original)} columns.",
                        **context,
                    )
                    row = pd.Series(list(original))
                    row_width = len(row)
                if not isinstance(value_col_idx, int) or value_col_idx < 0:
                    raise ValueError(
                        f"TRANSFORM STEP: Invalid 'value_column_idx' {value_col_idx}. "
                        f"Must be a non-negative integer."
                    )
                if value_col_idx >= row_width:
                    raise IndexError(
                        f"TRANSFORM STEP: value_column_idx {value_col_idx} out of bounds. "
                        f"Row width is {row_width}."
                    )
                raw_value = row.iloc[value_col_idx]

                # Skip rows where the measurement value is blank (NaN / empty)
                if raw_value is None or (
                    isinstance(raw_value, float) and math.isnan(raw_value)
                ):
                    LOGGER.warning(
                        f"[LOG_ID:{log_id}] SKIPPED BLANK ROW idx={idx}\n{row.to_string()}",
                        **context,
                    )
                    skipped += 1
                    continue

                # SIMPLIFIED: Extract positional data first (Column B=1, C=2)
                evaluation_item1 = self._get_optional_string_by_pos(row, eval_col_idx)
                measure_item = self._get_optional_string_by_pos(row, meas_col_idx)

                # Fallback to header-based only if positional extraction returned nothing
                if not evaluation_item1:
                    evaluation_item1 = self._get_optional_string(
                        row, "evaluation_item1"
                    )
                if not measure_item:
                    measure_item = self._get_optional_string(row, "measure_item")

                # HD files do not contain measure_position.
                measure_position = None

                # Prefer positional extraction when parser provides explicit indexes
                if isinstance(spec_col_idx, int):
                    specification = self._get_optional_string_by_pos(row, spec_col_idx)
                else:
                    specification = self._get_optional_string(row, "specification")

                if isinstance(upper_col_idx, int):
                    upper_limit = self._get_optional_float_by_pos(row, upper_col_idx)
                else:
                    upper_limit = self._get_optional_float(row, "upper_limit")

                if isinstance(lower_col_idx, int):
                    lower_limit = self._get_optional_float_by_pos(row, lower_col_idx)
                else:
                    lower_limit = self._get_optional_float(row, "lower_limit")

                if specification is None or upper_limit is None or lower_limit is None:
                    fallback_limits = self._derive_specification_limits_from_row(
                        row, value_col_idx
                    )
                    if specification is None:
                        specification = fallback_limits.get("specification")
                    if upper_limit is None:
                        upper_limit = fallback_limits.get("upper_limit")
                    if lower_limit is None:
                        lower_limit = fallback_limits.get("lower_limit")
                    if any(
                        fallback_limits.get(key) is not None
                        for key in ("specification", "upper_limit", "lower_limit")
                    ):
                        LOGGER.info(
                            f"[LOG_ID:{log_id}] TRANSFORM STEP: Derived spec/limit from row positions "
                            f"specification={specification!r} upper_limit={upper_limit!r} "
                            f"lower_limit={lower_limit!r}",
                            **context,
                        )
                rows.append(
                    {
                        "vehicle_model": vehicle_model,
                        "parts_code": parts_code,
                        "op_no": op_no,
                        "vpo_code": vpo_code,
                        "side": side,
                        "evaluation_item1": evaluation_item1,
                        "measure_item": measure_item,
                        "measure_position": measure_position,
                        "specification": specification,
                        "upper_limit": upper_limit,
                        "lower_limit": lower_limit,
                        "measure_value": float(raw_value),
                    }
                )
                LOGGER.info(
                    f"[LOG_ID:{log_id}] TRANSFORM STEP: Parsed row idx={idx} "
                    f"specification={specification!r} upper_limit={upper_limit!r} "
                    f"lower_limit={lower_limit!r}",
                    **context,
                )

                # If any critical business keys are missing, log the raw row for diagnosis
                if (
                    rows[-1]["evaluation_item1"] is None
                    or rows[-1]["measure_item"] is None
                ):
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] TRANSFORM STEP: Business-key-missing sample row idx={idx} values=\n{row.to_string()}",
                        **context,
                    )

            except (ValueError, IndexError) as e:
                LOGGER.warning(
                    f"[LOG_ID:{log_id}] TRANSFORM STEP: Skipping malformed row {idx} — {e}",
                    **context,
                )
                skipped += 1

        if skipped > 0:
            LOGGER.info(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: Skipped {skipped} blank/malformed rows (expected for sparse measurement files).",
                **context,
            )

        return rows

    def _parse_kts7106(
        self,
        df: pd.DataFrame,
        part_config: dict,
        context: dict,
        log_id: int,
    ) -> List[Dict[str, Any]]:
        vehicle_model = part_config.get("vehicle_model")
        parts_code = part_config.get("parts_code")
        op_no = part_config.get("op_no")
        vpo_code = part_config.get("vpo_code")
        side = part_config.get("side")

        rows = []

        for idx, row in df.iterrows():
            try:
                evaluation_item1_raw = self._get_optional_string_by_pos(row, 0)
                # KTS-7106 business rule:
                # only rows with evaluation item are valid measurement rows
                if not evaluation_item1_raw:
                    continue
                specification = self._get_optional_string_by_pos(row, 2)
                upper_limit = self._get_optional_string_by_pos(row, 3)
                lower_limit = self._get_optional_string_by_pos(row, 4)
                measure_value = self._get_optional_string_by_pos(row, 5)

                # skip only when value missing
                if measure_value is None:
                    LOGGER.info(
                        f"[LOG_ID:{log_id}] SKIP VALUE ROW idx={idx} "
                        f"raw_f={row.iloc[5]!r}",
                        **context,
                    )
                    continue

                evaluation_item1 = evaluation_item1_raw
                measure_item = ""
                measure_point = None

                tokens = evaluation_item1_raw.split("_")

                if len(tokens) >= 3:
                    evaluation_item1 = tokens[0].strip()
                    measure_item = tokens[1].strip()
                    measure_point = "_".join(tokens[2:]).strip()

                elif len(tokens) == 2:
                    evaluation_item1 = tokens[0].strip()
                    measure_item = tokens[1].strip()

                rows.append(
                    {
                        "vehicle_model": vehicle_model,
                        "parts_code": parts_code,
                        "op_no": op_no,
                        "vpo_code": vpo_code,
                        "side": side,
                        "evaluation_item1": evaluation_item1,
                        "measure_item": measure_item,
                        "measure_point": measure_point,
                        "specification": specification,
                        "upper_limit": upper_limit,
                        "lower_limit": lower_limit,
                        "measure_value": float(measure_value),
                    }
                )

            except Exception as e:
                LOGGER.warning(
                    f"[LOG_ID:{log_id}] KTS-7106: Skipped row {idx} - {e}",
                    **context,
                )

        LOGGER.info(
            f"[LOG_ID:{log_id}] KTS-7106 parser created {len(rows)} rows.",
            **context,
        )

        return rows

    def _parse_kts7898(
        self,
        file_path: Path,
        part_config: dict,
        machine_config: dict,
        context: dict,
        log_id: int,
    ) -> List[Dict[str, Any]]:
        vehicle_model = part_config.get("vehicle_model")
        parts_code = part_config.get("parts_code")
        op_no = part_config.get("op_no")
        vpo_code = part_config.get("vpo_code")
        side = part_config.get("side")

        parser = machine_config.get("parser", {})

        excluded_evaluation_items = set(parser.get("excluded_evaluation_items", []))
        rows = []
        current_reference_position = None
        with open(
            file_path,
            "r",
            encoding="cp932",
            errors="replace",
        ) as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                cols = [c.strip() for c in line.split(",")]
                # Handle reference-position header rows
                if cols and cols[0].startswith("----------"):
                    header_text = "".join(c for c in cols if c)
                    header_text = header_text.replace("----------", "").strip()
                    if header_text.lower().startswith("op"):
                        current_reference_position = header_text
                        LOGGER.info(
                            f"[LOG_ID:{log_id}] KTS-7898 REFERENCE POSITION="
                            f"'{current_reference_position}'",
                            **context,
                        )
                    continue

                cols = [c.strip() for c in line.split(",")]
                # Business rule: Measurement rows MUST have H column
                if len(cols) < 8:
                    continue
                try:
                    evaluation_item1 = cols[1].strip()
                    measure_item = cols[2].strip()
                    if evaluation_item1 in excluded_evaluation_items:
                        continue
                    specification = cols[4].strip()
                    upper_limit = cols[5].strip()
                    if (
                        upper_limit == "9.9999"
                        or upper_limit == "9.9990"
                        or upper_limit == "9.999"
                    ):
                        upper_limit = "-"
                    lower_limit = cols[6].strip()
                    if (
                        lower_limit == "-9.9999"
                        or lower_limit == "-9.9990"
                        or lower_limit == "-9.999"
                    ):
                        lower_limit = "-"
                    measure_value = cols[7].strip()
                except IndexError:
                    continue

                try:
                    measure_value_float = float(measure_value)
                except (TypeError, ValueError):
                    continue
                LOGGER.info(
                    f"[LOG_ID:{log_id}] ROW "
                    f"eval='{evaluation_item1}' "
                    f"item='{measure_item}' "
                    f"ref='{current_reference_position}'",
                    **context,
                )
                if not current_reference_position:
                    raise ValueError(
                        f"KTS-7898 reference reference position missing. "
                        f"eval={evaluation_item1} item={measure_item}"
                    )
                measure_point = None
                # 1. Number immediately after #
                match = re.search(r"#(\d+)", evaluation_item1)
                if match:
                    measure_point = match.group(1)
                # 2. Number immediately before 穴
                if not measure_point:
                    match = re.search(r"(\d+)穴", evaluation_item1)
                    if match:
                        measure_point = match.group(1)
                # 3. Number immediately before °
                if not measure_point:
                    match = re.search(r"(\d+°)", evaluation_item1)
                    if match:
                        measure_point = match.group(1)
                # 4. Any number outside brackets
                if not measure_point:
                    text_without_brackets = re.sub(
                        r"\([^)]*\)|\[[^\]]*\]",
                        "",
                        evaluation_item1,
                    )
                    match = re.search(r"(\d+)", text_without_brackets)
                    if match:
                        measure_point = match.group(1)

                rows.append(
                    {
                        "vehicle_model": vehicle_model,
                        "parts_code": parts_code,
                        "op_no": op_no,
                        "vpo_code": vpo_code,
                        "side": side,
                        "evaluation_item1": evaluation_item1,
                        "measure_item": measure_item,
                        "measure_point": measure_point,
                        "reference_position": current_reference_position,
                        "measure_position": None,
                        "specification": specification,
                        "upper_limit": upper_limit,
                        "lower_limit": lower_limit,
                        "measure_value": measure_value_float,
                    }
                )
        LOGGER.info(
            f"[LOG_ID:{log_id}] KTS-7898 parser created {len(rows)} rows.",
            **context,
        )
        return rows

    def _get_optional_value(self, row: pd.Series, key: str):
        if key in row:
            return row.get(key)

        lower_key = key.strip().lower()
        for col in row.index:
            if isinstance(col, str) and col.strip().lower() == lower_key:
                return row.get(col)

        return None

    def _get_optional_string_by_pos(self, row: pd.Series, idx: int) -> Optional[str]:
        try:
            val = row.iloc[idx]
        except Exception:
            return None
        if val is None or pd.isna(val):
            return None
        text = str(val).strip()
        if text == "" or text.lower() == "nan":
            return None
        return text

    def _get_optional_float_by_pos(self, row: pd.Series, idx: int) -> Optional[float]:
        try:
            val = row.iloc[idx]
        except Exception:
            return None
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _get_optional_string(self, row: pd.Series, key: str) -> Optional[str]:
        value = self._get_optional_value(row, key)
        if value is None:
            return None
        if pd.isna(value):
            return None
        text = str(value).strip()
        if text == "" or text.lower() == "nan":
            return None
        return text

    def _get_optional_float(self, row: pd.Series, key: str) -> Optional[float]:
        value = self._get_optional_value(row, key)
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _derive_specification_limits_from_row(
        self, row: pd.Series, value_col_idx: int
    ) -> Dict[str, Optional[Any]]:
        result: Dict[str, Optional[Any]] = {
            "specification": None,
            "upper_limit": None,
            "lower_limit": None,
        }

        if not isinstance(value_col_idx, int) or value_col_idx < 3:
            return result

        try:
            spec_source = row.iloc[value_col_idx - 3]
            upper_source = row.iloc[value_col_idx - 2]
            lower_source = row.iloc[value_col_idx - 1]
        except Exception:
            return result

        if spec_source is not None and not pd.isna(spec_source):
            text = str(spec_source).strip()
            if text != "" and text.lower() != "nan":
                result["specification"] = text

        if upper_source is not None and upper_source != "":
            try:
                result["upper_limit"] = float(upper_source)
            except (TypeError, ValueError):
                pass

        if lower_source is not None and lower_source != "":
            try:
                result["lower_limit"] = float(lower_source)
            except (TypeError, ValueError):
                pass

        return result