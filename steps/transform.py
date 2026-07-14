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

Parser settings like 'header_row', 'data_start_row',2'value_column_idx', and 'excel_sheet_index'3come from machine_config["parser"].
"""

import math
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

from utils.logger import LOGGER


class TransformStep:
    """
    Parses a raw measurement file into a list of clean row dictionaries.
    Supports both CSV and Excel file formats as specified in2machine_config["parser"].
    """

    # OPTIONAL_FIELD_ALIASES removed — rely on parser positional indexes or exact headers

    def run(
        self,
        file_path: Path,
        part_config: dict,
        machine_config: dict,
        machine_id: str,
        log_id: int,
    ) -> List[Dict[str, Any]]:
        """
        Reads and transforms a file into a list of measurement row dicts.

        Args:
            file_path (Path): Path to the file in the processing folder.
            part_config (dict):2    Classification metadata returned by Dispatcher from fs3.pre_m_file_pattern.
            machine_id (str): Machine identifier for log context.
            log_id (int): The pipeline run log ID for correlation.

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
                    f"data_start_row={data_start_row}"
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

        try:
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

        if raw_df.empty:
            raise ValueError(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: File '{file_path.name}' produced zero data rows. "
                f"Verify that 'data_start_row' ({data_start_row}) and the file format are correct "
                f"in machine_config['parser']."
            )

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

        serial_no_m_last4 = self._derive_serial_no_m_last4_from_filename(file_path.name)
        if serial_no_m_last4:
            for r in rows:
                r["serial_no_m_last4"] = serial_no_m_last4

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

    def _derive_serial_no_m_last4_from_filename(self, file_name: str) -> Optional[str]:
        """
        Derive serial_no_m_last4 from the filename.

        Example:
            193E★R10_連続測定_9999999999999999_実証　5_20260630171111.CSV
            -> 実証5
        """
        match = re.search(r"_([^_]+?)_(\d{14})(?:_[^.]+)?(?:\.[^.]+)?$", file_name)
        if not match:
            return None

        serial_part = match.group(1).strip()
        if not serial_part:
            return None

        normalized = re.sub(r"\s+", "", serial_part)
        return normalized if normalized else None

    def _derive_measurement_datetime_from_filename(
        self, file_name: str
    ) -> Optional[datetime]:
        """
        Derive the measurement datetime from the filename timestamp.

        Example:
            193E★R10_連続測定_9999999999999999_撮影_20260629205959.CSV
            -> 2026-06-29 20:59:59
        """
        match = re.search(r"_(\d{14})(?:_[^.]+)?(?:\.[^.]+)?$", file_name)
        if not match:
            return None

        try:
            return datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
        except ValueError:
            return None

    def _read_file(
        self,
        file_path: Path,
        file_type: str,
        header_row: int,
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

        # Capture the header label for the measurement value column so we
        # can use it as a source for `measure_position` when the row cell is missing.
        # value_col_header = None
        # try:
        #     value_col_header = df.columns[value_col_idx]
        # except Exception:
        #     value_col_header = None

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
                # BL logic will be implemented later from filename.
                measure_position = None

                # If measure_position is still missing, try to use the header
                # label for the measurement value column (this is source data).
                # if not measure_position and value_col_header is not None:
                #     try:
                #         header_lbl = (
                #             value_col_header.strip()
                #             if isinstance(value_col_header, str)
                #             else str(value_col_header)
                #         )
                #         if header_lbl != "" and header_lbl.lower() != "nan":
                #             measure_position = header_lbl
                #     except Exception:
                #         pass

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

    def _get_optional_value(self, row: pd.Series, key: str):
        if key in row:
            return row.get(key)

        lower_key = key.strip().lower()
        for col in row.index:
            if isinstance(col, str) and col.strip().lower() == lower_key:
                return row.get(col)

        # Alias lookup removed — only exact header matches are considered now

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
        # Treat pandas NA/NaN and literal 'nan' as missing
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
        """
        Derive specification / upper_limit / lower_limit from positional columns
        when the source file does not expose canonical header labels.

        For many CMM CSV layouts, the measurement value is located three columns
        after the spec/upper/lower set. This helper attempts that fallback.
        """
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
