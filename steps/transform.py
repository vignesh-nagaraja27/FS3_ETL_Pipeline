# -*- coding: utf-8 -*-
"""
Transform Step
--------------
Reads the raw measurement file (CSV or Excel) from the processing folder,
applies parsing logic based on the part config from parts_3d.json, and
returns a clean list of row dictionaries ready for the database load step.

This step does NOT write to the database. It only produces validated,
cleaned data structures in memory. This separation keeps each step testable
in isolation.

Parser settings like 'header_row', 'data_start_row', 'value_column_idx',
and 'excel_sheet_index' all come from the part config JSON — never hardcoded here.
"""

import math
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from ETL_Pipeline.utils.logger import LOGGER


class TransformStep:
    """
    Parses a raw measurement file into a list of clean row dictionaries.
    Supports both CSV and Excel file formats as specified in the part config.
    """

    def run(
        self,
        file_path: Path,
        part_config: dict,
        machine_id: str,
        log_id: int
    ) -> List[Dict[str, Any]]:
        """
        Reads and transforms a file into a list of measurement row dicts.

        Args:
            file_path (Path): Path to the file in the processing folder.
            part_config (dict): Matched part config block from parts_3d.json.
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
        context = dict(machine=machine_id, model=vehicle_model, part=parts_code, op=op_no)

        parser = part_config.get("parser", {})
        file_type = parser.get("file_type", "csv").lower()
        header_row = parser.get("header_row", 0)
        data_start_row = parser.get("data_start_row", 1)
        value_col_idx = parser.get("value_column_idx", 5)
        measure_no_col_idx = parser.get("measure_no_col_idx", 0)
        sheet_index = parser.get("excel_sheet_index", 0)

        LOGGER.info(
            f"[LOG_ID:{log_id}] TRANSFORM STEP started. Reading '{file_path.name}' as '{file_type}'.",
            **context
        )

        try:
            raw_df = self._read_file(
                file_path, file_type, header_row, data_start_row, sheet_index, context, log_id
            )
        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: Failed to read '{file_path.name}'. Error: {e}",
                exc_info=True, **context
            )
            raise

        if raw_df.empty:
            raise ValueError(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: File '{file_path.name}' produced zero data rows. "
                f"Verify that 'data_start_row' ({data_start_row}) and the file format are correct "
                f"in 'configs/parts_3d.json'."
            )

        rows = self._parse_rows(raw_df, part_config, value_col_idx, measure_no_col_idx, context, log_id)

        LOGGER.info(
            f"[LOG_ID:{log_id}] TRANSFORM STEP: Parsed {len(rows)} measurement rows from '{file_path.name}'.",
            **context
        )
        return rows

    def _read_file(
        self,
        file_path: Path,
        file_type: str,
        header_row: int,
        data_start_row: int,
        sheet_index: int,
        context: dict,
        log_id: int
    ) -> pd.DataFrame:
        """
        Reads the file into a pandas DataFrame based on its type.
        Uses 'header_row' and 'data_start_row' from the config, not guessing.
        """
        skip_rows = list(range(header_row)) + list(range(header_row + 1, data_start_row))

        if file_type == "csv":
            df = pd.read_csv(file_path, header=header_row, encoding="utf-8-sig")
        elif file_type in ("excel", "xlsx", "xls", "xlsm"):
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_index,
                header=header_row,
                skiprows=skip_rows
            )
        else:
            raise RuntimeError(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: Unrecognized file_type '{file_type}' "
                f"in part config. Supported values: 'csv', 'excel'."
            )

        return df

    def _parse_rows(
        self,
        df: pd.DataFrame,
        part_config: dict,
        value_col_idx: int,
        measure_no_col_idx: int,
        context: dict,
        log_id: int
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

        rows = []
        skipped = 0

        for idx, row in df.iterrows():
            try:
                raw_value = row.iloc[value_col_idx]
                measure_no = row.iloc[measure_no_col_idx]

                # Skip rows where the measurement value is blank (NaN / empty)
                if raw_value is None or (isinstance(raw_value, float) and math.isnan(raw_value)):
                    skipped += 1
                    continue

                rows.append({
                    "vehicle_model": vehicle_model,
                    "parts_code": parts_code,
                    "op_no": op_no,
                    "vpo_code": vpo_code,
                    "side": side,
                    "measure_no": int(measure_no) if measure_no is not None else None,
                    "measure_value": float(raw_value),
                })

            except (ValueError, IndexError) as e:
                LOGGER.warning(
                    f"[LOG_ID:{log_id}] TRANSFORM STEP: Skipping malformed row {idx} — {e}",
                    **context
                )
                skipped += 1

        if skipped > 0:
            LOGGER.info(
                f"[LOG_ID:{log_id}] TRANSFORM STEP: Skipped {skipped} blank/malformed rows (expected for sparse measurement files).",
                **context
            )

        return rows
