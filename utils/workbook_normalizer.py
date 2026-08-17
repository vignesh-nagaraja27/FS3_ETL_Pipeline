# -*- coding: utf-8 -*-
"""Workbook normalization helpers for Excel ETL.

Purpose
-------
The ETL platform must read structured workbook data without missing cells caused
by filters, hidden rows, merged cells, or inconsistent character normalization.

Business rules
--------------
- Remove workbook filters before parsing.
- Expand hidden rows and columns to avoid partial reads.
- Resolve merged cells to prevent blank values in the middle of a record.
- Normalize Unicode characters before business-key comparisons.

Why this logic exists
---------------------
A user may leave filters or merged cells in the workbook. Those conditions can
make business rows disappear or appear partially read. The normalizer makes the
workbook safe for deterministic parsing.
"""

from __future__ import annotations

import unicodedata
from pathlib import Path
from typing import Any, Iterable, List, Union

from openpyxl import load_workbook
from openpyxl.cell.cell import Cell, MergedCell
from openpyxl.workbook.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from utils.logger import LOGGER


PathLike = Union[str, Path]


class WorkbookNormalizer:
    """Normalize workbook layout and textual content before ETL parsing."""

    @staticmethod
    def normalize_value(value: Any) -> Any:
        """Apply Unicode normalization to business keys and labels."""
        if value is None:
            return None
        if isinstance(value, str):
            return unicodedata.normalize("NFKC", value)
        return value

    @classmethod
    def normalize_workbook(cls, workbook: Workbook) -> Workbook:
        """Clean workbook content in place to avoid partial reads."""
        LOGGER.info(
            f"[WORKBOOK NORMALIZE] Starting workbook normalization for "
            f"{len(workbook.worksheets)} worksheets",
            pipeline_type="CHECKSHEET",
        )
        for worksheet in workbook.worksheets:
            LOGGER.info(
                f"[WORKSHEET NORMALIZE] Starting worksheet={worksheet.title}",
                pipeline_type="CHECKSHEET",
            )
            cls._remove_filters(worksheet)
            cls._expand_hidden_rows_and_columns(worksheet)
            cls._resolve_merged_cells(worksheet)
            cls._normalize_cell_values(worksheet)
            LOGGER.info(
                f"[WORKSHEET NORMALIZED] Completed worksheet={worksheet.title}",
                pipeline_type="CHECKSHEET",
            )
        return workbook

    normalize = normalize_workbook

    @staticmethod
    def _remove_filters(worksheet: Worksheet) -> None:
        """Clear worksheet filtering state without changing table definitions."""
        if worksheet.auto_filter and worksheet.auto_filter.ref:
            worksheet.auto_filter.ref = None
        LOGGER.info(
            f"[FILTER REMOVED] worksheet={worksheet.title}",
            pipeline_type="CHECKSHEET",
        )

    @staticmethod
    def _expand_hidden_rows_and_columns(worksheet: Worksheet) -> None:
        """Force hidden rows and columns to be visible for a complete read.

        Dimension metadata is inspected directly. This avoids materializing cells
        and never touches ``MergedCell`` placeholders in merged header rows.
        """
        hidden_row_count = 0
        for row_dimension in worksheet.row_dimensions.values():
            if row_dimension.hidden:
                row_dimension.hidden = False
                hidden_row_count += 1
        LOGGER.info(
            f"[ROWS UNHIDDEN] worksheet={worksheet.title} "
            f"rows={hidden_row_count}",
            pipeline_type="CHECKSHEET",
        )

        hidden_column_count = 0
        for column_dimension in worksheet.column_dimensions.values():
            if not column_dimension.hidden:
                continue

            column_dimension.hidden = False
            if column_dimension.min is not None and column_dimension.max is not None:
                hidden_column_count += column_dimension.max - column_dimension.min + 1
            else:
                hidden_column_count += 1

        LOGGER.info(
            f"[COLUMNS UNHIDDEN] worksheet={worksheet.title} "
            f"columns={hidden_column_count}",
            pipeline_type="CHECKSHEET",
        )

    @staticmethod
    def _resolve_merged_cells(worksheet: Worksheet) -> None:
        """Unmerge ranges and propagate each anchor value across its former range."""
        merged_ranges = list(worksheet.merged_cells.ranges)
        expanded_range_count = 0
        expanded_cell_count = 0

        for merged_range in merged_ranges:
            range_coordinate = merged_range.coord
            anchor = worksheet.cell(
                row=merged_range.min_row,
                column=merged_range.min_col,
            )
            merged_value = anchor.value

            try:
                worksheet.unmerge_cells(range_coordinate)
                expanded_range_count += 1

                if merged_value is not None:
                    for row_index in range(
                        merged_range.min_row,
                        merged_range.max_row + 1,
                    ):
                        for column_index in range(
                            merged_range.min_col,
                            merged_range.max_col + 1,
                        ):
                            cell = worksheet.cell(
                                row=row_index,
                                column=column_index,
                            )
                            if isinstance(cell, Cell) and cell.value is None:
                                cell.value = merged_value
                                expanded_cell_count += 1

                LOGGER.info(
                    f"[MERGED CELLS EXPANDED] worksheet={worksheet.title} "
                    f"range={range_coordinate} value={merged_value!r}",
                    pipeline_type="CHECKSHEET",
                )
            except Exception:
                LOGGER.error(
                    f"[MERGED CELLS FAILED] worksheet={worksheet.title} "
                    f"range={range_coordinate}",
                    pipeline_type="CHECKSHEET",
                    exc_info=True,
                )
                raise

        LOGGER.info(
            f"[MERGED CELLS SUMMARY] worksheet={worksheet.title} "
            f"ranges={expanded_range_count} cells={expanded_cell_count}",
            pipeline_type="CHECKSHEET",
        )

    @staticmethod
    def _normalize_cell_values(worksheet: Worksheet) -> None:
        """Normalize text cells while preserving formulas and merged placeholders."""
        normalized_cell_count = 0
        formula_cell_count = 0

        for row in worksheet.iter_rows():
            for cell in row:
                if isinstance(cell, MergedCell):
                    continue
                if cell.data_type == "f":
                    formula_cell_count += 1
                    continue

                value = cell.value
                if isinstance(value, str):
                    normalized_value = unicodedata.normalize("NFKC", value)
                    if normalized_value != value:
                        cell.value = normalized_value
                        normalized_cell_count += 1

        LOGGER.info(
            f"[CELL VALUES NORMALIZED] worksheet={worksheet.title} "
            f"normalized={normalized_cell_count} "
            f"formulas_preserved={formula_cell_count}",
            pipeline_type="CHECKSHEET",
        )

    @classmethod
    def normalize_file(cls, file_path: PathLike) -> Workbook:
        """Open a workbook and return a normalized workbook object."""
        workbook = load_workbook(filename=str(file_path), data_only=True)
        try:
            return cls.normalize_workbook(workbook)
        except Exception:
            workbook.close()
            raise

    @classmethod
    def normalize_rows(cls, rows: Iterable[Iterable[Any]]) -> List[List[Any]]:
        """Normalize a table-like iterable of row values for downstream parsing."""
        normalized_rows: List[List[Any]] = []
        for row in rows:
            normalized_rows.append([cls.normalize_value(value) for value in row])
        return normalized_rows


__all__ = ["WorkbookNormalizer"]