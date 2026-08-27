# -*- coding: utf-8 -*-
"""Transform an FS3 checksheet workbook into canonical ETL records."""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.utils.datetime import from_excel

from utils.logger import LOGGER
from utils.workbook_normalizer import WorkbookNormalizer

PathLike = Union[str, Path]


class TransformChecksheetStep:
    """Parse normalized OP sheets into canonical master and transaction data."""

    CHECKSHEET_VERSION = "00000000"
    FIRST_MASTER_ROW = 7
    SERIAL_ROW = 3
    DATE_ROW = 4

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
            raise FileNotFoundError(
                f"Checksheet workbook not found: {path}"
            )

        filename_parts = self._extract_filename_parts(path.name)

        vehicle_model = vehicle_model or filename_parts["vehicle_model"]
        parts_code = parts_code or filename_parts["parts_code"]
        op_no = op_no or filename_parts["op_no"]

        vpo_code = self._build_vpo_code(
            vehicle_model,
            parts_code,
            op_no,
        )

        LOGGER.info(
            f"[WORKBOOK OPEN] Opening workbook: {path.name}",
            pipeline_type="CHECKSHEET",
        )

        workbook = load_workbook(
            filename=str(path),
            data_only=True,
            read_only=False,
        )

        value_workbook = load_workbook(
            filename=str(path),
            data_only=True,
            read_only=False,
        )

        try:
            WorkbookNormalizer.normalize_workbook(workbook)

            master_records: List[Dict[str, Any]] = []

            transaction_by_key: Dict[
                Tuple[str, date, str, int, int],
                Dict[str, Any],
            ] = {}
            occurrence_by_event: Dict[Tuple[date, str, int, int], int] = {}
            occurrence_count_by_master_serial_date: Dict[
                Tuple[date, str, int], int
            ] = {}

            op_sheet_count = 0

            # Workbook-wide master identity.
            #
            # This replaces the identity previously implied by:
            #
            #     data_001 ... data_500
            #
            # The value is stable for the lifetime of this transform run
            # and is shared by the corresponding master and transaction
            # records.
            next_master_index = 1

            for worksheet in workbook.worksheets:
                if (
                    not worksheet.title
                    or not worksheet.title.upper().startswith("OP")
                ):
                    continue

                op_sheet_count += 1

                LOGGER.info(
                    f"[OP SHEET FOUND] Found OP sheet: {worksheet.title}",
                    pipeline_type="CHECKSHEET",
                )

                value_worksheet = value_workbook[worksheet.title]

                sheet_masters, sheet_transactions, next_master_index = (
                    self._parse_sheet(
                        worksheet,
                        value_worksheet=value_worksheet,
                        first_master_index=next_master_index,
                        vpo_code=vpo_code,
                        vehicle_model=vehicle_model,
                        parts_code=parts_code,
                        op_no=op_no,
                        occurrence_by_event=occurrence_by_event,
                        occurrence_count_by_master_serial_date=(
                            occurrence_count_by_master_serial_date
                        ),
                    )
                )

                master_records.extend(sheet_masters)
                for transaction in sheet_transactions:
                    self._merge_transaction(
                        transaction_by_key,
                        transaction,
                    )

            if op_sheet_count == 0:
                LOGGER.warning(
                    f"[TRANSFORM COMPLETED] No OP sheets found: {path.name}",
                    pipeline_type="CHECKSHEET",
                )

            transaction_records = list(
                transaction_by_key.values()
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
        occurrence_by_event: Dict[Tuple[date, str, int, int], int],
        occurrence_count_by_master_serial_date: Dict[
            Tuple[date, str, int], int
        ],
    ) -> Tuple[
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        int,
    ]:
        """
        Parse one normalized OP worksheet.

        Returns:
            (
                master_records,
                transaction_records,
                next_available_master_index,
            )

        ``source_master_index`` is a workbook-wide temporary identifier.
        It is deliberately independent of final ``pi_no`` allocation.
        """
        mc_no = self._compose_mc_no(
            worksheet["B7"].value,
            worksheet["C7"].value,
        )

        master_records: List[Dict[str, Any]] = []

        next_master_index = first_master_index

        # ------------------------------------------------------------------
        # Master generation.
        #
        # Each master receives a workbook-wide source_master_index.
        #
        # This identifier replaces the identity that was previously encoded
        # indirectly by data_001 ... data_500.
        # ------------------------------------------------------------------
        for row_index in range(
            self.FIRST_MASTER_ROW,
            worksheet.max_row + 1,
        ):
            if not self._is_master_row(
                worksheet,
                row_index,
            ):
                continue

            source_master_index = next_master_index
            next_master_index += 1

            op_no_2nd, op_no_3rd = self._parse_op_no_2nd_3rd(
                worksheet.cell(
                    row=row_index,
                    column=5,
                ).value
            )

            op_name = worksheet.cell(
                row=row_index,
                column=6,
            ).value

            master_record = self._build_master_record(
                worksheet,
                row_index=row_index,
                source_master_index=source_master_index,
                vpo_code=vpo_code,
                vehicle_model=vehicle_model,
                parts_code=parts_code,
                op_no=op_no,
                mc_no=mc_no,
                op_no_2nd=op_no_2nd,
                op_no_3rd=op_no_3rd,
                op_name=op_name,
            )

            master_records.append(master_record)

            LOGGER.info(
                f"[MASTER ROW PARSED] "
                f"sheet={worksheet.title} "
                f"row={row_index} "
                f"source_master_index={source_master_index}",
                pipeline_type="CHECKSHEET",
            )

        # ------------------------------------------------------------------
        # Normalized transaction generation.
        #
        # One non-blank Excel measurement cell becomes one transaction row.
        #
        # The transformer deliberately does NOT assign pi_no.
        #
        # ``source_master_index`` identifies which master definition the
        # measurement belongs to. Master synchronization/repository logic
        # resolves that temporary identifier to the final pi_no.
        # ------------------------------------------------------------------
        transaction_records: List[Dict[str, Any]] = []

        for column_index in self._discover_transaction_columns(
            worksheet,
            value_worksheet,
        ):
            raw_serial = self._source_value(
                value_worksheet,
                worksheet,
                row=self.SERIAL_ROW,
                column=column_index,
            )

            serial_no = self._format_serial_no(
                raw_serial
            )

            serial_no_m_last4 = self._serial_no_m_last4(
                serial_no
            )

            LOGGER.info(
                f"[SERIAL TRACE] "
                f"raw={raw_serial} "
                f"formatted={serial_no} "
                f"last4={serial_no_m_last4}",
                pipeline_type="CHECKSHEET",
            )

            transaction_date = self._source_value(
                value_worksheet,
                worksheet,
                row=self.DATE_ROW,
                column=column_index,
            )

            parsed_date = self._coerce_date(
                transaction_date,
                epoch=getattr(
                    worksheet.parent,
                    "epoch",
                    None,
                ),
            )

            if parsed_date is None:
                continue

            column_transaction_count = 0

            for master_record in master_records:
                source_master_index = master_record[
                    "source_master_index"
                ]

                source_row_index = master_record[
                    "row_index"
                ]

                raw_value = self._source_value(
                    value_worksheet,
                    worksheet,
                    row=source_row_index,
                    column=column_index,
                )

                value = self._normalize_measurement_value(
                    raw_value
                )

                # Blank measurements are not transactions.
                if self._is_blank(value):
                    continue

                # Occurrence belongs to this master (and therefore its final
                # PI), not to the serial/date globally. Allocation happens
                # only for real transactions so blank cells cannot create
                # gaps in a master's independent recheck sequence.
                occurrence_event_key = (
                    parsed_date,
                    serial_no_m_last4,
                    source_master_index,
                    column_index,
                )
                measurement_occurrence_no = occurrence_by_event.get(
                    occurrence_event_key
                )
                if measurement_occurrence_no is None:
                    master_serial_date_key = (
                        parsed_date,
                        serial_no_m_last4,
                        source_master_index,
                    )
                    measurement_occurrence_no = (
                        occurrence_count_by_master_serial_date.get(
                            master_serial_date_key,
                            0,
                        )
                        + 1
                    )
                    occurrence_count_by_master_serial_date[
                        master_serial_date_key
                    ] = measurement_occurrence_no
                    occurrence_by_event[occurrence_event_key] = (
                        measurement_occurrence_no
                    )

                transaction_record: Dict[str, Any] = {
                    "version": self.CHECKSHEET_VERSION,
                    "mc_no": master_record.get(
                        "mc_no",
                        mc_no,
                    ),
                    "vpo_code": master_record.get(
                        "vpo_code",
                        vpo_code,
                    ),
                    "vehicle_model": master_record.get(
                        "vehicle_model",
                        vehicle_model,
                    ),
                    "parts_code": master_record.get(
                        "parts_code",
                        parts_code,
                    ),
                    "op_no": master_record.get(
                        "op_no",
                        op_no,
                    ),
                    "op_no_2nd": master_record.get(
                        "op_no_2nd"
                    ),
                    "op_no_3rd": master_record.get(
                        "op_no_3rd"
                    ),
                    "date": parsed_date,
                    "serial_no": serial_no,
                    "serial_no_m_last4": serial_no_m_last4,
                    "measurement_occurrence_no": measurement_occurrence_no,
                    "eng_no": master_record.get(
                        "eng_no"
                    ),
                    "value": value,
                    "watch_id": None,
                    "file_version_id": None,

                    # Temporary master identity.
                    #
                    # This is NOT pi_no. The repository/master sync layer
                    # must resolve it to the final pi_no.
                    "source_master_index": source_master_index,
                }

                transaction_records.append(
                    transaction_record
                )

                column_transaction_count += 1

            LOGGER.info(
                f"[TRANSACTION COLUMN PARSED] "
                f"sheet={worksheet.title} "
                f"column={get_column_letter(column_index)} "
                f"serial_no={serial_no} "
                f"transaction_rows={column_transaction_count}",
                pipeline_type="CHECKSHEET",
            )

        return (
            master_records,
            transaction_records,
            next_master_index,
        )

    @classmethod
    def _merge_transaction(
        cls,
        transaction_by_key: Dict[
            Tuple[str, date, str, int, int],
            Dict[str, Any],
        ],
        incoming: Dict[str, Any],
    ) -> None:
        """
        Insert a normalized transaction or validate an existing duplicate.

        Transaction identity during transformation is:

            (
                vpo_code,
                date,
                serial_no_m_last4,
                source_master_index,
                measurement_occurrence_no,
            )

        ``source_master_index`` is a temporary workbook-local master
        identity and is intentionally not a final ``pi_no``.

        Rules:

        1. Missing key identity is invalid.
        2. New key -> insert.
        3. Existing key with identical record -> ignore.
        4. Existing key with conflicting record -> raise ValueError.
        """
        source_master_index = incoming.get(
            "source_master_index"
        )

        if source_master_index is None:
            raise ValueError(
                "transaction record requires "
                "source_master_index"
            )

        try:
            source_master_index = int(
                source_master_index
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "transaction source_master_index "
                "must be an integer"
            ) from exc

        if source_master_index <= 0:
            raise ValueError(
                "transaction source_master_index "
                "must be greater than zero"
            )

        required_fields = (
            "vpo_code",
            "date",
            "serial_no",
            "serial_no_m_last4",
            "measurement_occurrence_no",
            "value",
        )

        for field_name in required_fields:
            if field_name not in incoming:
                raise ValueError(
                    f"transaction record missing "
                    f"required field: {field_name}"
                )

        key = (
            str(incoming["vpo_code"]),
            incoming["date"],
            str(incoming["serial_no_m_last4"]),
            source_master_index,
            int(incoming["measurement_occurrence_no"]),
        )

        existing = transaction_by_key.get(key)

        # No transaction with this identity yet.
        if existing is None:
            transaction_by_key[key] = incoming
            return

        # Same logical transaction with exactly the same data.
        # Duplicate is harmless and must not fail the transform.
        if existing == incoming:
            return

        # Same logical identity but different payload.
        # Silently choosing one would cause data loss.
        raise ValueError(
            "conflicting duplicate transaction: "
            f"key={key!r}; "
            f"existing={existing!r}; "
            f"incoming={incoming!r}"
        )

    @classmethod
    def _is_master_row(
        cls,
        worksheet: Any,
        row_index: int,
    ) -> bool:
        """Return whether a worksheet row defines a measurement master."""
        return not all(
            cls._is_blank(
                worksheet.cell(
                    row=row_index,
                    column=column,
                ).value
            )
            for column in (6, 8, 11)
        )

    @classmethod
    def _discover_transaction_columns(
        cls,
        worksheet: Any,
        value_worksheet: Any,
    ) -> List[int]:
        """
        Return every used column containing a serial number in row 3.

        The serial number is the source-side indicator that the column
        represents a transaction/measurement instance.
        """
        max_column = max(
            worksheet.max_column,
            value_worksheet.max_column,
            1,
        )

        return [
            column_index
            for column_index in range(
                1,
                max_column + 1,
            )
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
        source_master_index: int,
        vpo_code: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        mc_no: str,
        op_no_2nd: Any,
        op_no_3rd: Optional[Any],
        op_name: Any,
    ) -> Dict[str, Any]:
        """
        Build one canonical measurement-master record.

        ``source_master_index`` is a temporary workbook-local identifier.
        It exists solely to preserve master-to-transaction identity until
        master synchronization assigns the final ``pi_no``.
        """
        j_value = worksheet.cell(
            row=row_index,
            column=10,
        ).value

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
            "Inspection_no": worksheet.cell(
                row=row_index,
                column=7,
            ).value,
            "measure_item": worksheet.cell(
                row=row_index,
                column=8,
            ).value,
            "measure_point": worksheet.cell(
                row=row_index,
                column=9,
            ).value,
            "measure_direction": measure_direction,
            "measure_depth": measure_depth,
            "specification": worksheet.cell(
                row=row_index,
                column=11,
            ).value,
            "upper_limit": worksheet.cell(
                row=row_index,
                column=12,
            ).value,
            "lower_limit": worksheet.cell(
                row=row_index,
                column=13,
            ).value,
            "measure_method": worksheet.cell(
                row=row_index,
                column=18,
            ).value,
            "measure_frequency": worksheet.cell(
                row=row_index,
                column=19,
            ).value,

            # Temporary workbook-local identity used to connect this
            # master definition to its transaction rows.
            "source_master_index": source_master_index,

            # Physical source location remains useful for reading the
            # measurement cell during this transform run.
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
        """Read cached workbook value, falling back to normalized value."""
        value = value_worksheet.cell(
            row=row,
            column=column,
        ).value

        if value is not None:
            return value

        return normalized_worksheet.cell(
            row=row,
            column=column,
        ).value

    @staticmethod
    def _normalize_measurement_value(
        value: Any,
    ) -> Any:
        """Preserve Excel values while avoiding scientific notation."""
        if isinstance(value, float):
            text = format(
                value,
                ".15f",
            )

            return text.rstrip("0").rstrip(".")

        return value

    @staticmethod
    def _contains_direction_marker(
        value: Any,
    ) -> bool:
        """Return whether a measurement direction marker exists."""
        if not isinstance(value, str):
            return False

        comparison_value = unicodedata.normalize(
            "NFKC",
            value,
        ).upper()

        return any(
            marker in comparison_value
            for marker in ("X", "Y", "Z")
        )

    @staticmethod
    def _compose_mc_no(
        value_b: Any,
        value_c: Any,
    ) -> str:
        """Compose machine/checksheet number from columns B and C."""
        left = (
            ""
            if value_b is None
            else str(value_b)
        )

        right = (
            ""
            if value_c is None
            else str(value_c)
        )

        return f"{left}{right}"

    @staticmethod
    def _parse_op_no_2nd_3rd(
        value: Any,
    ) -> Tuple[Any, Optional[Any]]:
        """Split OP second/third-level identifiers."""
        if value is None:
            return None, None

        if isinstance(value, str):
            text = value.strip()

            if "-" in text:
                op_no_2nd, op_no_3rd = text.split(
                    "-",
                    1,
                )

                return (
                    op_no_2nd,
                    op_no_3rd or None,
                )

            return text, None

        return value, None

    @staticmethod
    def _format_serial_no(
        value: Any,
    ) -> str:
        """
        Format serial number while preserving strings and padding
        numeric values to four digits.
        """
        if value is None:
            raise ValueError(
                "serial_no is required"
            )

        if isinstance(
            value,
            (int, float),
        ):
            try:
                ival = int(value)

                return f"{ival:04d}"

            except (
                ValueError,
                OverflowError,
            ):
                text = str(value).strip()

        else:
            text = str(value).strip()

        if text.isdigit() and len(text) < 4:
            return text.zfill(4)

        return text

    @classmethod
    def _serial_no_m_last4(
        cls,
        value: Any,
    ) -> str:
        """Extract the last four characters from a formatted serial."""
        formatted = cls._format_serial_no(
            value
        )

        if not formatted:
            raise ValueError(
                "serial_no is required"
            )

        return formatted[-4:]

    @staticmethod
    def _coerce_date(
        value: Any,
        *,
        epoch: Any = None,
    ) -> Optional[date]:
        """Convert supported Excel/date representations to date."""
        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, date):
            return value

        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
        ):
            try:
                converted = (
                    from_excel(
                        value,
                        epoch=epoch,
                    )
                    if epoch
                    else from_excel(value)
                )

            except (
                TypeError,
                ValueError,
                OverflowError,
            ):
                return None

            if isinstance(
                converted,
                datetime,
            ):
                return converted.date()

            return (
                converted
                if isinstance(
                    converted,
                    date,
                )
                else None
            )

        if isinstance(value, str):
            text = unicodedata.normalize(
                "NFKC",
                value,
            ).strip()

            if not text:
                return None

            try:
                return datetime.fromisoformat(
                    text
                ).date()

            except ValueError:
                pass

            for date_format in (
                "%Y/%m/%d",
                "%Y.%m.%d",
                "%Y年%m月%d日",
                "%Y%m%d",
            ):
                try:
                    return datetime.strptime(
                        text,
                        date_format,
                    ).date()

                except ValueError:
                    continue

            try:
                numeric_value = float(text)

                converted = (
                    from_excel(
                        numeric_value,
                        epoch=epoch,
                    )
                    if epoch
                    else from_excel(
                        numeric_value
                    )
                )

            except (
                TypeError,
                ValueError,
                OverflowError,
            ):
                return None

            if isinstance(
                converted,
                datetime,
            ):
                return converted.date()

            return (
                converted
                if isinstance(
                    converted,
                    date,
                )
                else None
            )

        return None

    @staticmethod
    def _is_blank(
        value: Any,
    ) -> bool:
        """Return whether a value is null or whitespace-only."""
        return value is None or (
            isinstance(value, str)
            and not value.strip()
        )

    @staticmethod
    def _build_vpo_code(
        vehicle_model: str,
        parts_code: str,
        op_no: str,
    ) -> str:
        """Build the canonical VPO code."""
        return (
            f"{vehicle_model}_"
            f"{parts_code}_"
            f"{op_no}"
        )

    @staticmethod
    def _extract_filename_parts(
        file_name: str,
    ) -> Dict[str, str]:
        """Extract vehicle, parts, and OP identifiers from filename."""
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
                "VehicleModel_PartsCode_OPNO_Anything.xlsm"
            )

        for group_name, segment_name, pattern, allowed_characters in (
            ("vehicle_model", "VehicleModel", r"[A-Za-z0-9]+", "A-Z, a-z, and 0-9"),
            ("parts_code", "PartsCode", r"[A-Za-z0-9-]+", "A-Z, a-z, 0-9, and -"),
            ("op_no", "OPNO", r"OP[0-9]+", "OP followed by digits (0-9)"),
        ):
            value = match.group(group_name)
            if re.fullmatch(pattern, value) is None:
                raise ValueError(
                    f"Invalid checksheet filename segment '{segment_name}': "
                    f"value '{value}' in filename '{file_name}' must contain "
                    f"only {allowed_characters}"
                )
        return {
            "vehicle_model": match.group(
                "vehicle_model"
            ),
            "parts_code": match.group(
                "parts_code"
            ),
            "op_no": match.group(
                "op_no"
            ).upper(),
        }


__all__ = ["TransformChecksheetStep"]