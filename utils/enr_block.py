"""Filename and business-key rules shared by the E-NR/BL flow."""

import re
from datetime import datetime
from pathlib import Path
from typing import Any

from utils.workbook_normalizer import WorkbookNormalizer


def normalize_enr_name(value: Any) -> str:
    return str(WorkbookNormalizer.normalize_value(value)).strip() if value is not None else ""


def normalize_enr_op(value: Any) -> str:
    text = normalize_enr_name(value).upper()
    match = re.fullmatch(r"OP(\d{1,3})", text)
    if match is None:
        raise ValueError(f"Invalid E-NR/BL OP number: {value!r}; expected OP followed by 1-3 digits")
    return f"OP{int(match.group(1)):03d}"


def parse_enr_filename(file_name: str) -> tuple[str, datetime]:
    path = Path(file_name)
    if path.suffix.lower() != ".xls" or not path.name.startswith("E-NR"):
        raise ValueError("E-NR/BL measurement filenames must start with E-NR and use .xls")
    segments = path.stem.split("_")
    if len(segments) < 3 or not segments[-2].strip():
        raise ValueError("E-NR-BLOCK filename is missing its serial-number segment")
    timestamp = segments[-1].strip()
    if re.fullmatch(r"\d{14}", timestamp) is None:
        raise ValueError("E-NR-BLOCK filename timestamp must be yyyyMMddHHmmss")
    measured_at = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    if measured_at.year != 2026:
        raise ValueError("Only E-NR-BLOCK measurement files with a 2026 timestamp are eligible")
    return segments[-2].strip(), measured_at
