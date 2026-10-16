"""Filename and horizontal measurement-name rules for KTSF-0829."""

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def _normalize_op(value: str) -> str:
    match = re.fullmatch(r"(?:OP)?(\d{1,3})", value.strip(), re.IGNORECASE)
    if match is None:
        raise ValueError(f"Invalid KTSF-0829 operation: {value!r}")
    return f"OP{int(match.group(1)):03d}"


def parse_ktsf0829_filename(file_name: str) -> Dict[str, Any]:
    path = Path(file_name)
    segments = path.stem.split("_")
    if path.suffix.lower() != ".csv" or len(segments) != 7:
        raise ValueError(
            "KTSF-0829 filename must be "
            "model_part_OPxxx_frequency_position_serial_yyyyMMddHHmmss.csv"
        )
    model, part, operation, frequency, position, serial, timestamp = (
        segment.strip() for segment in segments
    )
    for field, value, max_length in (
        ("vehicle_model", model, 5),
        ("parts_code", part, 8),
        ("measure_frequency", frequency, 100),
        ("measure_position", position, 5),
        ("serial_no", serial, 16),
    ):
        if not value or len(value) > max_length:
            raise ValueError(
                f"KTSF-0829 filename {field} must contain 1-{max_length} characters"
            )
    if re.fullmatch(r"\d{14}", timestamp) is None:
        raise ValueError("KTSF-0829 filename timestamp must be yyyyMMddHHmmss")
    op_no = _normalize_op(operation)
    return {
        "vehicle_model": model,
        "parts_code": part,
        "op_no": op_no,
        "vpo_code": f"{model}_{part}_{op_no}",
        "measure_frequency": frequency,
        "measure_position": position,
        "serial_no": serial,
        "serial_no_m_last4": serial[-4:],
        "measurement_datetime": datetime.strptime(timestamp, "%Y%m%d%H%M%S"),
    }


def parse_ktsf0829_measurement_name(value: str) -> Dict[str, Any]:
    segments = [segment.strip() for segment in value.split("_")]
    if len(segments) < 5:
        raise ValueError(f"Incomplete KTSF-0829 measurement name: {value!r}")
    op_no = _normalize_op(segments.pop(0))
    second_operation = segments.pop(0)
    op_no_2nd = str(int(_normalize_op(second_operation)[2:])) if second_operation else None
    op_no_3rd = None
    if segments and re.fullmatch(r"(?:OP)?\d{1,3}", segments[0], re.IGNORECASE):
        op_no_3rd = _normalize_op(segments.pop(0))
    if len(segments) < 3 or not segments[0] or not segments[1]:
        raise ValueError(f"Incomplete KTSF-0829 measurement name: {value!r}")
    return {
        "op_no": op_no,
        "op_no_2nd": op_no_2nd,
        "op_no_3rd": op_no_3rd,
        "evaluation_item1": segments[0],
        "measure_item": segments[1],
        "machining_item": segments[2] or None,
        "measure_point": (segments[3] or None) if len(segments) > 3 else None,
        "measure_part": "_".join(segments[4:]) or None,
    }
