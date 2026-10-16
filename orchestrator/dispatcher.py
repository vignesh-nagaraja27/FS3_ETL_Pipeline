# -*- coding: utf-8 -*-
"""
File Dispatcher
---------------
The Dispatcher is the 'Classifier' of this system. Its job is narrow and critical:

  Given a file path, figure out:
    1. Is this a 3D HD file, 3D BL file, or a Checksheet file?
    2. Which vehicle model and part does it belong to?
    3. Which pipeline should process it?

The dispatcher never processes data itself — it only classifies and routes.
3D file classification comes from the database (fs3.pre_m_file_pattern) via
FilePatternRepository. Checksheet classification uses ConfigLoader regex matching.
"""

import re
from pathlib import Path
from typing import Optional, Tuple

from config_loader import ConfigLoader
from pipelines.pipeline_3d import Pipeline3D
from repositories.file_pattern_repository import FilePatternRepository
from utils.logger import LOGGER
from utils.enr_block import normalize_enr_op, parse_enr_filename
from utils.ktsf0829 import parse_ktsf0829_filename

try:
    from pipelines.pipeline_checksheet import PipelineChecksheet
except ImportError:  # pragma: no cover - compatibility with class naming in older installs
    from pipelines.pipeline_checksheet import ChecksheetPipeline as PipelineChecksheet


# Identifies the pipeline type based on dispatch result
PIPELINE_3D_HD = "3D_HD"
PIPELINE_3D_BL = "3D_BL"
PIPELINE_3D_CR = "3D_CR"
PIPELINE_CHECKSHEET = "CHECKSHEET"
PIPELINE_UNKNOWN = "UNKNOWN"


class Dispatcher:
    """
    Classifies incoming files against the configured part/model mapping rules
    and returns the matched context required by the pipeline executor.

    3D file classification uses FilePatternRepository (database-driven, startswith matching).
    Checksheet classification uses ConfigLoader (JSON-driven, regex matching).
    """

    def __init__(self, config: ConfigLoader):
        """
        Args:
            config (ConfigLoader): The shared config instance loaded at startup.
        """
        self._config = config
        self._file_pattern_repo = FilePatternRepository()
    def classify_from_watcher(self,file_path: Path,watch_config: dict,):
        pipeline_type = str(watch_config.get("pipeline_type", "")).strip()
        machine_id = str(watch_config.get("machine_id", "")).strip()
        if machine_id == "KTSF-0829":
            try:
                metadata = parse_ktsf0829_filename(file_path.name)
            except ValueError as exc:
                LOGGER.warning(
                    f"KTSF-0829 file skipped: {file_path.name}: {exc}",
                    machine=machine_id,
                    pipeline_type=PIPELINE_3D_CR,
                )
                return PIPELINE_UNKNOWN, None
            return PIPELINE_3D_CR, {
                **metadata,
                "machine_type": PIPELINE_3D_CR,
                "file_pattern": None,
                "side": None,
            }
        if (
            machine_id == "KTS-7106"
            and watch_config.get("enr_block_enabled", False)
            and file_path.name.startswith("E-NR")
        ):
            try:
                parse_enr_filename(file_path.name)
            except ValueError as exc:
                LOGGER.warning(
                    f"E-NR-BLOCK file skipped: {file_path.name}: {exc}",
                    machine=machine_id,
                    pipeline_type=PIPELINE_3D_BL,
                )
                return PIPELINE_UNKNOWN, None
            routing_op_match = re.search(
                r"(?:^|_)(OP\d{1,3})(?=[_-]|$)",
                file_path.stem,
                flags=re.IGNORECASE,
            )
            routing_op_no = (
                normalize_enr_op(routing_op_match.group(1))
                if routing_op_match is not None
                else ""
            )
            LOGGER.info(
                f"E-NR-BLOCK file accepted for specialized processing: {file_path.name}",
                machine=machine_id,
                model="E-NR",
                part="BL",
                op=routing_op_no,
                pipeline_type=PIPELINE_3D_BL,
            )
            return PIPELINE_3D_BL, {
                "machine_type": PIPELINE_3D_BL,
                "file_pattern": "E-NR",
                "vehicle_model": "E-NR",
                "parts_code": "BL",
                "side": None,
                "op_no": routing_op_no,
                "vpo_code": "",
                "enr_block_flow": True,
            }
        if pipeline_type in (PIPELINE_3D_HD,PIPELINE_3D_BL,PIPELINE_3D_CR,):
            match_3d = self._file_pattern_repo.find_match(file_path.name)
            return pipeline_type, match_3d
        if pipeline_type == PIPELINE_CHECKSHEET:
            return self.classify(file_path)
        return self.classify(file_path)

    def dispatch(self, pipeline_type: str, **kwargs):
        """Instantiate the pipeline for the selected pipeline type."""
        destination_root = (
            kwargs.get("destination_root")
            or self._config.get_destination_root()
        )

        if pipeline_type == PIPELINE_3D_HD:
            LOGGER.info(f"[PIPELINE SELECTED] {pipeline_type}",pipeline_type="3D")
            return Pipeline3D(destination_root=destination_root)

        if pipeline_type == PIPELINE_3D_BL:
            LOGGER.info(f"[PIPELINE SELECTED] {pipeline_type}",pipeline_type="3D")
            return Pipeline3D(destination_root=destination_root)

        if pipeline_type == PIPELINE_3D_CR:
            LOGGER.info(f"[PIPELINE SELECTED] {pipeline_type}",pipeline_type="3D")
            return Pipeline3D(destination_root=destination_root)

        if pipeline_type == PIPELINE_CHECKSHEET:
            LOGGER.info(f"[PIPELINE SELECTED] {pipeline_type}",pipeline_type="CHECKSHEET")
            return PipelineChecksheet(destination_root=destination_root)

        raise ValueError(f"Unsupported pipeline type: {pipeline_type}")

    def classify(self, file_path: Path) -> Tuple[str, Optional[dict]]:
        """
        The main classification method. Given a file path, returns:
          - pipeline_type (str): e.g. '3D_HD', 'CHECKSHEET', or 'UNKNOWN'
          - matched_config (dict or None): The matched config block.

        Usage in job_runner:
            pipeline_type, part_config = dispatcher.classify_from_watcher(file_path=file_path,watch_config=watch_config,)
            if pipeline_type == PIPELINE_UNKNOWN:
                # Log a warning and skip
            else:
                # Launch the correct pipeline with part_config

        Args:
            file_path (Path): Full path to the detected file.

        Returns:
            Tuple of (pipeline_type_string, part_config_dict_or_None)
        """
        filename = file_path.name

        # Checksheet filenames are more specific and must take precedence over 3D rules.
        match_cs = self._config.match_checksheet_part(filename)
        if match_cs:
            pipeline_type = PIPELINE_CHECKSHEET
            LOGGER.info(
                f"[DEBUG DISPATCH] CHECKSHEET MATCH filename={filename}",pipeline_type="CHECKSHEET"
            )
            LOGGER.info(
                f"Classified as '{pipeline_type}': {filename}",
                model=match_cs.get("vehicle_model"),
                part=match_cs.get("parts_code"),
                op=match_cs.get("op_no"),pipeline_type="CHECKSHEET"
            )
            return pipeline_type, match_cs

        # Try matching against 3D rules from database (fs3.pre_m_file_pattern)
        match_3d = self._file_pattern_repo.find_match(filename)
        if match_3d is not None:
            pipeline_type = match_3d.get("machine_type", PIPELINE_3D_HD)
            LOGGER.info(
                f"[DEBUG DISPATCH] 3D MATCH "
                f"filename={filename} "
                f"file_pattern={match_3d.get('file_pattern')} "
                f"machine_type={pipeline_type}",pipeline_type="3D"
            )
            LOGGER.info(
                f"Classified '{filename}' "
                f"using pattern '{match_3d.get('file_pattern')}' "
                f"as '{pipeline_type}'",
                model=match_3d.get("vehicle_model"),
                part=match_3d.get("parts_code"),
                op=match_3d.get("op_no"),pipeline_type="3D"
            )
            return pipeline_type, match_3d

        # No match found — quarantine the file and log clearly
        LOGGER.warning(
            f"[UNCLASSIFIED] No matching rule found for file '{filename}'. "
            f"Check 'fs3.pre_m_file_pattern' for 3D rules or "
            f"filename regex rules for checksheet files.",pipeline_type="SYSTEM",
        )
        return PIPELINE_UNKNOWN, None


__all__ = [
    "Dispatcher",
    "PIPELINE_3D_BL",
    "PIPELINE_3D_CR",
    "PIPELINE_3D_HD",
    "PIPELINE_CHECKSHEET",
    "PIPELINE_UNKNOWN",
]