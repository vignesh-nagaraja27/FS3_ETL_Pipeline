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
FilePatternRepository. Checksheet classification uses 'configs/parts_checksheet.json'.
"""

from pathlib import Path
from typing import Optional, Tuple

from config_loader import ConfigLoader
from repositories.file_pattern_repository import FilePatternRepository
from utils.logger import LOGGER


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

    def classify(self, file_path: Path) -> Tuple[str, Optional[dict]]:
        """
        The main classification method. Given a file path, returns:
          - pipeline_type (str): e.g. '3D_HD', 'CHECKSHEET', or 'UNKNOWN'
          - matched_config (dict or None): The matched config block.

        Usage in job_runner:
            pipeline_type, part_config = dispatcher.classify(file_path)
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

        # Try matching against 3D rules from database (fs3.pre_m_file_pattern)
        match_3d = self._file_pattern_repo.find_match(filename)
        if match_3d is not None:
            pipeline_type = match_3d.get("machine_type", PIPELINE_3D_HD)
            LOGGER.info(
                f"Classified '{filename}' "
                f"using pattern '{match_3d.get('file_pattern')}' "
                f"as '{pipeline_type}'",
                model=match_3d.get("vehicle_model"),
                part=match_3d.get("parts_code"),
                op=match_3d.get("op_no"),
            )
            return pipeline_type, match_3d

        # Try matching against Checksheet rules
        match_cs = self._config.match_checksheet_part(filename)
        if match_cs:
            pipeline_type = PIPELINE_CHECKSHEET
            LOGGER.info(
                f"Classified as '{pipeline_type}': {filename}",
                model=match_cs.get("vehicle_model"),
                part=match_cs.get("parts_code"),
                op=match_cs.get("op_no"),
            )
            return pipeline_type, match_cs

        # No match found — quarantine the file and log clearly
        LOGGER.warning(
            f"[UNCLASSIFIED] No matching rule found for file '{filename}'. "
            f"Check 'fs3.pre_m_file_pattern' for 3D rules or "
            f"'configs/parts_checksheet.json' for checksheet rules."
        )
        return PIPELINE_UNKNOWN, None
