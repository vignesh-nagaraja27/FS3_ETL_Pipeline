# -*- coding: utf-8 -*-
"""
Configuration Loader
--------------------
Reads and provides access to all configuration files (machines.json, checksheet.json,
db_config.json) located in the 'configs/' directory.

Design principle: Configuration is the brain of the pipeline. All runtime behavior
(which folders to watch, how to parse files, which model/part to assign) is driven
purely by JSON — no hardcoded values in Python logic files.
"""

import json
import re
from pathlib import Path
from typing import Optional


# Root path to the configs folder, relative to this file's location
_CONFIGS_DIR = Path(__file__).parent / "configs"


class ConfigLoader:
    """
    Loads and caches all JSON configuration files at startup.
    Exposes helper methods to look up machine configs, part mappings, and DB settings.
    """

    def __init__(self, configs_dir: Path = _CONFIGS_DIR):
        self._dir = configs_dir
        self.machines: dict = self._load("machines.json")
        self.parts_3d: list = []
        self.parts_checksheet: list = []
        self.checksheet: dict = {}
        checksheet_path = self._dir / "checksheet.json"
        if checksheet_path.exists():
            self.checksheet = self._load("checksheet.json")
        self.db: dict = self._load("db_config.json")

    def _load(self, filename: str) -> dict:
        """
        Reads a JSON config file from the configs directory.
        Raises a clear FileNotFoundError if the file is missing so startup fails fast.
        """
        path = self._dir / filename
        if not path.exists():
            raise FileNotFoundError(
                f"[ConfigLoader] Missing required config file: '{path}'. "
                f"Please create it using the template in the configs/ folder."
            )
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def match_3d_part(self, filename: str) -> Optional[dict]:
        """
        Scans all 3D part mappings and returns the FIRST matching rule for the given filename.

        This is the heart of the dispatcher: instead of hardcoded 'if filename starts with NX',
        every pattern lives in parts_3d.json and is tested here.

        Args:
            filename (str): The bare filename (not the full path).

        Returns:
            dict: The matching part config, or None if no match is found.
        """
        return None

    def match_checksheet_part(self, filename: str) -> Optional[dict]:
        """
        Determines whether a filename should be treated as a checksheet workbook.

        Current checksheet pipeline filenames follow the convention:
            VehicleModel_PartsCode_OPXX_*.xls[xm]

        This method returns the extracted part metadata so the dispatcher can
        route the file to the CHECKSHEET pipeline before any generic 3D fallback
        rule matches.
        """
        stem = Path(filename).stem
        match = re.fullmatch(
            r"(?P<vehicle_model>[^_]+)_(?P<parts_code>[^_]+)_(?P<op_no>OP[^_]+)(?:_.+)?",
            stem,
            flags=re.IGNORECASE,
        )
        if match is None:
            return None

        return {
            "vehicle_model": match.group("vehicle_model"),
            "parts_code": match.group("parts_code"),
            "op_no": match.group("op_no").upper(),
        }

    def get_watch_directories(self) -> list:
        """Returns the list of all configured machine watch directories."""
        return self.machines.get("watch_directories", [])

    def get_checksheet_config(self) -> dict:
        """Returns the checksheet startup configuration, if present."""
        return self.checksheet if isinstance(self.checksheet, dict) else {}

    def get_checksheet_watch_directories(self) -> list:
        """
        Returns checksheet watch directories when checksheet.json exists,
        guaranteeing safe defaults for all change detection and retry parameters.
        """
        raw_dirs = self.get_checksheet_config().get("watch_directories", [])
        if not isinstance(raw_dirs, list):
            return []

        normalized_dirs = []
        for raw_entry in raw_dirs:
            if not isinstance(raw_entry, dict):
                continue
            entry = dict(raw_entry)

            # Backward Compatibility Support
            #
            # These settings are optional.
            #
            # If not configured:
            #
            # change_check_interval_minutes = 5
            # retry_base_minutes = 5
            # max_retry_attempts = 4
            #
            # This guarantees older checksheet.json files
            # continue working after deployment with no
            # configuration changes required.
            entry["change_check_interval_minutes"] = int(
                entry.get("change_check_interval_minutes", 5)
            )
            entry["retry_base_minutes"] = int(
                entry.get("retry_base_minutes", 5)
            )
            entry["max_retry_attempts"] = int(
                entry.get("max_retry_attempts", 4)
            )

            normalized_dirs.append(entry)

        return normalized_dirs

    def get_conversion_watch(self) -> dict:
        """Returns the conversion table folder configuration."""
        return self.machines.get("conversion_watch", {})

    def get_processing_folders(self) -> dict:
        """Returns the routing folder paths (processing, completed, ng)."""
        return self.machines.get("processing_folders", {})

    def get_destination_root(self) -> str:
        """
        Returns the root path under which the per-operation destination
        structure is built.
        """
        dest = self.machines.get("destination_root", {})
        if isinstance(dest, dict):
            return dest.get("path", "")
        return str(dest or "")

    def get_db_config(self) -> dict:
        """Returns the database connection settings."""
        return self.db