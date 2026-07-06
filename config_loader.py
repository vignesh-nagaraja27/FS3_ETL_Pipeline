# -*- coding: utf-8 -*-
"""
Configuration Loader
--------------------
Reads and provides access to all configuration files (machines.json, parts_3d.json,
parts_checksheet.json, db_config.json) located in the 'configs/' directory.

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
        self.parts_3d: list = self._load("parts_3d.json").get("mappings", [])
        self.parts_checksheet: list = self._load("parts_checksheet.json").get("mappings", [])
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
        for mapping in self.parts_3d:
            pattern = mapping.get("match_pattern", "")
            if re.match(pattern, filename, re.IGNORECASE):
                return mapping
        return None

    def match_checksheet_part(self, filename: str) -> Optional[dict]:
        """
        Same as match_3d_part but for checksheet files.
        """
        for mapping in self.parts_checksheet:
            pattern = mapping.get("match_pattern", "")
            if re.match(pattern, filename, re.IGNORECASE):
                return mapping
        return None

    def get_watch_directories(self) -> list:
        """Returns the list of all configured machine watch directories."""
        return self.machines.get("watch_directories", [])

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

        Example return value: 'C:/FS3/DataCollect'

        The pipeline uses this with utils.path_helper.get_working_paths()
        to construct:
            <destination_root>/<MC_NO>/<VEHICLE_MODEL>/<PARTS_CODE>/<OP_NO>/_processing
            <destination_root>/<MC_NO>/<VEHICLE_MODEL>/<PARTS_CODE>/<OP_NO>/_completed
            <destination_root>/<MC_NO>/<VEHICLE_MODEL>/<PARTS_CODE>/<OP_NO>/_NG
        """
        dest = self.machines.get("destination_root", {})
        if isinstance(dest, dict):
            return dest.get("path", "")
        return str(dest or "")

    def get_db_config(self) -> dict:
        """Returns the database connection settings."""
        return self.db
