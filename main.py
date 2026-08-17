# -*- coding: utf-8 -*-
"""
ETL Pipeline — System Entry Point
----------------------------------
This is the only file you need to run to start the entire ETL system.

Usage:
  python main.py

What happens when you run this:
  1. Loads all configuration files from 'configs/'.
  2. Initializes the database connection pool (one shared pool for the system lifetime).
  3. Starts the Job Runner, which spins up watcher threads for every machine
     and the conversion table folder defined in machines.json.
  4. Blocks until you press Ctrl+C, then shuts down cleanly.

To add a new machine: edit 'configs/machines.json'.
To add a new vehicle model or part: edit 'configs/parts_3d.json'.
No Python code changes needed.
"""

import sys
from pathlib import Path

# Ensure the project root is on the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config_loader import ConfigLoader
from db.db_handler import DatabaseHandler
from orchestrator.job_runner import JobRunner
from utils.logger import LOGGER


def main():
    """
    System bootstrap sequence:
      1. Load configs → 2. Init DB pool → 3. Start orchestrator
    """
    LOGGER.info("=" * 60,pipeline_type="SYSTEM")
    LOGGER.info("FS3 ETL Data Pipeline — Starting Up",pipeline_type="SYSTEM")
    LOGGER.info("=" * 60,pipeline_type="SYSTEM")

    # ── STEP 1: Load Configuration ───────────────────────────────────────────
    try:
        config = ConfigLoader()
        LOGGER.info(
            f"Configuration loaded. "
            f"{len(config.get_watch_directories())} machine(s) configured, "
            f"{len(config.parts_3d)} 3D part rule(s) loaded.",pipeline_type="SYSTEM"
        )
        checksheet_path = Path(__file__).resolve().parent / "configs" / "checksheet.json"
        if checksheet_path.exists():
            LOGGER.info(
                f"Number of checksheet watch directories: "
                f"{len(config.get_checksheet_watch_directories())}",
                pipeline_type="SYSTEM"
            )
    except FileNotFoundError as e:
        LOGGER.critical(
            f"STARTUP FAILED: A required config file is missing. {e} "
            f"Check the 'ETL_Pipeline/configs/' folder.",pipeline_type="SYSTEM"
        )
        sys.exit(1)
    except Exception as e:
        LOGGER.critical(
            f"STARTUP FAILED: Error loading configuration: {e}",pipeline_type="SYSTEM" ,exc_info=True
        )
        sys.exit(1)

    # ── STEP 2: Initialize Database Connection Pool ──────────────────────────
    try:
        db_handler = DatabaseHandler()
        db_config = config.get_db_config()
        pool_config = db_config.get("pool", {})
        db_handler.initialize(
            db_config=db_config["db"],
            min_conn=pool_config.get("min_conn", 2),
            max_conn=pool_config.get("max_conn", 15),
            debug_pool_stats=True,
        )
        LOGGER.info(
            f"Database connection pool initialized. "
            f"Min connections: {pool_config.get('min_conn', 2)}, "
            f"Max connections: {pool_config.get('max_conn', 15)}."
            f"Debug Pool Stats: True",pipeline_type="SYSTEM"
        )
    except KeyError:
        LOGGER.critical(
            "STARTUP FAILED: 'db_config.json' is missing the 'db' key. "
            "Please fill in your database credentials in 'configs/db_config.json'.",pipeline_type="SYSTEM"
        )
        sys.exit(1)
    except Exception as e:
        LOGGER.critical(
            f"STARTUP FAILED: Could not connect to the database. "
            f"Verify 'configs/db_config.json' credentials. Error: {e}",pipeline_type="SYSTEM",
            exc_info=True,
        )
        sys.exit(1)

    # ── STEP 3: Start the Orchestrator ───────────────────────────────────────
    try:
        runner = JobRunner(config=config)
        runner.start()  # Blocks here until Ctrl+C
    except Exception as e:
        LOGGER.critical(f"FATAL: Job Runner crashed unexpectedly: {e}", pipeline_type="SYSTEM",exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
