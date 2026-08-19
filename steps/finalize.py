# -*- coding: utf-8 -*-
"""
Finalize Step
-------------
The last step in every successful pipeline run.

Responsibility: Move the processed file from the '_processing' folder to its
final destination (either '_completed' or '_NG') based on whether the pipeline
succeeded or failed.

This replaces the old '●' prefix renaming and the scattered move logic
spread across four separate legacy scripts.

After this step, the StatusManager marks the pipeline run as COMPLETED or FAILED.
"""

import shutil
from pathlib import Path

from utils.logger import LOGGER
from utils.path_helper import get_working_paths


class FinalizeStep:
    """
    Routes a processed file to its final folder based on success/failure outcome.
    """

    def __init__(
        self,
        base_completed_dir: str = "",
        base_ng_dir: str = "",
        destination_root: str = "",
    ):
        """
        Args:
            base_completed_dir (str): Legacy base path for completed files.
            base_ng_dir (str): Legacy base path for failed files.
            destination_root (str): Root path used to build per-operation
                                    _completed and _NG folders.
        """
        self._base_completed_dir = (
            Path(base_completed_dir) if base_completed_dir else None
        )
        self._base_ng_dir = Path(base_ng_dir) if base_ng_dir else None
        self._destination_root = destination_root or ""

    def run_success(
        self,
        source_path: Path,
        processing_path: Path,
        machine_id: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        log_id: int,
        pipeline_type: str,
    ) -> Path:
        """
        Moves the original source file to '<base_completed_dir>/<pipeline_type>/<vehicle_model>/<parts_code>/OK'
        and removes the staged processing copy on success.
        """
        context = dict(
            machine=machine_id, model=vehicle_model, part=parts_code, op=op_no,pipeline_type=pipeline_type,
        )
        dest_dir = self._resolve_destination_dir(
            success=True,
            machine_id=machine_id,
            vehicle_model=vehicle_model,
            parts_code=parts_code,
            op_no=op_no,
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = self._move(source_path, dest_dir, "completed", context, log_id)

        # Clean up the staged processing copy
        self._cleanup_staged(processing_path, context, log_id)
        return dest_path

    def run_failure(
        self,
        source_path: Path,
        processing_path: Path,
        machine_id: str,
        vehicle_model: str,
        parts_code: str,
        op_no: str,
        log_id: int,
        pipeline_type: str,
    ) -> Path:
        """
        Moves the original source file to '<base_ng_dir>/<pipeline_type>/<vehicle_model>/<parts_code>/NG'
        and removes the staged processing copy on failure.
        """
        context = dict(
            machine=machine_id, model=vehicle_model, part=parts_code, op=op_no,pipeline_type=pipeline_type,
        )
        dest_dir = self._resolve_destination_dir(
            success=False,
            machine_id=machine_id,
            vehicle_model=vehicle_model,
            parts_code=parts_code,
            op_no=op_no,
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = self._move(source_path, dest_dir, "NG", context, log_id)

        # Clean up the staged processing copy
        self._cleanup_staged(processing_path, context, log_id)
        return dest_path

    def _resolve_destination_dir(
            self,
            success: bool,
            machine_id: str,
            vehicle_model: str,
            parts_code: str,
            op_no: str,
    ) -> Path:
        if self._destination_root:
            _, completed_path, ng_path = get_working_paths(
            destination_root=self._destination_root,
            mc_no=machine_id,
            vehicle_model=vehicle_model,
            parts_code=parts_code,
            op_no=op_no,
            create=True,
            )
            return completed_path if success else ng_path

        if success:
            if self._base_completed_dir is None:
                raise ValueError("base_completed_dir is not configured")
            return self._base_completed_dir

        if self._base_ng_dir is None:
            raise ValueError("base_ng_dir is not configured")
        return self._base_ng_dir

    def _move(
        self,
        file_path: Path,
        destination_dir: Path,
        outcome: str,
        context: dict,
        log_id: int,
    ) -> Path:
        """
        Internal helper to physically move the original file and log the result.
        """
        dest_path = destination_dir / file_path.name

        try:
            if not file_path.exists():
                LOGGER.warning(
                    f"[LOG_ID:{log_id}] FINALIZE STEP: Original file '{file_path.name}' does not exist "
                    f"at source folder '{file_path}'. Cannot move original.",
                    **context,
                )
                return dest_path

            shutil.move(str(file_path), str(dest_path))
            LOGGER.info(
                f"[LOG_ID:{log_id}] FINALIZE STEP: Original file '{file_path.name}' → [{outcome}] "
                f"Moved from CMM folder to '{dest_path}'.",
                **context,
            )
            return dest_path

        except Exception as e:
            LOGGER.error(
                f"[LOG_ID:{log_id}] FINALIZE STEP: Failed to move original file '{file_path.name}' "
                f"to '{destination_dir}'. Error: {e}",
                exc_info=True,
                **context,
            )
            raise

    def _cleanup_staged(self, processing_path: Path, context: dict, log_id: int):
        """
        Safely removes the staged file in the '_processing' workspace folder.
        """
        if processing_path and processing_path.exists():
            try:
                processing_path.unlink()
                LOGGER.info(
                    f"[LOG_ID:{log_id}] FINALIZE STEP: Staged workspace file '{processing_path.name}' removed.",
                    **context,
                )
            except Exception as e:
                LOGGER.warning(
                    f"[LOG_ID:{log_id}] FINALIZE STEP: Failed to remove staged file '{processing_path}': {e}",
                    **context,
                )
