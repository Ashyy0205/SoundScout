from __future__ import annotations

import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def run_optional_import(enable: bool, inbox_dir: str, import_cmd: str | None) -> None:
    """Optional tagging/import step for audio you already have.

    This does NOT download music. It only runs a user-provided command (e.g. beets) against an inbox.
    """
    if not enable:
        return

    if not os.path.isdir(inbox_dir):
        logger.warning("IMPORT_INBOX_DIR does not exist: %s", inbox_dir)
        return

    if not import_cmd or not import_cmd.strip():
        logger.warning("ENABLE_IMPORT is set but IMPORT_CMD is empty; skipping")
        return

    logger.info("Running import command: %s", import_cmd)
    completed = subprocess.run(import_cmd, shell=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Import command failed with exit code {completed.returncode}")
