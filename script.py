"""ABP Pipeline - Notebook-friendly script.

Downloads, processes and transforms AddressBase Premium data
for UK address matching.

This script is designed to be run in Jupyter notebooks or similar environments.
Modify the settings below to configure the pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path

from abp_pipeline.pipeline import run
from abp_pipeline.settings import load_settings

# ============================================================================
# CONFIGURATION - Modify these settings as needed
# ============================================================================

# Path to config.yaml file
CONFIG_PATH = Path("config.yaml")

# Pipeline step to run: "download", "extract", "split", "flatfile", or "all"
STEP = "download"

# Force re-run even if outputs exist
FORCE = False

# List available downloads without downloading (only for step="download")
LIST_ONLY = False

# Enable verbose/debug logging
VERBOSE = False

# ============================================================================

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if VERBOSE else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    """Run the ABP pipeline with configured settings."""
    # Load settings
    settings = load_settings(CONFIG_PATH)
    logger.info("Loaded config from %s", CONFIG_PATH)

    # Run pipeline
    run(
        step=STEP,
        settings=settings,
        force=FORCE,
        list_only=LIST_ONLY,
    )
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
