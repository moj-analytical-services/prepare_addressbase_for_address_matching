"""Pipeline orchestrator module.

Coordinates the execution of pipeline stages with idempotency support.
"""

from __future__ import annotations

import logging
from time import perf_counter

from abp_pipeline.extract import run_extract_step
from abp_pipeline.os_downloads import run_download_step
from abp_pipeline.settings import Settings
from abp_pipeline.split_raw import run_split_step
from abp_pipeline.to_flatfile import run_flatfile_step

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Error during pipeline execution."""


def _run_download(settings: Settings, force: bool, list_only: bool) -> None:
    """Run download step."""
    t0 = perf_counter()
    run_download_step(settings, force=force, list_only=list_only)
    logger.info("Download step completed in %.2f seconds", perf_counter() - t0)


def _run_extract(settings: Settings, force: bool) -> None:
    """Run extract step."""
    t0 = perf_counter()
    run_extract_step(settings, force=force)
    logger.info("Extract step completed in %.2f seconds", perf_counter() - t0)


def _run_split(settings: Settings, force: bool) -> None:
    """Run split step."""
    t0 = perf_counter()
    run_split_step(settings, force=force)
    logger.info("Split step completed in %.2f seconds", perf_counter() - t0)


def _run_flatfile(settings: Settings, force: bool) -> None:
    """Run flatfile step."""
    t0 = perf_counter()
    run_flatfile_step(settings, force=force)
    logger.info("Flatfile step completed in %.2f seconds", perf_counter() - t0)


def run(
    step: str,
    settings: Settings,
    force: bool = False,
    list_only: bool = False,
) -> None:
    """Run the specified pipeline step(s).

    Args:
        step: Pipeline step to run. One of: download, extract, split, flatfile, all.
        settings: Application settings.
        force: Force re-run even if outputs exist.
        list_only: For download step, only list available downloads.

    Raises:
        PipelineError: If an invalid step is specified.
        FileNotFoundError: If required inputs for a step are missing.
    """
    total_start = perf_counter()

    logger.info("=" * 60)
    logger.info("ABP Pipeline - Starting step: %s", step)
    logger.info("=" * 60)
    logger.info("Config: %s", settings.config_path)
    logger.info("Work directory: %s", settings.paths.work_dir)
    logger.info("Force mode: %s", force)
    logger.info("")

    if step == "download":
        _run_download(settings, force, list_only)

    elif step == "extract":
        _run_extract(settings, force)

    elif step == "split":
        _run_split(settings, force)

    elif step == "flatfile":
        _run_flatfile(settings, force)

    elif step == "all":
        if list_only:
            # Just list downloads, don't run full pipeline
            _run_download(settings, force, list_only=True)
        else:
            logger.info("Running full pipeline...")
            logger.info("")

            _run_download(settings, force, list_only=False)
            logger.info("")

            _run_extract(settings, force)
            logger.info("")

            _run_split(settings, force)
            logger.info("")

            _run_flatfile(settings, force)

    else:
        raise PipelineError(
            f"Unknown step: {step}. Valid steps: download, extract, split, flatfile, all"
        )

    total_duration = perf_counter() - total_start
    logger.info("")
    logger.info("=" * 60)
    logger.info("Pipeline step '%s' completed in %.2f seconds", step, total_duration)
    logger.info("=" * 60)
