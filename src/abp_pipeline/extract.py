"""Extract module for ABP Pipeline.

Handles extraction of downloaded zip files and discovery of raw CSV files.
"""

from __future__ import annotations

import logging
import shutil
import zipfile
from pathlib import Path

from abp_pipeline.settings import Settings

logger = logging.getLogger(__name__)


def extract_zip(
    zip_path: Path,
    extracted_dir: Path,
    force: bool = False,
) -> Path:
    """Extract a zip file to the specified directory.

    Args:
        zip_path: Path to the zip file.
        extracted_dir: Directory to extract to.
        force: Force re-extraction even if directory exists.

    Returns:
        Path to the extraction directory.

    Raises:
        FileNotFoundError: If zip file doesn't exist.
        zipfile.BadZipFile: If zip file is corrupted.
    """
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    # Create extraction subdirectory named after the zip file
    extract_subdir = extracted_dir / zip_path.stem

    # Check if already extracted
    if extract_subdir.exists() and not force:
        logger.info("Already extracted: %s", extract_subdir)
        return extract_subdir

    # Clear existing directory on force
    if extract_subdir.exists() and force:
        logger.info("Removing existing extraction: %s", extract_subdir)
        shutil.rmtree(extract_subdir)

    # Extract
    extract_subdir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting %s to %s...", zip_path.name, extract_subdir)

    with zipfile.ZipFile(zip_path, "r") as zf:
        total_files = len(zf.namelist())
        for i, member in enumerate(zf.namelist(), 1):
            zf.extract(member, extract_subdir)
            if i % 100 == 0 or i == total_files:
                logger.debug("Extracted %d/%d files", i, total_files)

    logger.info("Extraction complete: %d files", total_files)
    return extract_subdir


def discover_raw_csv_files(extracted_dir: Path) -> list[Path]:
    """Discover raw ABP CSV files in the extracted directory.

    The ABP data comes as multiple CSV files (chunks) that need to be
    processed together.

    Args:
        extracted_dir: Directory containing extracted files.

    Returns:
        List of paths to CSV files to process.
    """
    if not extracted_dir.exists():
        logger.warning("Extracted directory does not exist: %s", extracted_dir)
        return []

    # Find all CSV files recursively
    csv_files = list(extracted_dir.rglob("*.csv"))

    # Sort for deterministic ordering
    csv_files.sort()

    logger.info("Discovered %d CSV file(s) in %s", len(csv_files), extracted_dir)
    for f in csv_files[:5]:  # Log first few
        logger.debug("  %s", f.name)
    if len(csv_files) > 5:
        logger.debug("  ... and %d more", len(csv_files) - 5)

    return csv_files


def find_downloaded_zips(downloads_dir: Path) -> list[Path]:
    """Find all downloaded zip files.

    Args:
        downloads_dir: Directory containing downloaded files.

    Returns:
        List of paths to zip files.
    """
    if not downloads_dir.exists():
        return []

    zip_files = list(downloads_dir.glob("*.zip"))
    zip_files.sort()

    return zip_files


def run_extract_step(settings: Settings, force: bool = False) -> list[Path]:
    """Run the extract step of the pipeline.

    Extracts all downloaded zip files and returns paths to extracted directories.

    Args:
        settings: Application settings.
        force: Force re-extraction even if files exist.

    Returns:
        List of extracted directory paths.
    """
    downloads_dir = settings.paths.downloads_dir
    extracted_dir = settings.paths.extracted_dir

    # Ensure directories exist
    extracted_dir.mkdir(parents=True, exist_ok=True)

    # Find downloaded zips
    zip_files = find_downloaded_zips(downloads_dir)
    if not zip_files:
        logger.warning("No zip files found in %s. Run --step download first.", downloads_dir)
        return []

    logger.info("Found %d zip file(s) to extract", len(zip_files))

    # Extract each zip
    extracted_dirs: list[Path] = []
    for zip_path in zip_files:
        extract_path = extract_zip(zip_path, extracted_dir, force=force)
        extracted_dirs.append(extract_path)

    logger.info("Extraction complete: %d directories", len(extracted_dirs))
    return extracted_dirs


def get_raw_csv_dir(settings: Settings) -> Path | None:
    """Get the directory containing raw CSV files.

    Returns the extracted directory which contains subdirectories with CSV files.
    The split step will use **/*.csv glob to find CSVs in all subdirectories.

    Args:
        settings: Application settings.

    Returns:
        Path to extracted directory, or None if no CSV files found.
    """
    extracted_dir = settings.paths.extracted_dir

    if not extracted_dir.exists():
        return None

    # Check if there are any CSV files (recursively) in the extracted directory
    csv_files = list(extracted_dir.rglob("*.csv"))
    if csv_files:
        logger.debug("Found %d CSV files in %s", len(csv_files), extracted_dir)
        return extracted_dir

    return None
