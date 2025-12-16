"""Pipeline runner for ABP flatfile transformation."""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

import duckdb

from abp_pipeline.settings import Settings
from abp_pipeline.transform.common import (
    assert_inputs_exist,
    create_macros,
    register_parquet_view,
)
from abp_pipeline.transform.stages import business, combine, lpi, misc, postal

logger = logging.getLogger(__name__)


def transform_to_flatfile(
    settings: Settings,
    force: bool = False,
) -> Path:
    """Transform split parquet files into a single flatfile for address matching.

    Args:
        settings: Application settings.
        force: Force re-processing even if output exists.

    Returns:
        Path to the output parquet file.

    Raises:
        FileNotFoundError: If required input files are missing.
        ToFlatfileError: If transformation fails.
    """
    parquet_dir = settings.paths.parquet_dir / "raw"
    output_dir = settings.paths.output_dir
    output_path = output_dir / "abp_for_uk_address_matcher.parquet"

    # Check inputs
    assert_inputs_exist(parquet_dir)

    # Check if output exists
    if output_path.exists() and not force:
        logger.info("Output already exists: %s. Use --force to re-process.", output_path)
        return output_path

    output_dir.mkdir(parents=True, exist_ok=True)

    total_start = perf_counter()
    logger.info("Starting flatfile transformation...")

    # Create connection and register views
    con = duckdb.connect()

    register_parquet_view(con, "blpu", parquet_dir / "blpu.parquet")
    register_parquet_view(con, "lpi", parquet_dir / "lpi.parquet")
    register_parquet_view(con, "street_descriptor", parquet_dir / "street_descriptor.parquet")
    register_parquet_view(con, "organisation", parquet_dir / "organisation.parquet")
    register_parquet_view(con, "delivery_point", parquet_dir / "delivery_point.parquet")
    register_parquet_view(con, "classification", parquet_dir / "classification.parquet")

    # Prepare macros and intermediate tables
    t0 = perf_counter()
    create_macros(con)
    lpi.prepare_street_descriptor_views(con)
    lpi.prepare_lpi_base(con)
    postal.prepare_best_delivery(con)
    misc.prepare_classification_best(con)
    logger.info("Preparation completed in %.2f seconds", perf_counter() - t0)

    # Render variants
    stages = [
        ("LPI variants", lpi.render_variants),
        ("Business variants", business.render_variants),
        ("Delivery point variants", postal.render_variants),
        ("Custom level variants", misc.render_custom_levels),
    ]

    for label, func in stages:
        t0 = perf_counter()
        func(con)
        logger.info("%s rendered in %.2f seconds", label, perf_counter() - t0)

    # Combine and write
    t0 = perf_counter()
    result = combine.combine_and_dedupe(con)
    logger.info("Combination and deduplication in %.2f seconds", perf_counter() - t0)

    # Data integrity check and statistics
    input_uprn_count = con.execute("SELECT COUNT(DISTINCT uprn) FROM lpi_base_distinct").fetchone()[
        0
    ]
    output_metrics = con.execute(
        "SELECT COUNT(DISTINCT uprn) AS output_uprn_count, COUNT(*) AS total_variants FROM result"
    ).fetchone()
    output_uprn_count = output_metrics[0]
    total_variants = output_metrics[1]

    assert input_uprn_count == output_uprn_count, (
        f"Lost UPRNs during processing! Input: {input_uprn_count}, Output: {output_uprn_count}"
    )

    variant_uplift_pct = ((total_variants - output_uprn_count) / output_uprn_count) * 100
    logger.info(
        "Address Statistics - Input UPRNs (Unique): %d | Output UPRNs (Unique): %d | Total Address Variants Generated: %d | Variant Uplift: %.1f%%",
        input_uprn_count,
        output_uprn_count,
        total_variants,
        variant_uplift_pct,
    )

    t0 = perf_counter()
    if output_path.exists():
        output_path.unlink()
    result.write_parquet(output_path.as_posix())
    logger.info("Parquet written in %.2f seconds", perf_counter() - t0)

    total_duration = perf_counter() - total_start
    logger.info("Flatfile transformation completed in %.2f seconds", total_duration)
    logger.info("Output: %s", output_path)

    return output_path


def run_flatfile_step(settings: Settings, force: bool = False) -> Path:
    """Run the flatfile step of the pipeline.

    Args:
        settings: Application settings.
        force: Force re-processing even if output exists.

    Returns:
        Path to the output parquet file.
    """
    logger.info("Starting flatfile step...")
    return transform_to_flatfile(settings, force=force)
